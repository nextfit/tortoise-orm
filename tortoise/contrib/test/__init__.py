import asyncio
import logging

from functools import wraps
from tortoise import Tortoise, _Tortoise
from tortoise.backends.base.config_generator import generate_config
from typing import Any, List
from unittest import SkipTest, expectedFailure, skip, skipIf, skipUnless, IsolatedAsyncioTestCase


logger = logging.getLogger("tortoise.test")


__all__ = (
    "TortoiseBaseTestCase",
    "TortoiseTransactionedTestModelsTestCase",
    "TortoiseTestModelsTestCase",
    "TortoiseIsolatedTestCase",
    "requireCapability",
    "SkipTest",
    "expectedFailure",
    "skip",
    "skipIf",
    "skipUnless",
)


expectedFailure.__doc__ = """
Mark test as expecting failure.

On success it will be marked as unexpected success.
"""


class TortoiseBaseTestCase(IsolatedAsyncioTestCase):
    """
    The Tortoise base test class.
    This will ensure that your DB environment has a test double set up for use.

    Note to use ``{}`` as a string-replacement parameter, for your DB_URL.
    That will create a randomised database name.

    If you define a ``tortoise_test_modules`` list, it overrides the DB setup module for the tests.
    """

    tortoise_test_db = None
    tortoise_test_modules: List[str] = ["tests.testmodels"]

    @classmethod
    def get_db_config(cls, app_label="models") -> dict:
        """
        DB Config factory, for use in testing.

        """
        return generate_config(
            cls.tortoise_test_db,
            app_modules={app_label: cls.tortoise_test_modules},
            testing=True,
            connection_label=app_label,
        )


class TortoiseIsolatedTestCase(TortoiseBaseTestCase):
    """
    Use this if your test needs perfect isolation.

    It will create and destroy a new DB instance for every test.
    This is obviously slow, but guarantees a fresh DB.

    """

    def _callSetUp(self):
        Tortoise.init(self.get_db_config())
        super()._callSetUp()

    async def _asyncioLoopRunner(self, fut):
        self._asyncioCallsQueue = queue = asyncio.Queue()
        fut.set_result(None)
        while True:
            query = await queue.get()
            queue.task_done()
            if query is None:
                break

            fut, awaitable = query
            try:
                if awaitable.__name__ == 'asyncSetUp':
                    await Tortoise.open_connections(create_db=True)
                    await Tortoise.generate_schemas(safe=False)

                ret = await awaitable
                if not fut.cancelled():
                    fut.set_result(ret)

                if awaitable.__name__ == 'asyncTearDown':
                    await Tortoise.drop_databases()

            except asyncio.CancelledError:
                raise

            except Exception as ex:
                if not fut.cancelled():
                    fut.set_exception(ex)


class TortoiseTestModelsTestCase(TortoiseBaseTestCase):
    """
    Use this when your tests contain transactions.

    This is slower than ``TortoiseTransactionedTestModelsTestCase`` but faster than ``TortoiseIsolatedTestCase``.
    Note that usage of this does not guarantee that auto-number-pks will be reset to 1.
    """

    tortoise_test = _Tortoise()
    wrap_in_transaction = False

    @classmethod
    def restore_tortoise(cls) -> None:
        Tortoise._inited = cls.tortoise_test._inited
        Tortoise._app_models_map = cls.tortoise_test._app_models_map.copy()
        Tortoise._db_client_map = cls.tortoise_test._db_client_map.copy()
        Tortoise._current_transaction_map = cls.tortoise_test._current_transaction_map.copy()

    @classmethod
    async def initialize(cls) -> None:

        """
        Sets up the global DB for testing. Must be called as part of test environment setup.

        """

        Tortoise.init(cls.get_db_config())

        await Tortoise.open_connections(create_db=True)
        await Tortoise.generate_schemas(safe=False)
        await Tortoise.close_connections()

        cls.tortoise_test._inited = Tortoise._inited
        cls.tortoise_test._app_models_map = Tortoise._app_models_map.copy()
        cls.tortoise_test._db_client_map = Tortoise._db_client_map.copy()
        cls.tortoise_test._current_transaction_map = Tortoise._current_transaction_map.copy()

        Tortoise._reset()

    @classmethod
    async def finalize(cls) -> None:
        """
        Cleans up the DB after testing. Must be called as part of the test environment teardown.
        """

        await cls.tortoise_test.drop_databases()

    async def _asyncioLoopRunner(self, fut):
        self.restore_tortoise()

        # if "models" db client does not support transactions, turn it off even if it is on
        models_db_client = Tortoise.get_db_client("models")
        if not models_db_client.capabilities.supports_transactions:
            self.wrap_in_transaction = False

        await Tortoise.open_connections()

        self._asyncioCallsQueue = queue = asyncio.Queue()
        fut.set_result(None)
        while True:
            query = await queue.get()
            queue.task_done()
            if query is None:
                await self.tortoise_test.close_connections()
                break

            fut, awaitable = query
            try:
                if awaitable.__name__ == 'asyncSetUp':
                    if self.wrap_in_transaction:
                        db_client = models_db_client.in_transaction().db_client
                        current_transaction = Tortoise._current_transaction_map[db_client.connection_name]
                        token = current_transaction.set(db_client)

                        await db_client.acquire()
                        await db_client.start()

                ret = await awaitable
                if not fut.cancelled():
                    fut.set_result(ret)

                if awaitable.__name__ == 'asyncTearDown':
                    if self.wrap_in_transaction:
                        await db_client.rollback()
                        current_transaction.reset(token)
                        await db_client.release()

                    else:
                        for models_map in Tortoise._app_models_map.values():
                            for model in models_map.values():
                                await model._meta.db.execute_script(f"DELETE FROM {model._meta.db_table}")  # nosec

            except asyncio.CancelledError:
                await self.tortoise_test.close_connections()
                raise

            except Exception as ex:
                if not fut.cancelled():
                    fut.set_exception(ex)


class TortoiseTransactionedTestModelsTestCase(TortoiseTestModelsTestCase):
    """
    An asyncio capable test class that will ensure that each test will be run at
    separate transaction that will rollback on finish.

    This is a fast test runner. Don't use it if your test uses transactions.
    """

    wrap_in_transaction = True


def requireCapability(connection_name: str = "models", **conditions: Any):
    """
    Skip a test if the required capabilities are not matched.

    .. note::
        The database must be initialized *before* the decorated test runs.

    Usage:

    .. code-block:: python3

        @requireCapability(dialect='sqlite')
        async def test_run_sqlite_only(self):
            ...

    Or to conditionally skip a class:

    .. code-block:: python3

        @requireCapability(dialect='sqlite')
        class TestSqlite(test.TortoiseTransactionedTestModelsTestCase):
            ...

    :param connection_name: name of the connection to retrieve capabilities from.
    :param conditions: capability tests which must all pass for the test to run.
    """

    def decorator(test_item):
        if isinstance(test_item, type):
            # A class is decorated
            funcs = {
                var: getattr(test_item, var)
                for var in dir(test_item)
                if var.startswith("test_") and callable(getattr(test_item, var))
            }

            for name, func in funcs.items():
                setattr(
                    test_item,
                    name,
                    requireCapability(connection_name=connection_name, **conditions)(func),
                )

            return test_item

        else:
            @wraps(test_item)
            def skip_wrapper(*args, **kwargs):
                db = Tortoise.get_db_client(connection_name)
                for key, val in conditions.items():
                    if getattr(db.capabilities, key) != val:
                        raise SkipTest(f"Capability {key} != {val}")
                return test_item(*args, **kwargs)

            return skip_wrapper

    return decorator
