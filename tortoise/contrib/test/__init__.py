import asyncio
import logging

from functools import wraps
from tortoise import Tortoise, _Tortoise
from tortoise.backends.base.config_generator import generate_config
from tortoise.transactions.context import TransactionContext
from typing import Any, List
from unittest import SkipTest, expectedFailure, skip, skipIf, skipUnless, IsolatedAsyncioTestCase


logger = logging.getLogger("tortoise.test")


__all__ = (
    "SimpleTestCase",
    "TestCase",
    "TruncationTestCase",
    "IsolatedTestCase",
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


class SimpleTestCase(IsolatedAsyncioTestCase):
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


class IsolatedTestCase(SimpleTestCase):
    """
    Use this if your test needs perfect isolation.

    It will create and destroy a new DB instance for every test.
    This is obviously slow, but guarantees a fresh DB.

    """

    def setUp(self) -> None:
        Tortoise.init(self.get_db_config())

    async def asyncSetUp(self) -> None:
        await Tortoise.open_connections(create_db=True)
        await Tortoise.generate_schemas(safe=False)

    async def asyncTearDown(self) -> None:
        await Tortoise._drop_databases()
        await Tortoise.close_connections()


class TestTransactionContext(TransactionContext):
    __slots__ = ("token", )

    async def __aenter__(self):
        current_transaction = Tortoise._current_transaction_map[self.connection_name]
        self.token = current_transaction.set(self.db_client)

        await self.db_client.acquire()
        await self.db_client.start()
        return self.db_client

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.db_client.rollback()
        Tortoise._current_transaction_map[self.connection_name].reset(self.token)
        await self.db_client.release()


class TruncationTestCase(SimpleTestCase):
    """
    Use this when your tests contain transactions.

    This is slower than ``TestCase`` but faster than ``IsolatedTestCase``.
    Note that usage of this does not guarantee that auto-number-pks will be reset to 1.
    """

    tortoise_test = _Tortoise()

    wrap_in_transaction = False
    avoid_transaction = {'asyncSetUp', 'asyncTearDown'}

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

        # try:
        #     await Tortoise.init(_CONFIG)
        #     await Tortoise._drop_databases()
        # except DBConnectionError:  # pragma: nocoverage
        #     pass

        Tortoise.init(cls.get_db_config())

        await Tortoise.open_connections(create_db=True)
        await Tortoise.generate_schemas(safe=False)
        await Tortoise.close_connections()

        cls.tortoise_test._inited = Tortoise._inited
        cls.tortoise_test._app_models_map = Tortoise._app_models_map.copy()
        cls.tortoise_test._db_client_map = Tortoise._db_client_map.copy()
        cls.tortoise_test._current_transaction_map = Tortoise._current_transaction_map.copy()

    @classmethod
    async def finalize(cls) -> None:
        """
        Cleans up the DB after testing. Must be called as part of the test environment teardown.
        """

        await cls.tortoise_test.open_connections()
        await cls.tortoise_test._drop_databases()

    async def asyncSetUp(self) -> None:
        if not self.wrap_in_transaction:
            for models_map in Tortoise._app_models_map.values():
                for model in models_map.values():
                    await model._meta.db.execute_script(f"DELETE FROM {model._meta.db_table}")  # nosec

    async def asyncTearDown(self) -> None:
        pass

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
                break

            fut, awaitable = query
            try:
                if self.wrap_in_transaction and awaitable.__name__ not in self.avoid_transaction:
                    db_client = models_db_client.in_transaction().db_client
                    async with TestTransactionContext(db_client):
                        ret = await awaitable

                else:
                    ret = await awaitable

                if not fut.cancelled():
                    fut.set_result(ret)

            except asyncio.CancelledError:
                raise

            except Exception as ex:
                if not fut.cancelled():
                    fut.set_exception(ex)

        await Tortoise.close_connections()


class TestCase(TruncationTestCase):
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
        class TestSqlite(test.TestCase):
            ...

    :param connection_name: name of the connection to retrieve capabilities from.
    :param conditions: capability tests which must all pass for the test to run.
    """

    def decorator(test_item):
        if not isinstance(test_item, type):

            @wraps(test_item)
            def skip_wrapper(*args, **kwargs):
                db = Tortoise.get_db_client(connection_name)
                for key, val in conditions.items():
                    if getattr(db.capabilities, key) != val:
                        raise SkipTest(f"Capability {key} != {val}")
                return test_item(*args, **kwargs)

            return skip_wrapper

        # Assume a class is decorated
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

    return decorator
