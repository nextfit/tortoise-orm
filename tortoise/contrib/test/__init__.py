
import tortoise

from functools import wraps
from tortoise.backends.base.config_generator import generate_config
from tortoise.transactions.context import TransactionContext
from typing import Any, List
from unittest import SkipTest, expectedFailure, skip, skipIf, skipUnless, IsolatedAsyncioTestCase


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
        self.isolated_tortoise = tortoise._Tortoise()
        self.isolated_tortoise.init(self.get_db_config())
        tortoise.Tortoise = self.isolated_tortoise

    async def asyncSetUp(self) -> None:
        await self.isolated_tortoise.open_connections(create_db=True)
        await self.isolated_tortoise.generate_schemas(safe=False)
        # self._db_client_map = self.isolated_tortoise._db_client_map.copy()

    async def asyncTearDown(self) -> None:
        # self.isolated_tortoise._db_client_map = self._db_client_map.copy()
        await self.isolated_tortoise._drop_databases()


class TruncationTestCase(SimpleTestCase):
    """
    Use this when your tests contain transactions.

    This is slower than ``TestCase`` but faster than ``IsolatedTestCase``.
    Note that usage of this does not guarantee that auto-number-pks will be reset to 1.
    """

    tortoise_test = tortoise._Tortoise()

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

        original_tortoise = tortoise.Tortoise
        tortoise.Tortoise = cls.tortoise_test

        cls.tortoise_test.init(cls.get_db_config())

        await cls.tortoise_test.open_connections(create_db=True)
        await cls.tortoise_test.generate_schemas(safe=False)
        tortoise.Tortoise = original_tortoise

    @classmethod
    async def finalize(cls) -> None:
        """
        Cleans up the DB after testing. Must be called as part of the test environment teardown.
        """

        await cls.tortoise_test.open_connections()
        await cls.tortoise_test._drop_databases()

    def setUp(self) -> None:
        self.original_tortoise = tortoise.Tortoise
        tortoise.Tortoise = self.tortoise_test

    def tearDown(self) -> None:
        tortoise.Tortoise = self.original_tortoise

    async def asyncSetUp(self) -> None:
        await self.tortoise_test.open_connections()

        # TODO: This is a naive implementation: Will fail to clear M2M and non-cascade foreign keys
        for models_map in self.tortoise_test._app_models_map.values():
            for model in models_map.values():
                await model._meta.db.execute_script(f"DELETE FROM {model._meta.db_table}")  # nosec

    #
    # async def asyncTearDown(self) -> None:
    #     _restore_default()
    #
    #     # TODO: This is a naive implementation: Will fail to clear M2M and non-cascade foreign keys
    #     for models_map in Tortoise._app_models_map.values():
    #         for model in models_map.values():
    #             await model._meta.db.execute_script(f"DELETE FROM {model._meta.db_table}")  # nosec
    #


class TestTransactionContext(TransactionContext):
    __slots__ = ("token", )

    async def __aenter__(self):
        current_transaction = tortoise.Tortoise._current_transaction_map[self.connection_name]
        self.token = current_transaction.set(self.db_client)

        await self.db_client.acquire()
        await self.db_client.start()
        return self.db_client

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.db_client.rollback()
        tortoise.Tortoise._current_transaction_map[self.connection_name].reset(self.token)
        await self.db_client.release()


class TestCase(TruncationTestCase):
    """
    An asyncio capable test class that will ensure that each test will be run at
    separate transaction that will rollback on finish.

    This is a fast test runner. Don't use it if your test uses transactions.
    """

    # async def _run_outcome(self, outcome, expecting_failure, testMethod) -> None:
    #     _restore_default()
    #     self.__db__ = Tortoise.get_db_client("models")
    #     if self.__db__.capabilities.supports_transactions:
    #         db_client = self.__db__.in_transaction().db_client
    #         async with TestTransactionContext(db_client):
    #             await super()._run_outcome(outcome, expecting_failure, testMethod)
    #     else:
    #         await super()._run_outcome(outcome, expecting_failure, testMethod)
    #
    # async def asyncTearDown(self) -> None:
    #     if self.__db__.capabilities.supports_transactions:
    #         _restore_default()
    #     else:
    #         await super()._tearDownDB()
    pass


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
                db = tortoise.Tortoise.get_db_client(connection_name)
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
