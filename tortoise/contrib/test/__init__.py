from functools import wraps
from typing import Any, List
from unittest import SkipTest, expectedFailure, skip, skipIf, skipUnless, IsolatedAsyncioTestCase

from tortoise import Tortoise
from tortoise.backends.base.config_generator import generate_config
from tortoise.exceptions import DBConnectionError
from tortoise.transactions.context import TransactionContext

__all__ = (
    "SimpleTestCase",
    "TestCase",
    "TruncationTestCase",
    "IsolatedTestCase",
    "getDBConfig",
    "requireCapability",
    "initializer",
    "finalizer",
    "SkipTest",
    "expectedFailure",
    "skip",
    "skipIf",
    "skipUnless",
)


_TORTOISE_TEST_DB = "sqlite://:memory:"
# pylint: disable=W0201

expectedFailure.__doc__ = """
Mark test as expecting failure.

On success it will be marked as unexpected success.
"""

_CONFIG: dict = {}
_CONNECTIONS: dict = {}
_MODULES: List[str] = []
_CONN_MAP: dict = {}


def getDBConfig(app_label: str, modules: List[str]) -> dict:
    """
    DB Config factory, for use in testing.

    :param app_label: Label of the app (must be distinct for multiple apps).
    :param modules: List of modules to look for models in.
    """
    return generate_config(
        _TORTOISE_TEST_DB,
        app_modules={app_label: modules},
        testing=True,
        connection_label=app_label,
    )


async def _init_db(config: dict) -> None:
    try:
        await Tortoise.init(config)
        await Tortoise._drop_databases()
    except DBConnectionError:  # pragma: nocoverage
        pass

    await Tortoise.init(config, _create_db=True)
    await Tortoise.generate_schemas(safe=False)


def _restore_default() -> None:
    Tortoise._app_models_map = {}
    Tortoise._db_client_map = _CONNECTIONS.copy()
    Tortoise._current_transaction_map.update(_CONN_MAP)
    Tortoise._init_apps(_CONFIG["apps"])
    Tortoise._inited = True


async def initializer(modules: List[str], db_url: str) -> None:

    """
    Sets up the DB for testing. Must be called as part of test environment setup.

    :param modules: List of modules to look for models in.
    :param db_url: The db_url, defaults to ``sqlite://:memory``.
    :param loop: Optional event loop.
    """
    # pylint: disable=W0603
    global _CONFIG
    global _CONNECTIONS
    global _TORTOISE_TEST_DB
    global _MODULES
    global _CONN_MAP
    _MODULES = modules
    if db_url is not None:  # pragma: nobranch
        _TORTOISE_TEST_DB = db_url

    _CONFIG = getDBConfig(app_label="models", modules=_MODULES)

    await _init_db(_CONFIG)

    _CONNECTIONS = Tortoise._db_client_map.copy()
    _CONN_MAP = Tortoise._current_transaction_map.copy()

    Tortoise._app_models_map = {}
    Tortoise._db_client_map = {}
    Tortoise._inited = False


async def finalizer() -> None:
    """
    Cleans up the DB after testing. Must be called as part of the test environment teardown.
    """
    _restore_default()
    await Tortoise._drop_databases()


class SimpleTestCase(IsolatedAsyncioTestCase):
    """
    The Tortoise base test class.
    This will ensure that your DB environment has a test double set up for use.

    """

    def tearDown(self) -> None:
        Tortoise._app_models_map = {}
        Tortoise._db_client_map = {}
        Tortoise._current_transaction_map = {}
        Tortoise._inited = False


class IsolatedTestCase(SimpleTestCase):
    """
    Use this if your test needs perfect isolation.

    Note to use ``{}`` as a string-replacement parameter, for your DB_URL.
    That will create a randomised database name.

    It will create and destroy a new DB instance for every test.
    This is obviously slow, but guarantees a fresh DB.

    If you define a ``tortoise_test_modules`` list, it overrides the DB setup module for the tests.
    """

    tortoise_test_modules: List[str] = []

    async def asyncSetUp(self) -> None:
        config = getDBConfig(app_label="models", modules=self.tortoise_test_modules or _MODULES)
        await Tortoise.init(config, _create_db=True)
        await Tortoise.generate_schemas(safe=False)
        self._db_client_map = Tortoise._db_client_map.copy()

    async def asyncTearDown(self) -> None:
        Tortoise._db_client_map = self._db_client_map.copy()
        await Tortoise._drop_databases()


class TruncationTestCase(SimpleTestCase):
    """
    Use this when your tests contain transactions.

    This is slower than ``TestCase`` but faster than ``IsolatedTestCase``.
    Note that usage of this does not guarantee that auto-number-pks will be reset to 1.
    """

    async def asyncSetUp(self) -> None:
        _restore_default()

        # TODO: This is a naive implementation: Will fail to clear M2M and non-cascade foreign keys
        for models_map in Tortoise._app_models_map.values():
            for model in models_map.values():
                await model._meta.db.execute_script(f"DELETE FROM {model._meta.db_table}")  # nosec

    # async def asyncTearDown(self) -> None:
    #     _restore_default()
    #
    #     # TODO: This is a naive implementation: Will fail to clear M2M and non-cascade foreign keys
    #     for models_map in Tortoise._app_models_map.values():
    #         for model in models_map.values():
    #             await model._meta.db.execute_script(f"DELETE FROM {model._meta.db_table}")  # nosec


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
