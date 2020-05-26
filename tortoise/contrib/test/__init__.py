import asyncio
import os as _os
import unittest
from asyncio.events import AbstractEventLoop
from functools import wraps
from typing import Any, List, Optional
from unittest import SkipTest, expectedFailure, skip, skipIf, skipUnless

from asynctest import TestCase as _TestCase
from asynctest import _fail_on
from asynctest.case import _Policy

from tortoise import Tortoise
from tortoise.backends.base.config_generator import generate_config as _generate_config
from tortoise.exceptions import DBConnectionError
from tortoise.transactions.context import TransactionContext

__all__ = (
    "SimpleTestCase",
    "TestCase",
    "TruncationTestCase",
    "IsolatedTestCase",
    "getDBConfig",
    "requireCapability",
    "env_initializer",
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
Mark test as expecting failiure.

On success it will be marked as unexpected success.
"""

_CONFIG: dict = {}
_CONNECTIONS: dict = {}
_SELECTOR = None
_LOOP: AbstractEventLoop = None  # type: ignore
_MODULES: List[str] = []
_CONN_MAP: dict = {}


def getDBConfig(app_label: str, modules: List[str]) -> dict:
    """
    DB Config factory, for use in testing.

    :param app_label: Label of the app (must be distinct for multiple apps).
    :param modules: List of modules to look for models in.
    """
    return _generate_config(
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


def initializer(
    modules: List[str], db_url: Optional[str] = None, loop: Optional[AbstractEventLoop] = None
) -> None:
    """
    Sets up the DB for testing. Must be called as part of test environment setup.

    :param modules: List of modules to look for models in.
    :param db_url: The db_url, defaults to ``sqlite://:memory``.
    :param loop: Optional event loop.
    """
    # pylint: disable=W0603
    global _CONFIG
    global _CONNECTIONS
    global _SELECTOR
    global _LOOP
    global _TORTOISE_TEST_DB
    global _MODULES
    global _CONN_MAP
    _MODULES = modules
    if db_url is not None:  # pragma: nobranch
        _TORTOISE_TEST_DB = db_url
    _CONFIG = getDBConfig(app_label="models", modules=_MODULES)

    loop = loop or asyncio.get_event_loop()
    _LOOP = loop
    _SELECTOR = loop._selector  # type: ignore
    loop.run_until_complete(_init_db(_CONFIG))
    _CONNECTIONS = Tortoise._db_client_map.copy()
    _CONN_MAP = Tortoise._current_transaction_map.copy()
    Tortoise._app_models_map = {}
    Tortoise._db_client_map = {}
    Tortoise._inited = False


def finalizer() -> None:
    """
    Cleans up the DB after testing. Must be called as part of the test environment teardown.
    """
    _restore_default()
    loop = _LOOP
    loop._selector = _SELECTOR  # type: ignore
    loop.run_until_complete(Tortoise._drop_databases())


def env_initializer() -> None:  # pragma: nocoverage
    """
    Calls ``initializer()`` with parameters mapped from environment variables.

    ``TORTOISE_TEST_MODULES``:
        A comma-separated list of modules to include *(required)*
    ``TORTOISE_TEST_DB``:
        The db_url of the test db. *(optional*)
    """
    modules = str(_os.environ.get("TORTOISE_TEST_MODULES", "tests.testmodels")).split(",")
    db_url = _os.environ.get("TORTOISE_TEST_DB", "sqlite://:memory:")
    if not modules:  # pragma: nocoverage
        raise Exception("TORTOISE_TEST_MODULES env var not defined")
    initializer(modules, db_url=db_url)


class SimpleTestCase(_TestCase):
    """
    The Tortoise base test class.

    This will ensure that your DB environment has a test double set up for use.

    An asyncio capable test class that provides some helper functions.

    Will run any ``test_*()`` function either as sync or async, depending
    on the signature of the function.
    If you specify ``async test_*()`` then it will run it in an event loop.

    Based on `asynctest <http://asynctest.readthedocs.io/>`_
    """

    use_default_loop = True

    def _init_loop(self) -> None:
        if self.use_default_loop:
            self.loop = _LOOP
            loop = None
        else:  # pragma: nocoverage
            loop = self.loop = asyncio.new_event_loop()

        policy = _Policy(asyncio.get_event_loop_policy(), loop, self.forbid_get_event_loop)

        asyncio.set_event_loop_policy(policy)

        self.loop = self._patch_loop(self.loop)

    async def _setUpDB(self) -> None:
        pass

    async def _tearDownDB(self) -> None:
        pass

    async def _setUp(self) -> None:

        # initialize post-test checks
        test = getattr(self, self._testMethodName)
        checker = getattr(test, _fail_on._FAIL_ON_ATTR, None)
        self._checker = checker or _fail_on._fail_on()
        self._checker.before_test(self)

        await self._setUpDB()
        if asyncio.iscoroutinefunction(self.setUp):
            await self.setUp()
        else:
            self.setUp()

        # don't take into account if the loop ran during setUp
        self.loop._asynctest_ran = False  # type: ignore

    async def _tearDown(self) -> None:
        if asyncio.iscoroutinefunction(self.tearDown):
            await self.tearDown()
        else:
            self.tearDown()
        await self._tearDownDB()
        Tortoise._app_models_map = {}
        Tortoise._db_client_map = {}
        Tortoise._inited = False

        # post-test checks
        self._checker.check_test(self)

    # Override unittest.TestCase methods which call setUp() and tearDown()
    def run(self, result=None):
        orig_result = result
        if result is None:  # pragma: nocoverage
            result = self.defaultTestResult()
            startTestRun = getattr(result, "startTestRun", None)
            if startTestRun is not None:
                startTestRun()

        result.startTest(self)

        testMethod = getattr(self, self._testMethodName)
        if getattr(self.__class__, "__unittest_skip__", False) or getattr(
            testMethod, "__unittest_skip__", False
        ):
            # If the class or method was skipped.
            try:
                skip_why = getattr(self.__class__, "__unittest_skip_why__", "") or getattr(
                    testMethod, "__unittest_skip_why__", ""
                )
                self._addSkip(result, self, skip_why)
            finally:
                result.stopTest(self)
            return
        expecting_failure = getattr(testMethod, "__unittest_expecting_failure__", False)
        outcome = unittest.case._Outcome(result)  # type: ignore
        try:
            self._outcome = outcome

            self._init_loop()

            self.loop.run_until_complete(self._run_outcome(outcome, expecting_failure, testMethod))

            self.loop.run_until_complete(self.doCleanups())
            self._unset_loop()
            for test, reason in outcome.skipped:
                self._addSkip(result, test, reason)
            self._feedErrorsToResult(result, outcome.errors)
            if outcome.success:
                if expecting_failure:
                    if outcome.expectedFailure:
                        self._addExpectedFailure(result, outcome.expectedFailure)
                    else:  # pragma: nocoverage
                        self._addUnexpectedSuccess(result)
                else:
                    result.addSuccess(self)
            return result
        finally:
            result.stopTest(self)
            if orig_result is None:  # pragma: nocoverage
                stopTestRun = getattr(result, "stopTestRun", None)
                if stopTestRun is not None:
                    stopTestRun()  # pylint: disable=E1102

            # explicitly break reference cycles:
            # outcome.errors -> frame -> outcome -> outcome.errors
            # outcome.expectedFailure -> frame -> outcome -> outcome.expectedFailure
            outcome.errors.clear()
            outcome.expectedFailure = None

            # clear the outcome, no more needed
            self._outcome = None

    async def _run_outcome(self, outcome, expecting_failure, testMethod) -> None:
        with outcome.testPartExecutor(self):
            await self._setUp()
        if outcome.success:
            outcome.expecting_failure = expecting_failure
            with outcome.testPartExecutor(self, isTest=True):
                await self._run_test_method(testMethod)
            outcome.expecting_failure = False
            with outcome.testPartExecutor(self):
                await self._tearDown()

    async def _run_test_method(self, method) -> None:
        # If the method is a coroutine or returns a coroutine, run it on the
        # loop
        result = method()
        if asyncio.iscoroutine(result):
            await result


class IsolatedTestCase(SimpleTestCase):
    """
    An asyncio capable test class that will ensure that an isolated test db
    is available for each test.

    Use this if your test needs perfect isolation.

    Note to use ``{}`` as a string-replacement parameter, for your DB_URL.
    That will create a randomised database name.

    It will create and destroy a new DB instance for every test.
    This is obviously slow, but guarantees a fresh DB.

    If you define a ``tortoise_test_modules`` list, it overrides the DB setup module for the tests.
    """

    tortoise_test_modules: List[str] = []

    async def _setUpDB(self) -> None:
        config = getDBConfig(app_label="models", modules=self.tortoise_test_modules or _MODULES)
        await Tortoise.init(config, _create_db=True)
        await Tortoise.generate_schemas(safe=False)
        self._db_client_map = Tortoise._db_client_map.copy()

    async def _tearDownDB(self) -> None:
        Tortoise._db_client_map = self._db_client_map.copy()
        await Tortoise._drop_databases()


class TruncationTestCase(SimpleTestCase):
    """
    An asyncio capable test class that will truncate the tables after a test.

    Use this when your tests contain transactions.

    This is slower than ``TestCase`` but faster than ``IsolatedTestCase``.
    Note that usage of this does not guarantee that auto-number-pks will be reset to 1.
    """

    async def _setUpDB(self) -> None:
        _restore_default()

    async def _tearDownDB(self) -> None:
        _restore_default()
        # TODO: This is a naive implementation: Will fail to clear M2M and non-cascade foreign keys
        for models_map in Tortoise._app_models_map.values():
            for model in models_map.values():
                await model._meta.db.execute_script(f"DELETE FROM {model._meta.db_table}")  # nosec


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

    async def _run_outcome(self, outcome, expecting_failure, testMethod) -> None:
        _restore_default()
        self.__db__ = Tortoise.get_db_client("models")
        if self.__db__.capabilities.supports_transactions:
            db_client = self.__db__.in_transaction().db_client
            async with TestTransactionContext(db_client):
                await super()._run_outcome(outcome, expecting_failure, testMethod)
        else:
            await super()._run_outcome(outcome, expecting_failure, testMethod)

    async def _setUpDB(self) -> None:
        pass

    async def _tearDownDB(self) -> None:
        if self.__db__.capabilities.supports_transactions:
            _restore_default()
        else:
            await super()._tearDownDB()


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
