
import asyncio
import logging
from typing import Any, List, Optional, Sequence, Tuple, Type, TYPE_CHECKING, TypeVar

from pypika import Query

from tortoise.backends.base.executor import BaseExecutor
from tortoise.backends.base.filters import BaseFilter
from tortoise.backends.base.schema_generator import BaseSchemaGenerator

if TYPE_CHECKING:
    from tortoise.transactions.context import TransactionContext

DBCLIENT = TypeVar('DBCLIENT', bound='BaseDBAsyncClient')


class Capabilities:
    """
    DB Client Capabilities indicates the supported feature-set,
    and is also used to note common workarounds to defeciences.

    Defaults are set with the following standard:

    * Deficiencies: assume it is working right.
    * Features: assume it doesn't have it.

    Fields:

    ``dialect``:
        Dialect name of the DB Client driver.
    ``requires_limit``:
        Indicates that this DB requires a ``LIMIT`` statement for an ``OFFSET`` statement to work.
    ``inline_comment``:
        Indicates that comments should be rendered in line with the DDL statement,
        and not as a separate statement.
    ``supports_transactions``:
        Indicates that this DB supports transactions.
    """

    def __init__(
        self,
        dialect: str,
        *,
        # Is the connection a Daemon?
        daemon: bool = True,
        # Deficiencies to work around:
        requires_limit: bool = False,
        inline_comment: bool = False,
        supports_transactions: bool = True,
    ) -> None:
        super().__setattr__("_mutable", True)

        self.dialect = dialect
        self.daemon = daemon
        self.requires_limit = requires_limit
        self.inline_comment = inline_comment
        self.supports_transactions = supports_transactions

        super().__setattr__("_mutable", False)

    def __setattr__(self, attr, value):
        if not getattr(self, "_mutable", False):
            raise AttributeError(attr)
        return super().__setattr__(attr, value)

    def __str__(self) -> str:
        return str(self.__dict__)


class ConnectionWrapper:
    async def __aenter__(self):
        raise NotImplementedError()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        raise NotImplementedError()


class LockConnectionWrapper(ConnectionWrapper):
    __slots__ = ("connection", "lock")

    def __init__(self, connection, lock: asyncio.Lock) -> None:
        self.connection = connection
        self.lock = lock

    async def __aenter__(self):
        await self.lock.acquire()
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.lock.release()


class BaseDBAsyncClient:
    log = logging.getLogger("tortoise")

    query_class: Type[Query] = Query
    filter_class: Type[BaseFilter] = BaseFilter
    executor_class: Type[BaseExecutor] = BaseExecutor
    schema_generator: Type[BaseSchemaGenerator] = BaseSchemaGenerator
    capabilities: Capabilities = Capabilities("sql")

    def __init__(self, connection_name: str, **kwargs) -> None:
        self.connection_name = connection_name

    def _copy(self: DBCLIENT, base: DBCLIENT) -> None:
        self.connection_name = base.connection_name

    async def create_connection(self, with_db: bool) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def close(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def db_create(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def db_delete(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    def acquire_connection(self) -> ConnectionWrapper:
        raise NotImplementedError()  # pragma: nocoverage

    def in_transaction(self) -> "TransactionContext":
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_insert(self, query: str, values: list) -> Any:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_query(
        self, query: str, values: Optional[list] = None
    ) -> Tuple[int, List[str], Sequence[Sequence[Any]]]:
        """
        Execute a query

        :param query: Query string
        :param values: Query values
        :return: Returns a tuple with three elements:
            (Number of rows, Column Names, Rows of Columns of Data)

        """

        raise NotImplementedError()  # pragma: nocoverage

    async def execute_script(self, query: str) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_many(self, query: str, values: List[list]) -> None:
        raise NotImplementedError()  # pragma: nocoverage


class AsyncDbClientTransactionMixin:

    # lock acquisition and release
    async def acquire(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def release(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    # transaction operations
    async def start(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def rollback(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def commit(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    def in_progress(self) -> bool:
        raise NotImplementedError()  # pragma: nocoverage


class TransactionDBAsyncClient(BaseDBAsyncClient, AsyncDbClientTransactionMixin):
    pass
