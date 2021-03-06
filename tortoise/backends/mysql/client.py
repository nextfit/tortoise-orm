
import asyncio
from typing import List, Optional, SupportsInt, Tuple, Sequence, Any

import aiomysql
import pymysql
from pymysql.charset import charset_by_name
from pypika import MySQLQuery

from tortoise.backends.base.client import (
    AsyncDbClientTransactionMixin,
    BaseDBAsyncClient,
    Capabilities,
    ConnectionWrapper,
    LockConnectionWrapper,
)
from tortoise.backends.mysql.executor import MySQLExecutor
from tortoise.backends.mysql.filters import MySQLFilter
from tortoise.backends.mysql.schema_generator import MySQLSchemaGenerator
from tortoise.exceptions import (
    DBConnectionError,
    IntegrityError,
    OperationalError,
    TransactionManagementError,
    translate_exceptions,
)
from tortoise.transactions.context import (
    LockTransactionContext,
    NestedTransactionContext,
    TransactionContext,
)

_mysql_exc_map = {
    pymysql.err.OperationalError: OperationalError,
    pymysql.err.ProgrammingError: OperationalError,
    pymysql.err.DataError: OperationalError,
    pymysql.err.InternalError: OperationalError,
    pymysql.err.NotSupportedError: OperationalError,
    pymysql.err.IntegrityError: IntegrityError
}


translate_mysql_exceptions = translate_exceptions(_mysql_exc_map)


class MySQLClient(BaseDBAsyncClient):

    query_class = MySQLQuery
    filter_class = MySQLFilter
    executor_class = MySQLExecutor
    schema_generator = MySQLSchemaGenerator
    capabilities = Capabilities("mysql", requires_limit=True, inline_comment=True)

    def __init__(
        self, *, user: str, password: str, database: str, host: str, port: SupportsInt, **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = int(port)  # make sure port is int type
        self.extra = kwargs.copy()
        self.storage_engine = self.extra.pop("storage_engine", "")
        self.extra.pop("connection_name", None)
        self.extra.pop("db", None)
        self.extra.pop("autocommit", None)
        self.extra.setdefault("sql_mode", "STRICT_TRANS_TABLES")
        self.charset = self.extra.pop("charset", "utf8mb4")
        self.pool_minsize = int(self.extra.pop("minsize", 1))
        self.pool_maxsize = int(self.extra.pop("maxsize", 5))

        self._pool: Optional[aiomysql.Pool] = None

    def _copy(self, base) -> None:
        super()._copy(base)

        self.user = base.user
        self.password = base.password
        self.database = base.database
        self.host = base.host
        self.port = base.port
        self.extra = base.extra
        self.storage_engine = base.storage_engine
        self.charset = base.charset
        self.pool_minsize = base.pool_minsize
        self.pool_maxsize = base.pool_maxsize

        self._pool = base._pool

    async def create_connection(self, with_db: bool) -> None:
        if charset_by_name(self.charset) is None:  # type: ignore
            raise DBConnectionError(f"Unknown charset {self.charset}")

        pool_template = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "db": self.database if with_db else None,
            "autocommit": True,
            "charset": self.charset,
            "minsize": self.pool_minsize,
            "maxsize": self.pool_maxsize,
            **self.extra,
        }

        try:
            self._pool = await aiomysql.create_pool(password=self.password, **pool_template)

            if isinstance(self._pool, aiomysql.Pool):
                async with self.acquire_connection() as connection:
                    async with connection.cursor() as cursor:
                        if self.storage_engine:
                            await cursor.execute(
                                f"SET default_storage_engine='{self.storage_engine}';"
                            )
                            if self.storage_engine.lower() != "innodb":  # pragma: nobranch
                                self.capabilities.__dict__["supports_transactions"] = False

            self.log.debug("Created connection %s pool with params: %s", self._pool, pool_template)

        except pymysql.err.OperationalError:
            raise DBConnectionError(f"Can't connect to MySQL server: {pool_template}")

    async def _expire_connections(self) -> None:
        if self._pool:  # pragma: nobranch
            for conn in self._pool._free:
                conn._reader.set_exception(EOFError("EOF"))

    async def close(self) -> None:
        if self._pool:  # pragma: nobranch
            self._pool.close()
            await self._pool.wait_closed()

            self.log.debug("Closed connection pool %s", self._pool)
            self._pool = None

    async def db_create(self) -> None:
        await self.create_connection(with_db=False)
        await self.execute_script(f"CREATE DATABASE {self.database}")
        await self.close()

    async def db_delete(self) -> None:
        await self.create_connection(with_db=False)
        try:
            await self.execute_script(f"DROP DATABASE {self.database}")
        except pymysql.err.DatabaseError:  # pragma: nocoverage
            pass
        await self.close()

    def acquire_connection(self) -> ConnectionWrapper:
        return self._pool.acquire()

    def in_transaction(self) -> "TransactionContext":
        return LockTransactionContext(TransactionWrapper(self))

    @translate_mysql_exceptions
    async def execute_insert(self, query: str, values: list) -> int:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            async with connection.cursor() as cursor:
                await cursor.execute(query, values)
                return cursor.lastrowid  # return auto-generated id

    @translate_mysql_exceptions
    async def execute_many(self, query: str, values: list) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            async with connection.cursor() as cursor:
                if self.capabilities.supports_transactions:
                    await connection.begin()
                    try:
                        await cursor.executemany(query, values)
                    except Exception:
                        await connection.rollback()
                        raise
                    else:
                        await connection.commit()
                else:
                    await cursor.executemany(query, values)

    @translate_mysql_exceptions
    async def execute_query(
        self, query: str, values: Optional[list] = None
    ) -> Tuple[int, List[str], Sequence[Sequence[Any]]]:

        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            async with connection.cursor() as cursor:
                await cursor.execute(query, values)
                rows = await cursor.fetchall()
                return cursor.rowcount, [f.name for f in cursor._result.fields] if rows else [], rows

    @translate_mysql_exceptions
    async def execute_script(self, query: str) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug(query)
            async with connection.cursor() as cursor:
                await cursor.execute(query)


class TransactionWrapper(MySQLClient, AsyncDbClientTransactionMixin):
    def __init__(self, db_client) -> None:
        super()._copy(db_client)
        self._lock = asyncio.Lock()

        self._connection: Optional[aiomysql.Connection] = None
        self._in_progress = False

    def in_transaction(self) -> TransactionContext:
        return NestedTransactionContext(self)

    def acquire_connection(self) -> ConnectionWrapper:
        return LockConnectionWrapper(self._connection, self._lock)

    @translate_mysql_exceptions
    async def execute_many(self, query: str, values: list) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            async with connection.cursor() as cursor:
                await cursor.executemany(query, values)

    async def acquire(self) -> None:
        if self._pool:
            self._connection = await self._pool.acquire()
        else:
            raise TransactionManagementError("You need to call create_connection() first.")

    async def release(self) -> None:
        if self._connection:
            await self._pool.release(self._connection)
            self._connection = None

    @translate_mysql_exceptions
    async def start(self) -> None:
        if self._connection:
            await self._connection.begin()
            self._in_progress = True
        else:
            raise TransactionManagementError("No connection is established. You need to call acquire() first")

    async def commit(self) -> None:
        if self._in_progress:
            await self._connection.commit()
            self._in_progress = False
        else:
            raise TransactionManagementError("No transaction is in progress. You need to call start() first.")

    async def rollback(self) -> None:
        if self._in_progress:
            await self._connection.rollback()
            self._in_progress = False
        else:
            raise TransactionManagementError("No transaction is in progress. You need to call start() first.")

    def in_progress(self) -> bool:
        return self._in_progress
