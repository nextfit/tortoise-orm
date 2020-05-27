import asyncio
from functools import wraps
from typing import Optional, SupportsInt, Tuple, Sequence, Any, List

import asyncpg
from asyncpg.transaction import Transaction
from pypika import PostgreSQLQuery

from tortoise.backends.asyncpg.executor import AsyncpgExecutor
from tortoise.backends.asyncpg.filters import AsyncpgFilter
from tortoise.backends.asyncpg.schema_generator import AsyncpgSchemaGenerator
from tortoise.backends.base.client import (
    AsyncDbClientTransactionMixin,
    BaseDBAsyncClient,
    Capabilities,
    ConnectionWrapper,
    LockConnectionWrapper,
    PoolConnectionWrapper,
)
from tortoise.exceptions import (
    DBConnectionError,
    IntegrityError,
    OperationalError,
    TransactionManagementError,
)
from tortoise.transactions.context import (
    LockTransactionContext,
    NestedTransactionContext,
    TransactionContext,
)


def translate_exceptions(func):
    @wraps(func)
    async def translate_exceptions_(self, *args):
        try:
            return await func(self, *args)
        except asyncpg.SyntaxOrAccessError as exc:
            raise OperationalError(exc)
        except asyncpg.IntegrityConstraintViolationError as exc:
            raise IntegrityError(exc)
        except asyncpg.InvalidTransactionStateError as exc:  # pragma: nocoverage
            raise TransactionManagementError(exc)

    return translate_exceptions_


class AsyncpgDBClient(BaseDBAsyncClient):
    DSN_TEMPLATE = "postgres://{user}:{password}@{host}:{port}/{database}"

    query_class = PostgreSQLQuery
    filter_class = AsyncpgFilter
    executor_class = AsyncpgExecutor
    schema_generator = AsyncpgSchemaGenerator
    capabilities = Capabilities("postgres")

    def __init__(
        self, user: str, password: str, database: str, host: str, port: SupportsInt, **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = int(port)  # make sure port is int type
        self.extra = kwargs.copy()
        self.schema = self.extra.pop("schema", None)
        self.extra.pop("connection_name", None)
        self.extra.pop("loop", None)
        self.extra.pop("connection_class", None)
        self.pool_minsize = int(self.extra.pop("minsize", 1))
        self.pool_maxsize = int(self.extra.pop("maxsize", 5))

        self._pool: Optional[asyncpg.pool] = None

    def _copy(self, base: "AsyncpgDBClient"):
        super()._copy(base)

        self.user = base.user
        self.password = base.password
        self.database = base.database
        self.host = base.host
        self.post = base.port
        self.extra = base.extra
        self.schema = base.schema
        self.pool_minsize = base.pool_minsize
        self.pool_maxsize = base.pool_maxsize
        self._pool = base._pool

    async def create_connection(self, with_db: bool) -> None:
        pool_template = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "database": self.database if with_db else None,
            "min_size": self.pool_minsize,
            "max_size": self.pool_maxsize,
            **self.extra,
        }

        if self.schema:
            pool_template["server_settings"] = {"search_path": self.schema}

        try:
            self._pool = await asyncpg.create_pool(None, password=self.password, **pool_template)
            self.log.debug("Created connection pool %s with params: %s", self._pool, pool_template)
        except asyncpg.InvalidCatalogNameError:
            raise DBConnectionError(f"Can't establish connection to database {self.database}")

    async def _expire_connections(self) -> None:
        if self._pool:  # pragma: nobranch
            await self._pool.expire_connections()

    async def _close(self) -> None:
        if self._pool:  # pragma: nobranch
            try:
                await asyncio.wait_for(self._pool.close(), 10)
            except asyncio.TimeoutError:  # pragma: nocoverage
                self._pool.terminate()

            self.log.debug("Closed connection pool %s", self._pool)
            self._pool = None

    async def close(self) -> None:
        await self._close()

    async def db_create(self) -> None:
        await self.create_connection(with_db=False)
        await self.execute_script(f'CREATE DATABASE "{self.database}" OWNER "{self.user}"')
        await self.close()

    async def db_delete(self) -> None:
        await self.create_connection(with_db=False)
        try:
            await self.execute_script(f'DROP DATABASE "{self.database}"')
        except asyncpg.InvalidCatalogNameError:  # pragma: nocoverage
            pass
        await self.close()

    def acquire_connection(self) -> ConnectionWrapper:
        return PoolConnectionWrapper(self._pool)

    def in_transaction(self) -> TransactionContext:
        return LockTransactionContext(TransactionWrapper(self))

    @translate_exceptions
    async def execute_insert(self, query: str, values: list) -> Optional[asyncpg.Record]:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            # TODO: Cache prepared statement
            return await connection.fetchrow(query, *values)

    @translate_exceptions
    async def execute_many(self, query: str, values: list) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            # TODO: Consider using copy_records_to_table instead
            transaction = connection.transaction()
            await transaction.start()
            try:
                await connection.executemany(query, values)
            except Exception:
                await transaction.rollback()
                raise
            else:
                await transaction.commit()

    @translate_exceptions
    async def execute_query(
        self, query: str, values: Optional[list] = None
    ) -> Tuple[int, List[str], Sequence[Sequence[Any]]]:

        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            params = [query, *(values or [])]

            if query.startswith("UPDATE") or query.startswith("DELETE"):
                res = await connection.execute(*params)
                try:
                    rows_affected = int(res.split(" ")[1])
                except Exception:  # pragma: nocoverage
                    rows_affected = 0

                return rows_affected, [], []

            else:
                rows = await connection.fetch(*params)
                return len(rows), list(rows[0].keys()) if len(rows) > 0 else [], rows

    @translate_exceptions
    async def execute_script(self, query: str) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug(query)
            await connection.execute(query)


class TransactionWrapper(AsyncpgDBClient, AsyncDbClientTransactionMixin):
    def __init__(self, db_client: AsyncpgDBClient) -> None:
        super()._copy(db_client)
        self._lock = asyncio.Lock()

        self._connection: Optional[asyncpg.Connection] = None
        self._transaction: Optional[Transaction] = None

    def in_transaction(self) -> TransactionContext:
        return NestedTransactionContext(self)

    def acquire_connection(self) -> ConnectionWrapper:
        return LockConnectionWrapper(self._connection, self._lock)

    @translate_exceptions
    async def execute_many(self, query: str, values: list) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            # TODO: Consider using copy_records_to_table instead
            await connection.executemany(query, values)

    async def acquire(self) -> None:
        if self._pool:
            self._connection = await self._pool.acquire()
        else:
            raise TransactionManagementError("You need to call create_connection() first.")

    async def release(self) -> None:
        if self._connection:
            await self._pool.release(self._connection)
            self._connection = None

    @translate_exceptions
    async def start(self) -> None:
        if self._connection:
            self._transaction = self._connection.transaction()
            await self._transaction.start()
        else:
            raise TransactionManagementError("No connection is established. You need to call acquire() first")

    async def commit(self) -> None:
        if self._transaction:
            await self._transaction.commit()
            self._transaction = None
        else:
            raise TransactionManagementError("No transaction is in progress. You need to call start() first.")

    async def rollback(self) -> None:
        if self._transaction:
            await self._transaction.rollback()
            self._transaction = None
        else:
            raise TransactionManagementError("No transaction is in progress. You need to call start() first.")

    def in_progress(self) -> bool:
        return self._transaction is not None
