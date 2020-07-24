
import asyncio
import os
import sqlite3
from typing import List, Optional, Sequence, Tuple, Any

import aiosqlite

from tortoise.backends.base.client import (
    AsyncDbClientTransactionMixin,
    BaseDBAsyncClient,
    Capabilities,
    ConnectionWrapper,
    LockConnectionWrapper,
)
from tortoise.backends.sqlite.executor import SqliteExecutor
from tortoise.backends.sqlite.schema_generator import SqliteSchemaGenerator
from tortoise.exceptions import (
    IntegrityError,
    OperationalError,
    TransactionManagementError,
    DBConnectionError,
    translate_exceptions
)
from tortoise.transactions.context import (
    LockTransactionContext,
    NestedTransactionContext,
    TransactionContext,
)


_sqlite3_exc_map = {
    sqlite3.OperationalError: OperationalError,
    sqlite3.IntegrityError: IntegrityError
}


translate_sqlite_exceptions = translate_exceptions(_sqlite3_exc_map)


class SqliteClient(BaseDBAsyncClient):
    executor_class = SqliteExecutor
    schema_generator = SqliteSchemaGenerator
    capabilities = Capabilities("sqlite", daemon=False, requires_limit=True, inline_comment=True)

    def __init__(self, file_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.filename = file_path

        self.pragmas = kwargs.copy()
        self.pragmas.pop("connection_name", None)
        self.pragmas.setdefault("journal_mode", "WAL")
        self.pragmas.setdefault("journal_size_limit", 16384)
        self.pragmas.setdefault("foreign_keys", "ON")

        self._connection: Optional[aiosqlite.Connection] = None
        self._lock = None

    def _copy(self, base) -> None:
        super()._copy(base)
        self.filename = base.filename
        self.pragmas = base.pragmas
        self._connection = base._connection
        self._lock = base._lock

    async def create_connection(self, with_db: bool) -> None:
        if self._connection:
            raise DBConnectionError("Called create_connection on active connection. Call close() first.")

        self._lock = asyncio.Lock()

        self._connection = await aiosqlite.connect(self.filename, isolation_level=None)
        self._connection._conn.row_factory = sqlite3.Row

        for pragma, val in self.pragmas.items():
            cursor = await self._connection.execute(f"PRAGMA {pragma}={val}")
            await cursor.close()

        self.log.debug(
            "Created connection %s with params: filename=%s %s",
            self._connection,
            self.filename,
            " ".join([f"{k}={v}" for k, v in self.pragmas.items()]),
        )

    async def close(self) -> None:
        if self._connection:
            async with self._lock:
                try:
                    await self._connection.close()
                finally:
                    self.log.debug("Closed connection: filename=%s", self.filename)
                    self._connection = None
                    self._lock = None

    async def db_create(self) -> None:
        pass

    async def db_delete(self) -> None:
        try:
            os.remove(self.filename)
        except FileNotFoundError:  # pragma: nocoverage
            pass

    def acquire_connection(self) -> ConnectionWrapper:
        return LockConnectionWrapper(self._connection, self._lock)

    def in_transaction(self) -> "TransactionContext":
        return LockTransactionContext(TransactionWrapper(self))

    @translate_sqlite_exceptions
    async def execute_insert(self, query: str, values: list) -> int:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            return (await connection.execute_insert(query, values))[0]

    @translate_sqlite_exceptions
    async def execute_many(self, query: str, values: List[list]) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            # This code is only ever called in AUTOCOMMIT mode
            await connection.execute("BEGIN")
            try:
                await connection.executemany(query, values)
            except Exception:
                await connection.rollback()
                raise
            else:
                await connection.commit()

    @translate_sqlite_exceptions
    async def execute_query(
        self, query: str, values: Optional[list] = None
    ) -> Tuple[int, List[str], Sequence[Sequence[Any]]]:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            start = connection.total_changes
            rows = await connection.execute_fetchall(query, values)
            num_affected_rows = (connection.total_changes - start) or len(rows)
            return num_affected_rows, list(rows[0].keys()) if len(rows) > 0 else [], rows

    @translate_sqlite_exceptions
    async def execute_script(self, query: str) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug(query)
            await connection.executescript(query)


class TransactionWrapper(SqliteClient, AsyncDbClientTransactionMixin):
    def __init__(self, db_client: SqliteClient) -> None:
        super()._copy(db_client)
        self._trxlock = self._lock
        self._lock = asyncio.Lock()
        self._in_progress = False

    def in_transaction(self) -> TransactionContext:
        return NestedTransactionContext(self)

    @translate_sqlite_exceptions
    async def execute_many(self, query: str, values: List[list]) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            # Already within transaction, so ideal for performance
            await connection.executemany(query, values)

    async def acquire(self) -> None:
        await self._trxlock.acquire()

    async def release(self) -> None:
        self._trxlock.release()

    async def start(self) -> None:
        try:
            await self._connection.commit()
            await self._connection.execute("BEGIN")
        except sqlite3.OperationalError as exc:  # pragma: nocoverage
            raise TransactionManagementError(exc)

        self._in_progress = True

    async def rollback(self) -> None:
        if self._in_progress:
            await self._connection.rollback()
            self._in_progress = False
        else:
            raise TransactionManagementError("No transaction is in progress. You need to call start() first.")

    async def commit(self) -> None:
        if self._in_progress:
            await self._connection.commit()
            self._in_progress = False
        else:
            raise TransactionManagementError("No transaction is in progress. You need to call start() first.")

    def in_progress(self) -> bool:
        return self._in_progress
