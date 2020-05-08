
import asyncio
import logging

from pypika import Query
from tortoise.backends.base.executor import BaseExecutor
from tortoise.backends.base.filters import BaseFilter
from tortoise.backends.base.schema_generator import BaseSchemaGenerator
from tortoise.exceptions import ConfigurationError
from typing import Any, List, Optional, Sequence, Tuple, Type, Set, Dict


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
        self.lock: asyncio.Lock = lock

    async def __aenter__(self):
        await self.lock.acquire()
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.lock.release()


class PoolConnectionWrapper(ConnectionWrapper):
    __slots__ = ("connection", "pool")

    def __init__(self, pool) -> None:
        self.pool = pool
        self.connection = None

    async def __aenter__(self):
        self.connection = await self.pool.acquire()
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.pool.release(self.connection)


class BaseDBAsyncClient:
    log = logging.getLogger("tortoise")

    query_class: Type[Query] = Query
    filter_class: Type[BaseFilter] = BaseFilter
    executor_class: Type[BaseExecutor] = BaseExecutor
    schema_generator: Type[BaseSchemaGenerator] = BaseSchemaGenerator
    capabilities: Capabilities = Capabilities("")

    def __init__(self, connection_name: str, **kwargs) -> None:
        self.connection_name = connection_name

    def _copy(self, base: "BaseDBAsyncClient"):
        self.connection_name = base.connection_name

    async def generate_schema(self, safe: bool) -> None:
        schema = self.get_schema_sql(safe)
        self.log.debug("Creating schema: %s", schema)

        if schema:  # pragma: nobranch
            await self.execute_script(schema)

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
    ) -> Tuple[int, Sequence[dict]]:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_script(self, query: str) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_many(self, query: str, values: List[list]) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    def get_schema_sql(self, safe=True) -> str:
        from tortoise import Tortoise

        models_to_create = Tortoise.get_models_for_connection(self.connection_name)
        for model in models_to_create:
            model.check()

        schema_generator = self.schema_generator(self)
        tables_to_create = []
        for model in models_to_create:
            tables_to_create.extend(schema_generator.get_table_sql_list(model, safe))

        primary_tables = {t.db_table: t for t in tables_to_create if t.primary}
        table_state_map: Dict[str, int] = dict()
        table_creation_sqls: List[str] = []

        def dfs(table_name):
            table_state = table_state_map.get(table_name, 0)  # 0 == NOT_VISITED
            if table_state == 1:  # 1 == VISITING
                raise ConfigurationError("Can't create schema due to cyclic fk references")

            if table_state == 0:  # 0 == NOT_VISITED
                table_state_map[table_name] = 1  # 1 == VISITING
                table = primary_tables[table_name]
                for ref_table in table.references:
                    if ref_table != table_name:  # avoid self references
                        dfs(ref_table)
                table_creation_sqls.append(table.creation_sql)
                table_state_map[table_name] = 2  # 2 == VISITED

            return []

        for db_table in primary_tables.keys():
            dfs(db_table)

        m2m_creation_sqls = [t.creation_sql
            for t in tables_to_create if not t.primary and t.db_table not in table_state_map]

        schema_creation_string = "\n".join(table_creation_sqls + m2m_creation_sqls)
        return schema_creation_string
