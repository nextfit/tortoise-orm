
from typing import List, Optional

import asyncpg
from pypika import Parameter

from tortoise.backends.base.executor import BaseExecutor
from tortoise.models import Model


class AsyncpgExecutor(BaseExecutor):
    EXPLAIN_PREFIX = "EXPLAIN (FORMAT JSON, VERBOSE)"

    def parameter(self, pos: int) -> Parameter:
        return Parameter("$%d" % (pos + 1,))

    def _prepare_insert_statement(self, columns: List[str]) -> str:
        query = (
            self.db.query_class.into(self.model._meta.table())
            .columns(*columns)
            .insert(*[self.parameter(i) for i in range(len(columns))])
        )

        query = query.returning(*self.model._meta.generated_column_names)

        return str(query)

    async def _process_insert_result(self, instance: Model, results: Optional[asyncpg.Record]):
        if results:
            col_to_field_name = instance._meta.db_column_to_field_name_map
            for column_name, val in zip(self.model._meta.generated_column_names, results):
                setattr(instance, col_to_field_name[column_name], val)
