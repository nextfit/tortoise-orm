import uuid
from typing import List, Optional

import asyncpg
from pypika import Parameter

from tortoise import Model
from tortoise.backends.base.executor import BaseExecutor


class AsyncpgExecutor(BaseExecutor):
    EXPLAIN_PREFIX = "EXPLAIN (FORMAT JSON, VERBOSE)"
    DB_NATIVE = BaseExecutor.DB_NATIVE | {uuid.UUID}

    def parameter(self, pos: int) -> Parameter:
        return Parameter("$%d" % (pos + 1,))

    def _prepare_insert_statement(self, columns: List[str], no_generated: bool = False) -> str:
        query = (
            self.db.query_class.into(self.model._meta.basetable)
            .columns(*columns)
            .insert(*[self.parameter(i) for i in range(len(columns))])
        )
        if not no_generated:
            generated_column_names = self.model._meta.generated_column_names
            if generated_column_names:
                query = query.returning(*generated_column_names)

        return str(query)

    async def _process_insert_result(self, instance: Model, results: Optional[asyncpg.Record]):
        if results:
            generated_column_names = self.model._meta.generated_column_names
            col_to_field_name = instance._meta.db_column_to_field_name_map
            for column_name, val in zip(generated_column_names, results):
                setattr(instance, col_to_field_name[column_name], val)
