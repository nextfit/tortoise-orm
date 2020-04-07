
from pypika import Parameter

from tortoise import Model
from tortoise.fields import BigIntegerField, IntegerField, SmallIntegerField
from tortoise.backends.base.executor import BaseExecutor


class MySQLExecutor(BaseExecutor):
    EXPLAIN_PREFIX = "EXPLAIN FORMAT=JSON"

    def parameter(self, pos: int) -> Parameter:
        return Parameter("%s")

    async def _process_insert_result(self, instance: Model, results: int):
        pk_field_object = self.model._meta.pk
        if (
            isinstance(pk_field_object, (SmallIntegerField, IntegerField, BigIntegerField))
            and pk_field_object.generated
        ):
            instance.pk = results

        # MySQL can only generate a single ROWID
        #   so if any other primary key, it won't generate what we want.
