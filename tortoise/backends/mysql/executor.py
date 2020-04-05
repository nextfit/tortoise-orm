
import operator

from pypika import Parameter

from tortoise import Model
from tortoise.fields import BigIntegerField, IntegerField, SmallIntegerField
from tortoise.backends.base.executor import BaseExecutor
import tortoise.backends.base.filters as tf
import tortoise.backends.mysql.filters as myf


class MySQLExecutor(BaseExecutor):
    EXPLAIN_PREFIX = "EXPLAIN FORMAT=JSON"

    FILTER_FUNC_MAP = {
        "": (operator.eq, None),
        "not": (tf.not_equal, None),
        "in": (tf.is_in, tf.list_encoder),
        "not_in": (tf.not_in, tf.list_encoder),
        "isnull": (tf.is_null, tf.bool_encoder),
        "not_isnull": (tf.not_null, tf.bool_encoder),
        "gte": (operator.ge, None),
        "lte": (operator.le, None),
        "gt": (operator.gt, None),
        "lt": (operator.lt, None),
        "contains": (myf.mysql_contains, tf.string_encoder),
        "startswith": (myf.mysql_starts_with, tf.string_encoder),
        "endswith": (myf.mysql_ends_with, tf.string_encoder),
        "iexact": (myf.mysql_insensitive_exact, tf.string_encoder),
        "icontains": (myf.mysql_insensitive_contains, tf.string_encoder),
        "istartswith": (myf.mysql_insensitive_starts_with, tf.string_encoder),
        "iendswith": (myf.mysql_insensitive_ends_with, tf.string_encoder),
    }

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
