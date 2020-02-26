import operator

from pypika import Parameter, functions
from pypika.enums import SqlTypes

from tortoise import Model
from tortoise.backends.base.executor import BaseExecutor
from tortoise.fields import BigIntegerField, IntegerField, SmallIntegerField
import tortoise.filters as tf


def mysql_contains(field, value):
    return functions.Cast(field, SqlTypes.CHAR).like(f"%{value}%")


def mysql_starts_with(field, value):
    return functions.Cast(field, SqlTypes.CHAR).like(f"{value}%")


def mysql_ends_with(field, value):
    return functions.Cast(field, SqlTypes.CHAR).like(f"%{value}")


def mysql_insensitive_exact(field, value):
    return functions.Upper(functions.Cast(field, SqlTypes.CHAR)).eq(functions.Upper(f"{value}"))


def mysql_insensitive_contains(field, value):
    return functions.Upper(functions.Cast(field, SqlTypes.CHAR)).like(functions.Upper(f"%{value}%"))


def mysql_insensitive_starts_with(field, value):
    return functions.Upper(functions.Cast(field, SqlTypes.CHAR)).like(functions.Upper(f"{value}%"))


def mysql_insensitive_ends_with(field, value):
    return functions.Upper(functions.Cast(field, SqlTypes.CHAR)).like(functions.Upper(f"%{value}"))


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
        "contains": (mysql_contains, tf.string_encoder),
        "startswith": (mysql_starts_with, tf.string_encoder),
        "endswith": (mysql_ends_with, tf.string_encoder),
        "iexact": (mysql_insensitive_exact, tf.string_encoder),
        "icontains": (mysql_insensitive_contains, tf.string_encoder),
        "istartswith": (mysql_insensitive_starts_with, tf.string_encoder),
        "iendswith": (mysql_insensitive_ends_with, tf.string_encoder),
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
