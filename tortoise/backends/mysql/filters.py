import operator

from pypika import functions
from pypika.enums import SqlTypes

from tortoise.backends.base.filters import BaseFilter
import tortoise.backends.base.filters as tf


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


class MySQLFilter(BaseFilter):
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
