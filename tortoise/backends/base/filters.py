

import operator
from functools import partial

from pypika import functions
from pypika.enums import SqlTypes

from tortoise.fields import BackwardFKField, Field, ManyToManyField, RelationField

#
# Encoders
#

def identity_encoder(value, *args):
    return value


def bool_encoder(value, *args):
    return bool(value)


def string_encoder(value, *args):
    return str(value)


def list_encoder(values, instance, field: Field):
    """Encodes an iterable of a given field into a database-compatible format."""
    return [field.to_db_value(element, instance) for element in values]

#
# to_db_value functions
#


def related_to_db_value_func(field: RelationField):
    return field.remote_model._meta.pk.to_db_value


def list_pk_encoder(values, instance, field: Field):
    return [field.to_db_value(getattr(v, "pk", v), instance) for v in values]


def related_list_to_db_values_func(field: RelationField):
    return partial(list_pk_encoder, field=field.remote_model._meta.pk)


#
# Filters
#


def is_in(field, value):
    return field.isin(value)


def not_in(field, value):
    return field.notin(value) | field.isnull()


def not_equal(field, value):
    return field.ne(value) | field.isnull()


def is_null(field, value):
    if value:
        return field.isnull()
    return field.notnull()


def not_null(field, value):
    if value:
        return field.notnull()
    return field.isnull()


def contains(field, value):
    return functions.Cast(field, SqlTypes.VARCHAR).like(f"%{value}%")


def starts_with(field, value):
    return functions.Cast(field, SqlTypes.VARCHAR).like(f"{value}%")


def ends_with(field, value):
    return functions.Cast(field, SqlTypes.VARCHAR).like(f"%{value}")


def insensitive_exact(field, value):
    return functions\
        .Upper(functions.Cast(field, SqlTypes.VARCHAR))\
        .eq(functions.Upper(f"{value}"))


def insensitive_contains(field, value):
    return functions\
        .Upper(functions.Cast(field, SqlTypes.VARCHAR))\
        .like(functions.Upper(f"%{value}%"))


def insensitive_starts_with(field, value):
    return functions\
        .Upper(functions.Cast(field, SqlTypes.VARCHAR))\
        .like(functions.Upper(f"{value}%"))


def insensitive_ends_with(field, value):
    return functions\
        .Upper(functions.Cast(field, SqlTypes.VARCHAR))\
        .like(functions.Upper(f"%{value}"))


class BaseFilter:
    FILTER_FUNC_MAP = {
        "": (operator.eq, None),
        "exact": (operator.eq, None),
        "not": (not_equal, None),
        "in": (is_in, list_encoder),
        "not_in": (not_in, list_encoder),
        "isnull": (is_null, bool_encoder),
        "not_isnull": (not_null, bool_encoder),
        "gte": (operator.ge, None),
        "lte": (operator.le, None),
        "gt": (operator.gt, None),
        "lt": (operator.lt, None),
        "contains": (contains, string_encoder),
        "startswith": (starts_with, string_encoder),
        "endswith": (ends_with, string_encoder),
        "iexact": (insensitive_exact, string_encoder),
        "icontains": (insensitive_contains, string_encoder),
        "istartswith": (insensitive_starts_with, string_encoder),
        "iendswith": (insensitive_ends_with, string_encoder),
    }

    RELATED_FILTER_FUNC_MAP = {
        "": (operator.eq, related_to_db_value_func),
        "exact": (operator.eq, related_to_db_value_func),
        "not": (not_equal, related_to_db_value_func),
        "in": (is_in, related_list_to_db_values_func),
        "not_in": (not_in, related_list_to_db_values_func)
    }

    @classmethod
    def get_filter_func_for(cls, field, comparison):
        if isinstance(field, (BackwardFKField, ManyToManyField)):
            if comparison not in cls.RELATED_FILTER_FUNC_MAP:
                return None

            (filter_operator, filter_encoder) = cls.RELATED_FILTER_FUNC_MAP[comparison]
            return filter_operator, filter_encoder(field)

        else:
            if comparison not in cls.FILTER_FUNC_MAP:
                return None

            return cls.FILTER_FUNC_MAP[comparison]
