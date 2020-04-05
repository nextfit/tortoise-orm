
from functools import partial
from pypika import functions
from pypika.enums import SqlTypes

from tortoise.fields import Field, RelationField

#
# Encoders
#


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

