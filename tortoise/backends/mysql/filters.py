
from pypika import functions
from pypika.enums import SqlTypes


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

