from pypika.terms import Node

from tortoise.context import QueryContext
from tortoise.query.queryset import QuerySet
from tortoise.query.base import MODEL


class RawQuery(Node):
    def __init__(self, raw_sql):
        self.raw_sql = raw_sql

    def __str__(self):
        return self.get_sql()

    def __hash__(self):
        return hash(self.get_sql())

    def get_sql(self, **kwargs):
        return self.raw_sql


class RawQuerySet(QuerySet[MODEL]):
    def __init__(self, base: QuerySet, raw_sql: str):
        base._copy(self)
        self.raw_sql = raw_sql

    def _copy(self, queryset):
        super()._copy(queryset)
        queryset.raw_sql = self.raw_sql

    def _make_query(self, context: QueryContext, alias=None) -> None:
        self.query = RawQuery(self.raw_sql.format(context=context))
