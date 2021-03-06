
from pypika.queries import QueryBuilder
from pypika.terms import Node

from tortoise.query.base import MODEL
from tortoise.query.context import QueryContext
from tortoise.query.queryset import QuerySet
from typing import Optional


class RawQuery(Node):
    def __init__(self, raw_sql: str):
        self.raw_sql = raw_sql

    def __str__(self) -> str:
        return self.get_sql()

    def __hash__(self):
        return hash(self.get_sql())

    def get_sql(self, **kwargs) -> str:
        return self.raw_sql


class RawQuerySet(QuerySet[MODEL]):
    def __init__(self, base: QuerySet, raw_sql: str):
        base._copy(self)
        self.raw_sql = raw_sql

    def _copy(self, queryset):
        super()._copy(queryset)
        queryset.raw_sql = self.raw_sql

    def create_query(self, parent_context: Optional[QueryContext]) -> QueryBuilder:
        return RawQuery(self.raw_sql.format(context=parent_context))
