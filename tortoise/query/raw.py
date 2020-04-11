
from tortoise.context import QueryContext
from tortoise.query.queryset import QuerySet
from tortoise.query.base import MODEL


class RawQuerySet(QuerySet[MODEL]):
    def _clone(self) -> "RawQuerySet[MODEL]":
        queryset = RawQuerySet.__new__(RawQuerySet)
        self._copy(queryset)
        return queryset

    def _make_query(self, context: QueryContext, alias=None) -> None:
        pass
