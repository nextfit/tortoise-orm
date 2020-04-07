
from typing import Optional, List, Tuple
from pypika import Table, Criterion

from tortoise.context import QueryContext


class EmptyCriterion(Criterion):  # type: ignore
    def __or__(self, other):
        return other

    def __and__(self, other):
        return other

    def __bool__(self):
        return False


def _and(left: Criterion, right: Criterion):
    if left and not right:
        return left
    return left & right


def _or(left: Criterion, right: Criterion):
    if left and not right:
        return left
    return left | right


class QueryModifier:
    def __init__(
        self,
        where_criterion: Optional[Criterion] = None,
        joins: Optional[List[Tuple[Table, Criterion]]] = None,
        having_criterion: Optional[Criterion] = None,
    ) -> None:
        self.where_criterion: Criterion = where_criterion or EmptyCriterion()
        self.joins = joins if joins else []
        self.having_criterion: Criterion = having_criterion or EmptyCriterion()

    def __and__(self, other: "QueryModifier") -> "QueryModifier":
        return QueryModifier(
            where_criterion=_and(self.where_criterion, other.where_criterion),
            joins=self.joins + other.joins,
            having_criterion=_and(self.having_criterion, other.having_criterion),
        )

    def __or__(self, other: "QueryModifier") -> "QueryModifier":
        if self.having_criterion or other.having_criterion:
            # TODO: This could be optimized?
            result_having_criterion = _or(
                _and(self.where_criterion, self.having_criterion),
                _and(other.where_criterion, other.having_criterion),
            )
            return QueryModifier(
                joins=self.joins + other.joins,
                having_criterion=result_having_criterion
            )

        if self.where_criterion and other.where_criterion:
            return QueryModifier(
                where_criterion=self.where_criterion | other.where_criterion,
                joins=self.joins + other.joins,
            )
        else:
            return QueryModifier(
                where_criterion=self.where_criterion or other.where_criterion,
                joins=self.joins + other.joins,
            )

    def __invert__(self) -> "QueryModifier":
        if not self.where_criterion and not self.having_criterion:
            return QueryModifier(joins=self.joins)

        if self.having_criterion:
            # TODO: This could be optimized?
            return QueryModifier(
                joins=self.joins,
                having_criterion=_and(self.where_criterion, self.having_criterion).negate(),
            )

        return QueryModifier(where_criterion=self.where_criterion.negate(), joins=self.joins)


class FieldFilter:
    def __init__(self, field_name: str, opr, value_encoder):
        self.field_name = field_name
        self.opr = opr
        self.value_encoder = value_encoder

    def __call__(self, context: QueryContext, value) -> QueryModifier:
        raise NotImplementedError()
