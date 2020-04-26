
from pypika import Criterion
from tortoise.exceptions import ParamsError
from typing import Optional


class EmptyCriterion(Criterion):  # type: ignore
    def __or__(self, other):
        return other

    def __and__(self, other):
        return other

    def __bool__(self):
        return False


def _and(left: Criterion, right: Criterion):
    if left and right:
        return left & right

    if left:
        return left

    return right


def _or(left: Criterion, right: Criterion):
    if left and right:
        return left | right

    if left:
        return left

    return right


class QueryClauses:
    def __init__(
        self,
        where_criterion: Optional[Criterion] = None,
        having_criterion: Optional[Criterion] = None,
    ) -> None:
        self.where_criterion: Criterion = where_criterion or EmptyCriterion()
        self.having_criterion: Criterion = having_criterion or EmptyCriterion()

    def __and__(self, other: "QueryClauses") -> "QueryClauses":
        return QueryClauses(
            where_criterion=_and(self.where_criterion, other.where_criterion),
            having_criterion=_and(self.having_criterion, other.having_criterion),
        )

    def __or__(self, other: "QueryClauses") -> "QueryClauses":
        if self.having_criterion or other.having_criterion:
            result_having_criterion = _or(
                _and(self.where_criterion, self.having_criterion),
                _and(other.where_criterion, other.having_criterion),
            )
            return QueryClauses(
                having_criterion=result_having_criterion
            )

        if self.where_criterion and other.where_criterion:
            return QueryClauses(
                where_criterion=self.where_criterion | other.where_criterion,
            )
        else:
            return QueryClauses(
                where_criterion=self.where_criterion or other.where_criterion,
            )

    def __invert__(self) -> "QueryClauses":
        if not self.where_criterion and not self.having_criterion:
            raise Exception("Inverting and empty QueryClause")

        if self.having_criterion:
            return QueryClauses(having_criterion=_and(self.where_criterion, self.having_criterion).negate(),)

        return QueryClauses(where_criterion=self.where_criterion.negate())
