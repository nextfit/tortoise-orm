
from typing import TYPE_CHECKING

from pypika import Field
from pypika.terms import ArithmeticExpression, Function, Term, ValueWrapper

from tortoise.exceptions import FieldError
from tortoise.query.context import QueryContext

if TYPE_CHECKING:
    from tortoise.query.base import AwaitableStatement


class F(ValueWrapper):

    @staticmethod
    def resolve(term: Term, queryset: 'AwaitableStatement', context: QueryContext):
        if isinstance(term, ArithmeticExpression):
            term.left = F.resolve(term.left, queryset, context)
            term.right = F.resolve(term.right, queryset, context)

        if isinstance(term, Function):
            term.args = [F.resolve(arg, queryset, context) for arg in term.args]

        elif isinstance(term, F):
            field_name = term.value
            if field_name in queryset.annotations:
                return queryset.annotations[field_name].field

            try:
                return Field(
                    name=context.top.model._meta.field_to_db_column_name_map[field_name],
                    table=context.top.table
                )

            except KeyError:
                raise FieldError(f"Field {term.value} not found")

        return term
