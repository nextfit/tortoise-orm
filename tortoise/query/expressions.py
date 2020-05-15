

from pypika import Field
from pypika.terms import ArithmeticExpression, Term, Function, ValueWrapper

from tortoise.query.context import QueryContext
from tortoise.exceptions import FieldError


class F(ValueWrapper):  # type: ignore

    @staticmethod
    def resolve(term: Term, context: QueryContext):
        if isinstance(term, ArithmeticExpression):
            term.left = F.resolve(term.left, context)
            term.right = F.resolve(term.right, context)

        if isinstance(term, Function):
            term.args = [F.resolve(arg, context) for arg in term.args]

        elif isinstance(term, F):
            try:
                return Field(
                    name=context.top.model._meta.field_to_db_column_name_map[term.value],
                    table=context.top.table
                )

            except KeyError:
                raise FieldError(f"Field {term.value} not found")

        return term
