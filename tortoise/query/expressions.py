

from pypika import Field
from pypika.terms import ArithmeticExpression, Term, Function

from tortoise.query.context import QueryContext
from tortoise.exceptions import FieldError


class F(Field):  # type: ignore

    @staticmethod
    def resolve(term: Term, context: QueryContext):
        if isinstance(term, ArithmeticExpression):
            term.left = F.resolve(term.left, context)
            term.right = F.resolve(term.right, context)

        if isinstance(term, Function):
            term.args = [F.resolve(arg, context) for arg in term.args]

        elif isinstance(term, Field):
            try:
                term.name = context.top.model._meta.field_to_db_column_name_map[term.name]
                term.table = context.top.table

            except KeyError:
                raise FieldError(f"Field {term.name} not found")

        return term
