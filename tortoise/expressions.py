
from typing import Union

from pypika import Field
from pypika.terms import ArithmeticExpression

from tortoise.exceptions import FieldError


class F(Field):  # type: ignore

    @staticmethod
    def resolve(
        field_to_db_column_name_map: dict,
        expression_or_field: Union[ArithmeticExpression, Field],
    ):

        if isinstance(expression_or_field, Field):
            try:
                expression_or_field.name = field_to_db_column_name_map[expression_or_field.name]

            except KeyError:
                raise FieldError(f"Field {expression_or_field.name} is virtual and can not be updated")

        elif isinstance(expression_or_field, ArithmeticExpression):
            expression_or_field.left = F.resolve(
                field_to_db_column_name_map,
                expression_or_field.left)

            expression_or_field.right = F.resolve(
                field_to_db_column_name_map,
                expression_or_field.right)

        return expression_or_field
