
from typing import List, Type, TypeVar

from pypika import Order
from tortoise.exceptions import FieldError


MODEL = TypeVar("MODEL", bound="Model")


class QueryOrdering:
    def __init__(self, field_name: str, direction: Order):
        self.field_name = field_name
        self.direction = direction

    @staticmethod
    def parse_orderings(model: Type[MODEL], annotations, *orderings: str) -> "List[QueryOrdering]":
        output = []
        for ordering in orderings:
            if ordering[0] == "-":
                field_name = ordering[1:]
                order_type = Order.desc
            else:
                field_name = ordering
                order_type = Order.asc

            if not (field_name.split("__")[0] in model._meta.fields_map or field_name in annotations):
                raise FieldError(f"Unknown field {field_name} for model {model.__name__}")

            output.append(QueryOrdering(field_name, order_type))

        return output
