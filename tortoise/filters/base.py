
from pypika import Criterion
from tortoise.query.context import QueryContext


class FieldFilter:
    def __init__(self, field_name: str, opr, value_encoder):
        self.field_name = field_name
        self.opr = opr
        self.value_encoder = value_encoder

    def __call__(self, context: QueryContext, value) -> Criterion:
        raise NotImplementedError()
