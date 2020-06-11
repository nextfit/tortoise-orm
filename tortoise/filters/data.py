
import pypika
from pypika.terms import Node, Term

from tortoise.fields import Field
from tortoise.filters.base import FieldFilter
from tortoise.query.context import QueryContext


class DataFieldFilter(FieldFilter):
    def __init__(self, field: Field, opr, value_encoder=None):
        super().__init__(field.model_field_name, opr, value_encoder)
        self.db_column = field.db_column

    def __call__(self, context: QueryContext, value) -> pypika.Criterion:
        context_item = context.top
        model = context_item.model
        table = context_item.table

        field_object = model._meta.fields_map[self.field_name]

        if isinstance(value, (Node, Term)):
            encoded_value = value

        elif self.value_encoder:
            encoded_value = self.value_encoder(value, model, field_object)

        else:
            encoded_value = field_object.db_value(value, model)

        encoded_key = table[self.db_column]
        return self.opr(encoded_key, encoded_value)


class JSONFieldFilter(FieldFilter):
    def __init__(self, field: Field, opr, value_encoder):
        super().__init__(field.model_field_name, opr, value_encoder)
        self.db_column = field.db_column

    def __call__(self, context: QueryContext, value) -> pypika.Criterion:
        context_item = context.top
        model = context_item.model
        table = context_item.table

        field_object = model._meta.fields_map[self.field_name]
        encoded_value = self.value_encoder(value, model, field_object) if self.value_encoder else value

        encoded_key = table[self.db_column]
        return self.opr(encoded_key, encoded_value)
