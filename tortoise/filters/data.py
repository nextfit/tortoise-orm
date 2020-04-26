
from tortoise.context import QueryContext
from tortoise.fields.base import Field
from tortoise.filters.base import FieldFilter, QueryClauses
from tortoise.functions import OuterRef, Subquery


class DataFieldFilter(FieldFilter):
    def __init__(self, field: Field, opr, value_encoder=None):
        super().__init__(field.model_field_name, opr, value_encoder)
        self.db_column = field.db_column or field.model_field_name

    def __call__(self, context: QueryContext, value) -> QueryClauses:
        context_item = context.top
        model = context_item.model
        table = context_item.table

        field_object = model._meta.fields_map[self.field_name]

        joins = []

        if isinstance(value, OuterRef):
            outer_table = context.stack[-2].table
            encoded_value = outer_table[value.ref_name]

        elif isinstance(value, Subquery):
            encoded_value = value.field

        elif self.value_encoder:
            encoded_value = self.value_encoder(value, model, field_object)

        else:
            encoded_value = model._meta.db.executor_class._field_to_db(field_object, value, model)

        encoded_key = table[self.db_column]
        criterion = self.opr(encoded_key, encoded_value)
        return QueryClauses(where_criterion=criterion, joins=joins)


class JSONFieldFilter(FieldFilter):
    def __init__(self, field: Field, opr, value_encoder):
        super().__init__(field.model_field_name, opr, value_encoder)
        self.db_column = field.db_column or field.model_field_name

    def __call__(self, context: QueryContext, value) -> QueryClauses:
        context_item = context.top
        model = context_item.model
        table = context_item.table

        field_object = model._meta.fields_map[self.field_name]
        encoded_value = self.value_encoder(value, model, field_object) if self.value_encoder else value

        encoded_key = table[self.db_column]
        criterion = self.opr(encoded_key, encoded_value)
        return QueryClauses(where_criterion=criterion)
