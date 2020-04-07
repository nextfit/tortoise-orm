
from tortoise.context import QueryContext
from tortoise.fields.base import Field
from tortoise.filters.base import FieldFilter, QueryModifier
from tortoise.functions import OuterRef, Subquery


class DataFieldFilter(FieldFilter):
    def __init__(self, field: Field, opr, value_encoder=None):
        super().__init__(field.model_field_name, opr, value_encoder)
        self.db_column = field.db_column or field.model_field_name

    def __call__(self, context: QueryContext, value) -> QueryModifier:
        context_item = context.stack[-1]
        model = context_item.model
        table = context_item.table

        field_object = model._meta.fields_map[self.field_name]

        joins = []

        if isinstance(value, OuterRef):
            outer_table = context.stack[-2].table
            encoded_value = outer_table[value.ref_name]

        elif isinstance(value, Subquery):
            annotation_info = value.resolve(context, "U{}".format(len(context.stack)))
            encoded_value = annotation_info.field
            joins.extend(annotation_info.joins)

        elif self.value_encoder:
            encoded_value = self.value_encoder(value, model, field_object)

        else:
            encoded_value = model._meta.db.executor_class._field_to_db(field_object, value, model)

        encoded_key = table[self.db_column]
        criterion = self.opr(encoded_key, encoded_value)
        return QueryModifier(where_criterion=criterion, joins=joins)

