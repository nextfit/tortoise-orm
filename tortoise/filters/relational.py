

from pypika import Field

from tortoise.context import QueryContext
from tortoise.fields.relational import BackwardFKField, ManyToManyField
from tortoise.filters.base import FieldFilter, QueryClauses


class RelationFilter(FieldFilter):
    def __init__(self, field_name: str, opr, value_encoder, backward_key):
        super().__init__(field_name, opr, value_encoder)
        self.backward_key = backward_key

    def __call__(self, context: QueryContext, value) -> QueryClauses:
        table = context.top.table

        context_item = context.stack[-2]
        remote_model = context_item.model
        remote_table = context_item.table
        remote_pk_db_column = remote_model._meta.pk_db_column

        joins = [(table, remote_table[remote_pk_db_column] == table[self.backward_key])]

        if isinstance(value, Field):
            encoded_value = value

        elif self.value_encoder:
            encoded_value = self.value_encoder(value, remote_model)

        else:
            encoded_value = value

        encoded_key = table[self.field_name]
        criterion = self.opr(encoded_key, encoded_value)
        return QueryClauses(where_criterion=criterion, joins=joins)


class BackwardFKFilter(RelationFilter):
    def __init__(self, field: BackwardFKField, opr, value_encoder):
        super().__init__(
            field.remote_model._meta.pk.model_field_name,
            opr,
            value_encoder,
            field.related_name)


class ManyToManyRelationFilter(RelationFilter):
    def __init__(self, field: ManyToManyField, opr, value_encoder):
        super().__init__(
            field.forward_key,
            opr,
            value_encoder,
            field.backward_key)
