

from pypika import Table

from tortoise.context import QueryContext
from tortoise.fields.relational import BackwardFKRelation, ManyToManyField
from tortoise.filters.base import FieldFilter, QueryModifier
from tortoise.functions import OuterRef


class RelationFilter(FieldFilter):
    def __init__(self, field_name: str, opr, value_encoder, table, backward_key):
        super().__init__(field_name, opr, value_encoder)

        self.table = table
        self.backward_key = backward_key

    def __call__(self, context: QueryContext, value) -> QueryModifier:
        context_item = context.stack[-1]
        model = context_item.model
        table = context_item.table

        pk_db_column = model._meta.pk_db_column
        joins = [(
            self.table,
            table[pk_db_column] == getattr(self.table, self.backward_key),
        )]

        if isinstance(value, OuterRef):
            outer_context_item = context.stack[-2]
            outer_model = outer_context_item.model
            outer_table = outer_context_item.table

            outer_field = outer_model._meta.fields_map[value.ref_name]

            if isinstance(outer_field, ManyToManyField):
                if outer_field.through in outer_context_item.through_tables:
                    outer_through_table = outer_context_item.through_tables[outer_field.through]
                    encoded_value = outer_through_table[outer_field.forward_key]

                else:
                    raise NotImplementedError()

            elif isinstance(outer_field, BackwardFKRelation):
                raise NotImplementedError()

            else:
                encoded_value = outer_table[value.ref_name]

        elif self.value_encoder:
            encoded_value = self.value_encoder(value, model)

        else:
            encoded_value = value

        encoded_key = self.table[self.field_name]
        criterion = self.opr(encoded_key, encoded_value)
        return QueryModifier(where_criterion=criterion, joins=joins)


class BackwardFKFilter(RelationFilter):
    def __init__(self, field: BackwardFKRelation, opr, value_encoder):
        super().__init__(
            field.remote_model._meta.pk.model_field_name,
            opr,
            value_encoder,
            Table(field.remote_model._meta.db_table),
            field.related_name)


class ManyToManyRelationFilter(RelationFilter):
    def __init__(self, field: ManyToManyField, opr, value_encoder):
        super().__init__(
            field.forward_key,
            opr,
            value_encoder,
            Table(field.through),
            field.backward_key)
