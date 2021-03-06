
from pypika import Criterion
from pypika.terms import Term

from tortoise.fields.relational import BackwardFKField, ManyToManyField
from tortoise.filters.base import FieldFilter
from tortoise.query.context import QueryContext


class RelationFilter(FieldFilter):
    def __init__(self, field_name: str, opr, value_encoder, backward_key):
        super().__init__(field_name, opr, value_encoder)
        self.backward_key = backward_key

    def __call__(self, context: QueryContext, value) -> Criterion:
        if isinstance(value, Term):
            encoded_value = value

        elif self.value_encoder:
            remote_model = context.stack[-2].model
            encoded_value = self.value_encoder(value, remote_model)

        else:
            encoded_value = value

        table = context.top.table
        encoded_key = table[self.field_name]
        return self.opr(encoded_key, encoded_value)


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
