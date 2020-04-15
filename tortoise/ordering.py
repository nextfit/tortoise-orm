
from typing import TypeVar
from pypika import Order
from pypika.terms import Node

from tortoise.constants import LOOKUP_SEP
from tortoise.context import QueryContext
from tortoise.exceptions import FieldError

MODEL = TypeVar("MODEL", bound="Model")


class QueryOrdering:
    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        raise NotImplementedError()


class QueryOrderingField(QueryOrdering):
    def __init__(self, field_name: str, direction: Order):
        self.field_name = field_name
        self.direction = direction

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        table = context.top.table
        model = context.top.model

        if self.field_name in model._meta.fetch_fields:
            raise FieldError(
                "Filtering by relation is not possible. Filter by nested field of related model"
            )

        relation_field_name, _, field_sub = self.field_name.partition(LOOKUP_SEP)
        if relation_field_name in model._meta.fetch_fields:
            relation_field = model._meta.fields_map[relation_field_name]
            related_table = queryset._join_table_by_field(table, relation_field)
            context.push(relation_field.remote_model, related_table)
            QueryOrderingField(field_sub, self.direction).resolve_into(queryset, context)
            context.pop()

        elif self.field_name in queryset.annotations:
            annotation = queryset.annotations[self.field_name]
            annotation_info = annotation.resolve(QueryContext().push(queryset.model, queryset.model._meta.basetable))
            queryset.query = queryset.query.orderby(annotation_info.field, order=self.direction)

        else:
            field_object = model._meta.fields_map.get(self.field_name)
            if not field_object:
                raise FieldError(f"Unknown field {self.field_name} for model {model.__name__}")

            field = table[field_object.db_column]
            func = field_object.get_for_dialect(model._meta.db.capabilities.dialect, "function_cast")
            if func:
                field = func(field_object, field)

            queryset.query = queryset.query.orderby(field, order=self.direction)


#
# PyPika Nodes to allow custom ordering methods like RANDOM() for PostgreSQL
#


class OrderingMethod(Node):
    alias = None

    def __str__(self):
        return self.get_sql(quote_char='"', secondary_quote_char="'")

    def __hash__(self):
        return hash(self.get_sql(with_alias=True))

    def get_sql(self, **kwargs):
        raise NotImplementedError()


class RandomOrderingMethod(OrderingMethod):
    def get_sql(self, **kwargs):
        return "RANDOM()"


class QueryOrderingMethod(QueryOrdering):
    def __init__(self, method: OrderingMethod):
        self.method = method

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        queryset.query = queryset.query.orderby(self.method)


class RandomOrdering(QueryOrderingMethod):
    random_method = RandomOrderingMethod()

    def __init__(self):
        super().__init__(method=self.random_method)
