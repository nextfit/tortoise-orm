
from typing import TypeVar
from pypika import Order
from pypika.terms import Node, Term

from tortoise.constants import LOOKUP_SEP
from tortoise.context import QueryContext
from tortoise.exceptions import FieldError
from tortoise.expressions import F

MODEL = TypeVar("MODEL", bound="Model")


class QueryOrdering:
    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        raise NotImplementedError()


class QueryOrderingField(QueryOrdering):
    def __init__(self, field_name: str, direction: Order, check_annotations=True):
        self.field_name = field_name
        self.direction = direction
        self.check_annotations = check_annotations

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        table = context.top.table
        model = context.top.model

        relation_field_name, _, field_sub = self.field_name.partition(LOOKUP_SEP)
        if self.check_annotations and self.field_name in queryset.annotations:
            annotation = queryset.annotations[self.field_name]
            queryset.query = queryset.query.orderby(annotation.field, order=self.direction)

        elif relation_field_name in model._meta.fetch_fields:
            if not field_sub:
                raise FieldError(
                    "Filtering by relation is not possible. Filter by nested field of related model"
                )

            relation_field = model._meta.fields_map[relation_field_name]
            related_table = queryset._join_table_by_field(table, relation_field)

            context.push(relation_field.remote_model, related_table)
            QueryOrderingField(field_sub, self.direction, False).resolve_into(queryset, context)
            context.pop()

        else:
            if field_sub:
                raise FieldError(f"{relation_field_name} is not a relation for model {model.__name__}")

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


class OrderingNode(Node):
    alias = None

    def __str__(self):
        return self.get_sql(quote_char='"', secondary_quote_char="'")

    def __hash__(self):
        return hash(self.get_sql(with_alias=True))

    def get_sql(self, **kwargs):
        raise NotImplementedError()


class RandomOrdering(OrderingNode):
    def get_sql(self, **kwargs):
        return "RANDOM()"


class QueryOrderingNode(QueryOrdering):
    def __init__(self, node: Node):
        self.node = node

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        if isinstance(self.node, Term):
            self.node = F.resolve(self.node, context)

        queryset.query = queryset.query.orderby(self.node)
