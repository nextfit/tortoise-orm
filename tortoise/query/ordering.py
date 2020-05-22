
from typing import TypeVar

from pypika import Order
from pypika.terms import Node, Term

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import FieldError, UnknownFieldError, NotARelationFieldError
from tortoise.query.context import QueryContext
from tortoise.query.expressions import F

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

        if self.check_annotations and self.field_name in queryset.annotations:
            annotation = queryset.annotations[self.field_name]
            queryset.query = queryset.query.orderby(annotation.field, order=self.direction)
            return

        field_name, _, field_sub = self.field_name.partition(LOOKUP_SEP)
        field_object = model._meta.fields_map.get(field_name)
        if not field_object:
            raise UnknownFieldError(field_name, model)

        if field_object.has_db_column:
            if field_sub:
                raise NotARelationFieldError(field_name, model)

            field = table[field_object.db_column]
            if not queryset.is_aggregate() or field in queryset.query._groupbys:
                func = field_object.get_for_dialect("function_cast")
                if func:
                    field = func(field_object, field)

                queryset.query = queryset.query.orderby(field, order=self.direction)

        else:
            if not field_sub:
                raise FieldError(
                    "Ordering by relation is not possible. Order by nested field of related model"
                )

            related_table = queryset.join_table_by_field(table, field_object)
            context.push(field_object.remote_model, related_table)
            QueryOrderingField(field_sub, self.direction, False).resolve_into(queryset, context)
            context.pop()

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
