
from typing import TypeVar, TYPE_CHECKING

from pypika import Order
from pypika.terms import Node, Term, Negative, Field

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import FieldError, UnknownFieldError, NotARelationFieldError
from tortoise.fields import RelationField
from tortoise.query.annotations import TermAnnotation
from tortoise.query.context import QueryContext

if TYPE_CHECKING:
    from tortoise.query.base import AwaitableQuery
    from tortoise.models import Model


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
            #
            # We need to make sure the annotation will show up
            # in the final query, since we are referring to it here,
            # otherwise, these two lines commented below, can create the
            # whole annotation inside our ordering clause, which
            # in all imaginable cases is NOT the desired behavior
            #
            # annotation = queryset.annotations[self.field_name]
            # queryset.query = queryset.query.orderby(annotation.field, order=self.direction)
            #

            queryset.query = queryset.query.orderby(Field(self.field_name), order=self.direction)
            return

        field_name, _, field_sub = self.field_name.partition(LOOKUP_SEP)
        field_object = model._meta.fields_map.get(field_name)
        if not field_object:
            raise UnknownFieldError(field_name, model)

        if isinstance(field_object, RelationField):
            if not field_sub:
                raise FieldError(
                    "Ordering by relation is not possible. Order by nested field of related model"
                )

            join_data = queryset.join_table_by_field(table, field_object)
            context.push(join_data.model, join_data.table)
            QueryOrderingField(field_sub, self.direction, False).resolve_into(queryset, context)
            context.pop()

        else:
            if field_sub:
                raise NotARelationFieldError(field_name, model)

            field = table[field_object.db_column]
            if not queryset.is_aggregate() or field in queryset.query._groupbys:
                func = field_object.get_for_dialect("function_cast")
                if func:
                    field = func(field)

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

    def get_sql(self, **kwargs) -> str:
        raise NotImplementedError()


class RandomOrdering(OrderingNode):
    def get_sql(self, **kwargs):
        return "RANDOM()"


class QueryOrderingNode(QueryOrdering):
    def __init__(self, node: Node):
        self.node = node

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        if isinstance(self.node, Term):
            term_annotation = TermAnnotation(self.node)
            term_annotation.resolve_into(queryset, context)

            field = term_annotation.field
            direction = Order.asc
            if isinstance(field, Negative):
                field = field.term
                direction = Order.desc

            queryset.query = queryset.query.orderby(field, order=direction)

        else:
            queryset.query = queryset.query.orderby(self.node)
