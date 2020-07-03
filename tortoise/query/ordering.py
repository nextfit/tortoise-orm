
from typing import TypeVar, TYPE_CHECKING

from pypika import Order
from pypika.terms import Node, Term, Negative

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
    def __init__(self, field_name: str, direction: Order):
        self.field_name = field_name
        self.direction = direction

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        # So far as I can imagine, the annotation will be expanded
        # independently, we just refer to it here.
        _, field = context.resolve_field_name(
            self.field_name, queryset, accept_relation=False, expand_annotation=False)

        if not queryset.is_aggregate() or context.query._groupbys:
            context.query = context.query.orderby(field, order=self.direction)

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

            context.query = context.query.orderby(field, order=direction)

        else:
            context.query = context.query.orderby(self.node)
