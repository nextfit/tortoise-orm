
from typing import Any, Dict, Tuple

from tortoise.constants import LOOKUP_SEP
from tortoise.context import QueryContext
from tortoise.exceptions import FieldError, OperationalError
from tortoise.fields.relational import ForeignKey, OneToOneField, ManyToManyField, BackwardFKField
from tortoise.filters import FieldFilter, QueryClauses
from tortoise.functions import OuterRef


class Q:
    __slots__ = (
        "children",
        "filters",
        "join_type",
        "_is_negated",
        "_annotations",
    )

    AND = "AND"
    OR = "OR"

    def __init__(self, *args: "Q", join_type=AND, **kwargs) -> None:
        if args and kwargs:
            newarg = Q(join_type=join_type, **kwargs)
            args = (newarg,) + args
            kwargs = {}

        if not all(isinstance(node, Q) for node in args):
            raise OperationalError("All ordered arguments must be Q nodes")

        self.children: Tuple[Q, ...] = args
        self.filters: Dict[str, FieldFilter] = kwargs

        if join_type not in {self.AND, self.OR}:
            raise OperationalError("join_type must be AND or OR")

        self.join_type = join_type
        self._is_negated = False
        self._annotations: Dict[str, Any] = {}

    def __and__(self, other) -> "Q":
        if not isinstance(other, Q):
            raise OperationalError("AND operation requires a Q node")
        return Q(self, other, join_type=self.AND)

    def __or__(self, other) -> "Q":
        if not isinstance(other, Q):
            raise OperationalError("OR operation requires a Q node")
        return Q(self, other, join_type=self.OR)

    def __invert__(self) -> "Q":
        q = Q(*self.children, join_type=self.join_type, **self.filters)
        q.negate()
        return q

    def negate(self) -> None:
        self._is_negated = not self._is_negated

    def _get_actual_key(self, model: "Model", key: str) -> str:
        if key in model._meta.fields_map:
            field = model._meta.fields_map[key]
            if isinstance(field, (ForeignKey, OneToOneField)):
                return field.id_field_name

            return key

        (field_name, sep, comparision) = key.partition(LOOKUP_SEP)
        if field_name == "pk":
            return f"{model._meta.pk_attr}{sep}{comparision}"

        if field_name in model._meta.fields_map or field_name in self._annotations:
            return key

        allowed = sorted(list(model._meta.fields_map.keys() | self._annotations.keys()))
        raise FieldError(f"Unknown filter param '{key}'. Allowed base values are {allowed}")

    def _get_actual_value(self, context: QueryContext, value):
        if isinstance(value, OuterRef):
            return value.get_field(context, self._annotations)

        if hasattr(value, "pk"):
            return value.pk

        return value

    def _resolve_filter(self, context: QueryContext, key, value) -> QueryClauses:
        context_item = context.top
        model = context_item.model
        table = context_item.table

        if value is None and "isnull" in model._meta.db.filter_class.FILTER_FUNC_MAP:
            value = True
            key = f"{key}{LOOKUP_SEP}isnull"

        relation_field_name, _, field_sub = key.partition(LOOKUP_SEP)
        if relation_field_name in self._annotations:
            (filter_operator, _) = model._meta.db.filter_class.FILTER_FUNC_MAP[field_sub]
            annotation = self._annotations[relation_field_name]
            if annotation.field.is_aggregate:
                return QueryClauses(having_criterion=filter_operator(annotation.field, value))
            else:
                return QueryClauses(where_criterion=filter_operator(annotation.field, value))

        key_filter = model._meta.get_filter(key)
        if key_filter:
            if relation_field_name in model._meta.fetch_fields:
                relation_field = model._meta.fields_map[relation_field_name]
                if isinstance(relation_field, (BackwardFKField, ManyToManyField)):
                    required_joins = relation_field.get_joins(table)[:1]
                    related_table = required_joins[-1][0]
                    context.push(relation_field.remote_model, related_table)
                    clauses = QueryClauses(
                        where_criterion=key_filter(context, value),
                        joins=required_joins)

                    context.pop()
                    return clauses

            return QueryClauses(where_criterion=key_filter(context, value))

        if relation_field_name in model._meta.fetch_fields:
            relation_field = model._meta.fields_map[relation_field_name]
            required_joins = relation_field.get_joins(table)

            related_table = required_joins[-1][0]
            context.push(relation_field.remote_model, related_table)
            modifier = Q(**{field_sub: value}).resolve(context=context, annotations={})
            context.pop()

            return QueryClauses(joins=required_joins) & modifier

        raise FieldError(f'Unknown field "{key}" for model "{model}"')

    def _resolve_filters(self, context: QueryContext) -> QueryClauses:
        modifier = QueryClauses()
        model = context.top.model
        for raw_key, raw_value in self.filters.items():
            key = self._get_actual_key(model, raw_key)
            value = self._get_actual_value(context, raw_value)
            filter_modifier = self._resolve_filter(context, key, value)

            if self.join_type == self.AND:
                modifier &= filter_modifier
            else:
                modifier |= filter_modifier

        if self._is_negated:
            modifier = ~modifier

        return modifier

    def _resolve_children(self, context: QueryContext) -> QueryClauses:
        modifier = QueryClauses()
        for node in self.children:
            node_modifier = node.resolve(context, self._annotations)
            if self.join_type == self.AND:
                modifier &= node_modifier
            else:
                modifier |= node_modifier

        if self._is_negated:
            modifier = ~modifier
        return modifier

    def resolve(self, context: QueryContext, annotations) -> QueryClauses:
        self._annotations = annotations
        if self.filters:
            return self._resolve_filters(context)

        return self._resolve_children(context)

