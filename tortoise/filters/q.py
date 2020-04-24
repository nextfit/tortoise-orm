
from typing import Any, Dict, Tuple

from tortoise.constants import LOOKUP_SEP
from tortoise.context import QueryContext
from tortoise.exceptions import FieldError, OperationalError
from tortoise.fields.relational import ForeignKey, OneToOneField
from tortoise.filters import FieldFilter, QueryModifier
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

    def _resolve_nested_filter(self, context: QueryContext, key, value) -> QueryModifier:
        context_item = context.top

        model = context_item.model
        table = context_item.table

        relation_field_name, _, relation_field_sub = key.partition(LOOKUP_SEP)
        relation_field = model._meta.fields_map[relation_field_name]
        required_joins = relation_field.get_joins(table)

        related_table = required_joins[-1][0]
        context.push(relation_field.remote_model, related_table)
        modifier = Q(**{relation_field_sub: value}).resolve(
            context=context, annotations=self._annotations)
        context.pop()

        return QueryModifier(joins=required_joins) & modifier

    def _resolve_field_filters(self, context: QueryContext, key, value) -> QueryModifier:
        model = context.top.model

        if value is None and "isnull" in model._meta.db.filter_class.FILTER_FUNC_MAP:
            value = True
            key = f"{key}{LOOKUP_SEP}isnull"

        key_filter = model._meta.get_filter(key)
        if key_filter:
            return key_filter(context, value)

        elif key.split(LOOKUP_SEP)[0] in model._meta.fetch_fields:
            return self._resolve_nested_filter(context, key, value)

        raise FieldError(f'Unknown field "{key}" for model "{model}"')

    def _resolve_annotation_filters(self, context: QueryContext, key, value) -> QueryModifier:
        model = context.top.model
        (field_name, sep, comparision) = key.partition(LOOKUP_SEP)
        (filter_operator, _) = model._meta.db.filter_class.FILTER_FUNC_MAP[comparision]

        annotation = self._annotations[field_name]
        annotation_info = annotation.resolve(context=context)

        if annotation_info.field.is_aggregate:
            return QueryModifier(having_criterion=filter_operator(annotation_info.field, value))
        else:
            return QueryModifier(where_criterion=filter_operator(annotation_info.field, value))

    def _get_actual_key(self, model: "Model", key: str) -> str:
        field_name = key
        if field_name in model._meta.fields_map:
            field = model._meta.fields_map[field_name]
            if isinstance(field, (ForeignKey, OneToOneField)):
                return field.id_field_name()

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
            return OuterRef(self._get_actual_key(context.stack[-2].model, value.ref_name))

        if hasattr(value, "pk"):
            return value.pk

        return value

    def _resolve_filters(self, context: QueryContext) -> QueryModifier:
        modifier = QueryModifier()
        for raw_key, raw_value in self.filters.items():
            key = self._get_actual_key(context.top.model, raw_key)
            value = self._get_actual_value(context, raw_value)

            if key.split(LOOKUP_SEP)[0] in self._annotations:
                filter_modifier = self._resolve_annotation_filters(context, key, value)
            else:
                filter_modifier = self._resolve_field_filters(context, key, value)

            if self.join_type == self.AND:
                modifier &= filter_modifier
            else:
                modifier |= filter_modifier

        if self._is_negated:
            modifier = ~modifier

        return modifier

    def _resolve_children(self, context: QueryContext) -> QueryModifier:
        modifier = QueryModifier()
        for node in self.children:
            node_modifier = node.resolve(context, self._annotations)
            if self.join_type == self.AND:
                modifier &= node_modifier
            else:
                modifier |= node_modifier

        if self._is_negated:
            modifier = ~modifier
        return modifier

    def resolve(self, context: QueryContext, annotations) -> QueryModifier:
        self._annotations = annotations
        if self.filters:
            return self._resolve_filters(context)

        return self._resolve_children(context)

