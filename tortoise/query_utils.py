from copy import copy
from typing import Any, Dict, List, Tuple

from pypika import Table
from pypika.terms import Criterion

from tortoise.context import QueryContext
from tortoise.exceptions import FieldError, OperationalError
from tortoise.fields.relational import BackwardFKRelation, ManyToManyFieldInstance
from tortoise.filters import FieldFilter, QueryModifier
from tortoise.functions import OuterRef


def _process_filter_kwarg(context: QueryContext, key, value) -> QueryModifier:
    context_item = context.stack[-1]
    model = context_item.model

    if value is None and f"{key}__isnull" in model._meta.filters:
        return model._meta.get_filter(f"{key}__isnull")(context, True)
    else:
        return model._meta.get_filter(key)(context, value)


def _get_joins_for_related_field(table, related_field, related_field_name) -> List[Tuple[Table, Criterion]]:
    required_joins = []

    table_pk = related_field.model._meta.db_pk_field
    related_table_pk = related_field.model_class._meta.db_pk_field

    if isinstance(related_field, ManyToManyFieldInstance):
        related_table = related_field.model_class._meta.basetable
        through_table = Table(related_field.through)
        required_joins.append(
            (
                through_table,
                getattr(table, table_pk) == getattr(through_table, related_field.backward_key),
            )
        )
        required_joins.append(
            (
                related_table,
                getattr(through_table, related_field.forward_key)
                == getattr(related_table, related_table_pk),
            )
        )

    elif isinstance(related_field, BackwardFKRelation):
        related_table = related_field.model_class._meta.basetable
        required_joins.append(
            (
                related_table,
                getattr(table, table_pk) == getattr(related_table, related_field.relation_field),
            )
        )

    else:
        related_table = related_field.model_class._meta.basetable
        required_joins.append(
            (
                related_table,
                getattr(related_table, related_table_pk)
                == getattr(table, f"{related_field_name}_id"),
            )
        )

    return required_joins


class Q:
    __slots__ = (
        "children",
        "filters",
        "join_type",
        "_is_negated",
        "_annotations",
        "_custom_filters",
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
        self._custom_filters: Dict[str, FieldFilter] = {}

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
        context_item = context.stack[-1]

        model = context_item.model
        table = context_item.table

        related_field_name = key.split("__")[0]
        related_field = model._meta.fields_map[related_field_name]
        related_table = related_field.model_class._meta.basetable

        required_joins = _get_joins_for_related_field(table, related_field, related_field_name)

        context.push(related_field.model_class, related_table)
        modifier = Q(**{"__".join(key.split("__")[1:]): value}).resolve(
            context=context,
            annotations=self._annotations,
            custom_filters=self._custom_filters,
        )
        context.pop()

        return QueryModifier(joins=required_joins) & modifier

    def _resolve_custom_kwarg(self, context: QueryContext, key, value) -> QueryModifier:
        having_info = self._custom_filters[key]
        annotation = self._annotations[having_info.field_name]
        annotation_info = annotation.resolve(context=context)
        operator = having_info.opr

        model = context.stack[-1].model
        overridden_operator = model._meta.db.executor_class.get_overridden_filter_func(filter_func=operator)

        if overridden_operator:
            operator = overridden_operator

        if annotation_info.field.is_aggregate:
            modifier = QueryModifier(having_criterion=operator(annotation_info.field, value))
        else:
            modifier = QueryModifier(where_criterion=operator(annotation_info.field, value))

        return modifier

    def _resolve_regular_kwarg(self, context: QueryContext, key, value) -> QueryModifier:
        model = context.stack[-1].model
        if key not in model._meta.filters and key.split("__")[0] in model._meta.fetch_fields:
            return self._resolve_nested_filter(context, key, value)
        else:
            return _process_filter_kwarg(context, key, value)

    def _get_actual_key(self, model: "Model", key: str) -> str:
        if key in model._meta.fk_fields or key in model._meta.o2o_fields:
            return model._meta.fields_map[key].source_field

        elif key in model._meta.m2m_fields:
            return key

        elif (
            key.split("__")[0] in model._meta.fetch_fields
            or key in self._custom_filters
            or key in model._meta.filters
        ):
            return key

        else:
            allowed = sorted(
                list(model._meta.fields | model._meta.fetch_fields | set(self._custom_filters))
            )
            raise FieldError(f"Unknown filter param '{key}'. Allowed base values are {allowed}")

    def _get_actual_value(self, context: QueryContext, value):
        if isinstance(value, OuterRef):
            return OuterRef(self._get_actual_key(context.stack[-2].model, value.ref_name))

        elif hasattr(value, "pk"):
            return value.pk

        else:
            return value

    def _resolve_kwargs(self, context: QueryContext) -> QueryModifier:
        modifier = QueryModifier()
        for raw_key, raw_value in self.filters.items():
            key = self._get_actual_key(context.stack[-1].model, raw_key)
            value = self._get_actual_value(context, raw_value)

            if key in self._custom_filters:
                filter_modifier = self._resolve_custom_kwarg(context, key, value)
            else:
                filter_modifier = self._resolve_regular_kwarg(context, key, value)

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
            node_modifier = node.resolve(context, self._annotations, self._custom_filters)
            if self.join_type == self.AND:
                modifier &= node_modifier
            else:
                modifier |= node_modifier

        if self._is_negated:
            modifier = ~modifier
        return modifier

    def resolve(self, context: QueryContext, annotations, custom_filters) -> QueryModifier:
        self._annotations = annotations
        self._custom_filters = custom_filters
        if self.filters:
            return self._resolve_kwargs(context)
        return self._resolve_children(context)


class Prefetch:
    __slots__ = ("relation", "queryset")

    def __init__(self, relation, queryset) -> None:
        self.relation = relation
        self.queryset = queryset
        self.queryset.query = copy(self.queryset.model._meta.basequery)

    def resolve_for_queryset(self, queryset) -> None:
        relation_split = self.relation.split("__")
        first_level_field = relation_split[0]
        if first_level_field not in queryset.model._meta.fetch_fields:
            if first_level_field in queryset.model._meta.fields:
                msg = f"Field {first_level_field} on {queryset.model._meta.table} is not a relation"
            else:
                msg = f"Relation {first_level_field} for {queryset.model._meta.table} not found"

            raise FieldError(msg)

        forwarded_prefetch = "__".join(relation_split[1:])
        if forwarded_prefetch:
            if first_level_field not in queryset._prefetch_map.keys():
                queryset._prefetch_map[first_level_field] = set()

            queryset._prefetch_map[first_level_field].add(
                Prefetch(forwarded_prefetch, self.queryset)
            )

        else:
            queryset._prefetch_queries[first_level_field] = self.queryset
