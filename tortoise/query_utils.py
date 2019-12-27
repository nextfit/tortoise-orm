from copy import copy
from typing import Any, Dict, List, Optional, Tuple

from pypika import Table
from pypika.terms import Criterion

from tortoise.context import QueryContext
from tortoise.exceptions import FieldError, OperationalError
from tortoise.fields.relational import BackwardFKRelation, ManyToManyFieldInstance
from tortoise.functions import OuterRef, Subquery


class QueryModifier:
    def __init__(
        self,
        where_criterion: Optional[Criterion] = None,
        joins: Optional[List[Tuple[Criterion, Criterion]]] = None,
        having_criterion: Optional[Criterion] = None,
    ) -> None:
        self.where_criterion: Criterion = where_criterion or EmptyCriterion()
        self.joins = joins if joins else []
        self.having_criterion: Criterion = having_criterion or EmptyCriterion()

    def __and__(self, other: "QueryModifier") -> "QueryModifier":
        return QueryModifier(
            where_criterion=_and(self.where_criterion, other.where_criterion),
            joins=self.joins + other.joins,
            having_criterion=_and(self.having_criterion, other.having_criterion),
        )

    def __or__(self, other: "QueryModifier") -> "QueryModifier":
        if self.having_criterion or other.having_criterion:
            # TODO: This could be optimized?
            result_having_criterion = _or(
                _and(self.where_criterion, self.having_criterion),
                _and(other.where_criterion, other.having_criterion),
            )
            return QueryModifier(
                joins=self.joins + other.joins, having_criterion=result_having_criterion
            )
        if self.where_criterion and other.where_criterion:
            return QueryModifier(
                where_criterion=self.where_criterion | other.where_criterion,
                joins=self.joins + other.joins,
            )
        else:
            return QueryModifier(
                where_criterion=self.where_criterion or other.where_criterion,
                joins=self.joins + other.joins,
            )

    def __invert__(self) -> "QueryModifier":
        if not self.where_criterion and not self.having_criterion:
            return QueryModifier(joins=self.joins)
        if self.having_criterion:
            # TODO: This could be optimized?
            return QueryModifier(
                joins=self.joins,
                having_criterion=_and(self.where_criterion, self.having_criterion).negate(),
            )
        return QueryModifier(where_criterion=self.where_criterion.negate(), joins=self.joins)

    def get_query_modifiers(self) -> Tuple[Criterion, List[Tuple[Table, Criterion]], Criterion]:
        return self.where_criterion, self.joins, self.having_criterion


def _process_filter_kwarg(context: QueryContext, key, value) -> QueryModifier:

    joins = []

    context_item = context.stack[-1]
    model = context_item.model
    table = context_item.table

    if value is None and f"{key}__isnull" in model._meta.filters:
        param = model._meta.get_filter(f"{key}__isnull")
        value = True
    else:
        param = model._meta.get_filter(key)

    pk_db_field = model._meta.db_pk_field
    if param.get("table"):
        joins.append((
            param["table"],
            table[pk_db_field] == getattr(param["table"], param["backward_key"]),
        ))

        if isinstance(value, OuterRef):
            outer_table = context.stack[-2].table
            encoded_value = outer_table[value.ref_name]

        elif param.get("value_encoder"):
            encoded_value = param["value_encoder"](value, model)

        else:
            encoded_value = value

        encoded_key = getattr(param["table"], param["field"])

    else:
        field_object = model._meta.fields_map[param["field"]]

        if isinstance(value, OuterRef):
            outer_table = context.stack[-2].table
            encoded_value = outer_table[value.ref_name]

        elif isinstance(value, Subquery):
            encoded_value = value.get_query(context, "U{}".format(len(context.stack)-1))

        elif param.get("value_encoder"):
            encoded_value = param["value_encoder"](value, model, field_object)

        else:
            encoded_value = model._meta.db.executor_class._field_to_db(field_object, value, model)

        encoded_key = table[param["source_field"]]

    criterion = param["operator"](encoded_key, encoded_value)
    return QueryModifier(where_criterion=criterion, joins=joins)


def _get_joins_for_related_field(
    table, related_field, related_field_name
) -> List[Tuple[Table, Criterion]]:
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


class EmptyCriterion(Criterion):  # type: ignore
    def __or__(self, other):
        return other

    def __and__(self, other):
        return other

    def __bool__(self):
        return False


def _and(left: Criterion, right: Criterion):
    if left and not right:
        return left
    return left & right


def _or(left: Criterion, right: Criterion):
    if left and not right:
        return left
    return left | right



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
        self.filters: Dict[str, Any] = kwargs
        if join_type not in {self.AND, self.OR}:
            raise OperationalError("join_type must be AND or OR")
        self.join_type = join_type
        self._is_negated = False
        self._annotations: Dict[str, Any] = {}
        self._custom_filters: Dict[str, Dict[str, Any]] = {}

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
        annotation = self._annotations[having_info["field"]]
        annotation_info = annotation.resolve(context=context)
        operator = having_info["operator"]

        model = context.stack[-1].model
        overridden_operator = model._meta.db.executor_class.get_overridden_filter_func(
            filter_func=operator
        )
        if overridden_operator:
            operator = overridden_operator
        if annotation_info["field"].is_aggregate:
            modifier = QueryModifier(having_criterion=operator(annotation_info["field"], value))
        else:
            modifier = QueryModifier(where_criterion=operator(annotation_info["field"], value))
        return modifier

    def _resolve_regular_kwarg(self, context: QueryContext, key, value) -> QueryModifier:
        model = context.stack[-1].model
        if key not in model._meta.filters and key.split("__")[0] in model._meta.fetch_fields:
            return self._resolve_nested_filter(context, key, value)
        else:
            return _process_filter_kwarg(context, key, value)

    def _get_actual_filter_params(self, context: QueryContext, key, value) -> Tuple[str, Any]:
        model = context.stack[-1].model
        if key in model._meta.fk_fields or key in model._meta.o2o_fields:
            field_object = model._meta.fields_map[key]
            filter_key = field_object.source_field
            if hasattr(value, "pk"):
                filter_value = value.pk
            else:
                filter_value = value

        elif key in model._meta.m2m_fields:
            filter_key = key

            if hasattr(value, "pk"):
                filter_value = value.pk
            else:
                filter_value = value

        elif (
            key.split("__")[0] in model._meta.fetch_fields
            or key in self._custom_filters
            or key in model._meta.filters
        ):
            filter_key = key
            filter_value = value

        else:
            allowed = sorted(
                list(model._meta.fields | model._meta.fetch_fields | set(self._custom_filters))
            )
            raise FieldError(f"Unknown filter param '{key}'. Allowed base values are {allowed}")

        return filter_key, filter_value

    def _resolve_kwargs(self, context: QueryContext) -> QueryModifier:
        modifier = QueryModifier()
        for raw_key, raw_value in self.filters.items():
            key, value = self._get_actual_filter_params(context, raw_key, raw_value)
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
            raise OperationalError(
                f"relation {first_level_field} for {queryset.model._meta.table} not found"
            )
        forwarded_prefetch = "__".join(relation_split[1:])
        if forwarded_prefetch:
            if first_level_field not in queryset._prefetch_map.keys():
                queryset._prefetch_map[first_level_field] = set()
            queryset._prefetch_map[first_level_field].add(
                Prefetch(forwarded_prefetch, self.queryset)
            )
        else:
            queryset._prefetch_queries[first_level_field] = self.queryset
