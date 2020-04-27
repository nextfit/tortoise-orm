
import operator
from typing import Dict, Tuple, Callable, Union

from tortoise.constants import LOOKUP_SEP
from tortoise.context import QueryContext
from tortoise.exceptions import FieldError, OperationalError
from tortoise.fields.relational import ForeignKey, OneToOneField
from tortoise.filters import FieldFilter
from tortoise.filters.clause import QueryClauses
from tortoise.functions import OuterRef, Subquery


class Q:
    __slots__ = (
        "children",
        "filters",
        "join_type",
        "_is_negated",
        "_check_annotations"
    )

    AND = operator.and_
    OR = operator.or_

    join_type_map = {
        "AND": AND,
        "OR": OR
    }

    def __init__(self, *args: "Q", join_type: Union[str, Callable] = AND, **kwargs) -> None:
        if args and kwargs:
            newarg = Q(join_type=join_type, **kwargs)
            args = (newarg,) + args
            kwargs = {}

        if not all(isinstance(node, Q) for node in args):
            raise OperationalError("All ordered arguments must be Q nodes")

        self.children: Tuple[Q, ...] = args
        self.filters: Dict[str, FieldFilter] = kwargs

        if join_type in self.join_type_map:
            join_type = self.join_type_map[join_type]

        if join_type not in {self.AND, self.OR}:
            raise OperationalError("join_type must be AND or OR")

        self.join_type = join_type
        self._is_negated = False

        self._check_annotations = True

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

    def _get_actual_key(self, queryset: "AwaitableQuery[MODEL]", model: "Model", key: str) -> str:
        if key in model._meta.fields_map:
            field = model._meta.fields_map[key]
            if isinstance(field, (ForeignKey, OneToOneField)):
                return field.id_field_name

            return key

        (field_name, sep, comparision) = key.partition(LOOKUP_SEP)
        if field_name == "pk":
            return f"{model._meta.pk_attr}{sep}{comparision}"

        if field_name in model._meta.fields_map or field_name in queryset.annotations:
            return key

        allowed = sorted(list(model._meta.fields_map.keys() | queryset.annotations.keys()))
        raise FieldError(f"Unknown filter param '{key}'. Allowed base values are {allowed}")

    def _get_actual_value(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext, value):
        if isinstance(value, OuterRef):
            return value.get_field(context, queryset.annotations)

        if isinstance(value, Subquery):
            value.resolve_into(queryset, context, "U{}".format(len(context.stack)))
            return value.field

        if hasattr(value, "pk"):
            return value.pk

        return value

    def _resolve_filter(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext, key, value) -> QueryClauses:
        context_item = context.top
        model = context_item.model
        table = context_item.table

        if value is None and "isnull" in model._meta.db.filter_class.FILTER_FUNC_MAP:
            value = True
            key = f"{key}{LOOKUP_SEP}isnull"

        relation_field_name, _, field_sub = key.partition(LOOKUP_SEP)
        if self._check_annotations and relation_field_name in queryset.annotations:
            (filter_operator, _) = model._meta.db.filter_class.FILTER_FUNC_MAP[field_sub]
            annotation = queryset.annotations[relation_field_name]
            if annotation.field.is_aggregate:
                return QueryClauses(having_criterion=filter_operator(annotation.field, value))
            else:
                return QueryClauses(where_criterion=filter_operator(annotation.field, value))

        key_filter = model._meta.get_filter(key)
        if key_filter:
            if relation_field_name in model._meta.fetch_fields:
                relation_field = model._meta.fields_map[relation_field_name]
                related_table = queryset.join_table_by_field(table, relation_field, full=False)
                if related_table:
                    context.push(relation_field.remote_model, related_table)
                    clauses = QueryClauses(where_criterion=key_filter(context, value))
                    context.pop()
                else:
                    clauses = QueryClauses(where_criterion=key_filter(context, value))

                return clauses

            return QueryClauses(where_criterion=key_filter(context, value))

        if relation_field_name in model._meta.fetch_fields:
            relation_field = model._meta.fields_map[relation_field_name]
            related_table = queryset.join_table_by_field(table, relation_field)
            context.push(relation_field.remote_model, related_table)

            q = Q(**{field_sub: value})
            q._check_annotations = False
            modifier = q._resolve(queryset=queryset, context=context)
            context.pop()

            return modifier

        raise FieldError(f'Unknown field "{key}" for model "{model}"')

    def _resolve_filters(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext) -> QueryClauses:
        clause_collector = QueryClauses()
        model = context.top.model
        for raw_key, raw_value in self.filters.items():
            key = self._get_actual_key(queryset, model, raw_key)
            value = self._get_actual_value(queryset, context, raw_value)
            filter_clause = self._resolve_filter(queryset, context, key, value)
            clause_collector = self.join_type(clause_collector, filter_clause)

        if self._is_negated:
            clause_collector = ~clause_collector
        return clause_collector

    def _resolve_children(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext) -> QueryClauses:
        clause_collector = QueryClauses()
        for node in self.children:
            node_clause = node._resolve(queryset, context)
            clause_collector = self.join_type(clause_collector, node_clause)

        if self._is_negated:
            clause_collector = ~clause_collector
        return clause_collector

    def _resolve(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext) -> QueryClauses:
        if self.filters:
            return self._resolve_filters(queryset, context)
        else:
            return self._resolve_children(queryset, context)

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        clauses = self._resolve(queryset, context)
        queryset.query._wheres = clauses.where_criterion
        queryset.query._havings = clauses.having_criterion
