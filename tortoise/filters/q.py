
import operator
from typing import Callable, Dict, Tuple, Union, TYPE_CHECKING, Type

from pypika.terms import Term

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import FieldError, OperationalError, UnknownFieldError, BaseFieldError
from tortoise.fields.relational import ForeignKey, OneToOneField, RelationField
from tortoise.filters import FieldFilter
from tortoise.filters.clause import QueryClauses
from tortoise.query.annotations import OuterRef, Subquery, Annotation, TermAnnotation
from tortoise.query.context import QueryContext


if TYPE_CHECKING:
    from tortoise.models import Model, MODEL
    from tortoise.query.base import AwaitableStatement


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

        self.join_type: Callable = join_type
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

    def _get_actual_key(self, queryset: "AwaitableStatement[MODEL]", model: Type["Model"], key: str) -> str:
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

    def _get_actual_value(self, queryset: "AwaitableStatement[MODEL]", context: QueryContext, value):
        if isinstance(value, OuterRef):
            return value.get_field(context, queryset.annotations)

        if isinstance(value, Term):
            value = TermAnnotation(value)
            value.resolve_into(queryset, context)
            return value.field

        if isinstance(value, Annotation):
            value.resolve_into(queryset, context)
            return value.field

        if hasattr(value, "pk"):
            return value.pk

        return value

    def _resolve_filter(self, queryset: "AwaitableStatement[MODEL]", context: QueryContext, key, value) -> QueryClauses:
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

        field_object = model._meta.fields_map.get(relation_field_name)
        if not field_object:
            raise UnknownFieldError(relation_field_name, model)

        key_filter = model._meta.get_filter(key)
        if key_filter:
            if isinstance(field_object, RelationField):
                join_data = context.join_table_by_field(table, field_object, full=False)
                if join_data:
                    #
                    # We are potentially adding two None here into the context
                    # however, since we have a valid key_filter it means no sub_field
                    # and hence to inner tables and no context is necessary, except
                    # for the correct position of the stack levels,
                    #
                    context.push(join_data.model, join_data.table)
                    clauses = QueryClauses(where_criterion=key_filter(context, value))
                    context.pop()
                else:
                    clauses = QueryClauses(where_criterion=key_filter(context, value))

                return clauses

            else:
                return QueryClauses(where_criterion=key_filter(context, value))

        if isinstance(field_object, RelationField):
            join_data = context.join_table_by_field(table, field_object)
            context.push(join_data.model, join_data.table)

            q = Q(**{field_sub: value})
            q._check_annotations = False
            modifier = q._resolve(queryset=queryset, context=context)
            context.pop()

            return modifier

        raise BaseFieldError(key, model)

    def _resolve(self, queryset: "AwaitableStatement[MODEL]", context: QueryContext) -> QueryClauses:
        clause_collector = QueryClauses()
        model = context.top.model

        #
        # In reality only one of children or filters is non-empty
        #
        for node in self.children:
            clause = node._resolve(queryset, context)
            clause_collector = self.join_type(clause_collector, clause)

        for raw_key, raw_value in self.filters.items():
            key = self._get_actual_key(queryset, model, raw_key)
            value = self._get_actual_value(queryset, context, raw_value)
            clause = self._resolve_filter(queryset, context, key, value)
            clause_collector = self.join_type(clause_collector, clause)

        if self._is_negated:
            clause_collector = ~clause_collector
        return clause_collector

    def resolve_into(self, queryset: "AwaitableStatement[MODEL]", context: QueryContext):
        clauses = self._resolve(queryset, context)
        context.query._wheres = clauses.where_criterion
        context.query._havings = clauses.having_criterion

        if not context.query._validate_table(clauses.where_criterion):
            context.query._foreign_table = True
