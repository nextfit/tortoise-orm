
import operator
from functools import partial
from typing import Dict, Optional, List, Tuple

from pypika import Table, functions, Criterion
from pypika.enums import SqlTypes

from tortoise.context import QueryContext
from tortoise.fields import Field
from tortoise.fields.relational import BackwardFKRelation, ManyToManyFieldInstance
from tortoise.functions import OuterRef, Subquery


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
                joins=self.joins + other.joins,
                having_criterion=result_having_criterion
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


class FieldFilter:
    def __init__(self, field_name: str, field: Optional[Field], opr, value_encoder):
        self.field_name = field_name
        self.field = field

        self.opr = opr
        self.value_encoder = value_encoder

    def __call__(self, context: QueryContext, value) -> QueryModifier:
        raise NotImplementedError()


class BaseFieldFilter(FieldFilter):
    def __init__(self, field_name: str, field: Optional[Field], source_field: str, opr, value_encoder=None):
        super().__init__(
            field.model_field_name if field_name == "pk" and field else field_name,
            field,
            opr,
            value_encoder
        )

        self.source_field = source_field

    def __call__(self, context: QueryContext, value) -> QueryModifier:
        context_item = context.stack[-1]
        model = context_item.model
        table = context_item.table

        field_object = model._meta.fields_map[self.field_name]

        joins = []

        if isinstance(value, OuterRef):
            outer_table = context.stack[-2].table
            encoded_value = outer_table[value.ref_name]

        elif isinstance(value, Subquery):
            annotation_info = value.resolve(context, "U{}".format(len(context.stack)))
            encoded_value = annotation_info.field
            joins.extend(annotation_info.joins)

        elif self.value_encoder:
            encoded_value = self.value_encoder(value, model, field_object)

        else:
            encoded_value = model._meta.db.executor_class._field_to_db(field_object, value, model)

        encoded_key = table[self.source_field]
        criterion = self.opr(encoded_key, encoded_value)
        return QueryModifier(where_criterion=criterion, joins=joins)


class RelationFilter(FieldFilter):
    def __init__(self, field_name: str, field: Optional[Field], opr, value_encoder, table, backward_key):
        super().__init__(field_name, field, opr, value_encoder)

        self.table = table
        self.backward_key = backward_key

    def __call__(self, context: QueryContext, value) -> QueryModifier:
        context_item = context.stack[-1]
        model = context_item.model
        table = context_item.table

        pk_db_field = model._meta.db_pk_field
        joins = [(
            self.table,
            table[pk_db_field] == getattr(self.table, self.backward_key),
        )]

        if isinstance(value, OuterRef):
            outer_context_item = context.stack[-2]
            outer_model = outer_context_item.model
            outer_table = outer_context_item.table

            outer_field = outer_model._meta.fields_map[value.ref_name]

            if isinstance(outer_field, ManyToManyFieldInstance):
                if outer_field.through in outer_context_item.through_tables:
                    outer_through_table = outer_context_item.through_tables[outer_field.through]
                    encoded_value = outer_through_table[outer_field.forward_key]

                else:
                    raise NotImplementedError()

            elif isinstance(outer_field, BackwardFKRelation):
                raise NotImplementedError()

            else:
                encoded_value = outer_table[value.ref_name]

        elif self.value_encoder:
            encoded_value = self.value_encoder(value, model)

        else:
            encoded_value = value

        encoded_key = self.table[self.field_name]
        criterion = self.opr(encoded_key, encoded_value)
        return QueryModifier(where_criterion=criterion, joins=joins)


class BackwardFKFilter(RelationFilter):
    def __init__(self, field: Optional[Field], opr, value_encoder):
        super().__init__(field.model_class._meta.pk_attr, field, opr, value_encoder,
            Table(field.model_class._meta.table), field.relation_field)


class ManyToManyRelationFilter(RelationFilter):
    def __init__(self, field: Optional[Field], opr, value_encoder):
        super().__init__(field.forward_key, field, opr, value_encoder,
            Table(field.through), field.backward_key)


def list_encoder(values, instance, field: Field):
    """Encodes an iterable of a given field into a database-compatible format."""
    return [field.to_db_value(element, instance) for element in values]


def related_list_encoder(values, instance, field: Field):
    return [field.to_db_value(getattr(element, "pk", element), instance) for element in values]


def bool_encoder(value, *args):
    return bool(value)


def string_encoder(value, *args):
    return str(value)


def is_in(field, value):
    return field.isin(value)


def not_in(field, value):
    return field.notin(value) | field.isnull()


def not_equal(field, value):
    return field.ne(value) | field.isnull()


def is_null(field, value):
    if value:
        return field.isnull()
    return field.notnull()


def not_null(field, value):
    if value:
        return field.notnull()
    return field.isnull()


def contains(field, value):
    return functions.Cast(field, SqlTypes.VARCHAR).like(f"%{value}%")


def starts_with(field, value):
    return functions.Cast(field, SqlTypes.VARCHAR).like(f"{value}%")


def ends_with(field, value):
    return functions.Cast(field, SqlTypes.VARCHAR).like(f"%{value}")


def insensitive_exact(field, value):
    return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).eq(functions.Upper(f"{value}"))


def insensitive_contains(field, value):
    return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).like(
        functions.Upper(f"%{value}%")
    )


def insensitive_starts_with(field, value):
    return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).like(
        functions.Upper(f"{value}%")
    )


def insensitive_ends_with(field, value):
    return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).like(
        functions.Upper(f"%{value}")
    )


def get_m2m_filters(field_name: str, field: ManyToManyFieldInstance) -> Dict[str, FieldFilter]:
    target_table_pk = field.model_class._meta.pk
    return {
        field_name: ManyToManyRelationFilter(field, operator.eq, target_table_pk.to_db_value),
        f"{field_name}__not": ManyToManyRelationFilter(field, not_equal, target_table_pk.to_db_value),
        f"{field_name}__in": ManyToManyRelationFilter(field, is_in,
            partial(related_list_encoder, field=target_table_pk)),

        f"{field_name}__not_in": ManyToManyRelationFilter(field, not_in,
            partial(related_list_encoder, field=target_table_pk)),
    }


def get_backward_fk_filters(field_name: str, field: BackwardFKRelation) -> Dict[str, FieldFilter]:
    target_table_pk = field.model_class._meta.pk
    return {
        field_name: BackwardFKFilter(field, operator.eq, target_table_pk.to_db_value),
        f"{field_name}__not": BackwardFKFilter(field, not_equal, target_table_pk.to_db_value),
        f"{field_name}__in": BackwardFKFilter(field, is_in, partial(related_list_encoder, field=target_table_pk)),
        f"{field_name}__not_in": BackwardFKFilter(field, not_in, partial(related_list_encoder, field=target_table_pk)),
    }


def get_filters_for_field(field_name: str, field: Optional[Field], source_field: str) -> Dict[str, FieldFilter]:

    if isinstance(field, ManyToManyFieldInstance):
        return get_m2m_filters(field_name, field)
    if isinstance(field, BackwardFKRelation):
        return get_backward_fk_filters(field_name, field)

    return {
        field_name: BaseFieldFilter(field_name, field, source_field, operator.eq, None),
        f"{field_name}__not": BaseFieldFilter(field_name, field, source_field, not_equal, None),
        f"{field_name}__in": BaseFieldFilter(field_name, field, source_field, is_in, list_encoder),
        f"{field_name}__not_in": BaseFieldFilter(field_name, field, source_field, not_in, list_encoder),
        f"{field_name}__isnull": BaseFieldFilter(field_name, field, source_field, is_null, bool_encoder),
        f"{field_name}__not_isnull": BaseFieldFilter(field_name, field, source_field, not_null, bool_encoder),
        f"{field_name}__gte": BaseFieldFilter(field_name, field, source_field, operator.ge, None),
        f"{field_name}__lte": BaseFieldFilter(field_name, field, source_field, operator.le, None),
        f"{field_name}__gt": BaseFieldFilter(field_name, field, source_field, operator.gt, None),
        f"{field_name}__lt": BaseFieldFilter(field_name, field, source_field, operator.lt, None),
        f"{field_name}__contains": BaseFieldFilter(field_name, field, source_field, contains, string_encoder),
        f"{field_name}__startswith": BaseFieldFilter(field_name, field, source_field, starts_with, string_encoder),
        f"{field_name}__endswith": BaseFieldFilter(field_name, field, source_field, ends_with, string_encoder),
        f"{field_name}__iexact": BaseFieldFilter(field_name, field, source_field, insensitive_exact, string_encoder),
        f"{field_name}__icontains": BaseFieldFilter(field_name, field, source_field,
            insensitive_contains, string_encoder),

        f"{field_name}__istartswith": BaseFieldFilter(field_name, field, source_field,
            insensitive_starts_with, string_encoder),

        f"{field_name}__iendswith": BaseFieldFilter(field_name, field, source_field,
            insensitive_ends_with, string_encoder),
    }
