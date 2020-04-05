
from typing import Optional, List, Tuple

from pypika import Table, Criterion

from tortoise.context import QueryContext
from tortoise.fields import Field
from tortoise.fields.relational import BackwardFKRelation, ManyToManyField
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
        joins: Optional[List[Tuple[Table, Criterion]]] = None,
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
    def __init__(self, field_name: str, opr, value_encoder):
        self.field_name = field_name
        self.opr = opr
        self.value_encoder = value_encoder

    def __call__(self, context: QueryContext, value) -> QueryModifier:
        raise NotImplementedError()


class BaseFieldFilter(FieldFilter):
    def __init__(self, field: Field, db_column: str, opr, value_encoder=None):
        super().__init__(field.model_field_name, opr, value_encoder)
        self.db_column = db_column

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

        encoded_key = table[self.db_column]
        criterion = self.opr(encoded_key, encoded_value)
        return QueryModifier(where_criterion=criterion, joins=joins)


class RelationFilter(FieldFilter):
    def __init__(self, field_name: str, opr, value_encoder, table, backward_key):
        super().__init__(field_name, opr, value_encoder)

        self.table = table
        self.backward_key = backward_key

    def __call__(self, context: QueryContext, value) -> QueryModifier:
        context_item = context.stack[-1]
        model = context_item.model
        table = context_item.table

        pk_db_column = model._meta.pk_db_column
        joins = [(
            self.table,
            table[pk_db_column] == getattr(self.table, self.backward_key),
        )]

        if isinstance(value, OuterRef):
            outer_context_item = context.stack[-2]
            outer_model = outer_context_item.model
            outer_table = outer_context_item.table

            outer_field = outer_model._meta.fields_map[value.ref_name]

            if isinstance(outer_field, ManyToManyField):
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
    def __init__(self, field: BackwardFKRelation, opr, value_encoder):
        super().__init__(
            field.remote_model._meta.pk.model_field_name,
            opr,
            value_encoder,
            Table(field.remote_model._meta.db_table),
            field.related_name)


class ManyToManyRelationFilter(RelationFilter):
    def __init__(self, field: ManyToManyField, opr, value_encoder):
        super().__init__(field.forward_key, opr, value_encoder,
            Table(field.through), field.backward_key)
