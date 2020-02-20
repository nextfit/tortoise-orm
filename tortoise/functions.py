from dataclasses import dataclass
from typing import Any, List

from pypika import functions
from pypika.terms import AggregateFunction, Term
from pypika.terms import Function as BaseFunction

from tortoise.context import QueryContext
from tortoise.exceptions import ConfigurationError
from tortoise.fields.relational import ForeignKeyField

##############################################################################
# Base
##############################################################################


@dataclass
class AnnotationInfo:
    field: Term
    joins: List


class Annotation:
    def resolve(self, context: QueryContext, alias=None) -> AnnotationInfo:
        raise NotImplementedError()


class Subquery(Annotation):
    __slots__ = ("_queryset", )

    def __init__(self, queryset):
        self._queryset = queryset

    def resolve(self, context: QueryContext, alias=None) -> AnnotationInfo:
        self._queryset._make_query(context=context, alias=alias)
        return AnnotationInfo(self._queryset.query, [])

    def __str__(self):
        return f"Subquery({self._queryset})"


class OuterRef:
    __slots__ = ("ref_name", )

    def __init__(self, ref_name):
        self.ref_name = ref_name

    def __str__(self):
        return f"OuterRef(\"{self.ref_name}\")"


class Function(Annotation):
    __slots__ = ("field", "field_object", "default_values")

    database_func = BaseFunction
    #: Enable populate_field_object where we want to try and preserve the field type.
    populate_field_object = False

    def __init__(self, field, *default_values) -> None:
        self.field = field
        self.field_object: Any = None
        self.default_values = default_values

    def _resolve_field(self, context: QueryContext, field: str, *default_values) -> AnnotationInfo:
        model = context.stack[-1].model
        table = context.stack[-1].table

        field_split = field.split("__")
        if not field_split[1:]:
            function_joins = []
            if field_split[0] in model._meta.fetch_fields:
                relation_field = model._meta.fields_map[field_split[0]]
                relation_field_meta = relation_field.remote_model._meta
                join = (table, relation_field)
                function_joins.append(join)
                field = relation_field_meta.basetable[relation_field_meta.pk_db_column]
            else:
                field = table[field_split[0]]
                if self.populate_field_object:
                    self.field_object = model._meta.fields_map.get(field_split[0], None)
                    if self.field_object:
                        func = self.field_object.get_for_dialect(
                            model._meta.db.capabilities.dialect, "function_cast")
                        if func:
                            field = func(self.field_object, field)

            function_field = self.database_func(field, *default_values)
            return AnnotationInfo(function_field, function_joins)

        if field_split[0] not in model._meta.fetch_fields:
            raise ConfigurationError(f"{field} not resolvable")

        relation_field = model._meta.fields_map[field_split[0]]

        remote_model = relation_field.remote_model
        remote_table = remote_model._meta.basetable
        if isinstance(relation_field, ForeignKeyField):
            # Only FK's can be to same table, so we only auto-alias FK join tables
            remote_table = remote_table.as_(f"{table.get_table_name()}__{field_split[0]}")

        context.push(remote_model, remote_table)
        annotation_info = self._resolve_field(
            context, "__".join(field_split[1:]), *default_values
        )
        context.pop()

        join = (table, relation_field)
        annotation_info.joins.append(join)
        return annotation_info

    def resolve(self, context: QueryContext, alias=None) -> AnnotationInfo:
        annotation_info = self._resolve_field(context, self.field, *self.default_values)
        annotation_info.joins = reversed(annotation_info.joins)
        return annotation_info


class Aggregate(Function):
    database_func = AggregateFunction


##############################################################################
# Standard functions
##############################################################################


class Trim(Function):
    database_func = functions.Trim


class Length(Function):
    database_func = functions.Length


class Coalesce(Function):
    database_func = functions.Coalesce


class Lower(Function):
    database_func = functions.Lower


class Upper(Function):
    database_func = functions.Upper


##############################################################################
# Aggregate functions
##############################################################################


class Count(Aggregate):
    database_func = functions.Count


class Sum(Aggregate):
    database_func = functions.Sum
    populate_field_object = True


class Max(Aggregate):
    database_func = functions.Max
    populate_field_object = True


class Min(Aggregate):
    database_func = functions.Min
    populate_field_object = True


class Avg(Aggregate):
    database_func = functions.Avg
    populate_field_object = True
