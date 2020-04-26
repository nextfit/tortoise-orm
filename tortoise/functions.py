
from typing import TypeVar

from pypika import functions
from pypika.terms import AggregateFunction, Field
from pypika.terms import Function as PyPikaFunction

from tortoise.constants import LOOKUP_SEP
from tortoise.context import QueryContext
from tortoise.exceptions import FieldError, BaseORMException, ParamsError
from tortoise.fields import ForeignKey, OneToOneField, ManyToManyField, BackwardFKField

MODEL = TypeVar("MODEL", bound="Model")


class Annotation:
    __slots__ = ("_field", )

    def __init__(self):
        self._field = None

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext, alias: str):
        raise NotImplementedError()

    def default_name(self):
        raise ParamsError("No obvious default name exists for this annotation")

    @property
    def field(self):
        if self._field:
            return self._field

        raise BaseORMException("Trying to access annotation field before it being set")


class Subquery(Annotation):
    __slots__ = ("_queryset", )

    def __init__(self, queryset):
        super().__init__()
        self._queryset = queryset

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext, alias: str):
        self._queryset._make_query(context=context)
        self._field = self._queryset.query.as_(alias)
        queryset.query._select_other(self._field)

    def __str__(self):
        return f"Subquery({self._queryset})"


class OuterRef:
    __slots__ = ("ref_name", )

    def __init__(self, ref_name):
        super().__init__()
        self.ref_name = ref_name

    def __str__(self):
        return f"OuterRef(\"{self.ref_name}\")"

    @staticmethod
    def get_actual_field_name(model, annotations, field_name: str):
        if field_name in model._meta.fields_map:
            field = model._meta.fields_map[field_name]
            if isinstance(field, (ForeignKey, OneToOneField)):
                return field.id_field_name

            return field_name

        if field_name == "pk":
            return model._meta.pk_attr

        if field_name in annotations:
            return field_name

        allowed = sorted(list(model._meta.fields_map.keys() | annotations.keys()))
        raise FieldError(f"Unknown field name '{field_name}'. Allowed base values are {allowed}")

    def get_field(self, context: QueryContext, annotations) -> Field:
        outer_context_item = context.stack[-2]
        outer_model = outer_context_item.model
        outer_table = outer_context_item.table

        outer_field_name = self.get_actual_field_name(outer_model, annotations, self.ref_name)
        outer_field = outer_model._meta.fields_map[outer_field_name]

        if isinstance(outer_field, ManyToManyField):
            outer_through_table = outer_context_item.through_tables[outer_field.through]
            return outer_through_table[outer_field.forward_key]

        elif isinstance(outer_field, BackwardFKField):
            raise NotImplementedError()

        else:
            return outer_table[self.ref_name]


class Function(Annotation):
    __slots__ = ("field_name", "default_values", "add_group_by")

    database_func = PyPikaFunction

    def __init__(self, field_name, *default_values, add_group_by=True) -> None:
        super().__init__()
        self.field_name = field_name
        self.default_values = default_values
        self.add_group_by = add_group_by

    def default_name(self):
        return "{}__{}".format(self.field_name, self.database_func(None).name.lower())

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext, alias: str):
        model = context.top.model
        table = context.top.table

        relation_field_name, _, field_sub = self.field_name.partition(LOOKUP_SEP)
        if relation_field_name in model._meta.fetch_fields:
            relation_field = model._meta.fields_map[relation_field_name]

            if field_sub:
                related_table = queryset._join_table_by_field(table, relation_field)

                context.push(relation_field.remote_model, related_table)
                sub_function = self.__class__(field_sub, add_group_by=False, *self.default_values)
                sub_function.resolve_into(queryset, context, alias)
                self._field = sub_function._field
                context.pop()

            else:
                related_table = queryset._join_table_by_field(table, relation_field)
                relation_field_meta = relation_field.remote_model._meta
                field = related_table[relation_field_meta.pk_db_column]

                self._field = self.database_func(field, *self.default_values).as_(alias)
                queryset.query._select_other(self._field)

        else:
            if field_sub:
                raise FieldError(f"{relation_field_name} is not a relation for model {model.__name__}")

            field_object = model._meta.fields_map.get(self.field_name)
            if not field_object:
                raise FieldError(f"Unknown field {self.field_name} for model {model.__name__}")

            field = table[field_object.db_column]
            func = field_object.get_for_dialect(model._meta.db.capabilities.dialect, "function_cast")
            if func:
                field = func(field_object, field)

            self._field = self.database_func(field, *self.default_values).as_(alias)
            queryset.query._select_other(self._field)

        if self.add_group_by and self._field.is_aggregate:
            queryset.query = queryset.query.groupby(table.id)


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


class Max(Aggregate):
    database_func = functions.Max


class Min(Aggregate):
    database_func = functions.Min


class Avg(Aggregate):
    database_func = functions.Avg
