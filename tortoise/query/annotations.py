
from typing import TypeVar, TYPE_CHECKING

from pypika.terms import Field as PyPikaField
from pypika.terms import Term as PyPikaTerm

from tortoise.exceptions import BaseORMException, FieldError, ParamsError
from tortoise.fields import BackwardFKField, Field, ForeignKey, ManyToManyField, OneToOneField
from tortoise.query.context import QueryContext
from tortoise.query.term_utils import term_name, resolve_term

if TYPE_CHECKING:
    from tortoise.query.base import AwaitableStatement
    from tortoise.models import Model


MODEL = TypeVar("MODEL", bound="Model")


class Annotation:
    __slots__ = ("_field", )

    def __init__(self):
        self._field: PyPikaField

    def resolve_into(self, queryset: "AwaitableStatement[MODEL]", context: QueryContext):
        raise NotImplementedError()

    def default_name(self):
        raise ParamsError("No obvious default name exists for this annotation")

    @property
    def field(self):
        if self._field:
            return self._field

        raise BaseORMException("Trying to access annotation field before it being set")

    def to_python_value(self, value):
        return value


class Subquery(Annotation):
    __slots__ = ("_queryset", )

    def __init__(self, queryset):
        super().__init__()
        self._queryset = queryset

    def resolve_into(self, queryset: "AwaitableStatement[MODEL]", context: QueryContext):
        self._queryset._make_query(context=context)
        self._field = self._queryset.query

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

    def get_field(self, context: QueryContext, annotations) -> PyPikaField:
        outer_context_item = context.stack[-2]
        outer_model = outer_context_item.model
        outer_table = outer_context_item.table

        outer_field_name = self.get_actual_field_name(outer_model, annotations, self.ref_name)
        outer_field = outer_model._meta.fields_map[outer_field_name]

        if isinstance(outer_field, ManyToManyField):
            outer_through_table = outer_context_item.through_tables[outer_field.through]
            return outer_through_table[outer_field.forward_key]

        elif isinstance(outer_field, BackwardFKField):
            # I am guessing this is the right code here, but has to be tested
            # return outer_table[outer_field.related_name]

            raise NotImplementedError()

        else:
            return outer_table[outer_field.db_column]


class TermAnnotation(Annotation):
    __slots__ = (
        "_term",
        "_field_object",
        "_add_group_by",
    )

    def __init__(self, term: PyPikaTerm) -> None:
        super().__init__()
        self._term = term
        self._add_group_by = True
        self._field_object: Field

    def default_name(self) -> str:
        try:
            return term_name(self._term)
        except ParamsError as e:
            raise ParamsError("No obvious default name exists for this annotation", e)

    def to_python_value(self, value):
        if self._field.is_aggregate and self._field_object:
            return self._field_object.to_python_value(value)
        else:
            return value

    def resolve_into(self, queryset: "AwaitableStatement[MODEL]", context: QueryContext):
        self._field_object, self._field = resolve_term(self._term, queryset, context, accept_relation=True)

        model = context.top.model
        table = context.top.table
        if self._add_group_by and self._field.is_aggregate:
            queryset.query = queryset.query.groupby(table[model._meta.pk_db_column])
