
from typing import Optional, Tuple, TypeVar

from pypika.terms import ArithmeticExpression
from pypika.terms import Field as PyPikaField
from pypika.terms import Function as PyPikaFunction
from pypika.terms import Term as PyPikaTerm
from pypika.terms import ValueWrapper

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import BaseORMException, FieldError, ParamsError
from tortoise.fields import BackwardFKField, Field, ForeignKey, ManyToManyField, OneToOneField
from tortoise.query.context import QueryContext

MODEL = TypeVar("MODEL", bound="Model")


class Annotation:
    __slots__ = ("_field", )

    def __init__(self):
        self._field: PyPikaField

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
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

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
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


def resolve_field_name_into(field_name, queryset: "AwaitableQuery[MODEL]",
    context: QueryContext) -> Tuple[Field, PyPikaField]:

    model = context.top.model
    table = context.top.table

    relation_field_name, _, field_sub = field_name.partition(LOOKUP_SEP)
    if relation_field_name in model._meta.fetch_fields:
        relation_field = model._meta.fields_map[relation_field_name]

        if field_sub:
            related_table = queryset.join_table_by_field(table, relation_field)

            context.push(relation_field.remote_model, related_table)
            (field_object, pypika_field) = resolve_field_name_into(field_sub, queryset, context)
            context.pop()
            return field_object, pypika_field

        else:
            related_table = queryset.join_table_by_field(table, relation_field)
            relation_field_meta = relation_field.remote_model._meta
            pypika_field = related_table[relation_field_meta.pk_db_column]
            field_object = relation_field_meta.pk

            return field_object, pypika_field

    else:
        if field_sub:
            raise FieldError(f"{relation_field_name} is not a relation for model {model.__name__}")

        field_object = model._meta.fields_map.get(field_name)
        if not field_object:
            raise FieldError(f"Unknown field {field_name} for model {model.__name__}")

        pypika_field = table[field_object.db_column]
        func = field_object.get_for_dialect(model._meta.db.capabilities.dialect, "function_cast")
        if func:
            pypika_field = func(field_object, pypika_field)

        return field_object, pypika_field


def resolve_term(term: PyPikaTerm, queryset: "AwaitableQuery[MODEL]",
    context: QueryContext) -> Tuple[Optional[Field], PyPikaTerm]:

    if isinstance(term, ArithmeticExpression):
        field_left, term.left = resolve_term(term.left, queryset, context)
        field_right, term.right = resolve_term(term.right, queryset, context)
        field = field_left or field_right

        return field, term

    if isinstance(term, PyPikaFunction):
        #
        # There are two options, either resolve all function args, like below,
        # in this case either all the string params are expected to be references
        # to model fields, and hence something like `Coalesce("desc", "demo")`
        # will raise FieldError if `demo` is not a model field. Now a reasonable solution
        # might be to allow unresolvable strings as is, without raising exceptions,
        # but that also has other undesired implication.
        #
        # term_new_args = []
        # field = None
        #
        # for arg in term.args:
        #     term_field, term_arg = resolve_term(arg, queryset, context)
        #     term_new_args.append(term_arg)
        #     field = field or term_field
        #
        # term.args = term_new_args
        # return field, term
        #
        # Another solution is allow on the the first parameter of the function to be
        # a field reference as we do here:
        #

        field = None
        if len(term.args) > 0:
            field, term.args[0] = resolve_term(term.args[0], queryset, context)

        return field, term

    elif isinstance(term, ValueWrapper):
        if isinstance(term.value, str):
            return resolve_field_name_into(term.value, queryset, context)

        return None, term

    raise FieldError(f"Unresolvable term: {term}")


def term_name(term: PyPikaTerm) -> str:
    if isinstance(term, ValueWrapper):
        return str(term.value)

    if isinstance(term, ArithmeticExpression):
        return "{}__{}__{}".format(term_name(term.left), str(term.operator), term_name(term.right))

    if isinstance(term, PyPikaFunction):
        return "{}__{}".format("__".join(map(term_name, term.args)), term.name.lower())

    raise ParamsError("Unable to find term name {}".format(term))


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

    def resolve_into(self, queryset: "AwaitableQuery[MODEL]", context: QueryContext):
        self._field_object, self._field = resolve_term(self._term, queryset, context)

        model = context.top.model
        table = context.top.table
        if self._add_group_by and self._field.is_aggregate:
            queryset.query = queryset.query.groupby(table[model._meta.pk_db_column])
