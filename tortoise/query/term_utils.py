
from typing import Optional, Tuple

from pypika.terms import ArithmeticExpression
from pypika.terms import Field as PyPikaField
from pypika.terms import Function as PyPikaFunction
from pypika.terms import Term as PyPikaTerm
from pypika.terms import ValueWrapper

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import FieldError, ParamsError
from tortoise.fields import Field
from tortoise.query.context import QueryContext


def resolve_field_name_into(
    field_name,
    queryset: "AwaitableQuery[MODEL]",
    context: QueryContext,
    accept_relation: bool) -> Tuple[Field, PyPikaField]:

    model = context.top.model
    table = context.top.table

    relation_field_name, _, field_sub = field_name.partition(LOOKUP_SEP)
    if relation_field_name in model._meta.fetch_fields:
        relation_field = model._meta.fields_map[relation_field_name]

        if field_sub:
            related_table = queryset.join_table_by_field(table, relation_field)

            context.push(relation_field.remote_model, related_table)
            (field_object, pypika_field) = resolve_field_name_into(field_sub, queryset, context, accept_relation)
            context.pop()
            return field_object, pypika_field

        elif accept_relation:
            related_table = queryset.join_table_by_field(table, relation_field)
            relation_field_meta = relation_field.remote_model._meta
            pypika_field = related_table[relation_field_meta.pk_db_column]
            field_object = relation_field_meta.pk

            return field_object, pypika_field

        else:
            raise FieldError("{} is a relation. Try a nested field of the related model".format(relation_field_name))

    else:
        if field_sub:
            raise FieldError(f"{relation_field_name} is not a relation for model {model.__name__}")

        field_object = model._meta.fields_map.get(field_name)
        if not field_object:
            raise FieldError(f"Unknown field {field_name} for model {model.__name__}")

        pypika_field = table[field_object.db_column]
        func = field_object.get_for_dialect("function_cast")
        if func:
            pypika_field = func(field_object, pypika_field)

        return field_object, pypika_field


def resolve_term(
    term: PyPikaTerm,
    queryset: "AwaitableQuery[MODEL]",
    context: QueryContext,
    accept_relation: bool) -> Tuple[Optional[Field], PyPikaTerm]:

    if isinstance(term, ArithmeticExpression):
        field_left, term.left = resolve_term(term.left, queryset, context, accept_relation)
        field_right, term.right = resolve_term(term.right, queryset, context, accept_relation)
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
            field, term.args[0] = resolve_term(term.args[0], queryset, context, accept_relation)

        return field, term

    elif isinstance(term, ValueWrapper):
        if isinstance(term.value, str):
            return resolve_field_name_into(term.value, queryset, context, accept_relation)

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