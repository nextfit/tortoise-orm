
from typing import Optional, Tuple, TYPE_CHECKING

from pypika.terms import ArithmeticExpression
from pypika.terms import Field as PyPikaField
from pypika.terms import Function as PyPikaFunction
from pypika.terms import Term as PyPikaTerm
from pypika.terms import ValueWrapper

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import FieldError, ParamsError, UnknownFieldError, NotARelationFieldError
from tortoise.fields import Field
from tortoise.query.context import QueryContext

if TYPE_CHECKING:
    from tortoise.models import MODEL
    from tortoise.query.base import AwaitableStatement


def resolve_field_name_into(
    field_name,
    queryset: "AwaitableStatement[MODEL]",
    context: QueryContext,
    accept_relation: bool) -> Tuple[Optional[Field], PyPikaField]:

    if field_name in queryset.annotations:
        return None, queryset.annotations[field_name].field

    model = context.top.model
    table = context.top.table

    relation_field_name, _, field_sub = field_name.partition(LOOKUP_SEP)
    relation_field = model._meta.fields_map.get(relation_field_name)
    if not relation_field:
        raise UnknownFieldError(relation_field_name, model)

    if relation_field.has_db_column:
        if field_sub:
            raise NotARelationFieldError(relation_field_name, model)

        field_object = relation_field
        pypika_field = table[field_object.db_column]
        func = field_object.get_for_dialect("function_cast")
        if func:
            pypika_field = func(pypika_field)

        return field_object, pypika_field

    else:
        if field_sub:
            join_data = queryset.join_table_by_field(table, relation_field)

            context.push(join_data.model, join_data.table)
            (field_object, pypika_field) = resolve_field_name_into(field_sub, queryset, context, accept_relation)
            context.pop()
            return field_object, pypika_field

        elif accept_relation:
            join_data = queryset.join_table_by_field(table, relation_field, full=False)
            return join_data.field_object, join_data.pypika_field

        else:
            raise FieldError("{} is a relation. Try a nested field of the related model".format(relation_field_name))


def resolve_term(
    term: PyPikaTerm,
    queryset: "AwaitableStatement[MODEL]",
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
