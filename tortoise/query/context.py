
from copy import copy

from pypika import Table, JoinType
from pypika.queries import QueryBuilder
from pypika.terms import ArithmeticExpression, Negative, ValueWrapper
from pypika.terms import Field as PyPikaField, Function as PyPikaFunction, Term as PyPikaTerm

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import FieldError, UnknownFieldError, NotARelationFieldError
from tortoise.fields import Field, RelationField, JSONField
from tortoise.fields.relational import JoinData
from typing import Dict, List, Optional, Tuple, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from tortoise.models import MODEL
    from tortoise.query.base import AwaitableStatement


class QueryContextItem:
    def __init__(self, model: Type["MODEL"], table: Table, through_tables: Optional[Dict[str, Table]] = None) -> None:
        self.model = model
        self.table = table
        self.through_tables = through_tables or {}


class QueryContext:
    def __init__(self, query: QueryBuilder, parent_context: Optional["QueryContext"] = None) -> None:
        self.query: QueryBuilder = query
        self.stack: List[QueryContextItem] = parent_context.stack.copy() if parent_context else []

    def push(self, model, table, through_tables: Optional[Dict[str, Table]] = None) -> "QueryContext":
        self.stack.append(QueryContextItem(model, table, through_tables))
        return self

    def pop(self) -> QueryContextItem:
        return self.stack.pop()

    @property
    def top(self) -> QueryContextItem:
        return self.stack[-1]

    @property
    def alias(self) -> Optional[str]:
        return "U{}".format(len(self.stack)) if self.stack else None

    def join_table_by_field(self, table, relation_field: RelationField, full=True) -> Optional[JoinData]:
        """
        :param table:
        :param relation_field:
        :param full: If needed to join fully, or only to the point where primary key of the relation is available.
            For example for ForeignKey and OneToOneField, when full is False, not joins is needed.
            Also for ManyToManyField, when full is False, only the through table is needed to be joined
        :return: related_table
        """

        joins = relation_field.get_joins(table, full)
        if joins:
            for join in joins:
                if not self.query.is_joined(join.table):
                    self.query = self.query.join(join.table, how=JoinType.left_outer).on(join.criterion)

            return joins[-1]

        else:
            return None

    def resolve_select_related(self, related_map: Dict[str, Dict]) -> None:
        """
        This method goes hand in hand with Model._init_from_db_row(row_iter, related_map)
        where this method created a selections columns to be queried, and _init_from_db_row
        follows the same path to "pickup" those columns to recreate the model object

        :param context: Query Context
        :param related_map: Map of pre-selected relations
        :return: None
        """

        model = self.top.model
        table = self.top.table

        for field_name in related_map:
            field_object = model._meta.fields_map[field_name]
            join_data = self.join_table_by_field(table, field_object)
            remote_table = join_data.table

            cols = [remote_table[col] for col in field_object.remote_model._meta.db_columns]
            self.query = self.query.select(*cols)
            if related_map[field_name]:
                self.push(join_data.model, join_data.table)
                self.resolve_select_related(related_map[field_name])
                self.pop()

    def resolve_field_name(
        self,
        field_name,
        queryset: "AwaitableStatement[MODEL]",
        accept_relation: bool,
        check_annotations=True,
        expand_annotation=True) -> Tuple[Optional[Field], PyPikaField]:

        #
        # When expand_annotation is False, we need to make sure the annotation
        # will show up (will be expanded) in the final query, since we are just
        # referring to it here.
        #

        if check_annotations and field_name in queryset.annotations:
            if expand_annotation:
                return None, queryset.annotations[field_name].field
            else:
                return None, PyPikaField(field_name)

        model = self.top.model
        table = self.top.table

        if field_name == "pk":
            field_name = model._meta.pk_attr

        relation_field_name, _, field_sub = field_name.partition(LOOKUP_SEP)
        relation_field = model._meta.fields_map.get(relation_field_name)
        if not relation_field:
            raise UnknownFieldError(relation_field_name, model)

        if isinstance(relation_field, RelationField):
            if field_sub:
                join_data = self.join_table_by_field(table, relation_field)

                self.push(join_data.model, join_data.table)
                (field_object, pypika_field) = self.resolve_field_name(
                    field_sub, queryset, accept_relation, check_annotations=False)

                self.pop()
                return field_object, pypika_field

            elif accept_relation:
                join_data = self.join_table_by_field(table, relation_field, full=False)
                if join_data:
                    return join_data.field_object, join_data.pypika_field

                else:
                    # this can happen only when relation_field is instance of ForeignKey or OneToOneField
                    field_object = model._meta.fields_map[relation_field.id_field_name]
                    pypika_field = table[field_object.db_column]
                    return field_object, pypika_field

            else:
                raise FieldError("{} is a relation. Try a nested field of the related model".format(relation_field_name))

        else:
            if field_sub:
                if isinstance(relation_field, JSONField):
                    path = "{{{}}}".format(field_sub.replace(LOOKUP_SEP, ','))
                    return None, table[relation_field.db_column].get_path_json_value(path)

                raise NotARelationFieldError(relation_field_name, model)

            field_object = relation_field
            pypika_field = table[field_object.db_column]
            func = field_object.get_for_dialect("function_cast")
            if func:
                pypika_field = func(pypika_field)

            return field_object, pypika_field

    def resolve_term(self, term: PyPikaTerm, queryset: "AwaitableStatement[MODEL]",
        accept_relation: bool) -> Tuple[Optional[Field], PyPikaTerm]:

        if isinstance(term, ArithmeticExpression):
            pypika_term = copy(term)
            field_left, pypika_term.left = self.resolve_term(term.left, queryset, accept_relation)
            field_right, pypika_term.right = self.resolve_term(term.right, queryset, accept_relation)
            field = field_left or field_right

            return field, pypika_term

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

            pypika_term = copy(term)
            field = None
            if len(term.args) > 0:
                pypika_term.args = copy(term.args)
                field, pypika_term.args[0] = self.resolve_term(term.args[0], queryset, accept_relation)

            return field, pypika_term

        elif isinstance(term, Negative):
            pypika_term = copy(term)
            field, pypika_term.term = self.resolve_term(term.term, queryset, accept_relation)
            return field, pypika_term

        elif isinstance(term, ValueWrapper):
            if isinstance(term.value, str):
                return self.resolve_field_name(term.value, queryset, accept_relation)

            return None, term

        raise FieldError(f"Unresolvable term: {term}")
