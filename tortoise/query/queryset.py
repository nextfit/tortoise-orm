
import itertools
from copy import copy
from typing import Dict, List, Set, Type, Union, TYPE_CHECKING

from pypika.terms import Term

from tortoise.exceptions import FieldError, ParamsError
from tortoise.query.annotations import Annotation, TermAnnotation
from tortoise.query.base import MODEL, AwaitableQuery
from tortoise.query.context import QueryContext
from tortoise.query.fieldselect import ValuesListQuery, ValuesQuery
from tortoise.query.prefetch import Prefetch, parse_select_related
from tortoise.query.single import FirstQuerySet
from tortoise.query.statements import CountQuery, DeleteQuery, UpdateQuery


if TYPE_CHECKING:
    from tortoise.query.raw import RawQuerySet


class QuerySet(AwaitableQuery[MODEL]):
    __slots__ = (
        "_prefetch_map",
        "_prefetch_queries",
        "_select_related",
    )

    def __init__(self, model: Type[MODEL]) -> None:
        super().__init__(model)

        self._prefetch_map: Dict[str, Set[str]] = {}
        self._prefetch_queries: Dict[str, QuerySet] = {}
        self._select_related: Dict[str, Dict] = {}

    def _copy(self, queryset) -> None:
        super()._copy(queryset)

        queryset._prefetch_map = copy(self._prefetch_map)
        queryset._prefetch_queries = copy(self._prefetch_queries)
        queryset._select_related = copy(self._select_related)

    def filter(self, *args, **kwargs) -> "QuerySet[MODEL]":
        """
        Filters QuerySet by given kwargs. You can filter by related objects like this:

        .. code-block:: python3

            Team.filter(events__tournament__name='Test')

        You can also pass Q objects to filters as args.
        """
        return self._filter_or_exclude(negate=False, *args, **kwargs)

    def exclude(self, *args, **kwargs) -> "QuerySet[MODEL]":
        """
        Same as .filter(), but with appends all args with NOT
        """
        return self._filter_or_exclude(negate=True, *args, **kwargs)

    def annotate(self, *args, **kwargs) -> "QuerySet[MODEL]":
        """
        Annotate result with aggregation or function result.
        """

        for annotation in itertools.chain(args, kwargs.values()):
            if not isinstance(annotation, (Term, Annotation)):
                raise TypeError("{} is expected to be instance of Annotation or pypika.terms.Term".format(annotation))

        args = [TermAnnotation(t) if isinstance(t, Term) else t for t in args]
        kwargs = {k: TermAnnotation(v) if isinstance(v, Term) else v for k, v in kwargs.items()}

        args_dict = {arg.default_name(): arg for arg in args}
        duplicate_keys = args_dict.keys() & kwargs.keys()
        if duplicate_keys:
            raise ParamsError("Duplicate annotations: {}".format(duplicate_keys))

        args_dict.update(kwargs)
        duplicate_keys = args_dict.keys() & self.annotations
        if duplicate_keys:
            raise ParamsError("Duplicate annotations: {}".format(duplicate_keys))

        duplicate_keys = args_dict.keys() & self.model._meta.fields_map.keys()
        if duplicate_keys:
            raise ParamsError("Annotations {} conflict with existing model fields".format(duplicate_keys))

        queryset = self._clone()
        queryset.annotations.update(args_dict)
        return queryset

    def aggregate(self, *args, **kwargs) -> FirstQuerySet:
        queryset = self.annotate(*args, **kwargs)
        for annotation in queryset.annotations.values():
            if isinstance(annotation, TermAnnotation):
                annotation._add_group_by = False

        return queryset.values(*queryset.annotations.keys()).first()

    def values_list(self, *fields_: str, flat: bool = False) -> ValuesListQuery:
        """
        Make QuerySet returns list of tuples for given args instead of objects.

        If ```flat=True`` and only one arg is passed can return flat list.

        If no arguments are passed it will default to a tuple containing all fields
        in order of declaration.
        """
        return ValuesListQuery(
            db=self._db,
            model=self.model,
            q_objects=self.q_objects,
            flat=flat,
            fields_for_select_list=fields_
            or [
                field_name
                for field_name, field_object in self.model._meta.fields_map.items()
                if field_object.has_db_column
            ],
            distinct=self._distinct,
            limit=self._limit,
            offset=self._offset,
            orderings=self._orderings,
            annotations=self.annotations,
        )

    def values(self, *args: str, **kwargs: str) -> ValuesQuery:
        """
        Make QuerySet return dicts instead of objects.

        Can pass names of fields to fetch, or as a ``field_name='name_in_dict'`` kwarg.

        If no arguments are passed it will default to a dict containing all fields.
        """
        if args or kwargs:
            fields_for_select: Dict[str, str] = {}
            for field in args:
                if field in fields_for_select:
                    raise FieldError(f"Duplicate key {field}")
                fields_for_select[field] = field

            for return_as, field in kwargs.items():
                if return_as in fields_for_select:
                    raise FieldError(f"Duplicate key {return_as}")
                fields_for_select[return_as] = field

        else:
            fields_for_select = {
                field_name: field_name
                for field_name, field_object in self.model._meta.fields_map.items()
                if field_object.has_db_column
            }

        return ValuesQuery(
            db=self._db,
            model=self.model,
            q_objects=self.q_objects,
            fields_for_select=fields_for_select,
            distinct=self._distinct,
            limit=self._limit,
            offset=self._offset,
            orderings=self._orderings,
            annotations=self.annotations,
        )

    def delete(self) -> DeleteQuery:
        """
        Delete all objects in QuerySet.
        """
        return DeleteQuery(
            db=self._db,
            model=self.model,
            q_objects=self.q_objects,
            annotations=self.annotations,
        )

    def update(self, **kwargs) -> UpdateQuery:
        """
        Update all objects in QuerySet with given kwargs.
        """
        return UpdateQuery(
            db=self._db,
            model=self.model,
            update_kwargs=kwargs,
            q_objects=self.q_objects,
            annotations=self.annotations,
        )

    def count(self) -> CountQuery:
        """
        Return count of objects in queryset instead of objects.
        """
        return CountQuery(
            db=self._db,
            model=self.model,
            q_objects=self.q_objects,
            annotations=self.annotations,
        )

    def all(self) -> "QuerySet[MODEL]":
        """
        Return the whole QuerySet.
        Essentially a no-op except as the only operation.
        """
        return self._clone()

    def raw(self, query: str) -> "RawQuerySet[MODEL]":
        from tortoise.query.raw import RawQuerySet
        return RawQuerySet(self, query)

    def prefetch_related(self, *args: Union[str, Prefetch]) -> "QuerySet[MODEL]":
        """
        Like ``.fetch_related()`` on instance, but works on all objects in QuerySet.
        """
        queryset = self._clone()
        for relation in args:
            if not isinstance(relation, Prefetch):
                relation = Prefetch(relation)

            relation.resolve_for_queryset(queryset)

        return queryset

    def select_related(self, *args: str) -> "QuerySet[MODEL]":
        """
        Like ``.fetch_related()`` on instance, but works on all objects in QuerySet.
        """
        queryset = self._clone()
        for relation in args:
            parse_select_related(relation, queryset.model, queryset._select_related)

        return queryset

    def _resolve_select_related(self, context: QueryContext, related_map: Dict[str, Dict]) -> None:
        """
        This method goes hand in hand with Model._init_from_db_row(row_iter, related_map)
        where this method created a selections columns to be queried, and _init_from_db_row
        follows the same path to "pickup" those columns to recreate the model object

        :param context: Query Context
        :param related_map: Map of pre-selected relations
        :return: None
        """

        model = context.top.model
        table = context.top.table

        for field_name in related_map:
            field_object = model._meta.fields_map[field_name]
            join_data = self.join_table_by_field(table, field_object)
            remote_table = join_data.table

            cols = [remote_table[col] for col in field_object.remote_model._meta.db_columns]
            self.query = self.query.select(*cols)
            if related_map[field_name]:
                context.push(join_data.model, join_data.table)
                self._resolve_select_related(context, related_map[field_name])
                context.pop()

    def _init_query_builder(self, context) -> None:
        self.query = self.query_builder(context.alias).select(*self.model._meta.db_columns)
        context.push(self.model, self.query._from[-1])
        self._resolve_select_related(context, self._select_related)
        context.pop()

    def _make_query(self, context: QueryContext) -> None:
        self._init_query_builder(context)

        context.push(self.model, self.query._from[-1])
        self._add_query_details(context=context)
        for return_as, annotation in self.annotations.items():
            self.query._select_other(annotation.field.as_(return_as))
        context.pop()

    async def _execute(self) -> List[MODEL]:
        db_client = self._get_db_client()
        executor = db_client.executor_class(
            model=self.model,
            db=db_client,
            prefetch_map=self._prefetch_map,
            prefetch_queries=self._prefetch_queries,
            select_related=self._select_related
        )

        return await executor.execute_select(self.query, custom_fields=list(self.annotations.keys()))
