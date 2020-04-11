
from copy import copy
from typing import List, Dict, TypeVar, Generator, Any, Type, Union, Optional, Set

from typing_extensions import Protocol

from tortoise import BaseDBAsyncClient
from tortoise.context import QueryContext
from tortoise.exceptions import MultipleObjectsReturned, DoesNotExist, FieldError
from tortoise.filters.q import Q
from tortoise.functions import Annotation
from tortoise.query.fieldselect import ValuesListQuery, ValuesQuery
from tortoise.query.statements import DeleteQuery, UpdateQuery, CountQuery
from tortoise.query.prefetch import Prefetch
from tortoise.query.base import AwaitableQuery, MODEL

T_co = TypeVar("T_co", covariant=True)


class QuerySetSingle(Protocol[T_co]):
    # pylint: disable=W0104
    def __await__(self) -> Generator[Any, None, T_co]:
        pass  # pragma: nocoverage


class QuerySet(AwaitableQuery[MODEL]):
    __slots__ = (
        "fields",
        "_prefetch_map",
        "_prefetch_queries",
        "_single",
        "_get",
        "_db",
        "_filter_kwargs",
    )

    def __init__(self, model: Type[MODEL]) -> None:
        super().__init__(model)
        self.fields = model._meta.db_columns

        self._prefetch_map: Dict[str, Set[str]] = {}
        self._prefetch_queries: Dict[str, QuerySet] = {}
        self._single: bool = False
        self._get: bool = False
        self._filter_kwargs: Dict[str, Any] = {}

    def _clone(self) -> "QuerySet[MODEL]":
        queryset = QuerySet.__new__(QuerySet)
        queryset.fields = self.fields
        queryset.model = self.model
        queryset.query = self.query
        queryset.capabilities = self.capabilities
        queryset._prefetch_map = copy(self._prefetch_map)
        queryset._prefetch_queries = copy(self._prefetch_queries)
        queryset._single = self._single
        queryset._get = self._get
        queryset._db = self._db
        queryset._limit = self._limit
        queryset._offset = self._offset
        queryset._filter_kwargs = copy(self._filter_kwargs)
        queryset._orderings = copy(self._orderings)
        queryset._joined_tables = copy(self._joined_tables)
        queryset._distinct = self._distinct

        queryset.q_objects = copy(self.q_objects)
        queryset.annotations = copy(self.annotations)

        return queryset

    def _filter_or_exclude(self, *args, negate: bool, **kwargs):
        queryset = self._clone()
        for arg in args:
            if not isinstance(arg, Q):
                raise TypeError("expected Q objects as args")
            if negate:
                queryset.q_objects.append(~arg)
            else:
                queryset.q_objects.append(arg)

        for key, value in kwargs.items():
            if negate:
                queryset.q_objects.append(~Q(**{key: value}))
            else:
                queryset.q_objects.append(Q(**{key: value}))

        return queryset

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

    def order_by(self, *orderings: str) -> "QuerySet[MODEL]":
        """
        Accept args to filter by in format like this:

        .. code-block:: python3

            .order_by('name', '-tournament__name')

        Supports ordering by related models too.
        """
        queryset = self._clone()
        queryset._orderings = self._parse_orderings(*orderings)
        return queryset

    def limit(self, limit: int) -> "QuerySet[MODEL]":
        """
        Limits QuerySet to given length.
        """
        queryset = self._clone()
        queryset._limit = limit
        return queryset

    def offset(self, offset: int) -> "QuerySet[MODEL]":
        """
        Query offset for QuerySet.
        """
        queryset = self._clone()
        queryset._offset = offset
        if self.capabilities.requires_limit and queryset._limit is None:
            queryset._limit = 1000000
        return queryset

    def distinct(self) -> "QuerySet[MODEL]":
        """
        Make QuerySet distinct.

        Only makes sense in combination with a ``.values()`` or ``.values_list()`` as it
        precedes all the fetched fields with a distinct.
        """
        queryset = self._clone()
        queryset._distinct = True
        return queryset

    def annotate(self, **kwargs) -> "QuerySet[MODEL]":
        """
        Annotate result with aggregation or function result.
        """
        queryset = self._clone()
        for key, annotation in kwargs.items():
            if not isinstance(annotation, Annotation):
                raise TypeError("value is expected to be Annotation instance")
            queryset.annotations[key] = annotation

        return queryset

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
                field
                for field in self.model._meta.fields_map.keys()
                if field in self.model._meta.db_columns
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
                field: field
                for field in self.model._meta.fields_map.keys()
                if field in self.model._meta.db_columns
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

    def first(self) -> QuerySetSingle[Optional[MODEL]]:
        """
        Limit queryset to one object and return one object instead of list.
        """
        queryset = self._clone()
        queryset._limit = 1
        queryset._single = True
        return queryset  # type: ignore

    def get(self, *args, **kwargs) -> QuerySetSingle[MODEL]:
        """
        Fetch exactly one object matching the parameters.
        """
        queryset = self.filter(*args, **kwargs)
        queryset._limit = 2
        queryset._get = True
        return queryset  # type: ignore

    def get_or_none(self, *args, **kwargs) -> QuerySetSingle[MODEL]:
        """
        Fetch exactly one object matching the parameters.
        """
        queryset = self.filter(*args, **kwargs)
        queryset._limit = 1
        queryset._single = True
        return queryset  # type: ignore

    def prefetch_related(self, *args: Union[str, Prefetch]) -> "QuerySet[MODEL]":
        """
        Like ``.fetch_related()`` on instance, but works on all objects in QuerySet.
        """
        queryset = self._clone()
        queryset._prefetch_map = {}

        for relation in args:
            if not isinstance(relation, Prefetch):
                relation = Prefetch(relation)

            relation.resolve_for_queryset(queryset)

        return queryset

    async def explain(self) -> Any:
        """Fetch and return information about the query execution plan.

        This is done by executing an ``EXPLAIN`` query whose exact prefix depends
        on the database backend, as documented below.

        - PostgreSQL: ``EXPLAIN (FORMAT JSON, VERBOSE) ...``
        - SQLite: ``EXPLAIN QUERY PLAN ...``
        - MySQL: ``EXPLAIN FORMAT=JSON ...``

        .. note::
            This is only meant to be used in an interactive environment for debugging
            and query optimization.
            **The output format may (and will) vary greatly depending on the database backend.**
        """
        if self._db is None:
            self._db = self.model._meta.db  # type: ignore
        self._make_query(context=QueryContext())
        return await self._db.executor_class(model=self.model, db=self._db).execute_explain(
            self.query
        )

    def using_db(self, _db: BaseDBAsyncClient) -> "QuerySet[MODEL]":
        """
        Executes query in provided db client.
        Useful for transactions workaround.
        """
        queryset = self._clone()
        queryset._db = _db
        return queryset

    def _resolve_annotations(self, context: QueryContext) -> None:
        if not self.annotations:
            return

        annotation_info_map = {
            key: annotation.resolve(context) for key, annotation in self.annotations.items()
        }

        if any(
            annotation_info.field.is_aggregate
            for annotation_info in annotation_info_map.values()
        ):
            table = context.stack[-1].table
            self.query = self.query.groupby(table.id)

        for key, annotation_info in annotation_info_map.items():
            for join in annotation_info.joins:
                self._join_table_by_field(*join)
            self.query._select_other(annotation_info.field.as_(key))

    def _make_query(self, context: QueryContext, alias=None) -> None:
        self.query = self.create_base_query_all_fields(alias)
        context.push(self.model, self.query._from[-1])
        self._add_query_details(context)
        context.pop()

    def _add_query_details(self, context: QueryContext):
        self._resolve_annotations(context=context)
        self.resolve_filters(context=context)

        if self._limit:
            self.query._limit = self._limit

        if self._offset:
            self.query._offset = self._offset

        if self._distinct:
            self.query._distinct = True

        self.resolve_ordering(context=context)

    async def _execute(self) -> List[MODEL]:
        executor = self._db.executor_class(
            model=self.model,
            db=self._db,
            prefetch_map=self._prefetch_map,
            prefetch_queries=self._prefetch_queries,
        )

        instance_list = await executor.execute_select(self.query, custom_fields=list(self.annotations.keys()))

        if self._get:
            if len(instance_list) == 1:
                return instance_list[0]
            if not instance_list:
                raise DoesNotExist("Object does not exist")
            raise MultipleObjectsReturned("Multiple objects returned, expected exactly one")

        if self._single:
            if not instance_list:
                return None  # type: ignore
            return instance_list[0]

        return instance_list

