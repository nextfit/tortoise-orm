
from copy import copy
from typing import TYPE_CHECKING
from typing import Any, AsyncIterator, Dict, Generator, Generic, List, Optional, Type, TypeVar, Union

from pypika import Table, JoinType, Order
from pypika.queries import QueryBuilder
from pypika.terms import Node

from tortoise import BaseDBAsyncClient, RelationField
from tortoise.backends.base.client import Capabilities
from tortoise.constants import LOOKUP_SEP
from tortoise.query.context import QueryContext
from tortoise.exceptions import FieldError, ParamsError
from tortoise.filters.q import Q
from tortoise.query.functions import Annotation
from tortoise.query.ordering import QueryOrdering, QueryOrderingField, QueryOrderingNode
from tortoise.query.single import FirstQuerySet, GetQuerySet

if TYPE_CHECKING:  # pragma: nocoverage
    from tortoise.models import Model


MODEL = TypeVar("MODEL", bound="Model")
QUERY: QueryBuilder = QueryBuilder()


class AwaitableStatement(Generic[MODEL]):
    __slots__ = (
        "_db",
        "capabilities",
        "model",
        "query",
        "_joined_tables",
        "q_objects",
        "annotations",
    )

    def __init__(self, model: Type[MODEL], db=None, q_objects=None, annotations=None) -> None:
        self._joined_tables: List[Table] = []
        self._db: BaseDBAsyncClient = db  # type: ignore

        self.model: "Type[Model]" = model
        self.query: QueryBuilder = QUERY
        self.capabilities: Capabilities = model._meta.db.capabilities

        self.q_objects: List[Q] = q_objects or []
        self.annotations: Dict[str, Annotation] = annotations or {}

    def _copy(self, queryset) -> None:
        queryset._db = self._db
        queryset.capabilities = self.capabilities
        queryset.model = self.model
        queryset.query = self.query
        queryset._joined_tables = copy(self._joined_tables)
        queryset.q_objects = copy(self.q_objects)
        queryset.annotations = copy(self.annotations)

    def _clone(self):
        queryset = self.__class__.__new__(self.__class__)
        self._copy(queryset)
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

    def __resolve_filters(self, context: QueryContext) -> None:
        Q(*self.q_objects).resolve_into(self, context)

    def __resolve_annotations(self, context: QueryContext) -> None:
        for key, annotation in self.annotations.items():
            annotation.resolve_into(self, context=context, alias=key)

    def is_aggregate(self):
        return any([annotation.field.is_aggregate for annotation in self.annotations.values()])

    def join_table_by_field(self, table, relation_field: RelationField, full=True) -> Optional[Table]:
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
                if join[0] not in self._joined_tables:
                    self.query = self.query.join(join[0], how=JoinType.left_outer).on(join[1])
                    self._joined_tables.append(join[0])

            return joins[-1][0]

        else:
            return None

    def query_builder_select_all_fields(self, alias=None):
        return self.model._meta.query_builder(alias).select(*self.model._meta.db_columns)

    def _add_query_details(self, context: QueryContext) -> None:
        self.__resolve_annotations(context=context)
        self.__resolve_filters(context)

    def _make_query(self, context: QueryContext, alias=None) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def _execute(self):
        raise NotImplementedError()  # pragma: nocoverage

    def __await__(self) -> Generator[Any, None, List[MODEL]]:
        if self._db is None:
            self._db = self.model._meta.db  # type: ignore
        self._make_query(context=QueryContext())
        return self._execute().__await__()

    def using_db(self, _db: BaseDBAsyncClient) -> "QuerySet[MODEL]":
        """
        Executes query in provided db client.
        Useful for transactions workaround.
        """
        queryset = self._clone()
        queryset._db = _db
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
        return await self._db\
            .executor_class(model=self.model, db=self._db)\
            .execute_explain(self.query)


class AwaitableQuery(AwaitableStatement[MODEL]):
    __slots__ = (
        "_orderings",
        "_distinct",
        "_offset",
        "_limit",
    )

    def __init__(self,
        model: Type[MODEL],
        db=None,
        q_objects=None,
        annotations=None,
        orderings: List[QueryOrdering] = None,
        distinct: bool = False,
        limit=None,
        offset=None):

        super().__init__(model, db, q_objects, annotations)

        self._orderings: List[QueryOrdering]
        if orderings:
            self._orderings = orderings
        elif model._meta.ordering:
            self.__parse_orderings(*model._meta.ordering)
        else:
            self._orderings = []

        self._distinct: bool = distinct
        self._limit: Optional[int] = limit
        self._offset: Optional[int] = offset

    def _copy(self, queryset) -> None:
        super()._copy(queryset)

        queryset._orderings = copy(self._orderings)
        queryset._distinct = self._distinct
        queryset._limit = self._limit
        queryset._offset = self._offset

    def order_by(self, *orderings: Union[str, Node]):
        """
        Accept args to filter by in format like this:

        .. code-block:: python3

            .order_by('name', '-tournament__name')

        Supports ordering by related models too.
        """
        queryset = self._clone()
        queryset.__parse_orderings(*orderings)
        return queryset

    def limit(self, limit: int):
        """
        Limits QuerySet to given length.
        """
        queryset = self._clone()
        queryset._limit = limit
        return queryset

    def offset(self, offset: int):
        """
        Query offset for QuerySet.
        """
        queryset = self._clone()
        queryset._offset = offset
        if self.capabilities.requires_limit and queryset._limit is None:
            queryset._limit = 1000000
        return queryset

    def distinct(self):
        """
        Make QuerySet distinct.

        Only makes sense in combination with a ``.values()`` or ``.values_list()`` as it
        precedes all the fetched fields with a distinct.
        """
        queryset = self._clone()
        queryset._distinct = True
        return queryset

    def first(self) -> FirstQuerySet[MODEL]:
        """
        Limit queryset to one object and return one object instead of list.
        """
        return FirstQuerySet(self.limit(1))

    def get(self, *args, **kwargs) -> GetQuerySet[MODEL]:
        """
        Fetch exactly one object matching the parameters or raise
        DoesNotExist or MultipleObjectsReturned exceptions
        """
        queryset = self._filter_or_exclude(negate=False, *args, **kwargs)
        queryset._limit = 2
        return GetQuerySet(queryset)

    def get_or_none(self, *args, **kwargs) -> FirstQuerySet[MODEL]:
        """
        Fetch exactly one object matching the parameters or
            return None if objects does not exist or
            raise MultipleObjectsReturned exception if multiple objects exist
        """
        queryset = self._filter_or_exclude(negate=False, *args, **kwargs)
        queryset._limit = 2
        return FirstQuerySet(queryset)

    def __parse_orderings(self, *orderings: Union[str, Node]) -> None:
        model = self.model

        parsed_orders: List[QueryOrdering] = []
        for ordering in orderings:
            if isinstance(ordering, Node):
                parsed_orders.append(QueryOrderingNode(ordering))

            elif isinstance(ordering, str):
                if ordering[0] == "-":
                    field_name = ordering[1:]
                    order_type = Order.desc
                else:
                    field_name = ordering
                    order_type = Order.asc

                if not (field_name.split(LOOKUP_SEP)[0] in model._meta.fields_map or field_name in self.annotations):
                    raise FieldError(f"Unknown field {field_name} for model {model.__name__}")

                parsed_orders.append(QueryOrderingField(field_name, order_type))

            else:
                raise ParamsError("Unknown ordering type: {} at {}".format(type(ordering), ordering))

        self._orderings = parsed_orders

    def __resolve_orderings(self, context: QueryContext) -> None:
        for ordering in self._orderings:
            ordering.resolve_into(self, context=context)

    def _add_query_details(self, context: QueryContext) -> None:
        super()._add_query_details(context)
        self.__resolve_orderings(context=context)
        if self._limit:
            self.query._limit = self._limit

        if self._offset:
            self.query._offset = self._offset

        if self._distinct:
            self.query._distinct = True

    async def __aiter__(self) -> AsyncIterator[Any]:
        for val in await self:
            yield val
