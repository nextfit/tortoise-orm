
from copy import copy, deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    Generator,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

from pypika import Order
from pypika.queries import QueryBuilder
from pypika.terms import Node

from tortoise.backends.base.client import BaseDBAsyncClient, Capabilities
from tortoise.exceptions import ParamsError
from tortoise.filters.q import Q
from tortoise.query.annotations import Annotation
from tortoise.query.context import QueryContext
from tortoise.query.ordering import QueryOrdering, QueryOrderingField, QueryOrderingNode
from tortoise.query.single import FirstQuerySet, GetQuerySet

if TYPE_CHECKING:
    from tortoise.models import Model


MODEL = TypeVar("MODEL", bound="Model")
STATEMENT = TypeVar('STATEMENT', bound='AwaitableStatement')


class AwaitableStatement(Generic[MODEL]):
    __slots__ = (
        "_db",
        "capabilities",
        "model",
        "annotations",
        "q_objects",
    )

    def __init__(self, model: Type[MODEL], db=None, q_objects=None, annotations=None) -> None:
        self._db: BaseDBAsyncClient = db

        self.model: Type[MODEL] = model
        self.capabilities: Capabilities = model._meta.db.capabilities

        self.q_objects: List[Q] = q_objects or []
        self.annotations: Dict[str, Annotation] = annotations or {}

    def _copy(self, queryset) -> None:
        queryset._db = self._db
        queryset.capabilities = self.capabilities
        queryset.model = self.model
        queryset.q_objects = deepcopy(self.q_objects)
        queryset.annotations = deepcopy(self.annotations)

    def _clone(self: STATEMENT) -> STATEMENT:
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
            annotation.resolve_into(self, context=context)

    def is_aggregate(self) -> bool:
        return any([annotation.field.is_aggregate for annotation in self.annotations.values()])

    def query_builder(self, alias=None) -> QueryBuilder:
        meta = self.model._meta
        return meta.db.query_class.from_(meta.table(alias))

    def _add_query_details(self, context: QueryContext) -> None:
        self.__resolve_annotations(context=context)
        self.__resolve_filters(context)

    def _get_db_client(self) -> BaseDBAsyncClient:
        return self._db or self.model._meta.db

    @property
    def query(self) -> QueryBuilder:
        return self.create_query(None)

    def create_query(self, parent_context: Optional[QueryContext]) -> QueryBuilder:
        raise NotImplementedError()  # pragma: nocoverage

    async def _execute(self) -> Any:
        raise NotImplementedError()  # pragma: nocoverage

    def __await__(self) -> Generator[Any, None, List[MODEL]]:
        return self._execute().__await__()

    def using_db(self, _db: BaseDBAsyncClient) -> "AwaitableStatement[MODEL]":
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

        db_client = self._get_db_client()
        return await db_client\
            .executor_class(model=self.model, db=db_client)\
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
            context.query._limit = self._limit

        if self._offset:
            context.query._offset = self._offset

        if self._distinct:
            context.query._distinct = True

    async def __aiter__(self) -> AsyncIterator[Any]:
        for val in await self:
            yield val
