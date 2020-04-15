
from copy import copy
from typing import Generic, Type, List, TYPE_CHECKING, Dict, Generator, Any, Optional, AsyncIterator, TypeVar, Union

from pypika import Table, JoinType, EmptyCriterion, Order
from pypika.queries import QueryBuilder

from tortoise import BaseDBAsyncClient
from tortoise.backends.base.client import Capabilities
from tortoise.constants import LOOKUP_SEP
from tortoise.context import QueryContext
from tortoise.exceptions import FieldError, ParamsError
from tortoise.filters import EmptyCriterion as TortoiseEmptyCriterion, QueryModifier
from tortoise.filters.q import Q
from tortoise.functions import Annotation
from tortoise.ordering import QueryOrdering, QueryOrderingField

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

    def __resolve_filters(self, context: QueryContext) -> None:
        modifier = QueryModifier()
        for node in self.q_objects:
            modifier &= node.resolve(context, self.annotations)

        for join in modifier.joins:
            if join[0] not in self._joined_tables:
                self.query = self.query.join(join[0], how=JoinType.left_outer).on(join[1])
                self._joined_tables.append(join[0])

        if not isinstance(modifier.where_criterion, (EmptyCriterion, TortoiseEmptyCriterion)):
            if not self.query._validate_table(modifier.where_criterion):
                self.query._foreign_table = True

        self.query._wheres = modifier.where_criterion
        self.query._havings = modifier.having_criterion

    def _join_table_by_field(self, table, relation_field) -> Table:
        """
        :param table:
        :param relation_field:
        :return: related_table
        """

        joins = relation_field.get_joins(table)
        for join in joins:
            if join[0] not in self._joined_tables:
                self.query = self.query.join(join[0], how=JoinType.left_outer).on(join[1])
                self._joined_tables.append(join[0])

        return joins[-1][0]

    def create_base_query(self, alias):
        if alias:
            table = Table(self.model._meta.db_table, alias=alias)
            return self.model._meta.db.query_class.from_(table)
        else:
            return copy(self.model._meta.basequery)

    def create_base_query_all_fields(self, alias):
        return self.create_base_query(alias).select(*self.model._meta.db_columns)

    def _add_query_details(self, context: QueryContext) -> None:
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

        self._orderings: List[QueryOrdering] = \
            orderings if orderings else \
            self.__parse_orderings(*model._meta.ordering) if model._meta.ordering \
            else []

        self._distinct: bool = distinct
        self._limit: Optional[int] = limit
        self._offset: Optional[int] = offset

    def _copy(self, queryset) -> None:
        super()._copy(queryset)

        queryset._orderings = copy(self._orderings)
        queryset._distinct = self._distinct
        queryset._limit = self._limit
        queryset._offset = self._offset

    def order_by(self, *orderings: Union[str, QueryOrdering]):
        """
        Accept args to filter by in format like this:

        .. code-block:: python3

            .order_by('name', '-tournament__name')

        Supports ordering by related models too.
        """
        queryset = self._clone()
        queryset._orderings = self.__parse_orderings(*orderings)
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

    def __parse_orderings(self, *orderings: Union[str, QueryOrdering]) -> "List[QueryOrdering]":
        model = self.model

        output = []
        for ordering in orderings:
            if isinstance(ordering, QueryOrdering):
                output.append(ordering)

            elif isinstance(ordering, str):
                if ordering[0] == "-":
                    field_name = ordering[1:]
                    order_type = Order.desc
                else:
                    field_name = ordering
                    order_type = Order.asc

                if not (field_name.split(LOOKUP_SEP)[0] in model._meta.fields_map or field_name in self.annotations):
                    raise FieldError(f"Unknown field {field_name} for model {model.__name__}")

                output.append(QueryOrderingField(field_name, order_type))

            else:
                raise ParamsError("Unknown ordering type: {} at {}".format(type(ordering), ordering))

        return output

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
