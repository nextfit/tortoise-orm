
from copy import copy
from typing import Generic, Type, List, TYPE_CHECKING, Dict, Generator, Any, Optional, AsyncIterator, TypeVar

from pypika import Table, JoinType, EmptyCriterion
from pypika.queries import QueryBuilder

from tortoise import BaseDBAsyncClient
from tortoise.backends.base.client import Capabilities
from tortoise.constants import LOOKUP_SEP
from tortoise.context import QueryContext
from tortoise.exceptions import FieldError
from tortoise.filters import EmptyCriterion as TortoiseEmptyCriterion, QueryModifier
from tortoise.filters.q import Q
from tortoise.functions import Annotation
from tortoise.ordering import QueryOrdering

if TYPE_CHECKING:  # pragma: nocoverage
    from tortoise.models import Model


MODEL = TypeVar("MODEL", bound="Model")
QUERY: QueryBuilder = QueryBuilder()


class AwaitableStatement(Generic[MODEL]):
    __slots__ = (
        "_joined_tables",
        "_db",
        "query",
        "model",
        "capabilities",
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

    def resolve_filters(self, context: QueryContext) -> None:
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
            self._parse_orderings(*model._meta.ordering) if model._meta.ordering \
            else []

        self._distinct: bool = distinct
        self._limit: Optional[int] = limit
        self._offset: Optional[int] = offset

    def _parse_orderings(self, *orderings: str) -> "List[QueryOrdering]":
        return QueryOrdering.parse_orderings(self.model, self.annotations, *orderings)

    def resolve_ordering(self, context: QueryContext) -> None:
        self.__resolve_ordering(context, self._orderings, self.annotations)

    def __resolve_ordering(self, context: QueryContext, orderings, annotations) -> None:
        table = context.top.table
        model = context.top.model

        for ordering in orderings:
            field_name = ordering.field_name
            if field_name in model._meta.fetch_fields:
                raise FieldError(
                    "Filtering by relation is not possible. Filter by nested field of related model"
                )

            relation_field_name, _, field_sub = field_name.partition(LOOKUP_SEP)
            if relation_field_name in model._meta.fetch_fields:
                relation_field = model._meta.fields_map[relation_field_name]
                related_table = self._join_table_by_field(table, relation_field)
                context.push(relation_field.remote_model, related_table)
                self.__resolve_ordering(
                    context,
                    [QueryOrdering(field_sub, ordering.direction)],
                    {},
                )
                context.pop()

            elif field_name in annotations:
                annotation = annotations[field_name]
                annotation_info = annotation.resolve(QueryContext().push(self.model, self.model._meta.basetable))
                self.query = self.query.orderby(annotation_info.field, order=ordering.direction)

            else:
                field_object = model._meta.fields_map.get(field_name)
                if not field_object:
                    raise FieldError(f"Unknown field {field_name} for model {model.__name__}")
                field_name = field_object.db_column or field_name
                field = table[field_name]

                func = field_object.get_for_dialect(model._meta.db.capabilities.dialect, "function_cast")
                if func:
                    field = func(field_object, field)

                self.query = self.query.orderby(field, order=ordering.direction)

    async def __aiter__(self) -> AsyncIterator[Any]:
        for val in await self:
            yield val
