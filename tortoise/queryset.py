
from copy import copy
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Generator,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from pypika import JoinType, Table, EmptyCriterion
from pypika.functions import Count
from pypika.queries import QueryBuilder
from pypika.terms import ArithmeticExpression
from typing_extensions import Protocol

from tortoise.backends.base.client import BaseDBAsyncClient, Capabilities
from tortoise.constants import LOOKUP_SEP
from tortoise.context import QueryContext
from tortoise.exceptions import DoesNotExist, FieldError, IntegrityError, MultipleObjectsReturned
from tortoise.fields import JSONField

from tortoise.fields.relational import ForeignKey, OneToOneField
from tortoise.filters import EmptyCriterion as TortoiseEmptyCriterion
from tortoise.functions import Annotation
from tortoise.expressions import F

from tortoise.query_utils import Prefetch, Q, QueryModifier

# Empty placeholder - Should never be edited.
from tortoise.ordering import QueryOrdering

QUERY: QueryBuilder = QueryBuilder()

if TYPE_CHECKING:  # pragma: nocoverage
    from tortoise.models import Model

MODEL = TypeVar("MODEL", bound="Model")
T_co = TypeVar("T_co", covariant=True)


class QuerySetSingle(Protocol[T_co]):
    # pylint: disable=W0104
    def __await__(self) -> Generator[Any, None, T_co]:
        pass  # pragma: nocoverage


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
        table = context.stack[-1].table
        model = context.stack[-1].model

        for ordering in orderings:
            field_name = ordering.field_name
            if field_name in model._meta.fetch_fields:
                raise FieldError(
                    "Filtering by relation is not possible. Filter by nested field of related model"
                )

            if field_name.split(LOOKUP_SEP)[0] in model._meta.fetch_fields:
                relation_field_name = field_name.split(LOOKUP_SEP)[0]
                relation_field = model._meta.fields_map[relation_field_name]
                related_table = self._join_table_by_field(table, relation_field)
                context.push(relation_field.remote_model, related_table)
                self.__resolve_ordering(
                    context,
                    [QueryOrdering(LOOKUP_SEP.join(field_name.split(LOOKUP_SEP)[1:]), ordering.direction)],
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
                field = getattr(table, field_name)

                func = field_object.get_for_dialect(model._meta.db.capabilities.dialect, "function_cast")
                if func:
                    field = func(field_object, field)

                self.query = self.query.orderby(field, order=ordering.direction)

    async def __aiter__(self) -> AsyncIterator[Any]:
        for val in await self:
            yield val


class QuerySet(AwaitableQuery[MODEL]):
    __slots__ = (
        "fields",
        "_prefetch_map",
        "_prefetch_queries",
        "_single",
        "_get",
        "_count",
        "_db",
        "_filter_kwargs",
        "_having",
    )

    def __init__(self, model: Type[MODEL]) -> None:
        super().__init__(model)
        self.fields = model._meta.db_columns

        self._prefetch_map: Dict[str, Set[str]] = {}
        self._prefetch_queries: Dict[str, QuerySet] = {}
        self._single: bool = False
        self._get: bool = False
        self._count: bool = False
        self._filter_kwargs: Dict[str, Any] = {}

        self._having: Dict[str, Any] = {}

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
        queryset._count = self._count
        queryset._db = self._db
        queryset._limit = self._limit
        queryset._offset = self._offset
        queryset._filter_kwargs = copy(self._filter_kwargs)
        queryset._orderings = copy(self._orderings)
        queryset._joined_tables = copy(self._joined_tables)
        queryset._distinct = self._distinct
        queryset._having = copy(self._having)

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

    def values_list(self, *fields_: str, flat: bool = False) -> "ValuesListQuery":
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

    def values(self, *args: str, **kwargs: str) -> "ValuesQuery":
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

    def delete(self) -> "DeleteQuery":
        """
        Delete all objects in QuerySet.
        """
        return DeleteQuery(
            db=self._db,
            model=self.model,
            q_objects=self.q_objects,
            annotations=self.annotations,
        )

    def update(self, **kwargs) -> "UpdateQuery":
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

    def count(self) -> "CountQuery":
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


class UpdateQuery(AwaitableStatement):
    __slots__ = ("update_kwargs",)

    def __init__(self, model, update_kwargs, db, q_objects, annotations) -> None:
        super().__init__(model, db, q_objects, annotations)
        self.update_kwargs = update_kwargs

    def _make_query(self, context: QueryContext, alias=None) -> None:
        table = self.model._meta.basetable
        self.query = self._db.query_class.update(table)

        context.push(self.model, table)
        self.resolve_filters(context=context)

        # Need to get executor to get correct column_map
        executor = self._db.executor_class(model=self.model, db=self._db)

        for key, value in self.update_kwargs.items():
            field_object = self.model._meta.fields_map.get(key)

            if not field_object:
                raise FieldError(f"Unknown keyword argument {key} for model {self.model}")

            if field_object.primary_key:
                raise IntegrityError(f"Field {key} is primary key and can not be updated")

            if isinstance(field_object, (ForeignKey, OneToOneField)):
                fk_field: str = field_object.db_column  # type: ignore
                column_name = self.model._meta.fields_map[fk_field].db_column
                value = executor.column_map[fk_field](value.pk, None)
            else:
                try:
                    column_name = self.model._meta.field_to_db_column_name_map[key]
                except KeyError:
                    raise FieldError(f"Field {key} is virtual and can not be updated")

                if isinstance(value, (F, ArithmeticExpression)):
                    value = F.resolve(self.model._meta.field_to_db_column_name_map, value)

                else:
                    value = executor.column_map[key](value, None)  # type: ignore

            self.query = self.query.set(column_name, value)

        context.pop()

    async def _execute(self) -> int:
        return (await self._db.execute_query(str(self.query)))[0]


class DeleteQuery(AwaitableStatement):
    __slots__ = ()

    def __init__(self, model, db, q_objects, annotations) -> None:
        super().__init__(model, db, q_objects, annotations)

    def _make_query(self, context: QueryContext, alias=None) -> None:
        self.query = self.create_base_query(alias)
        context.push(self.model, self.query._from[-1])
        self.resolve_filters(context=context)
        self.query._delete_from = True
        context.pop()

    async def _execute(self) -> int:
        return (await self._db.execute_query(str(self.query)))[0]


class CountQuery(AwaitableStatement):
    __slots__ = ()

    def __init__(self, model, db, q_objects, annotations) -> None:
        super().__init__(model, db, q_objects, annotations)

    def _make_query(self, context: QueryContext, alias=None) -> None:
        self.query = copy(self.model._meta.basequery)
        context.push(self.model, self.query._from[-1])
        self.resolve_filters(context=context)
        self.query._select_other(Count("*"))
        context.pop()

    async def _execute(self) -> int:
        _, result = await self._db.execute_query(str(self.query))
        return list(dict(result[0]).values())[0]


class FieldSelectQuery(AwaitableQuery):
    # pylint: disable=W0223
    __slots__ = (
        "fields_for_select",
    )

    def __init__(self, model, db, q_objects, annotations,
        orderings, distinct, limit, offset) -> None:

        super().__init__(model, db, q_objects, annotations,
            orderings, distinct, limit, offset)

    def _join_table_with_forwarded_fields(
        self, context: QueryContext, field_name: str, forwarded_fields: str
    ) -> Tuple[Table, str]:

        context_item = context.stack[-1]
        model = context_item.model
        table = context_item.table

        if field_name in model._meta.field_to_db_column_name_map and not forwarded_fields:
            return table, model._meta.field_to_db_column_name_map[field_name]

        if field_name in model._meta.field_to_db_column_name_map and forwarded_fields:
            raise FieldError(f'Field "{field_name}" for model "{model.__name__}" is not relation')

        if field_name in self.model._meta.fetch_fields and not forwarded_fields:
            raise ValueError(
                'Selecting relation "{}" is not possible, select concrete '
                "field on related model".format(field_name)
            )

        field_object = model._meta.fields_map.get(field_name)
        if not field_object:
            raise FieldError(f'Unknown field "{field_name}" for model "{model.__name__}"')

        field_table = self._join_table_by_field(table, field_object)
        forwarded_fields_split = forwarded_fields.split(LOOKUP_SEP)

        context.push(field_object.remote_model, field_table)
        output = self._join_table_with_forwarded_fields(
            context=context,
            field_name=forwarded_fields_split[0],
            forwarded_fields=LOOKUP_SEP.join(forwarded_fields_split[1:]),
        )
        context.pop()
        return output

    def add_field_to_select_query(self, context: QueryContext, field_name, return_as) -> None:
        table = context.stack[-1].table

        if field_name == "pk":
            field_name = self.model._meta.pk_attr

        if field_name in self.model._meta.field_to_db_column_name_map:
            db_column = self.model._meta.field_to_db_column_name_map[field_name]
            self.query._select_field(table[db_column].as_(return_as))
            return

        if field_name in self.model._meta.fetch_fields:
            raise ValueError(
                'Selecting relation "{}" is not possible, select '
                "concrete field on related model".format(field_name)
            )

        if field_name in self.annotations:
            annotation = self.annotations[field_name]
            annotation_info = annotation.resolve(context=context)
            self.query._select_other(annotation_info.field.as_(return_as))
            return

        base_field_name, _, sub_field = field_name.partition(LOOKUP_SEP)
        if base_field_name in self.model._meta.fetch_fields:
            context.push(model=self.model, table=self.model._meta.basetable)
            related_table, related_db_column = self._join_table_with_forwarded_fields(
                context=context, field_name=base_field_name, forwarded_fields=sub_field)
            context.pop()

            self.query._select_field(related_table[related_db_column].as_(return_as))
            return

        base_field = self.model._meta.fields_map.get(base_field_name)
        if isinstance(base_field, JSONField):
            path = "{{{}}}".format(sub_field.replace(LOOKUP_SEP, ','))
            db_column = self.model._meta.field_to_db_column_name_map[base_field_name]
            self.query._select_other(table[db_column].get_path_json_value(path).as_(return_as))
            return

        raise FieldError(f'Unknown field "{field_name}" for model "{self.model.__name__}"')

    def resolve_to_python_value(self, model: "Type[Model]", field_name: str) -> Callable:
        if field_name in model._meta.fetch_fields:
            return lambda x: x

        if field_name in self.annotations:
            field_object = self.annotations[field_name].field_object
            if field_object:
                return field_object.to_python_value
            else:
                return lambda x: x

        if field_name in model._meta.fields_map:
            field_object = model._meta.fields_map[field_name]
            if (field_object.skip_to_python_if_native and
                field_object.field_type in model._meta.db.executor_class.DB_NATIVE
            ):
                return lambda x: x
            else:
                return field_object.to_python_value

        base_field_name, _, sub_field = field_name.partition(LOOKUP_SEP)
        if base_field_name in model._meta.fetch_fields:
            remote_model = model._meta.fields_map[base_field_name].remote_model  # type: ignore
            return self.resolve_to_python_value(remote_model, sub_field)

        base_field_object = model._meta.fields_map.get(base_field_name)
        if isinstance(base_field_object, JSONField):
            return base_field_object.to_python_value

        raise FieldError(f'Unknown field "{field_name}" for model "{model}"')

    def _make_query(self, context: QueryContext, alias=None) -> None:
        self.query = self.create_base_query(alias)
        context.push(self.model, self.query._from[-1])

        for return_as, field_name in self.fields_for_select.items():
            self.add_field_to_select_query(context, field_name, return_as)

        self.resolve_filters(context=context)
        if self._limit:
            self.query._limit = self._limit
        if self._offset:
            self.query._offset = self._offset
        if self._distinct:
            self.query._distinct = True

        self.resolve_ordering(context=context)
        context.pop()


class ValuesListQuery(FieldSelectQuery):
    __slots__ = (
        "flat",
        "fields_for_select_list",
    )

    def __init__(
        self, model, db, q_objects, annotations, orderings, distinct,
        limit, offset, fields_for_select_list, flat,
    ) -> None:
        super().__init__(model, db, q_objects, annotations, orderings, distinct, limit, offset)

        if flat and (len(fields_for_select_list) != 1):
            raise TypeError("You can flat value_list only if contains one field")

        self.fields_for_select = {str(i): field for i, field in enumerate(fields_for_select_list)}
        self.fields_for_select_list = fields_for_select_list
        self.flat = flat

    async def _execute(self) -> List[Any]:
        _, result = await self._db.execute_query(str(self.query))
        columns = [
            (key, self.resolve_to_python_value(self.model, name))
            for key, name in sorted(list(self.fields_for_select.items()))
        ]
        if self.flat:
            func = columns[0][1]
            flatmap = lambda entry: func(entry["0"])  # noqa
            return list(map(flatmap, result))

        listmap = lambda entry: tuple(func(entry[column]) for column, func in columns)  # noqa
        return list(map(listmap, result))


class ValuesQuery(FieldSelectQuery):
    __slots__ = ()

    def __init__(
        self, model, db, q_objects, annotations, orderings, distinct,
        limit, offset, fields_for_select,
    ) -> None:
        super().__init__(model, db, q_objects, annotations, orderings, distinct, limit, offset)
        self.fields_for_select = fields_for_select

    async def _execute(self) -> List[dict]:
        column_mappers = [
            (alias, self.resolve_to_python_value(self.model, field_name))
            for alias, field_name in self.fields_for_select.items()
        ]

        result = await self._db.execute_query_dict(str(self.query))
        for row in result:
            for col_name, col_mapper in column_mappers:
                row[col_name] = col_mapper(row[col_name])

        return result
