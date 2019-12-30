import asyncio
import datetime
import decimal
from copy import copy
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Type

from pypika import JoinType, Parameter, Table

from tortoise.context import QueryContext
from tortoise.exceptions import OperationalError
from tortoise.fields.base import Field
from tortoise.fields.relational import ManyToManyFieldInstance
from tortoise.query_utils import QueryModifier


if TYPE_CHECKING:  # pragma: nocoverage
    from tortoise.models import Model
    from tortoise.backends.base.client import BaseDBAsyncClient

EXECUTOR_CACHE: Dict[
    str, Tuple[list, str, list, str, Dict[str, Callable], str, Dict[str, str]]
] = {}


class BaseExecutor:
    TO_DB_OVERRIDE: Dict[Type[Field], Callable] = {}
    FILTER_FUNC_OVERRIDE: Dict[Callable, Callable] = {}
    EXPLAIN_PREFIX: str = "EXPLAIN"
    DB_NATIVE = {bytes, str, int, bool, float, decimal.Decimal, datetime.datetime, datetime.date}

    def __init__(
        self,
        model: "Type[Model]",
        db: "BaseDBAsyncClient",
        prefetch_map=None,
        prefetch_queries=None,
    ) -> None:
        self.model = model
        self.db: "BaseDBAsyncClient" = db
        self.prefetch_map = prefetch_map if prefetch_map else {}
        self._prefetch_queries = prefetch_queries if prefetch_queries else {}

        key = f"{self.db.connection_name}:{self.model._meta.table}"
        if key not in EXECUTOR_CACHE:
            self.regular_columns, columns = self._prepare_insert_columns()
            self.insert_query = self._prepare_insert_statement(columns)
            self.regular_columns_all = self.regular_columns
            self.insert_query_all = self.insert_query
            if self.model._meta.generated_db_fields:
                self.regular_columns_all, columns_all = self._prepare_insert_columns(
                    include_generated=True
                )
                self.insert_query_all = self._prepare_insert_statement(
                    columns_all, no_generated=True
                )

            self.column_map: Dict[str, Callable[[Any, Any], Any]] = {}
            for column in self.regular_columns_all:
                field_object = self.model._meta.fields_map[column]
                if field_object.__class__ in self.TO_DB_OVERRIDE:
                    self.column_map[column] = partial(
                        self.TO_DB_OVERRIDE[field_object.__class__], field_object
                    )
                else:
                    self.column_map[column] = field_object.to_db_value

            table = self.model._meta.basetable
            self.delete_query = str(
                self.model._meta.basequery.where(
                    table[self.model._meta.db_pk_field] == self.Parameter(0)
                ).delete()
            )
            self.update_cache: Dict[str, str] = {}

            EXECUTOR_CACHE[key] = (
                self.regular_columns,
                self.insert_query,
                self.regular_columns_all,
                self.insert_query_all,
                self.column_map,
                self.delete_query,
                self.update_cache,
            )
        else:
            (
                self.regular_columns,
                self.insert_query,
                self.regular_columns_all,
                self.insert_query_all,
                self.column_map,
                self.delete_query,
                self.update_cache,
            ) = EXECUTOR_CACHE[key]

    async def execute_explain(self, query) -> Any:
        sql = " ".join((self.EXPLAIN_PREFIX, query.get_sql()))
        return (await self.db.execute_query(sql))[1]

    async def execute_select(self, query, custom_fields: Optional[list] = None) -> list:
        _, raw_results = await self.db.execute_query(query.get_sql())
        instance_list = []
        for row in raw_results:
            instance: "Model" = self.model._init_from_db(**row)
            if custom_fields:
                for field in custom_fields:
                    setattr(instance, field, row[field])
            instance_list.append(instance)
        await self._execute_prefetch_queries(instance_list)
        return instance_list

    def _prepare_insert_columns(self, include_generated=False) -> Tuple[List[str], List[str]]:
        regular_columns = []
        for column in self.model._meta.fields_db_projection.keys():
            field_object = self.model._meta.fields_map[column]
            if include_generated or not field_object.generated:
                regular_columns.append(column)
        result_columns = [self.model._meta.fields_db_projection[c] for c in regular_columns]
        return regular_columns, result_columns

    @classmethod
    def _field_to_db(cls, field_object: Field, attr: Any, instance) -> Any:
        if field_object.__class__ in cls.TO_DB_OVERRIDE:
            return cls.TO_DB_OVERRIDE[field_object.__class__](field_object, attr, instance)
        return field_object.to_db_value(attr, instance)

    def _prepare_insert_statement(self, columns: List[str], no_generated: bool = False) -> str:
        # Insert should implement returning new id to saved object
        # Each db has it's own methods for it, so each implementation should
        # go to descendant executors
        return str(
            self.db.query_class.into(self.model._meta.basetable)
            .columns(*columns)
            .insert(*[self.Parameter(i) for i in range(len(columns))])
        )

    async def _process_insert_result(self, instance: "Model", results: Any):
        raise NotImplementedError()  # pragma: nocoverage

    def Parameter(self, pos: int) -> Parameter:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_insert(self, instance: "Model") -> None:
        if not instance._custom_generated_pk:
            values = [
                self.column_map[column](getattr(instance, column), instance)
                for column in self.regular_columns
            ]
            insert_result = await self.db.execute_insert(self.insert_query, values)
            await self._process_insert_result(instance, insert_result)
        else:
            values = [
                self.column_map[column](getattr(instance, column), instance)
                for column in self.regular_columns_all
            ]
            await self.db.execute_insert(self.insert_query_all, values)

    async def execute_bulk_insert(self, instances: "List[Model]") -> None:
        values_lists = [
            [
                self.column_map[column](getattr(instance, column), instance)
                for column in self.regular_columns
            ]
            for instance in instances
        ]
        await self.db.execute_many(self.insert_query, values_lists)

    def get_update_sql(self, update_fields: Optional[List[str]]) -> str:
        """
        Generates the SQL for updating a model depending on provided update_fields.
        Result is cached for performance.
        """
        key = ",".join(update_fields) if update_fields else ""
        if key in self.update_cache:
            return self.update_cache[key]

        table = self.model._meta.basetable
        query = self.db.query_class.update(table)
        count = 0
        for field in update_fields or self.model._meta.fields_db_projection.keys():
            db_field = self.model._meta.fields_db_projection[field]
            field_object = self.model._meta.fields_map[field]
            if not field_object.pk:
                query = query.set(db_field, self.Parameter(count))
                count += 1

        query = query.where(table[self.model._meta.db_pk_field] == self.Parameter(count))

        sql = self.update_cache[key] = query.get_sql()
        return sql

    async def execute_update(self, instance, update_fields: Optional[List[str]]) -> int:
        values = [
            self.column_map[field](getattr(instance, field), instance)
            for field in update_fields or self.model._meta.fields_db_projection.keys()
            if not self.model._meta.fields_map[field].pk
        ]
        values.append(self.model._meta.pk.to_db_value(instance.pk, instance))
        return (await self.db.execute_query(self.get_update_sql(update_fields), values))[0]

    async def execute_delete(self, instance) -> int:
        return (
            await self.db.execute_query(
                self.delete_query, [self.model._meta.pk.to_db_value(instance.pk, instance)]
            )
        )[0]

    async def _prefetch_reverse_relation(
        self, instance_list: list, field: str, related_query
    ) -> list:
        instance_id_set: set = {
            self._field_to_db(instance._meta.pk, instance.pk, instance)
            for instance in instance_list
        }
        relation_field = self.model._meta.fields_map[field].relation_field  # type: ignore

        related_object_list = await related_query.filter(
            **{f"{relation_field}__in": list(instance_id_set)}
        )

        related_object_map: Dict[str, list] = {}
        for entry in related_object_list:
            object_id = getattr(entry, relation_field)
            if object_id in related_object_map.keys():
                related_object_map[object_id].append(entry)
            else:
                related_object_map[object_id] = [entry]
        for instance in instance_list:
            relation_container = getattr(instance, field)
            relation_container._set_result_for_query(related_object_map.get(instance.pk, []))
        return instance_list

    async def _prefetch_reverse_o2o_relation(
        self, instance_list: list, field: str, related_query
    ) -> list:
        instance_id_set: set = {
            self._field_to_db(instance._meta.pk, instance.pk, instance)
            for instance in instance_list
        }
        relation_field = self.model._meta.fields_map[field].relation_field  # type: ignore

        related_object_list = await related_query.filter(
            **{f"{relation_field}__in": list(instance_id_set)}
        )

        related_object_map = {}
        for entry in related_object_list:
            object_id = getattr(entry, relation_field)
            related_object_map[object_id] = entry

        for instance in instance_list:
            setattr(instance, f"_{field}", related_object_map.get(instance.pk, None))

        return instance_list

    async def _prefetch_m2m_relation(self, instance_list: list, field: str, related_query) -> list:
        instance_id_set: set = {
            self._field_to_db(instance._meta.pk, instance.pk, instance)
            for instance in instance_list
        }

        field_object: ManyToManyFieldInstance = self.model._meta.fields_map[  # type: ignore
            field
        ]

        through_table = Table(field_object.through)

        subquery = (
            self.db.query_class.from_(through_table)
            .select(
                through_table[field_object.backward_key],
                through_table[field_object.forward_key],
            )
            .where(through_table[field_object.backward_key].isin(instance_id_set))
        )

        related_query_table = related_query.model._meta.basetable
        related_pk_field = related_query.model._meta.db_pk_field
        query = (
            related_query.query.join(subquery)
            .on(getattr(subquery, field_object.forward_key) == related_query_table[related_pk_field])
            .select(
                getattr(subquery, field_object.backward_key),
                *[related_query_table[field].as_(field) for field in related_query.fields],
            )
        )

        if related_query._q_objects:

            modifier = QueryModifier()

            context = QueryContext().push(
                related_query.model,
                related_query_table,
                { field_object.through: through_table.as_(subquery.alias) }
            )

            joined_tables: List[Table] = []
            for node in related_query._q_objects:
                modifier &= node.resolve(
                    context=context,
                    annotations=related_query._annotations,
                    custom_filters=related_query._custom_filters,
                )

            for join in modifier.joins:
                if join[0] not in joined_tables:
                    query = query.join(join[0], how=JoinType.left_outer).on(join[1])
                    joined_tables.append(join[0])

            if modifier.where_criterion:
                query = query.where(modifier.where_criterion)

            if modifier.having_criterion:
                query = query.having(modifier.having_criterion)

        _, raw_results = await self.db.execute_query(query.get_sql())
        relations = {
            (
                self.model._meta.pk.to_python_value(e[field_object.backward_key]),
                field_object.model_class._meta.pk.to_python_value(e[related_pk_field]),
            )
            for e in raw_results
        }
        related_object_list = [related_query.model._init_from_db(**e) for e in raw_results]
        await self.__class__(
            model=related_query.model, db=self.db, prefetch_map=related_query._prefetch_map
        ).fetch_for_list(related_object_list)
        related_object_map = {e.pk: e for e in related_object_list}
        relation_map: Dict[str, list] = {}

        for object_id, related_object_id in relations:
            if object_id not in relation_map:
                relation_map[object_id] = []
            relation_map[object_id].append(related_object_map[related_object_id])

        for instance in instance_list:
            relation_container = getattr(instance, field)
            relation_container._set_result_for_query(relation_map.get(instance.pk, []))
        return instance_list

    async def _prefetch_direct_relation(
        self, instance_list: list, field: str, related_query
    ) -> list:
        related_objects_for_fetch = set()
        relation_key_field = f"{field}_id"
        for instance in instance_list:
            if getattr(instance, relation_key_field):
                related_objects_for_fetch.add(getattr(instance, relation_key_field))
            else:
                setattr(instance, field, None)

        if related_objects_for_fetch:
            related_object_list = await related_query.filter(pk__in=list(related_objects_for_fetch))
            related_object_map = {obj.pk: obj for obj in related_object_list}
            for instance in instance_list:
                setattr(
                    instance, field, related_object_map.get(getattr(instance, relation_key_field))
                )
        return instance_list

    def _make_prefetch_queries(self) -> None:
        for field, forwarded_prefetches in self.prefetch_map.items():
            if field in self._prefetch_queries:
                related_query = self._prefetch_queries.get(field)
            else:
                related_model_field = self.model._meta.fields_map[field]
                related_model: "Type[Model]" = related_model_field.model_class  # type: ignore
                related_query = related_model.all().using_db(self.db)
                related_query.query = copy(related_query.model._meta.basequery)
            if forwarded_prefetches:
                related_query = related_query.prefetch_related(*forwarded_prefetches)
            self._prefetch_queries[field] = related_query

    async def _do_prefetch(self, instance_id_list: list, field: str, related_query) -> list:
        if field in self.model._meta.backward_fk_fields:
            return await self._prefetch_reverse_relation(instance_id_list, field, related_query)

        if field in self.model._meta.backward_o2o_fields:
            return await self._prefetch_reverse_o2o_relation(instance_id_list, field, related_query)

        if field in self.model._meta.m2m_fields:
            return await self._prefetch_m2m_relation(instance_id_list, field, related_query)
        return await self._prefetch_direct_relation(instance_id_list, field, related_query)

    async def _execute_prefetch_queries(self, instance_list: list) -> list:
        if instance_list and (self.prefetch_map or self._prefetch_queries):
            self._make_prefetch_queries()
            prefetch_tasks = [
                self._do_prefetch(instance_list, field, related_query)
                for field, related_query in self._prefetch_queries.items()
            ]
            await asyncio.gather(*prefetch_tasks)

        return instance_list

    async def fetch_for_list(self, instance_list: list, *args) -> list:
        self.prefetch_map = {}
        for relation in args:
            relation_split = relation.split("__")
            first_level_field = relation_split[0]
            if first_level_field not in self.model._meta.fetch_fields:
                raise OperationalError(
                    f"relation {first_level_field} for {self.model._meta.table} not found"
                )
            if first_level_field not in self.prefetch_map.keys():
                self.prefetch_map[first_level_field] = set()
            forwarded_prefetch = "__".join(relation_split[1:])
            if forwarded_prefetch:
                self.prefetch_map[first_level_field].add(forwarded_prefetch)
        await self._execute_prefetch_queries(instance_list)
        return instance_list

    @classmethod
    def get_overridden_filter_func(cls, filter_func: Callable) -> Optional[Callable]:
        return cls.FILTER_FUNC_OVERRIDE.get(filter_func)
