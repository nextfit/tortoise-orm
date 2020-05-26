
import asyncio
import datetime
import decimal
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Type, Set

from pypika import Parameter

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import ParamsError, UnknownFieldError, NotARelationFieldError
from tortoise.fields.base import Field

if TYPE_CHECKING:  # pragma: nocoverage
    from tortoise.models import Model
    from tortoise.backends.base.client import BaseDBAsyncClient
    from tortoise.query.queryset import QuerySet


EXECUTOR_CACHE: Dict[
    str, Tuple[list, str, list, str, Dict[str, Callable], str, Dict[str, str]]
] = {}


class BaseExecutor:
    TO_DB_OVERRIDE: Dict[Type[Field], Callable] = {}
    EXPLAIN_PREFIX: str = "EXPLAIN"
    DB_NATIVE = {bytes, str, int, bool, float, decimal.Decimal, datetime.datetime, datetime.date}

    def __init__(
        self,
        model: Type["Model"],
        db: "BaseDBAsyncClient",
        prefetch_map: Optional[Dict[str, Set[str]]] = None,
        prefetch_queries: Optional[Dict[str, 'QuerySet']] = None,
        select_related: Optional[Dict[str, Dict]] = None,
    ) -> None:
        self.model = model
        self.db: "BaseDBAsyncClient" = db
        self._prefetch_map = prefetch_map or {}
        self._prefetch_queries = prefetch_queries or {}
        self._select_related = select_related or {}

        self._prefetch_map: Dict[str, Set[str]] = prefetch_map or {}
        self._prefetch_queries: Dict[str, 'QuerySet'] = prefetch_queries or {}
        self._select_related: Dict[str, Dict] = select_related or {}

        key = f"{self.db.connection_name}:{self.model._meta.db_table}"
        if key in EXECUTOR_CACHE:
            (
                self.field_names,
                self.insert_query,
                self.all_field_names,
                self.insert_query_all,
                self.column_map,
                self.delete_query,
                self.update_cache,
            ) = EXECUTOR_CACHE[key]

        else:
            self.field_names, column_names = self._prepare_insert_columns()
            self.insert_query = self._prepare_insert_statement(column_names)

            if self.model._meta.generated_column_names:
                self.all_field_names, all_column_names = \
                    self._prepare_insert_columns(include_generated=True)
                self.insert_query_all = \
                    self._prepare_insert_statement(all_column_names)

            else:
                self.all_field_names = self.field_names
                self.insert_query_all = self.insert_query

            self.column_map: Dict[str, Callable[[Any, Any], Any]] = {}
            for field_name in self.all_field_names:
                field_object = self.model._meta.fields_map[field_name]
                if field_object.__class__ in self.TO_DB_OVERRIDE:
                    self.column_map[field_name] = partial(
                        self.TO_DB_OVERRIDE[field_object.__class__], field_object
                    )
                else:
                    self.column_map[field_name] = field_object.to_db_value

            table = self.model._meta.table()
            self.delete_query = str(
                db.query_class.from_(table)
                    .where(table[self.model._meta.pk_db_column] == self.parameter(0))
                    .delete()
            )

            self.update_cache: Dict[str, str] = {}

            EXECUTOR_CACHE[key] = (
                self.field_names,
                self.insert_query,
                self.all_field_names,
                self.insert_query_all,
                self.column_map,
                self.delete_query,
                self.update_cache,
            )

    async def execute_explain(self, query) -> Any:
        sql = " ".join((self.EXPLAIN_PREFIX, str(query)))
        return (await self.db.execute_query(sql))[2]

    async def execute_select(self, query, custom_fields: Optional[list] = None) -> list:
        _, db_columns, raw_results = await self.db.execute_query(str(query))

        instance_list = []
        for row in raw_results:
            row_iter = iter(zip(db_columns, row))
            instance = self.model._init_from_db_row(row_iter, self._select_related)

            if custom_fields:
                for field_name in custom_fields:
                    db_column, value = next(row_iter)
                    setattr(instance, field_name, value)

            instance_list.append(instance)

        await self._execute_prefetch_queries(instance_list)
        return instance_list

    def _prepare_insert_columns(self, include_generated=False) -> Tuple[List[str], List[str]]:
        field_column_name = [(field_name, field.db_column)
            for field_name, field in self.model._meta.fields_map.items()
            if field.has_db_column and (include_generated or not field.generated)
        ]

        # return fields_names, column_names
        return tuple(zip(*field_column_name))

    @classmethod
    def _field_to_db(cls, field_object: Field, attr: Any, instance) -> Any:
        if field_object.__class__ in cls.TO_DB_OVERRIDE:
            return cls.TO_DB_OVERRIDE[field_object.__class__](field_object, attr, instance)
        return field_object.to_db_value(attr, instance)

    def _prepare_insert_statement(self, columns: List[str]) -> str:
        return self.db.query_class.into(self.model._meta.table())\
            .columns(*columns)\
            .insert(*[self.parameter(i) for i in range(len(columns))])\
            .get_sql()

    async def _process_insert_result(self, instance: "Model", results: Any):
        raise NotImplementedError()  # pragma: nocoverage

    def parameter(self, pos: int) -> Parameter:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_insert(self, instance: "Model") -> None:
        if instance._custom_generated_pk:
            values = [
                self.column_map[field_name](getattr(instance, field_name), instance)
                for field_name in self.all_field_names
            ]
            await self.db.execute_insert(self.insert_query_all, values)

        else:
            values = [
                self.column_map[field_name](getattr(instance, field_name), instance)
                for field_name in self.field_names
            ]
            insert_result = await self.db.execute_insert(self.insert_query, values)
            await self._process_insert_result(instance, insert_result)

    async def execute_bulk_insert(self, instances: "List[Model]") -> None:
        values_lists = [
            [
                self.column_map[field_name](getattr(instance, field_name), instance)
                for field_name in self.field_names
            ]
            for instance in instances
        ]
        await self.db.execute_many(self.insert_query, values_lists)

    def _get_update_sql(self, update_fields: Optional[List[str]]) -> str:
        """
        Generates the SQL for updating a model depending on provided update_fields.
        Result is cached for performance.
        """
        key = ",".join(update_fields) if update_fields else ""
        if key in self.update_cache:
            return self.update_cache[key]

        table = self.model._meta.table()
        query = self.db.query_class.update(table)
        count = 0

        for field_name in update_fields or self.model._meta.field_to_db_column_name_map.keys():
            field_object = self.model._meta.fields_map[field_name]
            if not field_object.primary_key:
                query = query.set(field_object.db_column, self.parameter(count))
                count += 1

        query = query.where(table[self.model._meta.pk_db_column] == self.parameter(count))

        sql = self.update_cache[key] = query.get_sql()
        return sql

    async def execute_update(self, instance, update_fields: Optional[List[str]]) -> int:
        values = [
            self.column_map[field_name](getattr(instance, field_name), instance)
            for field_name in update_fields or self.model._meta.field_to_db_column_name_map.keys()
            if not self.model._meta.fields_map[field_name].primary_key
        ]
        values.append(self.model._meta.pk.to_db_value(instance.pk, instance))
        return (await self.db.execute_query(self._get_update_sql(update_fields), values))[0]

    async def execute_bulk_update(self, instances: "List[Model]", update_fields: List[str]) -> None:
        if not update_fields:
            raise ParamsError("Update fields must be provided for bulk update")

        if any(self.model._meta.fields_map[field_name].primary_key for field_name in update_fields):
            raise ParamsError("Cannot update primary key")

        values_lists = [
            [self.column_map[field_name](getattr(instance, field_name), instance)
                for field_name in update_fields] +
            [self.model._meta.pk.to_db_value(instance.pk, instance)]

            for instance in instances
        ]

        await self.db.execute_many(self._get_update_sql(update_fields), values_lists)

    async def execute_delete(self, instance) -> int:
        return (
            await self.db.execute_query(
                self.delete_query, [self.model._meta.pk.to_db_value(instance.pk, instance)]
            )
        )[0]

    def _make_prefetch_queries(self) -> None:
        for field_name, forwarded_prefetches in self._prefetch_map.items():
            if field_name in self._prefetch_queries:
                related_query = self._prefetch_queries.get(field_name)
            else:
                relation_field = self.model._meta.fields_map[field_name]
                remote_model = relation_field.remote_model
                related_query = remote_model.all().using_db(self.db)

            if forwarded_prefetches:
                related_query = related_query.prefetch_related(*forwarded_prefetches)

            self._prefetch_queries[field_name] = related_query

    async def _execute_prefetch_queries(self, instance_list: list) -> list:
        if instance_list and (self._prefetch_map or self._prefetch_queries):
            self._make_prefetch_queries()
            fields_map = self.model._meta.fields_map

            prefetch_tasks = [
                fields_map[field_name].prefetch(instance_list, related_query)
                for field_name, related_query in self._prefetch_queries.items()
            ]
            await asyncio.gather(*prefetch_tasks)

        return instance_list

    async def fetch_for_list(self, instance_list: list, *args) -> list:
        self._prefetch_map = {}
        for relation in args:
            first_level_field, _, forwarded_prefetch = relation.partition(LOOKUP_SEP)
            field_object = self.model._meta.fields_map.get(first_level_field)
            if not field_object:
                raise UnknownFieldError(first_level_field, self.model)

            if field_object.has_db_column:
                raise NotARelationFieldError(first_level_field, self.model)

            if first_level_field not in self._prefetch_map.keys():
                self._prefetch_map[first_level_field] = set()

            if forwarded_prefetch:
                self._prefetch_map[first_level_field].add(forwarded_prefetch)

        await self._execute_prefetch_queries(instance_list)
        return instance_list
