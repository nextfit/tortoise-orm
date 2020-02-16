
import asyncio
import datetime
import decimal
import operator
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Type

from pypika import Parameter

from tortoise.exceptions import OperationalError
from tortoise.fields.base import Field
import tortoise.filters as tf

if TYPE_CHECKING:  # pragma: nocoverage
    from tortoise.models import Model
    from tortoise.backends.base.client import BaseDBAsyncClient

EXECUTOR_CACHE: Dict[
    str, Tuple[list, str, list, str, Dict[str, Callable], str, Dict[str, str]]
] = {}


class BaseExecutor:
    TO_DB_OVERRIDE: Dict[Type[Field], Callable] = {}
    EXPLAIN_PREFIX: str = "EXPLAIN"
    DB_NATIVE = {bytes, str, int, bool, float, decimal.Decimal, datetime.datetime, datetime.date}

    FILTER_FUNC_MAP = {
        "": (operator.eq, None),
        "not": (tf.not_equal, None),
        "in": (tf.is_in, tf.list_encoder),
        "not_in": (tf.not_in, tf.list_encoder),
        "isnull": (tf.is_null, tf.bool_encoder),
        "not_isnull": (tf.not_null, tf.bool_encoder),
        "gte": (operator.ge, None),
        "lte": (operator.le, None),
        "gt": (operator.gt, None),
        "lt": (operator.lt, None),
        "contains": (tf.contains, tf.string_encoder),
        "startswith": (tf.starts_with, tf.string_encoder),
        "endswith": (tf.ends_with, tf.string_encoder),
        "iexact": (tf.insensitive_exact, tf.string_encoder),
        "icontains": (tf.insensitive_contains, tf.string_encoder),
        "istartswith": (tf.insensitive_starts_with, tf.string_encoder),
        "iendswith": (tf.insensitive_ends_with, tf.string_encoder),
    }

    def __init__(
        self,
        model: "Type[Model]",
        db: "BaseDBAsyncClient",
        prefetch_map=None,
        prefetch_queries=None,
    ) -> None:
        self.model = model
        self.db: "BaseDBAsyncClient" = db
        self.prefetch_map = prefetch_map or {}
        self._prefetch_queries = prefetch_queries or {}

        key = f"{self.db.connection_name}:{self.model._meta.table}"
        if key not in EXECUTOR_CACHE:
            self.regular_columns, columns = self._prepare_insert_columns()
            self.insert_query = self._prepare_insert_statement(columns)

            if self.model._meta.generated_column_names:
                self.regular_columns_all, columns_all = self._prepare_insert_columns(
                    include_generated=True
                )
                self.insert_query_all = self._prepare_insert_statement(
                    columns_all, no_generated=True
                )
            else:
                self.regular_columns_all = self.regular_columns
                self.insert_query_all = self.insert_query

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
                    table[self.model._meta.db_pk_field] == self.parameter(0)
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
        fields_map = self.model._meta.fields_map
        regular_fields_names = [field_name
            for field_name in self.model._meta.field_to_db_column_name_map.keys()
            if include_generated or not fields_map[field_name].generated
        ]

        column_names = [self.model._meta.field_to_db_column_name_map[c]
            for c in regular_fields_names]

        return regular_fields_names, column_names

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
            .insert(*[self.parameter(i) for i in range(len(columns))])
        )

    async def _process_insert_result(self, instance: "Model", results: Any):
        raise NotImplementedError()  # pragma: nocoverage

    def parameter(self, pos: int) -> Parameter:
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
        for field in update_fields or self.model._meta.field_to_db_column_name_map.keys():
            db_field = self.model._meta.field_to_db_column_name_map[field]
            field_object = self.model._meta.fields_map[field]
            if not field_object.pk:
                query = query.set(db_field, self.parameter(count))
                count += 1

        query = query.where(table[self.model._meta.db_pk_field] == self.parameter(count))

        sql = self.update_cache[key] = query.get_sql()
        return sql

    async def execute_update(self, instance, update_fields: Optional[List[str]]) -> int:
        values = [
            self.column_map[field](getattr(instance, field), instance)
            for field in update_fields or self.model._meta.field_to_db_column_name_map.keys()
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

    def _make_prefetch_queries(self) -> None:
        for field, forwarded_prefetches in self.prefetch_map.items():
            if field in self._prefetch_queries:
                related_query = self._prefetch_queries.get(field)
            else:
                related_model_field = self.model._meta.fields_map[field]
                related_model: "Type[Model]" = related_model_field.model_class  # type: ignore
                related_query = related_model.all().using_db(self.db)

            if forwarded_prefetches:
                related_query = related_query.prefetch_related(*forwarded_prefetches)

            self._prefetch_queries[field] = related_query

    async def _execute_prefetch_queries(self, instance_list: list) -> list:
        if instance_list and (self.prefetch_map or self._prefetch_queries):
            self._make_prefetch_queries()
            fields_map = self.model._meta.fields_map

            prefetch_tasks = [
                fields_map[field].prefetch(instance_list, related_query)
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
