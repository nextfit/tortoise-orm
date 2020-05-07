
from typing import Tuple, List, Callable, Any

from pypika import Table

from tortoise.constants import LOOKUP_SEP
from tortoise.query.context import QueryContext
from tortoise.exceptions import FieldError
from tortoise.fields import JSONField
from tortoise.query.base import AwaitableQuery, MODEL


class FieldSelectQuery(AwaitableQuery[MODEL]):
    # pylint: disable=W0223
    __slots__ = (
        "fields_for_select",
    )

    def _copy(self, queryset):
        super()._copy(queryset)
        queryset.fields_for_select = self.fields_for_select

    def _join_table_with_forwarded_fields(
        self, context: QueryContext, field_name: str, forwarded_fields: str
    ) -> Tuple[Table, str]:

        context_item = context.top
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

        field_table = self.join_table_by_field(table, field_object)
        forwarded_base, _, forwarded_sub = forwarded_fields.partition(LOOKUP_SEP)

        context.push(field_object.remote_model, field_table)
        output = self._join_table_with_forwarded_fields(
            context=context,
            field_name=forwarded_base,
            forwarded_fields=forwarded_sub,
        )
        context.pop()
        return output

    def add_field_to_select_query(self, context: QueryContext, field_name, return_as) -> None:
        table = context.top.table

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
            self.query._select_other(self.annotations[field_name].field.as_(return_as))
            return

        base_field_name, _, sub_field = field_name.partition(LOOKUP_SEP)
        if base_field_name in self.model._meta.fetch_fields:
            context.push(model=self.model, table=self.model._meta.table())
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
            return self.annotations[field_name].to_python_value

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
        self.query = self.query_builder(alias)
        context.push(self.model, self.query._from[-1])
        self._add_query_details(context=context)
        for return_as, field_name in self.fields_for_select.items():
            self.add_field_to_select_query(context, field_name, return_as)

        context.pop()


class ValuesListQuery(FieldSelectQuery):
    __slots__ = (
        "flat",
    )

    def __init__(
        self, model, db, q_objects, annotations, orderings, distinct,
        limit, offset, fields_for_select_list, flat,
    ) -> None:
        super().__init__(model, db, q_objects, annotations, orderings, distinct, limit, offset)

        if flat and (len(fields_for_select_list) != 1):
            raise TypeError("You can flat value_list only if contains one field")

        self.fields_for_select = {str(i): field for i, field in enumerate(fields_for_select_list)}
        self.flat = flat

    def _copy(self, queryset):
        super()._copy(queryset)
        queryset.flat = self.flat

    async def _execute(self) -> List[Any]:
        _, result = await self._db.execute_query(str(self.query))
        columns = [
            (key, self.resolve_to_python_value(self.model, name))
            for key, name in sorted(self.fields_for_select.items())
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

        _, result = await self._db.execute_query(str(self.query))
        result = list(map(dict, result))
        for row in result:
            for col_name, col_mapper in column_mappers:
                row[col_name] = col_mapper(row[col_name])

        return result
