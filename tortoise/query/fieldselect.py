
from typing import Any, Callable, List, Tuple, Type, TYPE_CHECKING, Dict

from pypika import Table

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import UnknownFieldError, NotARelationFieldError
from tortoise.fields import JSONField, RelationField
from tortoise.query.base import MODEL, AwaitableQuery
from tortoise.query.context import QueryContext

if TYPE_CHECKING:
    from tortoise.models import Model


class FieldSelectQuery(AwaitableQuery[MODEL]):
    __slots__ = (
        "fields_for_select",
    )

    fields_for_select: Dict[str, str]

    def _copy(self, queryset) -> None:
        super()._copy(queryset)
        queryset.fields_for_select = self.fields_for_select

    def _join_table_with_forwarded_fields(
        self, context: QueryContext, field_name: str, forwarded_fields: str
    ) -> Tuple[Table, str]:

        context_item = context.top
        model = context_item.model
        table = context_item.table

        field_object = model._meta.fields_map.get(field_name)
        if not field_object:
            raise UnknownFieldError(field_name, model)

        if not isinstance(field_object, RelationField):
            if forwarded_fields:
                raise NotARelationFieldError(field_name, model)

            return table, field_object.db_column

        if not forwarded_fields:
            raise ValueError(
                'Selecting relation "{}" is not possible, select '
                'a field on the related model'.format(field_name)
            )

        join_data = self.join_table_by_field(table, field_object)
        forwarded_base, _, forwarded_sub = forwarded_fields.partition(LOOKUP_SEP)

        context.push(join_data.model, join_data.table)
        output = self._join_table_with_forwarded_fields(
            context=context,
            field_name=forwarded_base,
            forwarded_fields=forwarded_sub,
        )
        context.pop()
        return output

    def add_field_to_select_query(self, context: QueryContext, field_name, return_as) -> None:
        table = context.top.table

        if field_name in self.annotations:
            self.query._select_other(self.annotations[field_name].field.as_(return_as))
            return

        if field_name == "pk":
            field_name = self.model._meta.pk_attr

        base_field_name, _, sub_field = field_name.partition(LOOKUP_SEP)
        field_object = self.model._meta.fields_map.get(base_field_name)
        if not field_object:
            raise UnknownFieldError(base_field_name, self.model)

        if isinstance(field_object, RelationField):
            if not sub_field:
                raise ValueError(
                    'Selecting relation "{}" is not possible, select '
                    'a field on the related model'.format(field_name)
                )

            context.push(model=self.model, table=self.model._meta.table())
            related_table, related_db_column = self._join_table_with_forwarded_fields(
                context=context, field_name=base_field_name, forwarded_fields=sub_field)
            context.pop()

            self.query._select_field(related_table[related_db_column].as_(return_as))

        else:
            if sub_field:
                if isinstance(field_object, JSONField):
                    path = "{{{}}}".format(sub_field.replace(LOOKUP_SEP, ','))
                    self.query._select_other(table[field_object.db_column].get_path_json_value(path).as_(return_as))
                    return

                raise NotARelationFieldError(base_field_name, self.model)

            self.query._select_field(table[field_object.db_column].as_(return_as))

    def resolve_to_python_value(self, model: Type["Model"], field_name: str) -> Callable:
        if field_name in self.annotations:
            return self.annotations[field_name].to_python_value

        base_field_name, _, sub_field = field_name.partition(LOOKUP_SEP)
        field_object = model._meta.fields_map.get(base_field_name)
        if not field_object:
            raise UnknownFieldError(base_field_name, model)

        if isinstance(field_object, RelationField):
            if sub_field:
                return self.resolve_to_python_value(field_object.remote_model, sub_field)

            else:
                return lambda x: x

        else:
            if sub_field:
                if not isinstance(field_object, JSONField):
                    raise NotARelationFieldError(base_field_name, self.model)

            return field_object.to_python_value

    def _make_query(self, context: QueryContext) -> None:
        self.query = self.query_builder(context.alias)
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

        self.fields_for_select = {str(i): field_name for i, field_name in enumerate(fields_for_select_list)}
        self.flat = flat

    def _copy(self, queryset) -> None:
        super()._copy(queryset)
        queryset.flat = self.flat

    async def _execute(self) -> List[Any]:
        column_mappers = [
            self.resolve_to_python_value(self.model, field_name)
            for field_name in self.fields_for_select.values()
        ]

        if self.flat:
            func = column_mappers[0]
            row_mapper = lambda row: func(row[0])  # noqa
        else:
            row_mapper = lambda row: tuple(map(lambda p: p[0](p[1]), zip(column_mappers, row)))  # noqa

        _, db_columns, result = await self._get_db_client().execute_query(str(self.query))
        return list(map(row_mapper, result))


class ValuesQuery(FieldSelectQuery):
    __slots__ = ()

    def __init__(
        self, model, db, q_objects, annotations, orderings, distinct,
        limit, offset, fields_for_select,
    ) -> None:
        super().__init__(model, db, q_objects, annotations, orderings, distinct, limit, offset)
        self.fields_for_select = fields_for_select

    async def _execute(self) -> List[Dict[str, Any]]:
        column_mappers = [
            self.resolve_to_python_value(self.model, field_name)
            for field_name in self.fields_for_select.values()
        ]

        _, db_columns, result = await self._get_db_client().execute_query(str(self.query))
        return [
            dict(zip(db_columns, map(lambda p: p[0](p[1]), zip(column_mappers, row))))
            for row in result
        ]
