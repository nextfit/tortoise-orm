
from typing import Any, Callable, List, Type, TYPE_CHECKING, Dict

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import UnknownFieldError, NotARelationFieldError
from tortoise.fields import JSONField, RelationField
from tortoise.query.base import MODEL, AwaitableQuery
from tortoise.query.context import QueryContext
from tortoise.query.term_utils import resolve_field_name

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
            _, field = resolve_field_name(field_name, self, context, accept_relation=False)
            self.query._select_other(field.as_(return_as))

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
