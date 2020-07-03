
from pypika.functions import Count
from pypika.queries import QueryBuilder
from pypika.terms import Term

from tortoise.exceptions import IntegrityError, UnknownFieldError, NotADbColumnFieldError
from tortoise.fields import ForeignKey, OneToOneField, RelationField
from tortoise.query.annotations import Annotation, TermAnnotation
from tortoise.query.base import AwaitableStatement
from tortoise.query.context import QueryContext

from typing import Optional


class UpdateQuery(AwaitableStatement):
    __slots__ = ("update_kwargs",)

    def __init__(self, model, update_kwargs, db, q_objects, annotations) -> None:
        super().__init__(model, db, q_objects, annotations)
        self.update_kwargs = update_kwargs

    def create_query(self, parent_context: Optional[QueryContext]) -> QueryBuilder:

        db_client = self._get_db_client()
        table = self.model._meta.table(parent_context.alias if parent_context else None)

        context = QueryContext(query=db_client.query_class.update(table), parent_context=parent_context)
        context.push(self.model, table)
        self._add_query_details(context=context)

        for field_name, value in self.update_kwargs.items():
            field_object = self.model._meta.fields_map.get(field_name)

            if not field_object:
                raise UnknownFieldError(field_name, self.model)

            if field_object.primary_key:
                raise IntegrityError(f"Field {field_name} is primary key and can not be updated")

            if isinstance(field_object, RelationField):
                if isinstance(field_object, (ForeignKey, OneToOneField)):
                    fk_field_name: str = field_object.id_field_name
                    fk_field_object = self.model._meta.fields_map[fk_field_name]
                    value = fk_field_object.db_value(value.pk, None)
                    context.query = context.query.set(fk_field_object.db_column, value)

                else:
                    raise NotADbColumnFieldError(field_name, self.model)

            else:
                if isinstance(value, Term):
                    value = TermAnnotation(value)
                    value.resolve_into(self, context)
                    value = value.field

                elif isinstance(value, Annotation):
                    value.resolve_into(self, context)
                    value = value.field

                else:
                    value = field_object.db_value(value, None)

                context.query = context.query.set(field_object.db_column, value)

        context.pop()
        return context.query

    async def _execute(self) -> int:
        return (await self._get_db_client().execute_query(str(self.query)))[0]


class DeleteQuery(AwaitableStatement):
    __slots__ = ()

    def __init__(self, model, db, q_objects, annotations) -> None:
        super().__init__(model, db, q_objects, annotations)

    def create_query(self, parent_context: Optional[QueryContext]) -> QueryBuilder:
        query = self.query_builder(parent_context.alias if parent_context else None)
        context = QueryContext(query, parent_context)
        context.push(self.model, query._from[-1])
        self._add_query_details(context=context)
        context.query._delete_from = True
        context.pop()

        return context.query

    async def _execute(self) -> int:
        return (await self._get_db_client().execute_query(str(self.query)))[0]


class CountQuery(AwaitableStatement):
    __slots__ = ()

    def __init__(self, model, db, q_objects, annotations) -> None:
        super().__init__(model, db, q_objects, annotations)

    def create_query(self, parent_context: Optional[QueryContext]) -> QueryBuilder:
        query = self.query_builder(parent_context.alias if parent_context else None)

        context = QueryContext(query, parent_context)
        context.push(self.model, query._from[-1])
        self._add_query_details(context=context)
        context.query._select_other(Count("*"))
        context.pop()

        return context.query

    async def _execute(self) -> int:
        _, db_columns, result = await self._get_db_client().execute_query(str(self.query))
        return result[0][0]
