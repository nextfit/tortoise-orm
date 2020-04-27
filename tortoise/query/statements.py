
from pypika.functions import Count
from pypika.terms import Term

from tortoise.context import QueryContext
from tortoise.exceptions import IntegrityError, FieldError
from tortoise.expressions import F
from tortoise.fields import ForeignKey, OneToOneField
from tortoise.query.base import AwaitableStatement


class UpdateQuery(AwaitableStatement):
    __slots__ = ("update_kwargs",)

    def __init__(self, model, update_kwargs, db, q_objects, annotations) -> None:
        super().__init__(model, db, q_objects, annotations)
        self.update_kwargs = update_kwargs

    def _make_query(self, context: QueryContext, alias=None) -> None:
        table = self.model._meta.table()
        self.query = self._db.query_class.update(table)

        context.push(self.model, table)
        self._add_query_details(context=context)

        # Need to get executor to get correct column_map
        executor = self._db.executor_class(model=self.model, db=self._db)

        for key, value in self.update_kwargs.items():
            field_object = self.model._meta.fields_map.get(key)

            if not field_object:
                raise FieldError(f"Unknown keyword argument {key} for model {self.model}")

            if field_object.primary_key:
                raise IntegrityError(f"Field {key} is primary key and can not be updated")

            if isinstance(field_object, (ForeignKey, OneToOneField)):
                fk_field: str = field_object.id_field_name
                column_name = self.model._meta.fields_map[fk_field].db_column
                value = executor.column_map[fk_field](value.pk, None)
            else:
                try:
                    column_name = self.model._meta.field_to_db_column_name_map[key]
                except KeyError:
                    raise FieldError(f"Field {key} is virtual and can not be updated")

                if isinstance(value, Term):
                    value = F.resolve(value, context)

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
        self.query = self.model._meta.query_builder(alias)
        context.push(self.model, self.query._from[-1])
        self._add_query_details(context=context)
        self.query._delete_from = True
        context.pop()

    async def _execute(self) -> int:
        return (await self._db.execute_query(str(self.query)))[0]


class CountQuery(AwaitableStatement):
    __slots__ = ()

    def __init__(self, model, db, q_objects, annotations) -> None:
        super().__init__(model, db, q_objects, annotations)

    def _make_query(self, context: QueryContext, alias=None) -> None:
        self.query = self.model._meta.query_builder()
        context.push(self.model, self.query._from[-1])
        self._add_query_details(context=context)
        self.query._select_other(Count("*"))
        context.pop()

    async def _execute(self) -> int:
        _, result = await self._db.execute_query(str(self.query))
        return list(dict(result[0]).values())[0]
