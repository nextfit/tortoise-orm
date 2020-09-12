
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
from typing import (
    Any, Awaitable, Dict, Generic, List, Literal, Optional, Tuple, Type, TypeVar, Union, TYPE_CHECKING
)

from pypika import Criterion, Table, Field as PyPikaField

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import ConfigurationError, NoValuesFetched, OperationalError
from tortoise.fields.base import CASCADE, RESTRICT, SET_NULL, Field
from tortoise.filters import FieldFilter


if TYPE_CHECKING:
    from tortoise.models import Model
    from tortoise.query.queryset import QuerySet


MODEL = TypeVar("MODEL", bound="Model")


OneToOneNullableRelation = Union[Awaitable[Optional[MODEL]], Optional[MODEL]]
"""
Type hint for the result of accessing the :func:`.OneToOneField` field in the model
when obtained model can be nullable.
"""

OneToOneRelation = Union[Awaitable[MODEL], MODEL]
"""
Type hint for the result of accessing the :func:`.OneToOneField` field in the model.
"""

ForeignKeyNullableRelation = Union[Awaitable[Optional[MODEL]], Optional[MODEL]]
"""
Type hint for the result of accessing the :func:`.ForeignKey` field in the model
when obtained model can be nullable.
"""

ForeignKeyRelation = Union[Awaitable[MODEL], MODEL]
"""
Type hint for the result of accessing the :func:`.ForeignKey` field in the model.
"""


class _NoneAwaitable:
    __slots__ = ()

    def __await__(self):
        yield None

    def __bool__(self):
        return False


NoneAwaitable = _NoneAwaitable()


class ReverseRelation(Generic[MODEL]):
    """
    Relation container for :func:`.ForeignKey`.
    """

    def __init__(self, remote_model: Type[MODEL], related_name: str, instance) -> None:
        self.remote_model = remote_model
        self.related_name = related_name
        self.instance = instance

        self._fetched = False
        self._custom_query = False
        self._related_objects: list = []

    @property
    def _query(self):
        if not self.instance._saved_in_db:
            raise OperationalError(
                "This objects hasn't been instanced, call .save() before calling related queries"
            )
        return self.remote_model.filter(**{self.related_name: self.instance.pk})

    def __contains__(self, item) -> bool:
        if not self._fetched:
            raise NoValuesFetched(
                "No values were fetched for this relation, first use .fetch_related()"
            )
        return item in self._related_objects

    def __iter__(self):
        if not self._fetched:
            raise NoValuesFetched(
                "No values were fetched for this relation, first use .fetch_related()"
            )
        return self._related_objects.__iter__()

    def __len__(self) -> int:
        if not self._fetched:
            raise NoValuesFetched(
                "No values were fetched for this relation, first use .fetch_related()"
            )
        return len(self._related_objects)

    def __bool__(self) -> bool:
        if not self._fetched:
            raise NoValuesFetched(
                "No values were fetched for this relation, first use .fetch_related()"
            )
        return bool(self._related_objects)

    def __getitem__(self, item):
        if not self._fetched:
            raise NoValuesFetched(
                "No values were fetched for this relation, first use .fetch_related()"
            )
        return self._related_objects[item]

    def __await__(self):
        return self._query.__await__()

    async def __aiter__(self):
        if not self._fetched:
            self._related_objects = await self
            self._fetched = True

        for val in self._related_objects:
            yield val

    def filter(self, *args, **kwargs) -> "QuerySet[MODEL]":
        """
        Returns QuerySet with related elements filtered by args/kwargs.
        """
        return self._query.filter(*args, **kwargs)

    def all(self) -> "QuerySet[MODEL]":
        """
        Returns QuerySet with all related elements.
        """
        return self._query

    def order_by(self, *args, **kwargs) -> "QuerySet[MODEL]":
        """
        Returns QuerySet related elements in order.
        """
        return self._query.order_by(*args, **kwargs)

    def limit(self, *args, **kwargs) -> "QuerySet[MODEL]":
        """
        Returns a QuerySet with at most «limit» related elements.
        """
        return self._query.limit(*args, **kwargs)

    def offset(self, *args, **kwargs) -> "QuerySet[MODEL]":
        """
        Returns aQuerySet with all related elements offset by «offset».
        """
        return self._query.offset(*args, **kwargs)

    def _set_objects(self, sequence) -> None:
        self._fetched = True
        self._related_objects = sequence


class ManyToManyRelation(ReverseRelation[MODEL]):
    """
    Many to many relation container for :func:`.ManyToManyField`.
    """

    def __init__(self, instance, m2m_field: "ManyToManyField") -> None:
        super().__init__(m2m_field.remote_model, m2m_field.related_name, instance)
        self.field = m2m_field

    async def add(self, *instances, using_db=None) -> None:
        """
        Adds one or more of ``instances`` to the relation.

        If it is already added, it will be silently ignored.
        """
        if not instances:
            return

        if not self.instance._saved_in_db:
            raise OperationalError(f"You should first call .save() on {self.instance}")

        db = using_db if using_db else self.remote_model._meta.db
        pk_formatting_func = type(self.instance)._meta.pk.db_value
        related_pk_formatting_func = type(instances[0])._meta.pk.db_value
        through_table = Table(self.field.through)

        select_query = (
            db.query_class.from_(through_table)
            .where(
                through_table[self.field.backward_key]
                == pk_formatting_func(self.instance.pk, self.instance)
            )
            .select(self.field.backward_key, self.field.forward_key)
        )

        if len(instances) == 1:
            criterion = (through_table[self.field.forward_key]
                         == related_pk_formatting_func(instances[0].pk, instances[0]))
        else:
            criterion = (through_table[self.field.forward_key].isin(
                [related_pk_formatting_func(i.pk, i) for i in instances]))

        select_query = select_query.where(criterion)


        #
        # Note that columns in the returned rows follow the same order
        # as in selected fields. Note above that we have selected the fields
        # in this order for select_query:
        #       .select(self.field.backward_key, self.field.forward_key)
        #
        # therefore, we use r[0] for r[self.field.backward_key]
        # and use r[1] for r[self.field.forward_key]
        #
        # following the requirement that rows to be accessed as arrays with
        # db_columns as guide for the column names.
        #

        # TODO: This is highly inefficient. Should use UNIQUE db_index by default.
        #  And optionally allow duplicates.

        _, db_columns, existing_relations_raw = await db.execute_query(str(select_query))
        existing_relations = {
            (
                pk_formatting_func(r[0], self.instance),  # r[self.field.backward_key]
                related_pk_formatting_func(r[1], self.instance),  # r[self.field.forward_key]
            )
            for r in existing_relations_raw
        }

        insert_query = db.query_class.into(through_table).columns(
            through_table[self.field.forward_key],
            through_table[self.field.backward_key],
        )

        insert_is_required = False
        for instance_to_add in instances:
            if not instance_to_add._saved_in_db:
                raise OperationalError(f"You should first call .save() on {instance_to_add}")

            pk_b = pk_formatting_func(self.instance.pk, self.instance)
            pk_f = related_pk_formatting_func(instance_to_add.pk, instance_to_add)
            if (pk_b, pk_f) in existing_relations:
                continue

            insert_query = insert_query.insert(pk_f, pk_b)
            insert_is_required = True

        if insert_is_required:
            await db.execute_query(str(insert_query))

    async def clear(self, using_db=None) -> None:
        """
        Clears ALL relations.
        """
        db = using_db if using_db else self.remote_model._meta.db
        through_table = Table(self.field.through)
        pk_formatting_func = type(self.instance)._meta.pk.db_value
        query = (
            db.query_class.from_(through_table)
            .where(
                through_table[self.field.backward_key] == pk_formatting_func(self.instance.pk, self.instance)
            )
            .delete()
        )
        await db.execute_query(str(query))

    async def remove(self, *instances, using_db=None) -> None:
        """
        Removes one or more of ``instances`` from the relation.
        """
        db = using_db if using_db else self.remote_model._meta.db
        if not instances:
            raise OperationalError("remove() called on no instances")
        through_table = Table(self.field.through)
        pk_formatting_func = type(self.instance)._meta.pk.db_value
        related_pk_formatting_func = type(instances[0])._meta.pk.db_value

        if len(instances) == 1:
            condition = (
                through_table[self.field.forward_key]
                == related_pk_formatting_func(instances[0].pk, instances[0])
            ) & (
                through_table[self.field.backward_key]
                == pk_formatting_func(self.instance.pk, self.instance)
            )
        else:
            condition = (
                through_table[self.field.backward_key]
                == pk_formatting_func(self.instance.pk, self.instance)
            ) & (
                through_table[self.field.forward_key].isin(
                    [related_pk_formatting_func(i.pk, i) for i in instances])
            )
        query = db.query_class.from_(through_table).where(condition).delete()
        await db.execute_query(str(query))


@dataclass
class JoinData:
    table: Table
    criterion: Criterion
    pypika_field: PyPikaField
    model: Optional[Type[MODEL]]
    field_object: Optional[Field]


class RelationField(Field, Generic[MODEL]):
    has_db_column = False

    def __init__(self, remote_model: Type[MODEL], related_name: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.remote_model: Type[MODEL] = remote_model
        self.related_name: str = related_name

    def attribute_property(self) -> property:
        raise NotImplementedError()

    def create_relation(self, tortoise) -> None:
        raise NotImplementedError()

    def get_joins(self, table: Table, full: bool) -> List[JoinData]:
        """
        Get required joins for this relation

        :param table: Reference table to create joins to
        :param full: Join fully, or only to the point where primary key of the relation is available.
        :return: List of joins and their joined table
        """
        raise NotImplementedError()

    async def prefetch(self, instance_list: list, related_query: "QuerySet[MODEL]") -> list:
        raise NotImplementedError()

    def join_table_alias(self, table: Table) -> str:
        # return f"{table.get_table_name()}{LOOKUP_SEP}{self.model_field_name}"
        if table.alias:
            return "{}{}{}".format(table.alias, LOOKUP_SEP, self.model_field_name)
        else:
            return self.model_field_name

    def get_db_column_types(self) -> Optional[Dict[str, str]]:
        return None

    def describe(self, serializable: bool = True) -> dict:
        desc = super().describe(serializable)

        # RelationFields are entirely "virtual", so no direct DB representation
        del desc["db_column"]
        return desc


class BackwardFKField(RelationField):

    def __init__(
        self,
        remote_model: Type[MODEL],
        related_name: str,
        null: bool,
        description: Optional[str]
    ) -> None:
        super().__init__(remote_model=remote_model, related_name=related_name, null=null)
        self.description: Optional[str] = description
        self.auto_created = True

    @staticmethod
    def _rfk_getter(self, _key, ftype, related_name):
        val = getattr(self, _key, None)
        if val is None:
            val = ReverseRelation(ftype, related_name, self)
            setattr(self, _key, val)
        return val

    def attribute_property(self) -> property:
        _key = f"_{self.model_field_name}"
        return property(
            partial(
                BackwardFKField._rfk_getter,
                _key=_key,
                ftype=self.remote_model,
                related_name=self.related_name,
            )
        )

    def create_relation(self, tortoise) -> None:
        raise RuntimeError("This method on should not have been called on a generated relation.")

    def create_filter(self, opr, value_encoder) -> FieldFilter:
        from tortoise.filters.relational import BackwardFKFilter
        return BackwardFKFilter(self, opr, value_encoder)

    async def prefetch(self, instance_list: list, related_query: "QuerySet[MODEL]") -> list:
        instance_id_set: set = {
            instance._meta.pk.db_value(instance.pk, instance)
            for instance in instance_list
        }
        related_name = self.related_name

        related_object_list = await related_query.filter(
            **{f"{related_name}__in": list(instance_id_set)}
        )

        related_object_map: Dict[str, list] = {}
        for entry in related_object_list:
            object_id = getattr(entry, related_name)
            if object_id in related_object_map.keys():
                related_object_map[object_id].append(entry)
            else:
                related_object_map[object_id] = [entry]

        for instance in instance_list:
            relation_container = getattr(instance, self.model_field_name)
            relation_container._set_objects(related_object_map.get(instance.pk, []))

        return instance_list

    def get_joins(self, table: Table, full: bool) -> List[JoinData]:
        table_pk = self.model._meta.pk_db_column
        related_table = self.remote_model._meta.table(alias=self.join_table_alias(table))
        related_field = self.remote_model._meta.fields_map[self.related_name]
        return [JoinData(
            related_table,
            related_table[related_field.db_column] == table[table_pk],
            related_table[related_field.db_column],
            self.remote_model,
            related_field,
        )]


class ForeignKey(RelationField):
    """
    ForeignKey relation field.

    This field represents a foreign key relation to another model.

    See :ref:`foreign_key` for usage information.

    You must provide the following:

    ``model_name``:
        The name of the related model in a :samp:`'{app}.{model}'` format.

    The following is optional:

    ``related_name``:
        The attribute name on the related model to reverse resolve the foreign key.
    ``on_delete``:
        One of:
            ``field.CASCADE``:
                Indicate that the model should be cascade deleted if related model gets deleted.
            ``field.RESTRICT``:
                Indicate that the related model delete will be restricted as long as a
                foreign key points to it.
            ``field.SET_NULL``:
                Resets the field to NULL in case the related model gets deleted.
                Can only be set if field has ``null=True`` set.
            ``field.SET_DEFAULT``:
                Resets the field to ``default`` value in case the related model gets deleted.
                Can only be set is field has a ``default`` set.
    """

    backward_relation_class = BackwardFKField

    def __init__(
        self,
        model_name: str,
        primary_key: bool = False,
        unique: bool = False,
        related_name: Union[Optional[str], Literal[False]] = None,
        on_delete=CASCADE,
        **kwargs,
    ) -> None:

        super().__init__(
            remote_model=None,
            related_name=related_name,
            primary_key=primary_key,
            unique=unique,
            **kwargs)

        self.id_field_name: str

        if primary_key and not unique:
            raise ConfigurationError(f"{self.__class__.__name__} cannot be a primary key if not unique")

        if len(model_name.split(".")) != 2:
            raise ConfigurationError(f'{self.__class__.__name__} accepts model name in format "app.Model"')

        self.model_name = model_name
        if on_delete not in {CASCADE, RESTRICT, SET_NULL}:
            raise ConfigurationError("on_delete can only be CASCADE, RESTRICT or SET_NULL")

        if on_delete == SET_NULL and not bool(kwargs.get("null")):
            raise ConfigurationError("If on_delete is SET_NULL, then field must have null=True set")

        self.on_delete = on_delete

    @staticmethod
    def _fk_setter(self, value, _key, id_field_name):
        setattr(self, id_field_name, value.pk if value else None)
        setattr(self, _key, value)

    @staticmethod
    def _fk_getter(self, _key, ftype, id_field_name):
        try:
            return getattr(self, _key)
        except AttributeError:
            _pk = getattr(self, id_field_name)
            if _pk:
                return ftype.filter(pk=_pk).first()
            return NoneAwaitable

    def attribute_property(self) -> property:
        _key = f"_{self.model_field_name}"
        return property(
            partial(
                ForeignKey._fk_getter,
                _key=_key,
                ftype=self.remote_model,
                id_field_name=self.id_field_name,
            ),
            partial(ForeignKey._fk_setter, _key=_key, id_field_name=self.id_field_name),
            partial(ForeignKey._fk_setter, value=None, _key=_key, id_field_name=self.id_field_name),
        )

    async def prefetch(self, instance_list: list, related_query: "QuerySet[MODEL]") -> list:
        related_objects_for_fetch = set()
        for instance in instance_list:
            if getattr(instance, self.id_field_name):
                related_objects_for_fetch.add(getattr(instance, self.id_field_name))
            else:
                setattr(instance, self.model_field_name, None)

        if related_objects_for_fetch:
            related_object_list = await related_query.filter(pk__in=list(related_objects_for_fetch))
            related_object_map = {obj.pk: obj for obj in related_object_list}
            for instance in instance_list:
                setattr(instance, self.model_field_name,
                    related_object_map.get(getattr(instance, self.id_field_name)))

        return instance_list

    def create_relation(self, tortoise) -> None:
        remote_model = tortoise.get_model(self.model_name)

        self.id_field_name = f"{self.model_field_name}_id"

        id_field_object = deepcopy(remote_model._meta.pk)
        id_field_object.primary_key = self.primary_key
        id_field_object.unique = self.unique
        id_field_object.db_index = self.db_index
        id_field_object.default = self.default
        id_field_object.null = self.null
        id_field_object.generated = self.generated
        id_field_object.auto_created = True
        id_field_object.reference = self
        id_field_object.description = self.description
        id_field_object.db_column = self.db_column if self.db_column else self.id_field_name

        self.db_column = id_field_object.db_column
        self.model._meta.add_field(self.id_field_name, id_field_object)
        self.remote_model = remote_model

        if self.primary_key:
            self.model._meta.pk_attr = self.id_field_name

        backward_relation_name = self.related_name
        if backward_relation_name is not False:
            if not backward_relation_name:
                backward_relation_name = "{}_set".format(self.model.__name__.lower())

            if backward_relation_name in remote_model._meta.fields_map:
                raise ConfigurationError(
                    f"backward relation '{backward_relation_name}' duplicates in"
                    f" model {remote_model}"
                )

            backward_relation_field = self.backward_relation_class(
                self.model, self.id_field_name, True, self.description)
            remote_model._meta.add_field(backward_relation_name, backward_relation_field)

    def get_joins(self, table: Table, full: bool) -> List[JoinData]:
        if full:
            related_field = self.remote_model._meta.pk
            related_table = self.remote_model._meta.table(alias=self.join_table_alias(table))
            return [JoinData(
                related_table,
                related_table[related_field.db_column] == table[self.db_column],
                related_table[related_field.db_column],
                self.remote_model,
                related_field,
            )]

        else:
            return []

    def describe(self, serializable: bool = True) -> dict:
        desc = super().describe(serializable)
        desc["raw_field"] = self.id_field_name
        return desc


class BackwardOneToOneField(BackwardFKField):

    @staticmethod
    def _ro2o_getter(self, _key, ftype, field_name):
        if hasattr(self, _key):
            return getattr(self, _key)

        val = ftype.filter(**{field_name: self.pk}).first()
        setattr(self, _key, val)
        return val

    def attribute_property(self) -> property:
        _key = f"_{self.model_field_name}"
        return property(
            partial(
                BackwardOneToOneField._ro2o_getter,
                _key=_key,
                ftype=self.remote_model,
                field_name=self.related_name,
            ),
        )

    async def prefetch(self, instance_list: list, related_query: "QuerySet[MODEL]") -> list:
        instance_id_set: set = {
            instance._meta.pk.db_value(instance.pk, instance)
            for instance in instance_list
        }
        related_name = self.related_name

        related_object_list = await related_query.filter(
            **{f"{related_name}__in": list(instance_id_set)}
        )

        related_object_map = {getattr(entry, related_name): entry for entry in related_object_list}
        for instance in instance_list:
            setattr(instance, f"_{self.model_field_name}", related_object_map.get(instance.pk, None))

        return instance_list


class OneToOneField(ForeignKey):
    """
    OneToOne relation field.

    This field represents a foreign key relation to another model.

    See :ref:`one_to_one` for usage information.

    You must provide the following:

    ``model_name``:
        The name of the related model in a :samp:`'{app}.{model}'` format.

    The following is optional:

    ``related_name``:
        The attribute name on the related model to reverse resolve the foreign key.
    ``on_delete``:
        One of:
            ``field.CASCADE``:
                Indicate that the model should be cascade deleted if related model gets deleted.
            ``field.RESTRICT``:
                Indicate that the related model delete will be restricted as long as a
                foreign key points to it.
            ``field.SET_NULL``:
                Resets the field to NULL in case the related model gets deleted.
                Can only be set if field has ``null=True`` set.
            ``field.SET_DEFAULT``:
                Resets the field to ``default`` value in case the related model gets deleted.
                Can only be set is field has a ``default`` set.
    """

    backward_relation_class = BackwardOneToOneField

    def __init__(
        self,
        model_name: str,
        primary_key: bool = False,
        related_name: Union[Optional[str], Literal[False]] = None,
        on_delete=CASCADE,
        **kwargs,

    ) -> None:
        kwargs.pop("unique", None)
        super().__init__(
            model_name=model_name,
            primary_key=primary_key,
            unique=True,
            related_name=related_name,
            on_delete=on_delete,
            **kwargs)


class ManyToManyField(RelationField):
    """
    ManyToMany relation field.

    This field represents a many-to-many between this model and another model.

    See :ref:`many_to_many` for usage information.

    You must provide the following:

    ``model_name``:
        The name of the related model in a :samp:`'{app}.{model}'` format.

    The following is optional:

    ``through``:
        The DB table that represents the trough table.
        The default is normally safe.
    ``forward_key``:
        The forward lookup key on the through table.
        The default is normally safe.
    ``backward_key``:
        The backward lookup key on the through table.
        The default is normally safe.
    ``related_name``:
        The attribute name on the related model to reverse resolve the many to many.
    """

    def __init__(
        self,
        model_name: str,
        through: Optional[str] = None,
        forward_key: Optional[str] = None,
        backward_key: Optional[str] = None,
        related_name: Union[Optional[str], Literal[False]] = None,
        **kwargs,
    ) -> None:

        super().__init__(remote_model=None, related_name=related_name, **kwargs)

        if len(model_name.split(".")) != 2:
            raise ConfigurationError('Foreign key accepts model name in format "app.Model"')

        self.model_name: str = model_name
        self.forward_key: str = forward_key or f"{model_name.split('.')[1].lower()}_id"
        self.backward_key: str = backward_key
        self.through: str = through

    @staticmethod
    def _m2m_getter(self, _key, field_object):
        val = getattr(self, _key, None)
        if val is None:
            val = ManyToManyRelation(self, field_object)
            setattr(self, _key, val)
        return val

    def attribute_property(self) -> property:
        _key = f"_{self.model_field_name}"
        return property(partial(ManyToManyField._m2m_getter, _key=_key, field_object=self))

    def create_filter(self, opr, value_encoder) -> FieldFilter:
        from tortoise.filters.relational import ManyToManyRelationFilter
        return ManyToManyRelationFilter(self, opr, value_encoder)

    def create_relation(self, tortoise) -> None:

        backward_key = self.backward_key
        model_name_lower = self.model.__name__.lower()

        if not backward_key:
            backward_key = "{}_id".format(model_name_lower)

            if backward_key == self.forward_key:
                backward_key = "{}_rel_id".format(model_name_lower)

            self.backward_key = backward_key

        remote_model = tortoise.get_model(self.model_name)
        self.remote_model = remote_model

        if not self.through:
            self.through = "{}_{}".format(model_name_lower, remote_model.__name__.lower())

        if "." in self.through:
            through_model = tortoise.get_model(self.through)
            self.through = through_model._meta.db_table

        backward_relation_name = self.related_name
        if backward_relation_name is not False:
            if not backward_relation_name:
                backward_relation_name = self.related_name = "{}_set".format(model_name_lower)

            if backward_relation_name in remote_model._meta.fields_map:
                raise ConfigurationError(
                    f"backward relation '{backward_relation_name}' duplicates in"
                    f" model {remote_model}"
                )

            m2m_relation = ManyToManyField(
                self.model.full_name(),
                self.through,
                forward_key=self.backward_key,
                backward_key=self.forward_key,
                related_name=self.model_field_name,
                description=self.description,
            )
            m2m_relation.auto_created = True
            m2m_relation.remote_model = self.model
            remote_model._meta.add_field(backward_relation_name, m2m_relation)

    async def prefetch(self, instance_list: list, related_query: "QuerySet[MODEL]") -> list:
        instance_id_set = [
            instance._meta.pk.db_value(instance.pk, instance)
            for instance in instance_list
        ]

        field_object: ManyToManyField = self.model._meta.fields_map[self.model_field_name]
        through_table = Table(field_object.through)

        subquery = (
            self.model._meta.db.query_class.from_(through_table)
            .select(
                through_table[field_object.backward_key],
                through_table[field_object.forward_key],
            )
            .where(through_table[field_object.backward_key].isin(instance_id_set))
        )

        related_query_table = related_query.model._meta.table()
        related_pk_field = related_query.model._meta.pk_db_column
        context = related_query.create_query_context(parent_context=None)
        context.query = (
            context.query.join(subquery)
            .on(getattr(subquery, field_object.forward_key) == related_query_table[related_pk_field])
            .select(getattr(subquery, field_object.backward_key))
        )

        context.push(
            related_query.model,
            related_query_table,
            {field_object.through: through_table.as_(subquery.alias)}
        )
        related_query._add_query_details(context)

        #
        # Following few lines are transformed version of these lines, when I was trying
        # to convert row dictionary decoding to ordered (list) decoding.
        #
        #         relations = [
        #             (
        #                 self.model._meta.pk.to_python_value(e[field_object.backward_key]),
        #                 related_query.model._init_from_db_row(iter(zip(db_columns, e))),
        #             )
        #             for e in raw_results
        #         ]
        #

        _, db_columns, raw_results = await self.model._meta.db.execute_query(context.query.get_sql())
        relations: List[Tuple[Any, MODEL]] = []
        for row in raw_results:
            row_iter = iter(zip(db_columns, row))
            related_instance = related_query.model._init_from_db_row(row_iter, related_query._select_related)

            db_column, value = next(row_iter)  # row[field_object.backward_key]
            backward_key = self.model._meta.pk.to_python_value(value)
            relations.append((backward_key, related_instance))

        related_executor = self.model._meta.db.executor_class(
            model=related_query.model,
            db=self.model._meta.db,
            prefetch_map=related_query._prefetch_map,
            prefetch_queries=related_query._prefetch_queries,
        )

        await related_executor._execute_prefetch_queries([item for _, item in relations])

        relation_map: Dict[Any, List[MODEL]] = {}
        for k, item in relations:
            relation_map.setdefault(k, []).append(item)

        for instance in instance_list:
            relation_container = getattr(instance, self.model_field_name)
            relation_container._set_objects(relation_map.get(instance.pk, []))

        return instance_list

    def get_joins(self, table: Table, full: bool) -> List[JoinData]:
        table_pk = self.model._meta.pk_db_column

        through_table_name = "{}{}{}{}".format(table.get_table_name(), LOOKUP_SEP,
            self.remote_model.__name__.lower(), self.model.__name__.lower())
        through_table = Table(self.through).as_(through_table_name)
        joins = [JoinData(
            through_table,
            through_table[self.backward_key] == table[table_pk],
            through_table[self.backward_key],
            None,
            None,
        )]

        if full:
            related_table = self.remote_model._meta.table(alias=self.join_table_alias(table))
            related_field = self.remote_model._meta.pk
            joins.append(JoinData(
                related_table,
                related_table[related_field.db_column] == through_table[self.forward_key],
                related_table[related_field.db_column],
                self.remote_model,
                related_field
            ))

        return joins
