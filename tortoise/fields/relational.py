from copy import deepcopy
from functools import partial
from typing import Awaitable, Generic, Optional, TypeVar, Union, Dict

from pypika import Table
from typing_extensions import Literal

from tortoise.context import QueryContext
from tortoise.exceptions import ConfigurationError, NoValuesFetched, OperationalError
from tortoise.fields.base import CASCADE, RESTRICT, SET_NULL, Field

from typing import Type

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
Type hint for the result of accessing the :func:`.ForeignKeyField` field in the model
when obtained model can be nullable.
"""

ForeignKeyRelation = Union[Awaitable[MODEL], MODEL]
"""
Type hint for the result of accessing the :func:`.ForeignKeyField` field in the model.
"""


class _NoneAwaitable:
    __slots__ = ()

    def __await__(self):
        yield None

    def __bool__(self):
        return False


NoneAwaitable = _NoneAwaitable()


def _fk_setter(self, value, _key, relation_field):
    setattr(self, relation_field, value.pk if value else None)
    setattr(self, _key, value)


def _fk_getter(self, _key, ftype, relation_field):
    try:
        return getattr(self, _key)
    except AttributeError:
        _pk = getattr(self, relation_field)
        if _pk:
            return ftype.filter(pk=_pk).first()
        return NoneAwaitable


def _rfk_getter(self, _key, ftype, frelfield):
    val = getattr(self, _key, None)
    if val is None:
        val = ReverseRelation(ftype, frelfield, self)
        setattr(self, _key, val)
    return val


def _ro2o_getter(self, _key, ftype, frelfield):
    if hasattr(self, _key):
        return getattr(self, _key)

    val = ftype.filter(**{frelfield: self.pk}).first()
    setattr(self, _key, val)
    return val


def _m2m_getter(self, _key, field_object):
    val = getattr(self, _key, None)
    if val is None:
        val = ManyToManyRelation(self, field_object)
        setattr(self, _key, val)
    return val


class ReverseRelation(Generic[MODEL]):
    """
    Relation container for :func:`.ForeignKeyField`.
    """

    def __init__(self, remote_model: "Type[MODEL]", related_name: str, instance) -> None:
        self.remote_model = remote_model
        self.related_name = related_name
        self.instance = instance

        self._fetched = False
        self._custom_query = False
        self.related_objects: list = []

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
        return item in self.related_objects

    def __iter__(self):
        if not self._fetched:
            raise NoValuesFetched(
                "No values were fetched for this relation, first use .fetch_related()"
            )
        return self.related_objects.__iter__()

    def __len__(self) -> int:
        if not self._fetched:
            raise NoValuesFetched(
                "No values were fetched for this relation, first use .fetch_related()"
            )
        return len(self.related_objects)

    def __bool__(self) -> bool:
        if not self._fetched:
            raise NoValuesFetched(
                "No values were fetched for this relation, first use .fetch_related()"
            )
        return bool(self.related_objects)

    def __getitem__(self, item):
        if not self._fetched:
            raise NoValuesFetched(
                "No values were fetched for this relation, first use .fetch_related()"
            )
        return self.related_objects[item]

    def __await__(self):
        return self._query.__await__()

    async def __aiter__(self):
        if not self._fetched:
            self.related_objects = await self
            self._fetched = True

        for val in self.related_objects:
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

    def _set_result_for_query(self, sequence) -> None:
        self._fetched = True
        self.related_objects = sequence


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
        pk_formatting_func = type(self.instance)._meta.pk.to_db_value
        related_pk_formatting_func = type(instances[0])._meta.pk.to_db_value
        through_table = Table(self.field.through)
        select_query = (
            db.query_class.from_(through_table)
            .where(
                getattr(through_table, self.field.backward_key)
                == pk_formatting_func(self.instance.pk, self.instance)
            )
            .select(self.field.backward_key, self.field.forward_key)
        )
        query = db.query_class.into(through_table).columns(
            getattr(through_table, self.field.forward_key),
            getattr(through_table, self.field.backward_key),
        )

        if len(instances) == 1:
            criterion = getattr(
                through_table, self.field.forward_key
            ) == related_pk_formatting_func(instances[0].pk, instances[0])
        else:
            criterion = getattr(through_table, self.field.forward_key).isin(
                [related_pk_formatting_func(i.pk, i) for i in instances]
            )

        select_query = select_query.where(criterion)

        # TODO: This is highly inefficient. Should use UNIQUE db_index by default.
        #  And optionally allow duplicates.
        _, already_existing_relations_raw = await db.execute_query(str(select_query))
        already_existing_relations = {
            (
                pk_formatting_func(r[self.field.backward_key], self.instance),
                related_pk_formatting_func(r[self.field.forward_key], self.instance),
            )
            for r in already_existing_relations_raw
        }

        insert_is_required = False
        for instance_to_add in instances:
            if not instance_to_add._saved_in_db:
                raise OperationalError(f"You should first call .save() on {instance_to_add}")
            pk_f = related_pk_formatting_func(instance_to_add.pk, instance_to_add)
            pk_b = pk_formatting_func(self.instance.pk, self.instance)
            if (pk_b, pk_f) in already_existing_relations:
                continue
            query = query.insert(pk_f, pk_b)
            insert_is_required = True
        if insert_is_required:
            await db.execute_query(str(query))

    async def clear(self, using_db=None) -> None:
        """
        Clears ALL relations.
        """
        db = using_db if using_db else self.remote_model._meta.db
        through_table = Table(self.field.through)
        pk_formatting_func = type(self.instance)._meta.pk.to_db_value
        query = (
            db.query_class.from_(through_table)
            .where(
                getattr(through_table, self.field.backward_key)
                == pk_formatting_func(self.instance.pk, self.instance)
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
        pk_formatting_func = type(self.instance)._meta.pk.to_db_value
        related_pk_formatting_func = type(instances[0])._meta.pk.to_db_value

        if len(instances) == 1:
            condition = (
                getattr(through_table, self.field.forward_key)
                == related_pk_formatting_func(instances[0].pk, instances[0])
            ) & (
                getattr(through_table, self.field.backward_key)
                == pk_formatting_func(self.instance.pk, self.instance)
            )
        else:
            condition = (
                getattr(through_table, self.field.backward_key)
                == pk_formatting_func(self.instance.pk, self.instance)
            ) & (
                getattr(through_table, self.field.forward_key).isin(
                    [related_pk_formatting_func(i.pk, i) for i in instances]
                )
            )
        query = db.query_class.from_(through_table).where(condition).delete()
        await db.execute_query(str(query))


class RelationField(Field):
    has_db_column = False

    def attribute_property(self):
        raise NotImplementedError()

    def create_relation(self):
        raise NotImplementedError()

    def get_joins(self, table: Table):
        related_table_pk = self.remote_model._meta.pk_db_column
        related_table = self.remote_model._meta.basetable
        related_table = related_table.as_(f"{table.get_table_name()}__{self.model_field_name}")
        return [
            (related_table,
             getattr(related_table, related_table_pk) == getattr(table, f"{self.model_field_name}_id"),)
        ]

    async def prefetch(self, instance_list: list, related_query: "QuerySet[MODEL]") -> list:
        related_objects_for_fetch = set()
        relation_key_field = f"{self.model_field_name}_id"
        for instance in instance_list:
            if getattr(instance, relation_key_field):
                related_objects_for_fetch.add(getattr(instance, relation_key_field))
            else:
                setattr(instance, self.model_field_name, None)

        if related_objects_for_fetch:
            related_object_list = await related_query.filter(pk__in=list(related_objects_for_fetch))
            related_object_map = {obj.pk: obj for obj in related_object_list}
            for instance in instance_list:
                setattr(instance, self.model_field_name,
                    related_object_map.get(getattr(instance, relation_key_field)))

        return instance_list

    def describe(self, serializable: bool = True) -> dict:
        desc = super().describe(serializable)

        # RelationFields are entirely "virtual", so no direct DB representation
        del desc["db_column"]
        return desc


class BackwardFKRelation(RelationField):
    def __init__(
        self,
        remote_model: "Type[Model]",
        relation_field: str,
        null: bool,
        description: Optional[str]
    ) -> None:
        super().__init__(null=null)
        self.remote_model: "Type[Model]" = remote_model
        self.relation_field: str = relation_field
        self.description: Optional[str] = description
        self.auto_created = True

    def attribute_property(self):
        _key = f"_{self.model_field_name}"
        return property(
            partial(
                _rfk_getter,
                _key=_key,
                ftype=self.remote_model,
                frelfield=self.relation_field,
            )
        )

    def create_relation(self):
        raise RuntimeError("This method on should not have been called on a generated relation.")

    async def prefetch(self, instance_list: list, related_query: "QuerySet[MODEL]") -> list:
        instance_id_set: set = {
            self.model._meta.db.executor_class._field_to_db(instance._meta.pk, instance.pk, instance)
            for instance in instance_list
        }
        relation_field = self.model._meta.fields_map[self.model_field_name].relation_field  # type: ignore

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
            relation_container = getattr(instance, self.model_field_name)
            relation_container._set_result_for_query(related_object_map.get(instance.pk, []))

        return instance_list

    def get_joins(self, table: Table):
        table_pk = self.model._meta.pk_db_column
        related_table = self.remote_model._meta.basetable

        return [
            (related_table,
             getattr(table, table_pk) == getattr(related_table, self.relation_field),)
        ]


class ForeignKeyField(RelationField):
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

    backward_relation_class = BackwardFKRelation

    def __init__(
        self,
        model_name: str,
        primary_key: bool = False,
        unique: bool = False,
        related_name: Union[Optional[str], Literal[False]] = None,
        on_delete=CASCADE,
        **kwargs,
    ) -> None:

        super().__init__(primary_key=primary_key, unique=unique, **kwargs)
        if primary_key and not unique:
            raise ConfigurationError(f"{self.__class__.__name__} cannot be a primary key if not unique")

        if len(model_name.split(".")) != 2:
            raise ConfigurationError(f'{self.__class__.__name__} accepts model name in format "app.Model"')

        self.remote_model: "Type[Model]" = None  # type: ignore
        self.model_name = model_name
        self.related_name = related_name

        if on_delete not in {CASCADE, RESTRICT, SET_NULL}:
            raise ConfigurationError("on_delete can only be CASCADE, RESTRICT or SET_NULL")

        if on_delete == SET_NULL and not bool(kwargs.get("null")):
            raise ConfigurationError("If on_delete is SET_NULL, then field must have null=True set")

        self.on_delete = on_delete

    def attribute_property(self):
        _key = f"_{self.model_field_name}"
        relation_field = self.db_column
        return property(
            partial(
                _fk_getter,
                _key=_key,
                ftype=self.remote_model,  # type: ignore
                relation_field=relation_field,
            ),
            partial(_fk_setter, _key=_key, relation_field=relation_field),
            partial(_fk_setter, value=None, _key=_key, relation_field=relation_field),
        )

    def create_relation(self):
        from tortoise import Tortoise
        remote_model = Tortoise.get_model(self.model_name)

        key_field_name = f"{self.model_field_name}_id"
        key_field_object = deepcopy(remote_model._meta.pk)
        key_field_object.primary_key = self.primary_key
        key_field_object.unique = self.unique
        key_field_object.db_index = self.db_index
        key_field_object.default = self.default
        key_field_object.null = self.null
        key_field_object.generated = self.generated
        key_field_object.auto_created = True
        key_field_object.reference = self
        key_field_object.description = self.description
        key_field_object.db_column = self.db_column if self.db_column else key_field_name

        self.db_column = key_field_name
        self.model._meta.add_field(key_field_name, key_field_object)
        self.remote_model = remote_model

        backward_relation_name = self.related_name
        if backward_relation_name is not False:
            if not backward_relation_name:
                backward_relation_name = f"{self.model._meta.db_table}s"

            if backward_relation_name in remote_model._meta.fields_map:
                related_app_name, related_model_name = self.model_name.split(".")
                raise ConfigurationError(
                    f'backward relation "{backward_relation_name}" duplicates in'
                    f" model {related_model_name}"
                )

            backward_relation_field = self.backward_relation_class(
                self.model, key_field_name, True, self.description)
            remote_model._meta.add_field(backward_relation_name, backward_relation_field)

        if self.primary_key:
            self.model._meta.pk_attr = key_field_name

    def describe(self, serializable: bool = True) -> dict:
        desc = super().describe(serializable)
        desc["raw_field"] = self.db_column
        return desc


class BackwardOneToOneRelation(BackwardFKRelation):
    def attribute_property(self):
        _key = f"_{self.model_field_name}"
        return property(
            partial(
                _ro2o_getter,
                _key=_key,
                ftype=self.remote_model,
                frelfield=self.relation_field,
            ),
        )

    async def prefetch(self, instance_list: list, related_query: "QuerySet[MODEL]") -> list:
        instance_id_set: set = {
            self.model._meta.db.executor_class._field_to_db(instance._meta.pk, instance.pk, instance)
            for instance in instance_list
        }
        relation_field = self.model._meta.fields_map[self.model_field_name].relation_field  # type: ignore

        related_object_list = await related_query.filter(
            **{f"{relation_field}__in": list(instance_id_set)}
        )

        related_object_map = {getattr(entry, relation_field): entry for entry in related_object_list}
        for instance in instance_list:
            setattr(instance, f"_{self.model_field_name}", related_object_map.get(instance.pk, None))

        return instance_list


class OneToOneField(ForeignKeyField):
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

    backward_relation_class = BackwardOneToOneRelation

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
        related_name: Optional[str] = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)

        self.remote_model: "Type[Model]" = None
        if len(model_name.split(".")) != 2:
            raise ConfigurationError('Foreign key accepts model name in format "app.Model"')

        self.model_name: str = model_name
        self.related_name: str = related_name
        self.forward_key: str = forward_key or f"{model_name.split('.')[1].lower()}_id"
        self.backward_key: str = backward_key
        self.through: Optional[str] = through

    def attribute_property(self):
        _key = f"_{self.model_field_name}"
        return property(partial(_m2m_getter, _key=_key, field_object=self))

    def create_relation(self):
        from tortoise import Tortoise

        backward_key = self.backward_key
        if not backward_key:
            backward_key = f"{self.model._meta.db_table}_id"

            if backward_key == self.forward_key:
                backward_key = f"{self.model._meta.db_table}_rel_id"

            self.backward_key = backward_key

        remote_model = Tortoise.get_model(self.model_name)
        self.remote_model = remote_model

        backward_relation_name = self.related_name
        if not backward_relation_name:
            backward_relation_name = self.related_name = f"{self.model._meta.db_table}s"

        if backward_relation_name in remote_model._meta.fields_map:
            related_app_name, related_model_name = self.model_name.split(".")
            raise ConfigurationError(
                f'backward relation "{backward_relation_name}" duplicates in'
                f" model {related_model_name}"
            )

        if not self.through:
            related_model_table_name = remote_model._meta.db_table or remote_model.__name__.lower()
            self.through = f"{self.model._meta.db_table}_{related_model_table_name}"

        elif "." in self.through:
            through_model = Tortoise.get_model(self.through)
            self.through = through_model._meta.db_table

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
            self.model._meta.db.executor_class._field_to_db(instance._meta.pk, instance.pk, instance)
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

        related_query_table = related_query.model._meta.basetable
        related_pk_field = related_query.model._meta.pk_db_column
        related_query.query = related_query.create_base_query_all_fields(alias=None)
        related_query.query = (
            related_query.query.join(subquery)
            .on(getattr(subquery, field_object.forward_key) == related_query_table[related_pk_field])
            .select(getattr(subquery, field_object.backward_key))
        )

        related_query._add_query_details(QueryContext().push(
            related_query.model,
            related_query_table,
            {field_object.through: through_table.as_(subquery.alias)}
        ))

        _, raw_results = await self.model._meta.db.execute_query(related_query.query.get_sql())
        relations = [
            (
                self.model._meta.pk.to_python_value(e[field_object.backward_key]),
                related_query.model._init_from_db(**e),
            )
            for e in raw_results
        ]

        related_executor = self.model._meta.db.executor_class(
            model=related_query.model,
            db=self.model._meta.db,
            prefetch_map=related_query._prefetch_map,
            prefetch_queries=related_query._prefetch_queries,
        )

        await related_executor._execute_prefetch_queries([item for _, item in relations])

        relation_map = {}
        for k, item in relations:
            relation_map.setdefault(k, []).append(item)

        for instance in instance_list:
            relation_container = getattr(instance, self.model_field_name)
            relation_container._set_result_for_query(relation_map.get(instance.pk, []))

        return instance_list

    def get_joins(self, table: Table):
        table_pk = self.model._meta.pk_db_column
        related_table_pk = self.remote_model._meta.pk_db_column
        related_table = self.remote_model._meta.basetable
        through_table = Table(self.through)
        return [
            (through_table,
                getattr(table, table_pk) == getattr(through_table, self.backward_key),),
            (related_table,
                getattr(through_table, self.forward_key) == getattr(related_table, related_table_pk),)
        ]
