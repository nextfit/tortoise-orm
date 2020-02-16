
from copy import deepcopy
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Type, TypeVar

from pypika import Query, Table, Order

from tortoise.exceptions import ConfigurationError, OperationalError
from tortoise.fields.base import Field
from tortoise.fields.data import IntField
from tortoise.fields.relational import (
    BackwardFKRelation,
    BackwardOneToOneRelation,
    ForeignKeyField,
    ManyToManyField,
    OneToOneField,
    RelationField)

from tortoise.filters import FieldFilter, BaseFieldFilter, ManyToManyRelationFilter, RELATED_FILTER_FUNC_MAP, \
    BackwardFKFilter
from tortoise.queryset import QuerySet, QuerySetSingle
from tortoise.transactions import current_transaction_map

MODEL = TypeVar("MODEL", bound="Model")
# TODO: Define Filter type object. Possibly tuple?


def get_together(meta, together: str) -> Tuple[Tuple[str, ...], ...]:
    _together = getattr(meta, together, ())

    if isinstance(_together, (list, tuple)):
        if _together and isinstance(_together[0], str):
            _together = (_together,)

    # return without validation, validation will be done further in the code
    return _together


class MetaInfo:
    __slots__ = (
        "abstract",
        "table",
        "ordering",
        "app",
        "db_fields",
        "fetch_fields",
        "field_to_db_column_name_map",
        "_inited",
        "db_column_to_field_name_map",
        "fields_map",
        "default_connection",
        "basequery",
        "basetable",
        "unique_together",
        "indexes",
        "pk_attr",
        "generated_column_names",
        "_model",
        "table_description",
        "pk",
        "db_pk_field",
        "_filter_cache",
    )

    def __init__(self, meta) -> None:
        self.abstract: bool = getattr(meta, "abstract", False)
        self.table: str = getattr(meta, "table", "")
        self.ordering: List[Tuple[str, Order]] = getattr(meta, "ordering", None)
        self.app: Optional[str] = getattr(meta, "app", None)
        self.unique_together: Tuple[Tuple[str, ...], ...] = get_together(meta, "unique_together")
        self.indexes: Tuple[Tuple[str, ...], ...] = get_together(meta, "indexes")
        self.db_fields: Set[str] = set()
        self.fetch_fields: Set[str] = set()
        self.field_to_db_column_name_map: Dict[str, str] = {}
        self.db_column_to_field_name_map: Dict[str, str] = {}
        self.fields_map: Dict[str, Field] = {}
        self._inited: bool = False
        self.default_connection: Optional[str] = None
        self.basequery: Query = Query()
        self.basetable: Table = Table("")
        self.pk_attr: str = getattr(meta, "pk_attr", "")
        self.generated_column_names: Tuple[str] = None  # type: ignore
        self._model: "Model" = None  # type: ignore
        self.table_description: str = getattr(meta, "table_description", "")
        self.pk: Field = None  # type: ignore
        self.db_pk_field: str = ""

        self._filter_cache: Dict[str, Optional[FieldFilter]] = {}

    def add_field(self, name: str, value: Field):
        if name in self.fields_map:
            raise ConfigurationError(f"Field {name} already present in meta")
        value.model = self._model
        self.fields_map[name] = value

        if value.has_db_field:
            self.field_to_db_column_name_map[name] = value.db_column or name

        self.finalise_fields()

    @property
    def db(self) -> "BaseDBAsyncClient":
        try:
            return current_transaction_map[self.default_connection].get()
        except KeyError:
            raise ConfigurationError("No DB associated to model")

    def __create_filter(self, key: str) -> Optional[FieldFilter]:
        (field_name, sep, comparision) = key.partition('__')
        if field_name not in self.fields_map:
            return None

        field = self.fields_map[field_name]
        db_column = field.db_column or field_name

        if isinstance(field, ManyToManyField):
            if comparision not in RELATED_FILTER_FUNC_MAP:
                return None

            (filter_operator, filter_encoder) = RELATED_FILTER_FUNC_MAP[comparision]
            return ManyToManyRelationFilter(field, filter_operator, filter_encoder(field))

        if isinstance(field, BackwardFKRelation):
            if comparision not in RELATED_FILTER_FUNC_MAP:
                return None

            (filter_operator, filter_encoder) = RELATED_FILTER_FUNC_MAP[comparision]
            return BackwardFKFilter(field, filter_operator, filter_encoder(field))

        if comparision not in self.db.executor_class.FILTER_FUNC_MAP:
            return None

        return BaseFieldFilter(
            field_name,
            field,
            db_column,
            *self.db.executor_class.FILTER_FUNC_MAP[comparision])

    def get_filter(self, key: str) -> Optional[FieldFilter]:
        if key in self._filter_cache:
            return self._filter_cache[key]

        else:
            key_filter = self.__create_filter(key)
            self._filter_cache[key] = key_filter
            return key_filter

    def finalise_pk(self) -> None:
        self.pk = self.fields_map[self.pk_attr]
        self.db_pk_field = self.pk.db_column or self.pk_attr

    def finalise_model(self) -> None:
        """
        Finalise the model after it had been fully loaded.
        """
        self.finalise_fields()
        self._generate_relation_properties()

    def finalise_fields(self) -> None:
        self.db_fields = set(self.field_to_db_column_name_map.values())
        self.db_column_to_field_name_map = {
            value: key for key, value in self.field_to_db_column_name_map.items()
        }

        self.fetch_fields = {key for key, field in self.fields_map.items() if not field.has_db_field}
        self.generated_column_names = [field.db_column or field.model_field_name
            for field in self.fields_map.values() if field.generated]

    def _generate_relation_properties(self) -> None:
        for key, field in self.fields_map.items():
            if isinstance(field, RelationField):
                setattr(self._model, key, field.attribute_property())


class ModelMeta(type):
    __slots__ = ()

    def __new__(mcs, name: str, bases, attrs: dict, *args, **kwargs):
        field_to_db_column_name_map: Dict[str, str] = {}
        fields_map: Dict[str, Field] = {}
        meta_class = attrs.get("Meta", type("Meta", (), {}))
        pk_attr: str = "id"

        # Searching for Field attributes in the class hierarchy
        def __search_for_field_attributes(base, attrs: dict):
            """
            Searching for class attributes of type fields.Field
            in the given class.

            If an attribute of the class is an instance of fields.Field,
            then it will be added to the fields dict. But only, if the
            key is not already in the dict. So derived classes have a higher
            precedence. Multiple Inheritance is supported from left to right.

            After checking the given class, the function will look into
            the classes according to the MRO (method resolution order).

            The MRO is 'natural' order, in which python traverses methods and
            fields. For more information on the magic behind check out:
            `The Python 2.3 Method Resolution Order
            <https://www.python.org/download/releases/2.3/mro/>`_.
            """
            for parent in base.__mro__[1:]:
                __search_for_field_attributes(parent, attrs)
            meta = getattr(base, "_meta", None)
            if meta:
                # For abstract classes
                for key, value in meta.fields_map.items():
                    attrs[key] = value
            else:
                # For mixin classes
                for key, value in base.__dict__.items():
                    if isinstance(value, Field) and key not in attrs:
                        attrs[key] = value

        # Start searching for fields in the base classes.
        inherited_attrs: dict = {}
        for base in bases:
            __search_for_field_attributes(base, inherited_attrs)
        if inherited_attrs:
            # Ensure that the inherited fields are before the defined ones.
            attrs = {**inherited_attrs, **attrs}

        if name != "Model":
            custom_pk_present = False
            for key, value in attrs.items():
                if isinstance(value, Field):
                    if value.pk:
                        if custom_pk_present:
                            raise ConfigurationError(
                                f"Can't create model {name} with two primary keys,"
                                " only single pk are supported"
                            )
                        if value.generated and not value.allows_generated:
                            raise ConfigurationError(
                                f"Field '{key}' ({value.__class__.__name__}) can't be DB-generated"
                            )
                        custom_pk_present = True
                        pk_attr = key

            if not custom_pk_present and not getattr(meta_class, "abstract", None):
                if "id" not in attrs:
                    attrs = {"id": IntField(pk=True), **attrs}

                if not isinstance(attrs["id"], Field) or not attrs["id"].pk:
                    raise ConfigurationError(
                        f"Can't create model {name} without explicit primary key if field 'id'"
                        " already present"
                    )

            for key, value in attrs.items():
                if isinstance(value, Field):
                    if getattr(meta_class, "abstract", None):
                        value = deepcopy(value)

                    fields_map[key] = value
                    value.model_field_name = key

                    if value.has_db_field:
                        field_to_db_column_name_map[key] = value.db_column or key

        # Clean the class attributes
        for slot in fields_map:
            attrs.pop(slot, None)
        attrs["_meta"] = meta = MetaInfo(meta_class)

        meta.fields_map = fields_map
        meta.field_to_db_column_name_map = field_to_db_column_name_map
        meta.default_connection = None
        meta.pk_attr = pk_attr
        meta._inited = False
        if not fields_map:
            meta.abstract = True

        new_class: "Model" = super().__new__(mcs, name, bases, attrs)  # type: ignore
        for field in meta.fields_map.values():
            field.model = new_class

        meta._model = new_class
        meta.finalise_fields()
        return new_class


class Model(metaclass=ModelMeta):
    # I don' like this here, but it makes auto completion and static analysis much happier
    _meta = MetaInfo(None)

    def __init__(self, **kwargs) -> None:
        # self._meta is a very common attribute lookup, lets cache it.
        meta = self._meta
        self._saved_in_db = False
        self._custom_generated_pk = False

        # Assign values and do type conversions
        passed_fields = {*kwargs.keys()} | meta.fetch_fields

        for key, value in kwargs.items():
            if key in meta.fields_map:
                field_object = meta.fields_map[key]

                if isinstance(field_object, (ForeignKeyField, OneToOneField)):
                    if value and not value._saved_in_db:
                        raise OperationalError(
                            f"You should first call .save() on {value} before referring to it"
                        )
                    setattr(self, key, value)
                    passed_fields.add(meta.fields_map[key].db_column)  # type: ignore

                elif key in meta.field_to_db_column_name_map:
                    if field_object.generated:
                        self._custom_generated_pk = True
                    if value is None and not field_object.null:
                        raise ValueError(f"{key} is non nullable field, but null was passed")
                    setattr(self, key, field_object.to_python_value(value))

                elif isinstance(field_object, BackwardOneToOneRelation):
                    raise ConfigurationError(
                        "You can't set backward one to one relations through init,"
                        " change related model instead"
                    )

                elif isinstance(field_object, BackwardFKRelation):
                    raise ConfigurationError(
                        "You can't set backward relations through init, change related model instead"
                    )

                elif isinstance(field_object, ManyToManyField):
                    raise ConfigurationError(
                        "You can't set m2m relations through init, use m2m_manager instead"
                    )

        # Assign defaults for missing fields
        missing_fields = set(meta.fields_map.keys()).difference(passed_fields)
        for key in missing_fields:
            field_object = meta.fields_map[key]
            if callable(field_object.default):
                setattr(self, key, field_object.default())
            else:
                setattr(self, key, field_object.default)

    @classmethod
    def _init_from_db(cls: Type[MODEL], **kwargs) -> MODEL:
        self = cls.__new__(cls)
        self._saved_in_db = True

        meta = self._meta
        for key in meta.db_fields:
            field_name = meta.db_column_to_field_name_map[key]
            field_object = meta.fields_map[field_name]

            if (field_object.skip_to_python_if_native and
                field_object.field_type in meta.db.executor_class.DB_NATIVE):
                setattr(self, field_name, kwargs[key])

            else:
                setattr(self, field_name, field_object.to_python_value(kwargs[key]))

        return self

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}>"

    def __repr__(self) -> str:
        if self.pk:
            return f"<{self.__class__.__name__}: {self.pk}>"
        return f"<{self.__class__.__name__}>"

    def __hash__(self) -> int:
        if not self.pk:
            raise TypeError("Model instances without id are unhashable")
        return hash(self.pk)

    def __eq__(self, other) -> bool:
        return type(other) is type(self) and self.pk == other.pk

    def _get_pk_val(self):
        return getattr(self, self._meta.pk_attr)

    def _set_pk_val(self, value):
        setattr(self, self._meta.pk_attr, value)

    pk = property(_get_pk_val, _set_pk_val)
    """
    Alias to the models Primary Key.
    Can be used as a field name when doing filtering e.g. ``.filter(pk=...)`` etc...
    """

    async def save(
        self,
        using_db: Optional["BaseDBAsyncClient"] = None,
        update_fields: Optional[List[str]] = None,
    ) -> None:
        """
        Creates/Updates the current model object.

        If ``update_fields`` is provided, it should be a tuple/list of fields by name.
        This is the subset of fields that should be updated.
        If the object needs to be created ``update_fields`` will be ignored.
        """
        db = using_db or self._meta.db
        executor = db.executor_class(model=self.__class__, db=db)
        if self._saved_in_db:
            await executor.execute_update(self, update_fields)
        else:
            await executor.execute_insert(self)
            self._saved_in_db = True

    async def delete(self, using_db=None) -> None:
        """
        Deletes the current model object.

        :raises OperationalError: If object has never been persisted.
        """
        db = using_db or self._meta.db
        if not self._saved_in_db:
            raise OperationalError("Can't delete unpersisted record")
        await db.executor_class(model=self.__class__, db=db).execute_delete(self)

    async def fetch_related(self, *args, using_db: Optional["BaseDBAsyncClient"] = None) -> None:
        """
        Fetch related fields.

        .. code-block:: python3

            User.fetch_related("emails", "manager")

        :param args: The related fields that should be fetched.
        """
        db = using_db or self._meta.db
        await db.executor_class(model=self.__class__, db=db).fetch_for_list([self], *args)

    @classmethod
    async def get_or_create(
        cls: Type[MODEL],
        using_db: Optional["BaseDBAsyncClient"] = None,
        defaults: Optional[dict] = None,
        **kwargs,
    ) -> Tuple[MODEL, bool]:
        """
        Fetches the object if exists (filtering on the provided parameters),
        else creates an instance with any unspecified parameters as default values.
        """
        if not defaults:
            defaults = {}
        instance = await cls.filter(**kwargs).first()
        if instance:
            return instance, False
        return await cls.create(**defaults, **kwargs, using_db=using_db), True

    @classmethod
    async def create(cls: Type[MODEL], **kwargs) -> MODEL:
        """
        Create a record in the DB and returns the object.

        .. code-block:: python3

            user = await User.create(name="...", email="...")

        Equivalent to:

        .. code-block:: python3

            user = User(name="...", email="...")
            await user.save()
        """
        instance = cls(**kwargs)
        db = kwargs.get("using_db") or cls._meta.db
        await db.executor_class(model=cls, db=db).execute_insert(instance)
        instance._saved_in_db = True
        return instance

    @classmethod
    async def bulk_create(
        cls: Type[MODEL], objects: List[MODEL], using_db: Optional["BaseDBAsyncClient"] = None
    ) -> None:
        """
        Bulk insert operation:

        .. note::
            The bulk insert operation will do the minimum to ensure that the object
            created in the DB has all the defaults and generated fields set,
            but may be incomplete reference in Python.

            e.g. ``IntField`` primary keys will not be populated.

        This is recommend only for throw away inserts where you want to ensure optimal
        insert performance.

        .. code-block:: python3

            User.bulk_create([
                User(name="...", email="..."),
                User(name="...", email="...")
            ])

        :param objects: List of objects to bulk create
        """
        db = using_db or cls._meta.db
        await db.executor_class(model=cls, db=db).execute_bulk_insert(objects)  # type: ignore

    @classmethod
    def first(cls: Type[MODEL]) -> QuerySetSingle[Optional[MODEL]]:
        """
        Generates a QuerySet that returns the first record.
        """
        return QuerySet(cls).first()

    @classmethod
    def filter(cls: Type[MODEL], *args, **kwargs) -> QuerySet[MODEL]:
        """
        Generates a QuerySet with the filter applied.
        """
        return QuerySet(cls).filter(*args, **kwargs)

    @classmethod
    def exclude(cls: Type[MODEL], *args, **kwargs) -> QuerySet[MODEL]:
        """
        Generates a QuerySet with the exclude applied.
        """
        return QuerySet(cls).exclude(*args, **kwargs)

    @classmethod
    def annotate(cls: Type[MODEL], **kwargs) -> QuerySet[MODEL]:
        return QuerySet(cls).annotate(**kwargs)

    @classmethod
    def all(cls: Type[MODEL]) -> QuerySet[MODEL]:
        """
        Returns the complete QuerySet.
        """
        return QuerySet(cls)

    @classmethod
    def get(cls: Type[MODEL], *args, **kwargs) -> QuerySetSingle[MODEL]:
        """
        Fetches a single record for a Model type using the provided filter parameters.

        .. code-block:: python3

            user = await User.get(username="foo")

        :raises MultipleObjectsReturned: If provided search returned more than one object.
        :raises DoesNotExist: If object can not be found.
        """
        return QuerySet(cls).get(*args, **kwargs)

    @classmethod
    def get_or_none(cls: Type[MODEL], *args, **kwargs) -> QuerySetSingle[Optional[MODEL]]:
        """
        Fetches a single record for a Model type using the provided filter parameters or None.

        .. code-block:: python3

            user = await User.get(username="foo")
        """
        return QuerySet(cls).filter(*args, **kwargs).first()

    @classmethod
    async def fetch_for_list(
        cls, instance_list: List[MODEL], *args, using_db: Optional["BaseDBAsyncClient"] = None
    ) -> None:
        db = using_db or cls._meta.db
        await db.executor_class(model=cls, db=db).fetch_for_list(instance_list, *args)

    @classmethod
    def check(cls) -> None:
        """
        Calls various checks to validate the model.

        :raises ConfigurationError: If the model has not been configured correctly.
        """
        cls._check_together("unique_together")
        cls._check_together("indexes")

    @classmethod
    def _check_together(cls, together: str) -> None:
        """Check the value of "unique_together" option."""
        _together = getattr(cls._meta, together)
        if not isinstance(_together, (tuple, list)):
            raise ConfigurationError(f"'{cls.__name__}.{together}' must be a list or tuple.")

        if any(not isinstance(unique_fields, (tuple, list)) for unique_fields in _together):
            raise ConfigurationError(
                f"All '{cls.__name__}.{together}' elements must be lists or tuples."
            )

        for fields_tuple in _together:
            for field_name in fields_tuple:
                field = cls._meta.fields_map.get(field_name)

                if not field:
                    raise ConfigurationError(
                        f"'{cls.__name__}.{together}' has no '{field_name}' field."
                    )

                if isinstance(field, ManyToManyField):
                    raise ConfigurationError(
                        f"'{cls.__name__}.{together}' '{field_name}' field refers"
                        " to ManyToMany field."
                    )

    def __await__(self: MODEL) -> Generator[Any, None, MODEL]:
        async def _self() -> MODEL:
            return self

        return _self().__await__()

    class Meta:
        """
        The ``Meta`` class is used to configure metadata for the Model.

        Usage:

        .. code-block:: python3

            class Foo(Model):
                ...

                class Meta:
                    table="custom_table"
                    unique_together=(("field_a", "field_b"), )
        """
