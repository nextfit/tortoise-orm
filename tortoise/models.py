
import itertools
from copy import deepcopy
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Type, TypeVar, Iterator, TYPE_CHECKING, Iterable

from pypika import Order, Table

from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import ConfigurationError, OperationalError, UnknownFieldError
from tortoise.fields.base import Field
from tortoise.fields.data import IntegerField
from tortoise.fields.relational import (
    BackwardFKField,
    BackwardOneToOneField,
    ForeignKey,
    ManyToManyField,
    OneToOneField,
    RelationField,
)
from tortoise.filters import FieldFilter
from tortoise.query.queryset import QuerySet
from tortoise.query.raw import RawQuerySet
from tortoise.query.single import FirstQuerySet, GetQuerySet

if TYPE_CHECKING:
    from tortoise.backends.base.client import BaseDBAsyncClient


MODEL = TypeVar("MODEL", bound="Model")


class MetaInfo:
    __slots__ = (
        "abstract",
        "_inited",

        "_model",
        "app_label",
        "connection_name",
        "db_table",
        "table_description",

        "ordering",
        "unique_together",
        "indexes",
        "pk_attr",

        "fields_map",
        "field_to_db_column_name_map",
        "db_column_to_field_name_map",
        "generated_column_names",

        "_filter_cache",
    )

    def __init__(self, meta) -> None:
        self.abstract: bool = getattr(meta, "abstract", False)
        self.ordering: List[Tuple[str, Order]] = getattr(meta, "ordering", None)
        self.app_label: Optional[str] = getattr(meta, "app_label", None)
        self.unique_together: Tuple[Tuple[str, ...], ...] = self.__get_unique_together(meta)
        self.indexes: Tuple[Tuple[str, ...], ...] = self.__get_indexes(meta)
        self.table_description: str = getattr(meta, "table_description", "")

        self.connection_name: Optional[str] = None
        self._inited: bool = False

        self._model: Type["Model"]
        self.db_table: str

        self.fields_map: Dict[str, Field]
        self.field_to_db_column_name_map: Dict[str, str]
        self.db_column_to_field_name_map: Dict[str, str]

        self.pk_attr: str
        self.generated_column_names: List[str]

        self._filter_cache: Dict[str, Optional[FieldFilter]] = {}

    @staticmethod
    def __get_unique_together(meta) -> Tuple[Tuple[str, ...], ...]:
        _together = getattr(meta, "unique_together", ())

        if isinstance(_together, (list, tuple)):
            if _together and all(isinstance(t, str) for t in _together):
                _together = (_together,)

        return _together

    @staticmethod
    def __get_indexes(meta) -> Tuple[Tuple[str, ...], ...]:
        _indexes = getattr(meta, "indexes", ())

        if isinstance(_indexes, (list, tuple)):
            if _indexes and all(isinstance(t, str) for t in _indexes):
                _indexes = [(t,) for t in _indexes]

        return _indexes

    def add_field(self, name: str, field: Field):
        if name in self.fields_map:
            raise ConfigurationError(f"Field {name} already present in meta")

        field.model_field_name = name
        field.model = self._model

        self.fields_map[name] = field
        if field.has_db_column:
            if not field.db_column:
                field.db_column = name
            self.field_to_db_column_name_map[name] = field.db_column

    def get_field(self, name: str):
        if name in self.fields_map:
            return self.fields_map[name]

        raise UnknownFieldError(name, self)

    def table(self, alias=None) -> Table:
        return Table(self.db_table, alias=alias)

    @property
    def db(self) -> "BaseDBAsyncClient":
        try:
            from tortoise import Tortoise
            return Tortoise.get_transaction_db_client(self.connection_name)
        except KeyError:
            raise ConfigurationError("No DB associated to model")

    def __create_filter(self, key: str) -> Optional[FieldFilter]:
        (field_name, sep, comparison) = key.partition(LOOKUP_SEP)
        field = self.fields_map.get(field_name)
        if not field:
            return None

        filter_funcs = self.db.filter_class.get_filter_func_for(field, comparison)
        return field.create_filter(*filter_funcs) if filter_funcs else None

    def get_filter(self, key: str) -> Optional[FieldFilter]:
        if key in self._filter_cache:
            return self._filter_cache[key]

        else:
            key_filter = self.__create_filter(key)
            self._filter_cache[key] = key_filter
            return key_filter

    @property
    def pk(self) -> Field:
        return self.fields_map[self.pk_attr]

    @property
    def db_columns(self) -> Dict[str, str]:
        return self.db_column_to_field_name_map

    @property
    def pk_db_column(self) -> str:
        return self.pk.db_column

    def finalize_model(self) -> None:
        """
        Finalise the model after it had been fully loaded.
        """
        self._setup_relation_properties()
        self._finalize_model_data()

    def _finalize_model_data(self) -> None:
        self.db_column_to_field_name_map = {
            db_column: field_name for field_name, db_column in self.field_to_db_column_name_map.items()
        }

        self.generated_column_names = [field.db_column
            for field in self.fields_map.values() if field.generated]

    def _setup_relation_properties(self) -> None:
        for key, field in self.fields_map.items():
            if isinstance(field, RelationField):
                setattr(self._model, key, field.attribute_property())


class ModelMeta(type):
    __slots__ = ()

    def __new__(mcs, name: str, bases, attrs: dict, *args, **kwargs):
        field_to_db_column_name_map: Dict[str, str] = dict()
        fields_map: Dict[str, Field] = dict()
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
                    if value.primary_key:
                        if custom_pk_present:
                            raise ConfigurationError(
                                f"Can't create model {name} with two primary keys,"
                                " only single primary key is supported"
                            )
                        if value.generated and not value.allows_generated:
                            raise ConfigurationError(
                                f"Field '{key}' ({value.__class__.__name__}) can't be DB-generated"
                            )
                        custom_pk_present = True
                        pk_attr = key

            if not custom_pk_present and not getattr(meta_class, "abstract", None):
                if pk_attr not in attrs:
                    attrs = {pk_attr: IntegerField(primary_key=True), **attrs}

                if not isinstance(attrs[pk_attr], Field) or not attrs[pk_attr].primary_key:
                    raise ConfigurationError(
                        f"Can't create model {name} without explicit primary key if field '{pk_attr}'"
                        " already present"
                    )

            for key, value in attrs.items():
                if isinstance(value, Field):
                    if getattr(meta_class, "abstract", None):
                        value = deepcopy(value)

                    fields_map[key] = value
                    value.model_field_name = key

                    if value.has_db_column:
                        if not value.db_column:
                            value.db_column = key

                        field_to_db_column_name_map[key] = value.db_column

        # Clean the class attributes
        for slot in fields_map:
            attrs.pop(slot, None)

        attrs["_meta"] = meta = MetaInfo(meta_class)

        meta.fields_map = fields_map
        meta.field_to_db_column_name_map = field_to_db_column_name_map
        meta.pk_attr = pk_attr

        if not fields_map:
            meta.abstract = True

        new_class = super().__new__(mcs, name, bases, attrs)
        meta.db_table = getattr(meta_class, "db_table", new_class.__name__.lower())
        for field in meta.fields_map.values():
            field.model = new_class

        meta._model = new_class
        return new_class


class Model(metaclass=ModelMeta):
    class Meta:
        """
        The ``Meta`` class is used to configure metadata for the Model.

        Usage:

        .. code-block:: python3

            class Foo(Model):
                ...

                class Meta:
                    db_table="custom_table"
                    unique_together=(("field_a", "field_b"), )
        """
        pass

    # I don' like this here, but it makes auto completion and static analysis much happier
    _meta = MetaInfo(None)

    def __init__(self, **kwargs) -> None:

        meta = self._meta  # self._meta is a very common lookup, cache it.
        self._saved_in_db = False
        self._custom_generated_pk = False

        ignore_fields: Set[str] = set()

        for field_name, field_object in meta.fields_map.items():
            if field_name in ignore_fields:
                continue

            if field_name in kwargs:
                value = kwargs[field_name]
                if field_object.has_db_column:
                    if field_object.generated:
                        self._custom_generated_pk = True
                    if value is None and not field_object.null:
                        raise ValueError(f"{field_name} is non nullable field, but null was passed")
                    setattr(self, field_name, field_object.to_python_value(value))

                elif isinstance(field_object, (ForeignKey, OneToOneField)):
                    if value and not value._saved_in_db:
                        raise OperationalError(
                            f"You should first call .save() on {value} before referring to it"
                        )
                    setattr(self, field_name, value)
                    ignore_fields.add(field_object.id_field_name)

                elif isinstance(field_object, (BackwardFKField, BackwardOneToOneField)):
                    raise ConfigurationError(
                        "You can't set backward relations through init, change related model instead"
                    )

                elif isinstance(field_object, ManyToManyField):
                    raise ConfigurationError(
                        "You can't set m2m relations through init, use m2m_manager instead"
                    )

            elif field_object.has_db_column:
                value = field_object.default
                if callable(value):
                    value = value()

                setattr(self, field_name, value)

    @classmethod
    def _init_from_db_row(cls: Type[MODEL], row_iter: Iterator[Tuple[str, Any]],
        related_map: Optional[Dict[str, Dict]] = None) -> MODEL:

        self = cls.__new__(cls)
        self._saved_in_db = True

        meta = self._meta

        for db_column, value in itertools.islice(row_iter, len(meta.db_columns)):
            field_name = meta.db_column_to_field_name_map[db_column]
            field_object = meta.fields_map[field_name]
            setattr(self, field_name, field_object.to_python_value(value))

        if related_map:
            for field_name, sub_related in related_map.items():
                field_object = meta.fields_map[field_name]
                setattr(self, field_name, field_object.remote_model._init_from_db_row(row_iter, sub_related))

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

        :param using_db:
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
        cls: Type[MODEL], objects: Iterable[MODEL], using_db: Optional["BaseDBAsyncClient"] = None
    ) -> None:
        """
        Bulk insert operation:

        .. note::
            The bulk insert operation will do the minimum to ensure that the object
            created in the DB has all the defaults and generated fields set,
            but may be incomplete reference in Python.

            e.g. ``IntegerField`` primary keys will not be populated.

        This is recommend only for throw away inserts where you want to ensure optimal
        insert performance.

        .. code-block:: python3

            User.bulk_create([
                User(name="...", email="..."),
                User(name="...", email="...")
            ])

        :param using_db:
        :param objects: Iterable of objects to bulk create

        """
        db = using_db or cls._meta.db
        await db.executor_class(model=cls, db=db).execute_bulk_insert(objects)  # type: ignore

    @classmethod
    async def bulk_update(
        cls: Type[MODEL], objects: Iterable[MODEL], update_fields: List[str], using_db: Optional["BaseDBAsyncClient"] = None
    ) -> None:
        """
        Bulk update operation:


        .. code-block:: python3

            User.bulk_update([
                User(name="...", email="..."),
                User(name="...", email="...")
            ], update_fields=["name", "email"])

        :param update_fields: List of fields to be updated
        :param using_db:
        :param objects: Iterable of objects to bulk update

        """
        db = using_db or cls._meta.db
        await db.executor_class(model=cls, db=db).execute_bulk_update(objects, update_fields)  # type: ignore

    @classmethod
    def first(cls: Type[MODEL]) -> FirstQuerySet[MODEL]:
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
    def annotate(cls: Type[MODEL], *args, **kwargs) -> QuerySet[MODEL]:
        return QuerySet(cls).annotate(*args, **kwargs)

    @classmethod
    def all(cls: Type[MODEL]) -> QuerySet[MODEL]:
        """
        Returns the complete QuerySet.
        """
        return QuerySet(cls)

    @classmethod
    def raw(cls: Type[MODEL], raw_sql) -> RawQuerySet[MODEL]:
        """
        Returns the complete QuerySet.
        """
        return QuerySet(cls).raw(raw_sql)

    @classmethod
    def get(cls: Type[MODEL], *args, **kwargs) -> GetQuerySet[MODEL]:
        """
        Fetches a single record for a Model type using the provided filter parameters.

        .. code-block:: python3

            user = await User.get(username="foo")

        :raises MultipleObjectsReturned: If provided search returned more than one object.
        :raises DoesNotExist: If object can not be found.
        """
        return QuerySet(cls).get(*args, **kwargs)

    @classmethod
    def get_or_none(cls: Type[MODEL], *args, **kwargs) -> FirstQuerySet[MODEL]:
        """
        Fetches a single record for a Model type using the provided filter parameters or None.

        .. code-block:: python3

            user = await User.get(username="foo")
        """
        return QuerySet(cls).get_or_none(*args, **kwargs)

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

    @classmethod
    def full_name(cls) -> str:
        return f"{cls._meta.app_label}.{cls.__name__}"

    @classmethod
    def describe(cls, serializable: bool = True) -> dict:
        """
        Describes the given list of models or ALL registered models.

        :param serializable:
            ``False`` if you want raw python objects,
            ``True`` for JSON-serialisable data. (Defaults to ``True``)

        :return:
            A dictionary containing the model description.

            The base dict has a fixed set of keys that reference a list of fields
            (or a single field in the case of the primary key):

            .. code-block:: python3

                {
                    "name":                 str     # Qualified model name
                    "app_label":            str     # 'App' namespace
                    "db_table":             str     # DB table name
                    "abstract":             bool    # Is the model Abstract?
                    "description":          str     # Description of table (nullable)
                    "unique_together":      [...]   # List of List containing field names that
                                                    #  are unique together
                    "pk_field":             {...}   # Primary key field
                    "data_fields":          [...]   # Data fields
                }

            Each field is specified as follows
            (This assumes ``serializable=True``, which is the default):

            .. code-block:: python3

                {
                    "name":         str     # Field name
                    "field_type":   str     # Field type
                    "db_column":    str     # Name of DB column
                                            #  Optional: Only for pk/data fields
                    "raw_field":    str     # Name of raw field of the Foreign Key
                                            #  Optional: Only for Foreign Keys
                    "db_column_types": dict  # DB Field types for default and DB overrides
                    "python_type":  str     # Python type
                    "generated":    bool    # Is the field generated by the DB?
                    "auto_created": bool    # Is the field auto created by Tortoise?
                    "nullable":     bool    # Is the column nullable?
                    "unique":       bool    # Is the field unique?
                    "db_index":     bool    # Is the field indexed?
                    "default":      ...     # The default value (coerced to int/float/str/bool/null)
                    "description":  str     # Description of the field (nullable)
                }

            When ``serializable=False`` is specified some fields are not coerced to valid
            JSON types. The changes are:

            .. code-block:: python3

                {
                    "field_type":   Field   # The Field class used
                    "python_type":  Type    # The actual Python type
                    "default":      ...     # The default value as native type OR a callable
                }
        """

        _meta = cls._meta

        return {
            "name": cls.full_name(),
            "app_label": _meta.app_label,
            "db_table": _meta.db_table,
            "abstract": _meta.abstract,
            "description": _meta.table_description or None,
            "unique_together": _meta.unique_together or [],
            "pk_field": _meta.pk.describe(serializable),
            "fields": [
                field.describe(serializable) for field in _meta.fields_map.values()
                if field.model_field_name != _meta.pk_attr
            ]
        }
