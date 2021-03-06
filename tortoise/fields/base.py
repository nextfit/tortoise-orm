import types
from typing import Any, Dict, List, Optional, Type, Union, TYPE_CHECKING

from tortoise.exceptions import ConfigurationError
from tortoise.filters import FieldFilter

if TYPE_CHECKING:
    from tortoise.models import Model


CASCADE = "CASCADE"
RESTRICT = "RESTRICT"
SET_NULL = "SET NULL"
SET_DEFAULT = "SET DEFAULT"
DO_NOTHING = "NO ACTION"
NO_ACTION = "NO ACTION"


class _FieldMeta(type):
    def __new__(mcs, name, bases, attrs):
        if len(bases) > 1 and bases[0] is Field:
            # Instantiate class with only the 1st base class (should be Field)
            cls: Type[Field] = type.__new__(mcs, name, (bases[0],), attrs)

            # All other base classes are our meta types, we store them in class attributes
            cls.field_type = bases[1] if len(bases) == 2 else bases[1:]

            return cls

        return type.__new__(mcs, name, bases, attrs)


class Field(metaclass=_FieldMeta):
    """
    Base Field type.
    """

    # Field_type is a readonly property for the instance, it is set by _FieldMeta
    field_type: Type[Any] = None  # type: ignore
    indexable: bool = True
    has_db_column = True
    allows_generated = False
    function_cast = None

    # This method is just to make IDE/Linters happy
    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)

    def __init__(
        self,
        db_column: Optional[str] = None,
        generated: bool = False,
        primary_key: bool = False,
        null: bool = False,
        default: Any = None,
        unique: bool = False,
        db_index: bool = False,
        description: Optional[str] = None,
        **kwargs,
    ) -> None:

        if not self.indexable and (unique or db_index):
            raise ConfigurationError(f"{self.__class__.__name__} can't be indexed")

        if primary_key:
            db_index = True
            unique = True

        self.db_index = db_index
        self.db_column: str = db_column
        self.unique = unique

        self.generated = generated
        self.primary_key = primary_key
        self.default = default
        self.null = null
        self.description = description
        self.auto_created = False

        self.model_field_name: str
        self.model: Type["Model"]
        self.reference: Optional['Field'] = None

    def __str__(self):
        return f"{self.model_field_name} ({self.db_column})"

    def db_value(self, value: Any, instance) -> Any:
        return self.get_for_dialect('to_db_value')(value, instance)

    def to_db_value(self, value: Any, instance) -> Any:
        if value is None or isinstance(value, self.field_type):
            return value
        return self.field_type(value)  # pylint: disable=E1102

    def to_python_value(self, value: Any) -> Any:
        if value is None or isinstance(value, self.field_type):
            return value
        return self.field_type(value)  # pylint: disable=E1102

    def create_filter(self, opr, value_encoder) -> FieldFilter:
        from tortoise.filters.data import DataFieldFilter
        return DataFieldFilter(self, opr, value_encoder)

    @property
    def required(self):
        return self.default is None and not self.null and not self.generated

    def _get_dialects(self) -> Dict[str, dict]:
        return {
            dialect[4:]: {
                key: val
                for key, val in getattr(self, dialect).__dict__.items()
                if not key.startswith("_")
            }
            for dialect in dir(self) if dialect.startswith("_db_")
        }

    def get_for_dialect(self, key: str) -> Any:
        #
        # The following two lines is the original code,
        # but is an overkill unless _get_dialects is cached.
        #
        #   dialect_data = self._get_dialects().get(dialect, {})
        #   return dialect_data.get(key, getattr(self, key, None))
        #

        dialect = self.model._meta.db.capabilities.dialect
        db_meta = getattr(self, f"_db_{dialect}", None)

        if db_meta and key in db_meta.__dict__:
            attrib = db_meta.__dict__.get(key)
            if callable(attrib):
                attrib = types.MethodType(attrib, self)

            return attrib

        return getattr(self, key, None)

    def get_db_column_types(self) -> Optional[Dict[str, str]]:
        return {
            "": getattr(self, "SQL_TYPE"),
            **{
                dialect: dialect_options["SQL_TYPE"]
                for dialect, dialect_options in self._get_dialects().items()
                if "SQL_TYPE" in dialect_options
            },
        }

    def describe(self, serializable: bool = True) -> dict:

        def default_name(default: Any) -> Optional[Union[int, float, str, bool]]:
            if isinstance(default, (int, float, str, bool, type(None))):
                return default
            if callable(default):
                return f"<function {default.__module__}.{default.__name__}>"
            return str(default)

        def _type_name(typ) -> str:
            if typ.__module__ == "builtins":
                return typ.__name__
            return f"{typ.__module__}.{typ.__name__}"

        def type_name(typ: Any) -> Union[str, List[str]]:
            try:
                from tortoise.models import Model
                if issubclass(typ, Model):
                    return typ.full_name()
            except TypeError:
                pass

            try:
                return _type_name(typ)
            except AttributeError:
                return [_type_name(_typ) for _typ in typ]

        # TODO: db_type
        field_type = getattr(self, "remote_model", self.field_type)
        desc = {
            "name": self.model_field_name,
            "field_type": self.__class__.__name__ if serializable else self.__class__,
            "db_column": self.db_column,
            "db_column_types": self.get_db_column_types(),
            "python_type": type_name(field_type) if serializable else field_type,
            "generated": self.generated,
            "auto_created": self.auto_created,
            "nullable": self.null,
            "unique": self.unique,
            "db_index": self.db_index or self.unique,
            "default": default_name(self.default) if serializable else self.default,
            "description": self.description,
        }

        # Delete db fields for non-db fields
        if not desc["db_column_types"]:
            del desc["db_column_types"]

        return desc
