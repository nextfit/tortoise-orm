
class BaseORMException(Exception):
    """
    Base ORM Exception.
    """


class FieldError(BaseORMException):
    """
    The FieldError exception is raised when there is a problem with a model field.
    """


class BaseFieldError(FieldError):
    error_pattern = "FieldError({field_name}, {model})"

    def __init__(self, field_name: str, model: "Type[Model]", extra_msg=''):
        super().__init__(self.error_pattern.format(field_name=field_name, model=model) + extra_msg)
        self.field_name = field_name
        self.model = model

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self.field_name == other.field_name and \
            self.model == other.model


class UnknownFieldError(FieldError):
    error_pattern = 'Unknown field "{field_name}" for model "{model}"'


class NotARelationFieldError(FieldError):
    error_pattern = 'Field "{field_name}" is not a relation for model "{model}"'


class ParamsError(BaseORMException):
    """
    The ParamsError is raised when function can not be run with given parameters
    """


class ConfigurationError(BaseORMException):
    """
    The ConfigurationError exception is raised when the configuration of the ORM is invalid.
    """


class TransactionManagementError(BaseORMException):
    """
    The TransactionManagementError is raised when any transaction error occurs.
    """


class OperationalError(BaseORMException):
    """
    The OperationalError exception is raised when an operational error occurs.
    """


class IntegrityError(OperationalError):
    """
    The IntegrityError exception is raised when there is an integrity error.
    """


class NoValuesFetched(OperationalError):
    """
    The NoValuesFetched exception is raised when the related model was never fetched.
    """


class MultipleObjectsReturned(OperationalError):
    """
    The MultipleObjectsReturned exception is raised when doing a ``.get()`` operation,
    and more than one object is returned.
    """


class DoesNotExist(OperationalError):
    """
    The DoesNotExist exception is raised when expecting data, such as a ``.get()`` operation.
    """


class DBConnectionError(BaseORMException, ConnectionError):
    """
    The DBConnectionError is raised when problems with connecting to db occurs
    """
