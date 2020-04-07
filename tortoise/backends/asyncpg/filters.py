import operator

from tortoise.backends.base.filters import BaseFilter
from tortoise.backends.base.filters import (
    not_equal,
    is_in,
    list_encoder,
    not_in,
    is_null,
    bool_encoder,
    not_null,
    contains,
    string_encoder,
    starts_with,
    ends_with,
    insensitive_exact,
    insensitive_contains,
    insensitive_starts_with,
    insensitive_ends_with
)
from tortoise.constants import LOOKUP_SEP
from tortoise.fields import JSONField


class AsyncpgFilter(BaseFilter):
    JSON_FILTER_FUNC_MAP = {
        "": (operator.eq, None),
        "exact": (operator.eq, None),
        "not": (not_equal, None),
        "in": (is_in, list_encoder),
        "not_in": (not_in, list_encoder),
        "isnull": (is_null, bool_encoder),
        "not_isnull": (not_null, bool_encoder),
        "gte": (operator.ge, None),
        "lte": (operator.le, None),
        "gt": (operator.gt, None),
        "lt": (operator.lt, None),
        "contains": (contains, string_encoder),
        "startswith": (starts_with, string_encoder),
        "endswith": (ends_with, string_encoder),
        "iexact": (insensitive_exact, string_encoder),
        "icontains": (insensitive_contains, string_encoder),
        "istartswith": (insensitive_starts_with, string_encoder),
        "iendswith": (insensitive_ends_with, string_encoder),
    }

    @staticmethod
    def json_value_getter(field):
        pass

    @classmethod
    def get_filter_func_for(cls, field, comparison):
        if isinstance(field, JSONField):
            (field_name, sep, detailed_comp) = comparison.partition(LOOKUP_SEP)
            if detailed_comp in cls.JSON_FILTER_FUNC_MAP:
                return cls.JSON_FILTER_FUNC_MAP[detailed_comp][0], AsyncpgFilter.json_value_getter

        return super().get_filter_func_for(field, comparison)
