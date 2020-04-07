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
        "": (operator.eq, string_encoder),
        "exact": (operator.eq, string_encoder),
        "not": (not_equal, string_encoder),
        "in": (is_in, list_encoder),
        "not_in": (not_in, list_encoder),
        "isnull": (is_null, bool_encoder),
        "not_isnull": (not_null, bool_encoder),
        "gte": (operator.ge, string_encoder),
        "lte": (operator.le, string_encoder),
        "gt": (operator.gt, string_encoder),
        "lt": (operator.lt, string_encoder),
        "contains": (contains, string_encoder),
        "startswith": (starts_with, string_encoder),
        "endswith": (ends_with, string_encoder),
        "iexact": (insensitive_exact, string_encoder),
        "icontains": (insensitive_contains, string_encoder),
        "istartswith": (insensitive_starts_with, string_encoder),
        "iendswith": (insensitive_ends_with, string_encoder),
    }

    @staticmethod
    def deep_selector_opr(selector, opr):
        def selected_opration(a, b):
            path = "{{{}}}".format(selector.replace(LOOKUP_SEP, ','))
            return opr(a.get_path_text_value(path), b)

        return selected_opration

    @classmethod
    def get_filter_func_for(cls, field, comparison):
        if isinstance(field, JSONField):
            if comparison not in BaseFilter.FILTER_FUNC_MAP:
                (json_selector, sep, detailed_comp) = comparison.rpartition(LOOKUP_SEP)
                if json_selector and detailed_comp in cls.JSON_FILTER_FUNC_MAP:
                    opr, value_encoder = cls.JSON_FILTER_FUNC_MAP[detailed_comp]
                    return cls.deep_selector_opr(json_selector, opr), value_encoder

                else:
                    opr, value_encoder = cls.JSON_FILTER_FUNC_MAP[""]
                    return cls.deep_selector_opr(comparison, opr), value_encoder

        # Fallback to original
        return super().get_filter_func_for(field, comparison)
