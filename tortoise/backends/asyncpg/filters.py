
from pypika.enums import SqlTypes
from pypika.functions import Cast

from tortoise.backends.base.filters import BaseFilter
from tortoise.constants import LOOKUP_SEP
from tortoise.fields import JSONField, Field
from typing import Callable, Tuple, Optional


class AsyncpgFilter(BaseFilter):
    TYPE_MAP = {
        int: SqlTypes.INTEGER,
    }

    @staticmethod
    def deep_selector_opr(selector: str, opr: Callable) -> Callable:
        def inside_operation(a, b):
            path = "{{{}}}".format(selector.replace(LOOKUP_SEP, ','))
            a = a.get_path_text_value(path)

            type_b = type(b)
            if type_b in AsyncpgFilter.TYPE_MAP:
                a = Cast(a, AsyncpgFilter.TYPE_MAP[type_b])

            return opr(a, b)

        return inside_operation

    @classmethod
    def get_filter_func_for(cls, field: Field, comparison: str) -> Optional[Tuple[Callable, Callable]]:
        if isinstance(field, JSONField):
            if comparison not in BaseFilter.FILTER_FUNC_MAP:
                (json_selector, sep, inside_comparison) = comparison.rpartition(LOOKUP_SEP)
                if not (json_selector and inside_comparison in cls.FILTER_FUNC_MAP):
                    json_selector = comparison
                    inside_comparison = ""

                opr, value_encoder = cls.FILTER_FUNC_MAP[inside_comparison]
                return cls.deep_selector_opr(json_selector, opr), value_encoder

        # Fallback to original
        return super().get_filter_func_for(field, comparison)
