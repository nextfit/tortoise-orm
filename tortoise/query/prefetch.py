
from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import FieldError


class Prefetch:
    __slots__ = ("relation", "queryset")

    def __init__(self, relation, queryset=None) -> None:
        self.relation = relation
        self.queryset = queryset

    def resolve_for_queryset(self, queryset) -> None:
        first_level_field, _, forwarded_prefetch = self.relation.partition(LOOKUP_SEP)
        if first_level_field not in queryset.model._meta.fetch_fields:
            if first_level_field in queryset.model._meta.fields_map:
                msg = f"Field {first_level_field} on {queryset.model.full_name()} is not a relation"
            else:
                msg = f"Relation {first_level_field} for {queryset.model.full_name()} not found"

            raise FieldError(msg)

        if forwarded_prefetch:
            if first_level_field not in queryset._prefetch_map.keys():
                queryset._prefetch_map[first_level_field] = set()

            queryset._prefetch_map[first_level_field].add(
                Prefetch(forwarded_prefetch, self.queryset))

        elif self.queryset is None:
            if first_level_field not in queryset._prefetch_map.keys():
                queryset._prefetch_map[first_level_field] = set()

        else:
            queryset._prefetch_queries[first_level_field] = self.queryset
