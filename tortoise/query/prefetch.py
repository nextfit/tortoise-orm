
from tortoise.constants import LOOKUP_SEP
from tortoise.exceptions import UnknownFieldError, NotARelationFieldError, BaseFieldError
from typing import Dict, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from tortoise.models import Model


class Prefetch:
    __slots__ = ("relation", "queryset")

    def __init__(self, relation, queryset=None) -> None:
        self.relation = relation
        self.queryset = queryset

    def resolve_for_queryset(self, queryset) -> None:
        model = queryset.model

        first_level_field, _, forwarded_prefetch = self.relation.partition(LOOKUP_SEP)
        field_object = model._meta.fields_map.get(first_level_field)
        if not field_object:
            raise UnknownFieldError(first_level_field, model)

        if field_object.has_db_column:
            raise NotARelationFieldError(first_level_field, model)

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


def parse_select_related(relation: str, model: Type["Model"], related_dict: Dict[str, Dict]) -> None:
    from tortoise.fields import ForeignKey, OneToOneField

    model_meta = model._meta

    first_level_field, _, forwarded_prefetch = relation.partition(LOOKUP_SEP)
    field_object = model_meta.fields_map.get(first_level_field)
    if not field_object:
        raise UnknownFieldError(first_level_field, model)

    if not isinstance(field_object, (ForeignKey, OneToOneField)):
        raise BaseFieldError(first_level_field, model,
            "select_related only works with ForeignKey or OneToOneFields")

    if first_level_field not in related_dict:
        related_dict[first_level_field] = {}

    if forwarded_prefetch:
        field_object = model_meta.fields_map[first_level_field]
        parse_select_related(forwarded_prefetch, field_object.remote_model, related_dict[first_level_field])
