
from typing import Generator, Optional, Generic, TypeVar
from tortoise.exceptions import DoesNotExist, MultipleObjectsReturned


MODEL = TypeVar("MODEL", bound="Model")


class SingleQuerySet(Generic[MODEL]):
    __slots__ = ("queryset",)

    def __init__(self, queryset: "AwaitableQuery[MODEL]") -> None:
        self.queryset = queryset

    async def _execute(self):
        raise NotImplementedError()

    def __await__(self) -> Generator:
        return self._execute().__await__()


class GetQuerySet(SingleQuerySet[MODEL]):
    async def _execute(self) -> MODEL:
        instance_list = await self.queryset

        if not instance_list:
            raise DoesNotExist("Object does not exist")

        if len(instance_list) == 1:
            return instance_list[0]

        raise MultipleObjectsReturned("Multiple objects returned, expected exactly one")


class FirstQuerySet(SingleQuerySet[MODEL]):
    async def _execute(self) -> Optional[MODEL]:
        instance_list = await self.queryset

        if not instance_list:
            return None

        if len(instance_list) == 1:
            return instance_list[0]

        raise MultipleObjectsReturned("Multiple objects returned, expected exactly one")
