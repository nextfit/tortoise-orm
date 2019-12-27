
from typing import List, Type, TypeVar
from pypika import Table


MODEL = TypeVar("MODEL", bound="Model")


class QueryContextItem:
    def __init__(self, model: Type[MODEL], table: Table):
        self.model = model
        self.table = table


class QueryContext:
    def __init__(self):
        self.stack: List[QueryContextItem] = []

    def push(self, model, table):
        self.stack.append(QueryContextItem(model, table))
        return self

    def pop(self):
        return self.stack.pop()
