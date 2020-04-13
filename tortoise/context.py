
from typing import List, Type, Dict, TypeVar
from pypika import Table

MODEL = TypeVar("MODEL", bound="Model")


class QueryContextItem:
    def __init__(self, model: Type[MODEL], table: Table, through_tables: Dict[str, Table] = None):
        self.model = model
        self.table = table
        self.through_tables = through_tables or {}


class QueryContext:
    def __init__(self):
        self.stack: List[QueryContextItem] = []

    def push(self, model, table, through_tables: Dict[str, Table] = None):
        self.stack.append(QueryContextItem(model, table, through_tables))
        return self

    def pop(self) -> QueryContextItem:
        return self.stack.pop()

    @property
    def top(self) -> QueryContextItem:
        return self.stack[-1]
