
from typing import Dict, List, Optional, Type, TYPE_CHECKING
from pypika import Table

if TYPE_CHECKING:
    from tortoise.models import Model


class QueryContextItem:
    def __init__(self, model: Type["Model"], table: Table, through_tables: Optional[Dict[str, Table]] = None):
        self.model = model
        self.table = table
        self.through_tables = through_tables or {}


class QueryContext:
    def __init__(self):
        self.stack: List[QueryContextItem] = []

    def push(self, model, table, through_tables: Optional[Dict[str, Table]] = None):
        self.stack.append(QueryContextItem(model, table, through_tables))
        return self

    def pop(self) -> QueryContextItem:
        return self.stack.pop()

    @property
    def top(self) -> QueryContextItem:
        return self.stack[-1]

    @property
    def alias(self) -> Optional[str]:
        return "U{}".format(len(self.stack)) if self.stack else None
