
from contextvars import ContextVar
from functools import wraps
from typing import Callable, Optional, Dict
from tortoise.exceptions import ParamsError
from tortoise.backends.base.client import BaseDBAsyncClient


current_transaction_map: Dict[str, ContextVar] = {}


def _get_db_client(connection_name: Optional[str]) -> BaseDBAsyncClient:
    from tortoise import Tortoise

    if connection_name:
        return current_transaction_map[connection_name].get()

    elif len(Tortoise._db_client_map) == 1:
        return list(current_transaction_map.values())[0].get()

    else:
        raise ParamsError(
            "You are running with multiple databases, so you should specify"
            f" connection_name: {list(Tortoise._db_client_map.keys())}"
        )


def in_transaction(connection_name: Optional[str] = None) -> "TransactionContext":
    """
    Transaction context manager.

    You can run your code inside ``async with in_transaction():`` statement to run it
    into one transaction. If error occurs transaction will rollback.

    :param connection_name: name of connection to run with, optional if you have only
                            one db connection
    """
    db_client = _get_db_client(connection_name)
    return db_client.in_transaction()


def atomic(connection_name: Optional[str] = None) -> Callable:
    """
    Transaction decorator.

    You can wrap your function with this decorator to run it into one transaction.
    If error occurs transaction will rollback.

    :param connection_name: name of connection to run with, optional if you have only
                            one db connection
    """

    def wrapper(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            db_client = _get_db_client(connection_name)
            async with db_client.in_transaction():
                return await func(*args, **kwargs)

        return wrapped

    return wrapper


class BaseTransactionWrapper:
    async def start(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    def release(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def rollback(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def commit(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage
