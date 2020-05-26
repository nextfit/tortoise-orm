
from functools import wraps
from typing import Callable, Optional, TYPE_CHECKING
from tortoise import Tortoise

if TYPE_CHECKING:
    from tortoise.transactions.context import TransactionContext


def in_transaction(connection_name: Optional[str] = None) -> "TransactionContext":
    """
    Transaction context manager.

    You can run your code inside ``async with in_transaction():`` statement to run it
    into one transaction. If error occurs transaction will rollback.

    :param connection_name: name of connection to run with, optional if you have only
                            one db connection
    """
    db_client = Tortoise.get_transaction_db_client(connection_name)
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
            db_client = Tortoise.get_transaction_db_client(connection_name)
            async with db_client.in_transaction():
                return await func(*args, **kwargs)

        return wrapped

    return wrapper
