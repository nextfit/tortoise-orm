
from tortoise import Tortoise
from tortoise.backends.base.client import TransactionDBAsyncClient
from tortoise.exceptions import TransactionManagementError


class TransactionContext:
    __slots__ = ("db_client", "connection_name", )

    def __init__(self, db_client: TransactionDBAsyncClient) -> None:
        self.db_client = db_client
        self.connection_name = db_client.connection_name

    async def __aenter__(self) -> TransactionDBAsyncClient:
        raise NotImplementedError()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        raise NotImplementedError()


class LockTransactionContext(TransactionContext):
    __slots__ = ("token", )

    async def __aenter__(self) -> TransactionDBAsyncClient:
        current_transaction = Tortoise._current_transaction_map[self.connection_name]
        self.token = current_transaction.set(self.db_client)

        await self.db_client.acquire()
        await self.db_client.start()
        return self.db_client

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.db_client.in_progress():
            if exc_type:
                # Can't rollback a transaction that already failed.
                if exc_type is not TransactionManagementError:
                    await self.db_client.rollback()
            else:
                await self.db_client.commit()

        Tortoise._current_transaction_map[self.connection_name].reset(self.token)
        await self.db_client.release()


class NestedTransactionContext(TransactionContext):
    async def __aenter__(self) -> TransactionDBAsyncClient:
        await self.db_client.start()
        return self.db_client

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.db_client.in_progress():
            if exc_type:
                # Can't rollback a transaction that already failed.
                if exc_type is not TransactionManagementError:
                    await self.db_client.rollback()
            else:
                await self.db_client.commit()
