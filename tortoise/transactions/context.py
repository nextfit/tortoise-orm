
from tortoise.exceptions import TransactionManagementError
from tortoise.transactions import current_transaction_map


class TransactionContext:
    __slots__ = ("db_client", "connection_name", )

    def __init__(self, db_client) -> None:
        self.db_client = db_client
        self.connection_name = db_client.connection_name

    async def __aenter__(self):
        raise NotImplementedError()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        raise NotImplementedError()


class LockTransactionContext(TransactionContext):
    __slots__ = ("token", "lock")

    def __init__(self, db_client) -> None:
        super().__init__(db_client)
        self.lock = getattr(db_client, "_trxlock", None)

    async def __aenter__(self):
        current_transaction = current_transaction_map[self.connection_name]
        self.token = current_transaction.set(self.db_client)
        await self.lock.acquire()
        await self.db_client.start()
        return self.db_client

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self.db_client._finalized:
            if exc_type:
                # Can't rollback a transaction that already failed.
                if exc_type is not TransactionManagementError:
                    await self.db_client.rollback()
            else:
                await self.db_client.commit()

        current_transaction_map[self.connection_name].reset(self.token)
        self.lock.release()


class PoolTransactionContext(TransactionContext):
    __slots__ = ("token", )

    async def __aenter__(self):
        current_transaction = current_transaction_map[self.connection_name]
        self.token = current_transaction.set(self.db_client)
        self.db_client._connection = await self.db_client._parent._pool.acquire()
        await self.db_client.start()
        return self.db_client

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self.db_client._finalized:
            if exc_type:
                # Can't rollback a transaction that already failed.
                if exc_type is not TransactionManagementError:
                    await self.db_client.rollback()
            else:
                await self.db_client.commit()

        current_transaction_map[self.connection_name].reset(self.token)
        if self.db_client._parent._pool:
            await self.db_client._parent._pool.release(self.db_client._connection)


class NestedTransactionContext(TransactionContext):
    async def __aenter__(self):
        await self.db_client.start()
        return self.db_client

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self.db_client._finalized:
            if exc_type:
                # Can't rollback a transaction that already failed.
                if exc_type is not TransactionManagementError:
                    await self.db_client.rollback()
            else:
                await self.db_client.commit()
