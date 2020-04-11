
from tortoise.exceptions import TransactionManagementError
from tortoise.transactions import current_transaction_map


class TransactionContext:
    __slots__ = ("connection", "connection_name", "token", "lock")

    def __init__(self, connection) -> None:
        self.connection = connection
        self.connection_name = connection.connection_name
        self.lock = getattr(connection, "_trxlock", None)

    async def __aenter__(self):
        await self.lock.acquire()
        current_transaction = current_transaction_map[self.connection_name]
        self.token = current_transaction.set(self.connection)
        await self.connection.start()
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self.connection._finalized:
            if exc_type:
                # Can't rollback a transaction that already failed.
                if exc_type is not TransactionManagementError:
                    await self.connection.rollback()
            else:
                await self.connection.commit()
        current_transaction_map[self.connection_name].reset(self.token)
        self.lock.release()


class TransactionContextPooled(TransactionContext):
    __slots__ = ("connection", "connection_name", "token")

    async def __aenter__(self):
        current_transaction = current_transaction_map[self.connection_name]
        self.token = current_transaction.set(self.connection)
        self.connection._connection = await self.connection._parent._pool.acquire()
        await self.connection.start()
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self.connection._finalized:
            if exc_type:
                # Can't rollback a transaction that already failed.
                if exc_type is not TransactionManagementError:
                    await self.connection.rollback()
            else:
                await self.connection.commit()
        current_transaction_map[self.connection_name].reset(self.token)
        if self.connection._parent._pool:
            await self.connection._parent._pool.release(self.connection._connection)


class NestedTransactionContext(TransactionContext):
    async def __aenter__(self):
        await self.connection.start()
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self.connection._finalized:
            if exc_type:
                # Can't rollback a transaction that already failed.
                if exc_type is not TransactionManagementError:
                    await self.connection.rollback()
            else:
                await self.connection.commit()
