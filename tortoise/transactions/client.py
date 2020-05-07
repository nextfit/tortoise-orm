
class AsyncDbClientTransactionMixin:

    # lock acquisition and release
    async def acquire(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def release(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    # transaction operations
    async def start(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def rollback(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def commit(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage
