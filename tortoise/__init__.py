
import asyncio
from typing import Coroutine

from tortoise.core import _Tortoise

Tortoise = _Tortoise()


def run_async(coro: Coroutine) -> None:
    """
    Simple async runner that cleans up DB connections on exit.
    This is meant for simple scripts.

    Usage::

        from tortoise import Tortoise, run_async

        async def do_stuff():
            Tortoise.init(
                db_url='sqlite://db.sqlite3',
                models={'models': ['app.models']}
            )
            await Tortoise.open_connections()

            ...

        run_async(do_stuff())
    """
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(Tortoise.close_connections())


__version__ = "0.15.9-sina"
