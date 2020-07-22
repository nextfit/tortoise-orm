
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from tortoise import Tortoise


class TestConnectionParams(IsolatedAsyncioTestCase):
    async def test_mysql_connection_params(self):
        Tortoise._init_connections(
            {
                "models": {
                    "engine": "tortoise.backends.mysql",
                    "database": "test",
                    "host": "127.0.0.1",
                    "password": "foomip",
                    "port": 3306,
                    "user": "root",
                    "connect_timeout": 1.5,
                    "charset": "utf8mb4",
                }
            },
        )

        with patch("aiomysql.create_pool", new=AsyncMock()) as mysql_connect:
            await Tortoise.open_connections()
            mysql_connect.assert_awaited_once_with(  # nosec
                autocommit=True,
                charset="utf8mb4",
                connect_timeout=1.5,
                db="test",
                host="127.0.0.1",
                password="foomip",
                port=3306,
                user="root",
                maxsize=5,
                minsize=1,
                sql_mode="STRICT_TRANS_TABLES",
            )

            await Tortoise.close_connections()

    async def test_postgres_connection_params(self):
        try:
            Tortoise._init_connections(
                {
                    "models": {
                        "engine": "tortoise.backends.asyncpg",
                        "database": "test",
                        "host": "127.0.0.1",
                        "password": "foomip",
                        "port": 5432,
                        "user": "root",
                        "timeout": 30,
                        "ssl": True,
                    }
                },
            )

            with patch("asyncpg.create_pool", new=AsyncMock()) as asyncpg_connect:
                await Tortoise.open_connections()
                asyncpg_connect.assert_awaited_once_with(  # nosec
                    None,
                    database="test",
                    host="127.0.0.1",
                    password="foomip",
                    port=5432,
                    ssl=True,
                    timeout=30,
                    user="root",
                    max_size=5,
                    min_size=1,
                )

                await Tortoise.close_connections()

        except ImportError:
            self.skipTest("asyncpg not installed")
