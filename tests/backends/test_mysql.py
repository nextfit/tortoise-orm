"""
Test some mysql-specific features
"""
import ssl

from tortoise import Tortoise
from tortoise.contrib import test
from tortoise.exceptions import DBConnectionError


class TestMySQL(test.TortoiseBaseTestCase):
    def setUp(self) -> None:
        self.db_config = self.get_db_config()
        if self.db_config["connections"]["models"]["engine"] != "tortoise.backends.mysql":
            raise test.SkipTest("MySQL only")

    async def asyncTearDown(self) -> None:
        try:
            await Tortoise.drop_databases()

        except DBConnectionError:
            pass

    async def test_bad_charset(self):
        self.db_config["connections"]["models"]["charset"] = "terrible"
        with self.assertRaisesRegex(ConnectionError, "Unknown charset"):
            Tortoise.init(self.db_config)
            await Tortoise.open_connections()

    async def test_ssl_true(self):
        self.db_config["connections"]["models"]["ssl"] = True
        try:
            Tortoise.init(self.db_config)
            await Tortoise.open_connections()

        except (ConnectionError, ssl.SSLError):
            pass
        else:
            self.assertFalse(True, "Expected ConnectionError or SSLError")

    async def test_ssl_custom(self):
        # Expect connectionerror or pass
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        self.db_config["connections"]["models"]["ssl"] = ctx
        try:
            Tortoise.init(self.db_config)
            await Tortoise.open_connections(create_db=True)

        except ConnectionError:
            pass
