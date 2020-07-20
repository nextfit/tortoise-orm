"""
Test some mysql-specific features
"""
import ssl

from tortoise import Tortoise
from tortoise.contrib import test


class TestMySQL(test.SimpleTestCase):
    def setUp(self) -> None:
        self.db_config = self.get_db_config()
        if self.db_config["connections"]["models"]["engine"] != "tortoise.backends.mysql":
            raise test.SkipTest("MySQL only")

    async def asyncTearDown(self) -> None:
        if Tortoise._inited:
            await Tortoise._drop_databases()

    async def test_bad_charset(self):
        self.db_config["connections"]["models"]["charset"] = "terrible"
        with self.assertRaisesRegex(ConnectionError, "Unknown charset"):
            await Tortoise.init(self.db_config)

    async def test_ssl_true(self):
        self.db_config["connections"]["models"]["ssl"] = True
        try:
            await Tortoise.init(self.db_config)
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
            await Tortoise.init(self.db_config, _create_db=True)
        except ConnectionError:
            pass
