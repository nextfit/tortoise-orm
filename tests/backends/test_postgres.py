"""
Test some PostgreSQL-specific features
"""
import ssl

from tests.testmodels import Tournament
from tortoise import Tortoise
from tortoise.contrib import test
from tortoise.exceptions import OperationalError


class TestPostgreSQL(test.SimpleTestCase):
    def setUp(self):
        self.db_config = self.get_db_config()
        if self.db_config["connections"]["models"]["engine"] != "tortoise.backends.asyncpg":
            raise test.SkipTest("PostgreSQL only")

    async def asyncTearDown(self) -> None:
        if Tortoise._inited:
            await Tortoise.drop_databases()

    async def test_schema(self):
        from asyncpg.exceptions import InvalidSchemaNameError

        self.db_config["connections"]["models"]["schema"] = "mytestschema"
        Tortoise.init(self.db_config)
        await Tortoise.open_connections(create_db=True)

        with self.assertRaises(InvalidSchemaNameError):
            await Tortoise.generate_schemas()

        conn = Tortoise.get_db_client("models")
        await conn.execute_script("CREATE SCHEMA mytestschema;")
        await Tortoise.generate_schemas()

        tournament = await Tournament.create(name="Test")
        await Tortoise.close_connections()

        del self.db_config["connections"]["models"]["schema"]
        Tortoise.init(self.db_config)
        await Tortoise.open_connections()

        with self.assertRaises(OperationalError):
            await Tournament.filter(name="Test").first()

        conn = Tortoise.get_db_client("models")
        _, db_columns, res = await conn.execute_query(
            "SELECT id, name FROM mytestschema.tournament WHERE name='Test' LIMIT 1"
        )

        self.assertEqual(len(res), 1)
        self.assertEqual(tournament.id, res[0][0])
        self.assertEqual(tournament.name, res[0][1])

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
