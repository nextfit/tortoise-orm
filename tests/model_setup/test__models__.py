"""
Tests for __models__
"""

import re

from tortoise import Tortoise
from tortoise.contrib import test


class TestGenerateSchema(test.TortoiseBaseTestCase):

    tortoise_test_modules = []

    def setUp(self):
        Tortoise._app_models_map = {}
        Tortoise._db_client_map = {}
        Tortoise._inited = False
        self.sqls = ""
        self.post_sqls = ""
        self.engine = self.get_db_config()["connections"]["models"]["engine"]

    async def asyncTearDown(self) -> None:
        Tortoise._reset()

    async def init_for(self, module: str, safe=False) -> None:
        if self.engine != "tortoise.backends.sqlite":
            raise test.SkipTest("sqlite only")

        Tortoise.init(
            {
                "connections": {
                    "default": {
                        "engine": "tortoise.backends.sqlite",
                        "file_path": ":memory:",
                    }
                },
                "apps": {"models": {"models": [module], "default_connection": "default"}},
            }
        )
        self.sqls = Tortoise.get_schema_sql(Tortoise.get_db_client("default"), safe=safe).split(";\n")

    def get_sql(self, text: str) -> str:
        return re.sub(r"[ \t\n\r]+", " ", [sql for sql in self.sqls if text in sql][0])

    async def test_good(self):
        await self.init_for("tests.model_setup.models__models__good")
        self.assertIn("goodtournament", "; ".join(self.sqls))
        self.assertIn("inaclasstournament", "; ".join(self.sqls))
        self.assertNotIn("badtournament", "; ".join(self.sqls))

    async def test_bad(self):
        await self.init_for("tests.model_setup.models__models__bad")
        self.assertNotIn("goodtournament", "; ".join(self.sqls))
        self.assertNotIn("inaclasstournament", "; ".join(self.sqls))
        self.assertIn("badtournament", "; ".join(self.sqls))
