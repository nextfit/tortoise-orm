
import re
from tortoise import Tortoise
from tortoise.contrib import test
from tortoise.exceptions import ConfigurationError


class TestGenerateSchema(test.TortoiseBaseTestCase):

    tortoise_test_modules = []

    def setUp(self):
        Tortoise._app_models_map = {}
        Tortoise._db_client_map = {}
        Tortoise._inited = False

        self.sqls = ""
        self.post_sqls = ""
        self.engine = self.get_db_config()["connections"]["models"]["engine"]

    def tearDown(self) -> None:
        Tortoise._reset()

    def init_for(self, module: str, safe=False) -> None:
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
        return re.sub(r"[ \t\n\r]+", " ", " ".join([sql for sql in self.sqls if text in sql]))

    def test_noid(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql('"models_noid"')
        self.assertIn('"name" VARCHAR(255)', sql)
        self.assertIn('"id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL', sql)

    def test_minrelation(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql('"models_minrelation"')
        self.assertIn(
            '"tournament_id" SMALLINT NOT NULL REFERENCES "models_tournament" ("id") ON DELETE CASCADE',
            sql,
        )
        self.assertNotIn("participants", sql)

        sql = self.get_sql('"models_minrelation_team"')
        self.assertIn(
            '"minrelation_id" INT NOT NULL REFERENCES "models_minrelation" ("id") ON DELETE CASCADE', sql
        )
        self.assertIn('"team_id" INT NOT NULL REFERENCES "models_team" ("id") ON DELETE CASCADE', sql)

    def test_safe_generation(self):
        """Assert that the IF NOT EXISTS clause is included when safely generating schema."""
        self.init_for("tests.testmodels", True)
        sql = self.get_sql("")
        self.assertIn("IF NOT EXISTS", sql)

    def test_unsafe_generation(self):
        """Assert that the IF NOT EXISTS clause is not included when generating schema."""
        self.init_for("tests.testmodels", False)
        sql = self.get_sql("")
        self.assertNotIn("IF NOT EXISTS", sql)

    def test_cyclic(self):
        with self.assertRaisesRegex(
            ConfigurationError, "Can't create schema due to cyclic fk references"
        ):
            self.init_for("tests.schema.models_cyclic")

    def test_create_index(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql("CREATE INDEX")
        self.assertIsNotNone(re.search(r"idx_models_tour_created_\w+", sql))

    def test_fk_bad_model_name(self):
        with self.assertRaisesRegex(
            ConfigurationError, "No model with name 'moo' registered in app 'models'."
        ):
            self.init_for("tests.schema.models_fk_1")

    def test_fk_bad_on_delete(self):
        with self.assertRaisesRegex(
            ConfigurationError, "on_delete can only be CASCADE, RESTRICT or SET_NULL"
        ):
            self.init_for("tests.schema.models_fk_2")

    def test_fk_bad_null(self):
        with self.assertRaisesRegex(
            ConfigurationError, "If on_delete is SET_NULL, then field must have null=True set"
        ):
            self.init_for("tests.schema.models_fk_3")

    def test_o2o_bad_on_delete(self):
        with self.assertRaisesRegex(
            ConfigurationError, "on_delete can only be CASCADE, RESTRICT or SET_NULL"
        ):
            self.init_for("tests.schema.models_o2o_2")

    def test_o2o_bad_null(self):
        with self.assertRaisesRegex(
            ConfigurationError, "If on_delete is SET_NULL, then field must have null=True set"
        ):
            self.init_for("tests.schema.models_o2o_3")

    def test_m2m_bad_model_name(self):
        with self.assertRaisesRegex(
            ConfigurationError, "No model with name 'Two' registered in app 'models'."
        ):
            self.init_for("tests.schema.models_m2m_1")

    def test_table_and_row_comment_generation(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql("comments")
        self.assertRegex(sql, r".*\/\* Upvotes done on the comment.*\*\/")
        self.assertRegex(sql, r".*\\n.*")
        self.assertIn("\\/", sql)

    def test_schema(self):
        self.maxDiff = None
        self.init_for("tests.schema.models_schema_create")
        sql = Tortoise.get_schema_sql(Tortoise.get_db_client("default"), safe=False)
        self.assertEqual(
            sql.strip(),
            """
CREATE TABLE "models_defaultpk" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "val" INT NOT NULL
);
CREATE TABLE "models_tournament" (
    "tid" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "name" VARCHAR(100) NOT NULL  /* Tournament name */,
    "created" TIMESTAMP NOT NULL  /* Created *\\/'`\\/* datetime */
) /* What Tournaments *\\/'`\\/* we have */;
CREATE INDEX "idx_models_tour_name_67cf9d" ON "models_tournament" ("name");
CREATE TABLE "models_event" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL /* Event ID */,
    "name" TEXT NOT NULL,
    "modified" TIMESTAMP NOT NULL,
    "prize" VARCHAR(40),
    "token" VARCHAR(100) NOT NULL UNIQUE /* Unique token */,
    "key" VARCHAR(100) NOT NULL,
    "tournament_id" SMALLINT NOT NULL REFERENCES "models_tournament" ("tid") ON DELETE CASCADE /* FK to tournament */,
    CONSTRAINT "uid_models_even_name_bb6540" UNIQUE ("name", "prize"),
    CONSTRAINT "uid_models_even_tournam_8ac3b2" UNIQUE ("tournament_id", "key")
) /* This table contains a list of all the events */;
CREATE TABLE "models_inheritedmodel" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "zero" INT NOT NULL,
    "one" VARCHAR(40),
    "new_field" VARCHAR(100) NOT NULL,
    "two" VARCHAR(40) NOT NULL,
    "name" TEXT NOT NULL
);
CREATE TABLE "sometable" (
    "sometable_id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "some_chars_table" VARCHAR(255) NOT NULL,
    "fk_sometable" INT REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE
);
CREATE INDEX "idx_sometable_some_ch_3d69eb" ON "sometable" ("some_chars_table");
CREATE TABLE "models_team" (
    "name" VARCHAR(50) NOT NULL  PRIMARY KEY /* The TEAM name (and PK) */,
    "key" INT NOT NULL,
    "manager_id" VARCHAR(50) REFERENCES "models_team" ("name") ON DELETE CASCADE
) /* The TEAMS! */;
CREATE INDEX "idx_models_team_manager_34eaef" ON "models_team" ("manager_id", "key");
CREATE INDEX "idx_models_team_manager_c571ed" ON "models_team" ("manager_id", "name");
CREATE TABLE "models_teamaddress" (
    "city" VARCHAR(50) NOT NULL  /* City */,
    "country" VARCHAR(50) NOT NULL  /* Country */,
    "street" VARCHAR(128) NOT NULL  /* Street Address */,
    "team_id" VARCHAR(50) NOT NULL  PRIMARY KEY REFERENCES "models_team" ("name") ON DELETE CASCADE
);
CREATE TABLE "models_venueinformation" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "name" VARCHAR(128) NOT NULL,
    "capacity" INT NOT NULL,
    "rent" REAL NOT NULL,
    "team_id" VARCHAR(50)  UNIQUE REFERENCES "models_team" ("name") ON DELETE SET NULL
);
CREATE TABLE "teamevents" (
    "event_id" BIGINT NOT NULL REFERENCES "models_event" ("id") ON DELETE CASCADE,
    "team_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE
) /* How participants relate */;
CREATE TABLE "sometable_self" (
    "backward_sts" INT NOT NULL REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE,
    "sts_forward" INT NOT NULL REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE
);
CREATE TABLE "models_team_team" (
    "team_rel_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE,
    "team_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE
);
""".strip(),
        )

    def test_schema_safe(self):
        self.maxDiff = None
        self.init_for("tests.schema.models_schema_create")
        sql = Tortoise.get_schema_sql(Tortoise.get_db_client("default"), safe=True)
        self.assertEqual(
            sql.strip(),
            """
CREATE TABLE IF NOT EXISTS "models_defaultpk" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "val" INT NOT NULL
);
CREATE TABLE IF NOT EXISTS "models_tournament" (
    "tid" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "name" VARCHAR(100) NOT NULL  /* Tournament name */,
    "created" TIMESTAMP NOT NULL  /* Created *\\/'`\\/* datetime */
) /* What Tournaments *\\/'`\\/* we have */;
CREATE INDEX IF NOT EXISTS "idx_models_tour_name_67cf9d" ON "models_tournament" ("name");
CREATE TABLE IF NOT EXISTS "models_event" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL /* Event ID */,
    "name" TEXT NOT NULL,
    "modified" TIMESTAMP NOT NULL,
    "prize" VARCHAR(40),
    "token" VARCHAR(100) NOT NULL UNIQUE /* Unique token */,
    "key" VARCHAR(100) NOT NULL,
    "tournament_id" SMALLINT NOT NULL REFERENCES "models_tournament" ("tid") ON DELETE CASCADE /* FK to tournament */,
    CONSTRAINT "uid_models_even_name_bb6540" UNIQUE ("name", "prize"),
    CONSTRAINT "uid_models_even_tournam_8ac3b2" UNIQUE ("tournament_id", "key")
) /* This table contains a list of all the events */;
CREATE TABLE IF NOT EXISTS "models_inheritedmodel" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "zero" INT NOT NULL,
    "one" VARCHAR(40),
    "new_field" VARCHAR(100) NOT NULL,
    "two" VARCHAR(40) NOT NULL,
    "name" TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS "sometable" (
    "sometable_id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "some_chars_table" VARCHAR(255) NOT NULL,
    "fk_sometable" INT REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_sometable_some_ch_3d69eb" ON "sometable" ("some_chars_table");
CREATE TABLE IF NOT EXISTS "models_team" (
    "name" VARCHAR(50) NOT NULL  PRIMARY KEY /* The TEAM name (and PK) */,
    "key" INT NOT NULL,
    "manager_id" VARCHAR(50) REFERENCES "models_team" ("name") ON DELETE CASCADE
) /* The TEAMS! */;
CREATE INDEX IF NOT EXISTS "idx_models_team_manager_34eaef" ON "models_team" ("manager_id", "key");
CREATE INDEX IF NOT EXISTS "idx_models_team_manager_c571ed" ON "models_team" ("manager_id", "name");
CREATE TABLE IF NOT EXISTS "models_teamaddress" (
    "city" VARCHAR(50) NOT NULL  /* City */,
    "country" VARCHAR(50) NOT NULL  /* Country */,
    "street" VARCHAR(128) NOT NULL  /* Street Address */,
    "team_id" VARCHAR(50) NOT NULL  PRIMARY KEY REFERENCES "models_team" ("name") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "models_venueinformation" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "name" VARCHAR(128) NOT NULL,
    "capacity" INT NOT NULL,
    "rent" REAL NOT NULL,
    "team_id" VARCHAR(50)  UNIQUE REFERENCES "models_team" ("name") ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS "teamevents" (
    "event_id" BIGINT NOT NULL REFERENCES "models_event" ("id") ON DELETE CASCADE,
    "team_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE
) /* How participants relate */;
CREATE TABLE IF NOT EXISTS "sometable_self" (
    "backward_sts" INT NOT NULL REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE,
    "sts_forward" INT NOT NULL REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "models_team_team" (
    "team_rel_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE,
    "team_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE
);
""".strip(),
        )


class TestGenerateSchemaMySQL(TestGenerateSchema):
    def init_for(self, module: str, safe=False) -> None:
        Tortoise.init(
            {
                "connections": {
                    "default": {
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
                "apps": {"models": {"models": [module], "default_connection": "default"}},
            }
        )
        self.sqls = Tortoise.get_schema_sql(Tortoise.get_db_client("default"), safe=safe).split("; ")

    def test_noid(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql("`models_noid`")
        self.assertIn("`name` VARCHAR(255)", sql)
        self.assertIn("`id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT", sql)

    def test_create_index(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql("KEY")
        self.assertIsNotNone(re.search(r"idx_models_tour_created_\w+", sql))

    def test_minrelation(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql("`models_minrelation`")
        self.assertIn("`tournament_id` SMALLINT NOT NULL,", sql)
        self.assertIn(
            "FOREIGN KEY (`tournament_id`) REFERENCES `models_tournament` (`id`) ON DELETE CASCADE", sql
        )
        self.assertNotIn("participants", sql)

        sql = self.get_sql("`models_minrelation_team`")
        self.assertIn("`minrelation_id` INT NOT NULL", sql)
        self.assertIn(
            "FOREIGN KEY (`minrelation_id`) REFERENCES `models_minrelation` (`id`) ON DELETE CASCADE", sql
        )
        self.assertIn("`team_id` INT NOT NULL", sql)
        self.assertIn("FOREIGN KEY (`team_id`) REFERENCES `models_team` (`id`) ON DELETE CASCADE", sql)

    def test_table_and_row_comment_generation(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql("comments")
        self.assertIn("COMMENT='Test Table comment'", sql)
        self.assertIn("COMMENT 'This column acts as it\\'s own comment'", sql)
        self.assertRegex(sql, r".*\\n.*")
        self.assertRegex(sql, r".*it\\'s.*")

    def test_schema(self):
        self.maxDiff = None
        self.init_for("tests.schema.models_schema_create")
        sql = Tortoise.get_schema_sql(Tortoise.get_db_client("default"), safe=False)
        self.assertEqual(
            sql.strip(),
            """
CREATE TABLE `models_defaultpk` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `val` INT NOT NULL
) CHARACTER SET utf8mb4;
CREATE TABLE `models_tournament` (
    `tid` SMALLINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(100) NOT NULL  COMMENT 'Tournament name',
    `created` DATETIME(6) NOT NULL  COMMENT 'Created */\\'`/* datetime',
    KEY `idx_models_tour_name_67cf9d` (`name`)
) CHARACTER SET utf8mb4 COMMENT='What Tournaments */\\'`/* we have';
CREATE TABLE `models_event` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Event ID',
    `name` LONGTEXT NOT NULL,
    `modified` DATETIME(6) NOT NULL,
    `prize` DECIMAL(10,2),
    `token` VARCHAR(100) NOT NULL UNIQUE COMMENT 'Unique token',
    `key` VARCHAR(100) NOT NULL,
    `tournament_id` SMALLINT NOT NULL COMMENT 'FK to tournament',
    UNIQUE KEY `uid_models_even_name_bb6540` (`name`, `prize`),
    UNIQUE KEY `uid_models_even_tournam_8ac3b2` (`tournament_id`, `key`),
    CONSTRAINT `fk_models_e_models_t_6d548004` FOREIGN KEY (`tournament_id`) REFERENCES `models_tournament` (`tid`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='This table contains a list of all the events';
CREATE TABLE `models_inheritedmodel` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `zero` INT NOT NULL,
    `one` VARCHAR(40),
    `new_field` VARCHAR(100) NOT NULL,
    `two` VARCHAR(40) NOT NULL,
    `name` LONGTEXT NOT NULL
) CHARACTER SET utf8mb4;
CREATE TABLE `sometable` (
    `sometable_id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `some_chars_table` VARCHAR(255) NOT NULL,
    `fk_sometable` INT,
    CONSTRAINT `fk_sometabl_sometabl_6efae9bd` FOREIGN KEY (`fk_sometable`) REFERENCES `sometable` (`sometable_id`) ON DELETE CASCADE,
    KEY `idx_sometable_some_ch_3d69eb` (`some_chars_table`)
) CHARACTER SET utf8mb4;
CREATE TABLE `models_team` (
    `name` VARCHAR(50) NOT NULL  PRIMARY KEY COMMENT 'The TEAM name (and PK)',
    `key` INT NOT NULL,
    `manager_id` VARCHAR(50),
    CONSTRAINT `fk_models_t_models_t_ed161113` FOREIGN KEY (`manager_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE,
    KEY `idx_models_team_manager_34eaef` (`manager_id`, `key`),
    KEY `idx_models_team_manager_c571ed` (`manager_id`, `name`)
) CHARACTER SET utf8mb4 COMMENT='The TEAMS!';
CREATE TABLE `models_teamaddress` (
    `city` VARCHAR(50) NOT NULL  COMMENT 'City',
    `country` VARCHAR(50) NOT NULL  COMMENT 'Country',
    `street` VARCHAR(128) NOT NULL  COMMENT 'Street Address',
    `team_id` VARCHAR(50) NOT NULL  PRIMARY KEY,
    CONSTRAINT `fk_models_t_models_t_9e656881` FOREIGN KEY (`team_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE
) CHARACTER SET utf8mb4;
CREATE TABLE `models_venueinformation` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL,
    `capacity` INT NOT NULL,
    `rent` DOUBLE NOT NULL,
    `team_id` VARCHAR(50)  UNIQUE,
    CONSTRAINT `fk_models_v_models_t_cf93f20c` FOREIGN KEY (`team_id`) REFERENCES `models_team` (`name`) ON DELETE SET NULL
) CHARACTER SET utf8mb4;
CREATE TABLE `teamevents` (
    `event_id` BIGINT NOT NULL,
    `team_id` VARCHAR(50) NOT NULL,
    FOREIGN KEY (`event_id`) REFERENCES `models_event` (`id`) ON DELETE CASCADE,
    FOREIGN KEY (`team_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='How participants relate';
CREATE TABLE `sometable_self` (
    `backward_sts` INT NOT NULL,
    `sts_forward` INT NOT NULL,
    FOREIGN KEY (`backward_sts`) REFERENCES `sometable` (`sometable_id`) ON DELETE CASCADE,
    FOREIGN KEY (`sts_forward`) REFERENCES `sometable` (`sometable_id`) ON DELETE CASCADE
) CHARACTER SET utf8mb4;
CREATE TABLE `models_team_team` (
    `team_rel_id` VARCHAR(50) NOT NULL,
    `team_id` VARCHAR(50) NOT NULL,
    FOREIGN KEY (`team_rel_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE,
    FOREIGN KEY (`team_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE
) CHARACTER SET utf8mb4;
""".strip(),
        )

    def test_schema_safe(self):
        self.maxDiff = None
        self.init_for("tests.schema.models_schema_create")
        sql = Tortoise.get_schema_sql(Tortoise.get_db_client("default"), safe=True)

        self.assertEqual(
            sql.strip(),
            """
CREATE TABLE IF NOT EXISTS `models_defaultpk` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `val` INT NOT NULL
) CHARACTER SET utf8mb4;
CREATE TABLE IF NOT EXISTS `models_tournament` (
    `tid` SMALLINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(100) NOT NULL  COMMENT 'Tournament name',
    `created` DATETIME(6) NOT NULL  COMMENT 'Created */\\'`/* datetime',
    KEY `idx_models_tour_name_67cf9d` (`name`)
) CHARACTER SET utf8mb4 COMMENT='What Tournaments */\\'`/* we have';
CREATE TABLE IF NOT EXISTS `models_event` (
    `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT COMMENT 'Event ID',
    `name` LONGTEXT NOT NULL,
    `modified` DATETIME(6) NOT NULL,
    `prize` DECIMAL(10,2),
    `token` VARCHAR(100) NOT NULL UNIQUE COMMENT 'Unique token',
    `key` VARCHAR(100) NOT NULL,
    `tournament_id` SMALLINT NOT NULL COMMENT 'FK to tournament',
    UNIQUE KEY `uid_models_even_name_bb6540` (`name`, `prize`),
    UNIQUE KEY `uid_models_even_tournam_8ac3b2` (`tournament_id`, `key`),
    CONSTRAINT `fk_models_e_models_t_6d548004` FOREIGN KEY (`tournament_id`) REFERENCES `models_tournament` (`tid`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='This table contains a list of all the events';
CREATE TABLE IF NOT EXISTS `models_inheritedmodel` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `zero` INT NOT NULL,
    `one` VARCHAR(40),
    `new_field` VARCHAR(100) NOT NULL,
    `two` VARCHAR(40) NOT NULL,
    `name` LONGTEXT NOT NULL
) CHARACTER SET utf8mb4;
CREATE TABLE IF NOT EXISTS `sometable` (
    `sometable_id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `some_chars_table` VARCHAR(255) NOT NULL,
    `fk_sometable` INT,
    CONSTRAINT `fk_sometabl_sometabl_6efae9bd` FOREIGN KEY (`fk_sometable`) REFERENCES `sometable` (`sometable_id`) ON DELETE CASCADE,
    KEY `idx_sometable_some_ch_3d69eb` (`some_chars_table`)
) CHARACTER SET utf8mb4;
CREATE TABLE IF NOT EXISTS `models_team` (
    `name` VARCHAR(50) NOT NULL  PRIMARY KEY COMMENT 'The TEAM name (and PK)',
    `key` INT NOT NULL,
    `manager_id` VARCHAR(50),
    CONSTRAINT `fk_models_t_models_t_ed161113` FOREIGN KEY (`manager_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE,
    KEY `idx_models_team_manager_34eaef` (`manager_id`, `key`),
    KEY `idx_models_team_manager_c571ed` (`manager_id`, `name`)
) CHARACTER SET utf8mb4 COMMENT='The TEAMS!';
CREATE TABLE IF NOT EXISTS `models_teamaddress` (
    `city` VARCHAR(50) NOT NULL  COMMENT 'City',
    `country` VARCHAR(50) NOT NULL  COMMENT 'Country',
    `street` VARCHAR(128) NOT NULL  COMMENT 'Street Address',
    `team_id` VARCHAR(50) NOT NULL  PRIMARY KEY,
    CONSTRAINT `fk_models_t_models_t_9e656881` FOREIGN KEY (`team_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE
) CHARACTER SET utf8mb4;
CREATE TABLE IF NOT EXISTS `models_venueinformation` (
    `id` INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `name` VARCHAR(128) NOT NULL,
    `capacity` INT NOT NULL,
    `rent` DOUBLE NOT NULL,
    `team_id` VARCHAR(50)  UNIQUE,
    CONSTRAINT `fk_models_v_models_t_cf93f20c` FOREIGN KEY (`team_id`) REFERENCES `models_team` (`name`) ON DELETE SET NULL
) CHARACTER SET utf8mb4;
CREATE TABLE IF NOT EXISTS `teamevents` (
    `event_id` BIGINT NOT NULL,
    `team_id` VARCHAR(50) NOT NULL,
    FOREIGN KEY (`event_id`) REFERENCES `models_event` (`id`) ON DELETE CASCADE,
    FOREIGN KEY (`team_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COMMENT='How participants relate';
CREATE TABLE IF NOT EXISTS `sometable_self` (
    `backward_sts` INT NOT NULL,
    `sts_forward` INT NOT NULL,
    FOREIGN KEY (`backward_sts`) REFERENCES `sometable` (`sometable_id`) ON DELETE CASCADE,
    FOREIGN KEY (`sts_forward`) REFERENCES `sometable` (`sometable_id`) ON DELETE CASCADE
) CHARACTER SET utf8mb4;
CREATE TABLE IF NOT EXISTS `models_team_team` (
    `team_rel_id` VARCHAR(50) NOT NULL,
    `team_id` VARCHAR(50) NOT NULL,
    FOREIGN KEY (`team_rel_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE,
    FOREIGN KEY (`team_id`) REFERENCES `models_team` (`name`) ON DELETE CASCADE
) CHARACTER SET utf8mb4;
""".strip(),
        )


class TestGenerateSchemaPostgresSQL(TestGenerateSchema):
    def init_for(self, module: str, safe=False) -> None:
        Tortoise.init(
            {
                "connections": {
                    "default": {
                        "engine": "tortoise.backends.asyncpg",
                        "database": "test",
                        "host": "127.0.0.1",
                        "password": "foomip",
                        "port": 3306,
                        "user": "root",
                    }
                },
                "apps": {"models": {"models": [module], "default_connection": "default"}},
            }
        )
        self.sqls = Tortoise.get_schema_sql(Tortoise.get_db_client("default"), safe=safe).split("; ")

    def test_noid(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql('"models_noid"')
        self.assertIn('"name" VARCHAR(255)', sql)
        self.assertIn('"id" SERIAL NOT NULL PRIMARY KEY', sql)

    def test_table_and_row_comment_generation(self):
        self.init_for("tests.testmodels")
        sql = self.get_sql("comments")
        self.assertIn("COMMENT ON TABLE \"comments\" IS 'Test Table comment'", sql)
        self.assertIn(
            'COMMENT ON COLUMN "comments"."escaped_comment_field" IS '
            "'This column acts as it''s own comment'",
            sql,
        )
        self.assertIn(
            'COMMENT ON COLUMN "comments"."multiline_comment" IS \'Some \\n comment\'', sql
        )

    def test_schema(self):
        self.maxDiff = None
        self.init_for("tests.schema.models_schema_create")
        sql = Tortoise.get_schema_sql(Tortoise.get_db_client("default"), safe=False)
        self.assertEqual(
            sql.strip(),
            """
CREATE TABLE "models_defaultpk" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "val" INT NOT NULL
);
CREATE TABLE "models_tournament" (
    "tid" SMALLSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(100) NOT NULL,
    "created" TIMESTAMP NOT NULL
);
CREATE INDEX "idx_models_tour_name_67cf9d" ON "models_tournament" ("name");
COMMENT ON COLUMN "models_tournament"."name" IS 'Tournament name';
COMMENT ON COLUMN "models_tournament"."created" IS 'Created */''`/* datetime';
COMMENT ON TABLE "models_tournament" IS 'What Tournaments */''`/* we have';
CREATE TABLE "models_event" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "modified" TIMESTAMP NOT NULL,
    "prize" DECIMAL(10,2),
    "token" VARCHAR(100) NOT NULL UNIQUE,
    "key" VARCHAR(100) NOT NULL,
    "tournament_id" SMALLINT NOT NULL REFERENCES "models_tournament" ("tid") ON DELETE CASCADE,
    CONSTRAINT "uid_models_even_name_bb6540" UNIQUE ("name", "prize"),
    CONSTRAINT "uid_models_even_tournam_8ac3b2" UNIQUE ("tournament_id", "key")
);
COMMENT ON COLUMN "models_event"."id" IS 'Event ID';
COMMENT ON COLUMN "models_event"."token" IS 'Unique token';
COMMENT ON COLUMN "models_event"."tournament_id" IS 'FK to tournament';
COMMENT ON TABLE "models_event" IS 'This table contains a list of all the events';
CREATE TABLE "models_inheritedmodel" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "zero" INT NOT NULL,
    "one" VARCHAR(40),
    "new_field" VARCHAR(100) NOT NULL,
    "two" VARCHAR(40) NOT NULL,
    "name" TEXT NOT NULL
);
CREATE TABLE "sometable" (
    "sometable_id" SERIAL NOT NULL PRIMARY KEY,
    "some_chars_table" VARCHAR(255) NOT NULL,
    "fk_sometable" INT REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE
);
CREATE INDEX "idx_sometable_some_ch_3d69eb" ON "sometable" ("some_chars_table");
CREATE TABLE "models_team" (
    "name" VARCHAR(50) NOT NULL  PRIMARY KEY,
    "key" INT NOT NULL,
    "manager_id" VARCHAR(50) REFERENCES "models_team" ("name") ON DELETE CASCADE
);
CREATE INDEX "idx_models_team_manager_34eaef" ON "models_team" ("manager_id", "key");
CREATE INDEX "idx_models_team_manager_c571ed" ON "models_team" ("manager_id", "name");
COMMENT ON COLUMN "models_team"."name" IS 'The TEAM name (and PK)';
COMMENT ON TABLE "models_team" IS 'The TEAMS!';
CREATE TABLE "models_teamaddress" (
    "city" VARCHAR(50) NOT NULL,
    "country" VARCHAR(50) NOT NULL,
    "street" VARCHAR(128) NOT NULL,
    "team_id" VARCHAR(50) NOT NULL  PRIMARY KEY REFERENCES "models_team" ("name") ON DELETE CASCADE
);
COMMENT ON COLUMN "models_teamaddress"."city" IS 'City';
COMMENT ON COLUMN "models_teamaddress"."country" IS 'Country';
COMMENT ON COLUMN "models_teamaddress"."street" IS 'Street Address';
CREATE TABLE "models_venueinformation" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(128) NOT NULL,
    "capacity" INT NOT NULL,
    "rent" DOUBLE PRECISION NOT NULL,
    "team_id" VARCHAR(50)  UNIQUE REFERENCES "models_team" ("name") ON DELETE SET NULL
);
CREATE TABLE "teamevents" (
    "event_id" BIGINT NOT NULL REFERENCES "models_event" ("id") ON DELETE CASCADE,
    "team_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE
);
COMMENT ON TABLE "teamevents" IS 'How participants relate';
CREATE TABLE "sometable_self" (
    "backward_sts" INT NOT NULL REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE,
    "sts_forward" INT NOT NULL REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE
);
CREATE TABLE "models_team_team" (
    "team_rel_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE,
    "team_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE
);
""".strip(),
        )

    def test_schema_safe(self):
        self.maxDiff = None
        self.init_for("tests.schema.models_schema_create")
        sql = Tortoise.get_schema_sql(Tortoise.get_db_client("default"), safe=True)
        self.assertEqual(
            sql.strip(),
            """
CREATE TABLE IF NOT EXISTS "models_defaultpk" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "val" INT NOT NULL
);
CREATE TABLE IF NOT EXISTS "models_tournament" (
    "tid" SMALLSERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(100) NOT NULL,
    "created" TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_models_tour_name_67cf9d" ON "models_tournament" ("name");
COMMENT ON COLUMN "models_tournament"."name" IS 'Tournament name';
COMMENT ON COLUMN "models_tournament"."created" IS 'Created */''`/* datetime';
COMMENT ON TABLE "models_tournament" IS 'What Tournaments */''`/* we have';
CREATE TABLE IF NOT EXISTS "models_event" (
    "id" BIGSERIAL NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "modified" TIMESTAMP NOT NULL,
    "prize" DECIMAL(10,2),
    "token" VARCHAR(100) NOT NULL UNIQUE,
    "key" VARCHAR(100) NOT NULL,
    "tournament_id" SMALLINT NOT NULL REFERENCES "models_tournament" ("tid") ON DELETE CASCADE,
    CONSTRAINT "uid_models_even_name_bb6540" UNIQUE ("name", "prize"),
    CONSTRAINT "uid_models_even_tournam_8ac3b2" UNIQUE ("tournament_id", "key")
);
COMMENT ON COLUMN "models_event"."id" IS 'Event ID';
COMMENT ON COLUMN "models_event"."token" IS 'Unique token';
COMMENT ON COLUMN "models_event"."tournament_id" IS 'FK to tournament';
COMMENT ON TABLE "models_event" IS 'This table contains a list of all the events';
CREATE TABLE IF NOT EXISTS "models_inheritedmodel" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "zero" INT NOT NULL,
    "one" VARCHAR(40),
    "new_field" VARCHAR(100) NOT NULL,
    "two" VARCHAR(40) NOT NULL,
    "name" TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS "sometable" (
    "sometable_id" SERIAL NOT NULL PRIMARY KEY,
    "some_chars_table" VARCHAR(255) NOT NULL,
    "fk_sometable" INT REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_sometable_some_ch_3d69eb" ON "sometable" ("some_chars_table");
CREATE TABLE IF NOT EXISTS "models_team" (
    "name" VARCHAR(50) NOT NULL  PRIMARY KEY,
    "key" INT NOT NULL,
    "manager_id" VARCHAR(50) REFERENCES "models_team" ("name") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "idx_models_team_manager_34eaef" ON "models_team" ("manager_id", "key");
CREATE INDEX IF NOT EXISTS "idx_models_team_manager_c571ed" ON "models_team" ("manager_id", "name");
COMMENT ON COLUMN "models_team"."name" IS 'The TEAM name (and PK)';
COMMENT ON TABLE "models_team" IS 'The TEAMS!';
CREATE TABLE IF NOT EXISTS "models_teamaddress" (
    "city" VARCHAR(50) NOT NULL,
    "country" VARCHAR(50) NOT NULL,
    "street" VARCHAR(128) NOT NULL,
    "team_id" VARCHAR(50) NOT NULL  PRIMARY KEY REFERENCES "models_team" ("name") ON DELETE CASCADE
);
COMMENT ON COLUMN "models_teamaddress"."city" IS 'City';
COMMENT ON COLUMN "models_teamaddress"."country" IS 'Country';
COMMENT ON COLUMN "models_teamaddress"."street" IS 'Street Address';
CREATE TABLE IF NOT EXISTS "models_venueinformation" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "name" VARCHAR(128) NOT NULL,
    "capacity" INT NOT NULL,
    "rent" DOUBLE PRECISION NOT NULL,
    "team_id" VARCHAR(50)  UNIQUE REFERENCES "models_team" ("name") ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS "teamevents" (
    "event_id" BIGINT NOT NULL REFERENCES "models_event" ("id") ON DELETE CASCADE,
    "team_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE
);
COMMENT ON TABLE "teamevents" IS 'How participants relate';
CREATE TABLE IF NOT EXISTS "sometable_self" (
    "backward_sts" INT NOT NULL REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE,
    "sts_forward" INT NOT NULL REFERENCES "sometable" ("sometable_id") ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS "models_team_team" (
    "team_rel_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE,
    "team_id" VARCHAR(50) NOT NULL REFERENCES "models_team" ("name") ON DELETE CASCADE
);
""".strip(),
        )
