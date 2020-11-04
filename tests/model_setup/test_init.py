
import os
from unittest import IsolatedAsyncioTestCase
from tortoise import Tortoise
from tortoise.exceptions import ConfigurationError


class TestInitErrors(IsolatedAsyncioTestCase):
    def setUp(self):
        Tortoise._app_models_map = {}
        Tortoise._db_client_map = {}
        Tortoise._inited = False

    async def asyncTearDown(self):
        await Tortoise.close_connections()
        Tortoise._reset()

    def test_basic_init(self):
        Tortoise.init(
            {
                "connections": {
                    "default": {
                        "engine": "tortoise.backends.sqlite",
                        "file_path": ":memory:",
                    }
                },
                "apps": {
                    "models": {"models": ["tests.testmodels"], "default_connection": "default"}
                },
            }
        )
        self.assertIn("models", Tortoise._app_models_map)
        self.assertIsNotNone(Tortoise.get_db_client("default"))

    def test_empty_modules_init(self):
        with self.assertWarnsRegex(RuntimeWarning, 'Module "tests.model_setup" has no models'):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {"models": ["tests.model_setup"], "default_connection": "default"}
                    },
                }
            )

    def test_dup1_init(self):
        with self.assertRaisesRegex(
            ConfigurationError,
            "backward relation 'events' duplicates in model <class 'tests.model_setup.models_dup1.Tournament'>"
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {
                            "models": ["tests.model_setup.models_dup1"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    def test_dup2_init(self):
        with self.assertRaisesRegex(
            ConfigurationError,
            "backward relation 'events' duplicates in model <class 'tests.model_setup.models_dup2.Team'>"
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {
                            "models": ["tests.model_setup.models_dup2"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    def test_dup3_init(self):
        with self.assertRaisesRegex(
            ConfigurationError,
            "backward relation 'event' duplicates in model <class 'tests.model_setup.models_dup3.Tournament'>"
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {
                            "models": ["tests.model_setup.models_dup3"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    def test_m2m_dup_init(self):
        with self.assertRaisesRegex(
            ConfigurationError,
            "through model <class 'tests.model_setup.models_m2m_dup.TeamEvent'> has more than one field pointing to <class 'tests.model_setup.models_m2m_dup.Team'>. specify `forward_key` in <class 'tests.model_setup.models_m2m_dup.Event'>.participants"
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {
                            "models": ["tests.model_setup.models_m2m_dup"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    def test_m2m_missing_init(self):
        with self.assertRaisesRegex(
            ConfigurationError,
            "through model <class 'tests.model_setup.models_m2m_missing.TeamEvent'> must have a ForeignKey relation to model <class 'tests.model_setup.models_m2m_missing.Event'>"
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {
                            "models": ["tests.model_setup.models_m2m_missing"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    def test_generated_nonint(self):
        with self.assertRaisesRegex(
            ConfigurationError, "Field 'val' \\(CharField\\) can't be DB-generated"
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {
                            "models": ["tests.model_setup.model_generated_nonint"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    def test_multiple_pk(self):
        with self.assertRaisesRegex(
            ConfigurationError,
            "Can't create model Tournament with two primary keys, only single primary key is supported",
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {
                            "models": ["tests.model_setup.model_multiple_pk"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    def test_nonpk_id(self):
        with self.assertRaisesRegex(
            ConfigurationError,
            "Can't create model Tournament without explicit primary key if"
            " field 'id' already present",
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {
                            "models": ["tests.model_setup.model_nonpk_id"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    def test_unknown_connection(self):
        with self.assertRaisesRegex(ConfigurationError, 'Unknown connection "fioop"'):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {"models": ["tests.testmodels"], "default_connection": "fioop"}
                    },
                }
            )

    def test_url_without_modules(self):
        with self.assertRaisesRegex(
            ConfigurationError, 'You must specify "db_url" and "modules" together'
        ):
            Tortoise.init(db_url=f"sqlite://{':memory:'}")

    def test_default_connection_init(self):
        Tortoise.init(
            {
                "connections": {
                    "default": {
                        "engine": "tortoise.backends.sqlite",
                        "file_path": ":memory:",
                    }
                },
                "apps": {"models": {"models": ["tests.testmodels"]}},
            }
        )
        self.assertIn("models", Tortoise._app_models_map)
        self.assertIsNotNone(Tortoise.get_db_client("default"))

    def test_db_url_init(self):
        Tortoise.init(
            {
                "connections": {"default": f"sqlite://{':memory:'}"},
                "apps": {
                    "models": {"models": ["tests.testmodels"], "default_connection": "default"}
                },
            }
        )
        self.assertIn("models", Tortoise._app_models_map)
        self.assertIsNotNone(Tortoise.get_db_client("default"))

    def test_shorthand_init(self):
        Tortoise.init(
            db_url=f"sqlite://{':memory:'}", modules={"models": ["tests.testmodels"]}
        )
        self.assertIn("models", Tortoise._app_models_map)
        self.assertIsNotNone(Tortoise.get_db_client("default"))

    def test_init_wrong_connection_engine(self):
        with self.assertRaisesRegex(ImportError, "tortoise.backends.test"):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.test",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {"models": ["tests.testmodels"], "default_connection": "default"}
                    },
                }
            )

    def test_init_wrong_connection_engine_2(self):
        with self.assertRaisesRegex(
            ConfigurationError,
            'Backend for engine "tortoise.backends" does not implement db client',
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {"models": ["tests.testmodels"], "default_connection": "default"}
                    },
                }
            )

    def test_init_no_connections(self):
        with self.assertRaisesRegex(ConfigurationError, 'Config must define "connections" section'):
            Tortoise.init(
                {
                    "apps": {
                        "models": {"models": ["tests.testmodels"], "default_connection": "default"}
                    }
                }
            )

    def test_init_no_apps(self):
        with self.assertRaisesRegex(ConfigurationError, 'Config must define "apps" section'):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    }
                }
            )

    def test_init_config_and_config_file(self):
        with self.assertRaisesRegex(
            ConfigurationError, 'You should init either from "config", "config_file" or "db_url"'
        ):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {"models": ["tests.testmodels"], "default_connection": "default"}
                    },
                },
                config_file="file.json",
            )

    def test_init_config_file_wrong_extension(self):
        with self.assertRaisesRegex(
            ConfigurationError, "Unknown config extension .ini, only .yml and .json are supported"
        ):
            Tortoise.init(config_file="config.ini")

    def test_init_json_file(self):
        Tortoise.init(config_file=os.path.dirname(__file__) + "/init.json")
        self.assertIn("models", Tortoise._app_models_map)
        self.assertIsNotNone(Tortoise.get_db_client("default"))

    def test_init_yaml_file(self):
        Tortoise.init(config_file=os.path.dirname(__file__) + "/init.yaml")
        self.assertIn("models", Tortoise._app_models_map)
        self.assertIsNotNone(Tortoise.get_db_client("default"))

    async def test_generate_schema_without_init(self):
        with self.assertRaisesRegex(
            ConfigurationError, r"You have to call \.init\(\) first before generating schemas"
        ):
            await Tortoise.generate_schemas()

    async def test_drop_databases_without_init(self):
        with self.assertRaisesRegex(
            ConfigurationError, r"You have to call \.init\(\) first before deleting schemas"
        ):
            await Tortoise.drop_databases()

    def test_bad_models(self):
        with self.assertRaisesRegex(ConfigurationError, 'Module "tests.testmodels2" not found'):
            Tortoise.init(
                {
                    "connections": {
                        "default": {
                            "engine": "tortoise.backends.sqlite",
                            "file_path": ":memory:",
                        }
                    },
                    "apps": {
                        "models": {"models": ["tests.testmodels2"], "default_connection": "default"}
                    },
                }
            )
