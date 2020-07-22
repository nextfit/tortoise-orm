from tortoise import Tortoise
from tortoise.contrib import test
from tortoise.exceptions import ConfigurationError


class TestBadReleationReferenceErrors(test.SimpleTestCase):
    def setUp(self):
        Tortoise._app_models_map = {}
        Tortoise._db_client_map = {}
        Tortoise._inited = False

    async def asyncTearDown(self) -> None:
        await Tortoise.close_connections()
        Tortoise._reset()

    async def test_wrong_app_init(self):
        with self.assertRaisesRegex(ConfigurationError, "No app with name 'app' registered."):
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
                            "models": ["tests.model_setup.model_bad_rel1"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    async def test_wrong_model_init(self):
        with self.assertRaisesRegex(
            ConfigurationError, "No model with name 'Tour' registered in app 'models'."
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
                            "models": ["tests.model_setup.model_bad_rel2"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    async def test_no_app_in_reference_init(self):
        with self.assertRaisesRegex(
            ConfigurationError, 'ForeignKey accepts model name in format "app.Model"'
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
                            "models": ["tests.model_setup.model_bad_rel3"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    async def test_more_than_two_dots_in_reference_init(self):
        with self.assertRaisesRegex(
            ConfigurationError, 'ForeignKey accepts model name in format "app.Model"'
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
                            "models": ["tests.model_setup.model_bad_rel4"],
                            "default_connection": "default",
                        }
                    },
                }
            )

    async def test_no_app_in_o2o_reference_init(self):
        with self.assertRaisesRegex(
            ConfigurationError, 'OneToOneField accepts model name in format "app.Model"'
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
                            "models": ["tests.model_setup.model_bad_rel5"],
                            "default_connection": "default",
                        }
                    },
                }
            )
