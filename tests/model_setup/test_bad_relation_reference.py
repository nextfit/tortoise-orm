
from unittest import IsolatedAsyncioTestCase
from tortoise import Tortoise
from tortoise.exceptions import ConfigurationError


class TestBadRelationReferenceErrors(IsolatedAsyncioTestCase):

    def test_wrong_app_init(self):
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

    def test_wrong_model_init(self):
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

    def test_more_than_two_dots_in_reference_init(self):
        with self.assertRaisesRegex(
            ConfigurationError, 'Model name needs to be in format "app.Model" or "Model"'
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
