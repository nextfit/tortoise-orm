
import asyncio
import importlib
import json
import logging
import os
import warnings
from inspect import isclass
from typing import Coroutine, Dict, List, Optional, Type

from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.backends.base.config_generator import expand_db_url, generate_config, obscure_password
from tortoise.exceptions import ConfigurationError, ParamsError
from tortoise.fields.relational import RelationField

from tortoise.models import Model
from contextvars import ContextVar


logger = logging.getLogger("tortoise")


class Tortoise:
    _inited: bool = False

    _app_models_map: Dict[str, Dict[str, Type[Model]]] = {}
    _db_client_map: Dict[str, BaseDBAsyncClient] = {}
    _current_transaction_map: Dict[str, ContextVar] = {}

    @classmethod
    def get_db_client(cls, connection_name: str) -> BaseDBAsyncClient:
        """
        Returns db_client by name.

        :raises KeyError: If db_client name does not exist.
        """
        return cls._db_client_map[connection_name]

    @classmethod
    def get_transaction_db_client(cls, connection_name: Optional[str]) -> BaseDBAsyncClient:
        if connection_name:
            return cls._current_transaction_map[connection_name].get()

        elif len(cls._current_transaction_map) == 1:
            return list(cls._current_transaction_map.values())[0].get()

        else:
            raise ParamsError(
                "You are running with multiple databases, so you should specify"
                f" connection_name: {list(cls._current_transaction_map.keys())}"
            )

    @classmethod
    def get_connection_names(cls) -> List[str]:
        return list(cls._db_client_map.keys())

    @classmethod
    def get_model(cls, full_name: str):
        """
        Test, if app and model really exist. Throws a ConfigurationError with a hopefully
        helpful message. If successful, returns the requested model.
        """
        if len(full_name.split(".")) != 2:
            raise ConfigurationError('Model name needs to be in format "app.Model"')

        app_name, model_name = full_name.split(".")
        if app_name not in cls._app_models_map:
            raise ConfigurationError(f"No app with name '{app_name}' registered.")

        related_app = cls._app_models_map[app_name]
        if model_name not in related_app:
            raise ConfigurationError(
                f"No model with name '{model_name}' registered in app '{app_name}'."
            )

        return related_app[model_name]

    @classmethod
    def describe_models(cls,
        models: Optional[List[Type[Model]]] = None, serializable: bool = True) -> Dict[str, dict]:
        """
        Describes the given list of models or ALL registered models.

        :param models:
            List of models to describe, if not provided then describes ALL registered models

        :param serializable:
            ``False`` if you want raw python objects,
            ``True`` for JSON-serializable data. (Defaults to ``True``)

        :return:
            A dictionary containing the model qualifier as key,
            and the same output as ``Model.describe(...)`` as value:

            .. code-block:: python3

                {
                    "models.User": {...},
                    "models.Permission": {...}
                }
        """

        if not models:
            models = [model for models_map in cls._app_models_map.values() for model in models_map.values()]

        return {model.full_name(): model.describe(serializable) for model in models}

    @classmethod
    def _discover_client_class(cls, engine: str) -> Type[BaseDBAsyncClient]:
        # Let exception bubble up for transparency
        engine_module = importlib.import_module(engine)

        try:
            return engine_module.client_class  # type: ignore
        except AttributeError:
            raise ConfigurationError(f'Backend for engine "{engine}" does not implement db client')

    @classmethod
    async def _init_connections(cls, connections_config: dict, create_db: bool) -> None:
        for connection_name, conn_config in connections_config.items():
            if isinstance(conn_config, str):
                conn_config = expand_db_url(conn_config)

            client_class = cls._discover_client_class(conn_config.get("engine"))
            db_params = conn_config["credentials"].copy()
            db_params.update({"connection_name": connection_name})
            db_client = client_class(**db_params)  # type: ignore

            if create_db:
                await db_client.db_create()

            await db_client.create_connection(with_db=True)
            cls._db_client_map[connection_name] = db_client
            cls._current_transaction_map[connection_name] = ContextVar(connection_name, default=db_client)

    @classmethod
    def _discover_models(cls, models_path: str, app_label: str) -> List[Type[Model]]:
        try:
            module = importlib.import_module(models_path)
        except ImportError:
            raise ConfigurationError(f'Module "{models_path}" not found')

        possible_models = getattr(module, "__models__", None)

        try:
            possible_models = [*possible_models]
        except TypeError:
            possible_models = None

        if not possible_models:
            possible_models = [getattr(module, attr_name) for attr_name in dir(module)]

        discovered_models = []
        for model in possible_models:
            if isclass(model) and issubclass(model, Model) and not model._meta.abstract:
                if not model._meta.app or model._meta.app == app_label:
                    model._meta.app = app_label
                    discovered_models.append(model)

        if not discovered_models:
            warnings.warn(f'Module "{models_path}" has no models', RuntimeWarning, stacklevel=4)

        return discovered_models

    @classmethod
    def _init_apps(cls, apps_config: dict) -> None:
        for app_name, app_config in apps_config.items():
            connection_name = app_config.get("default_connection", "default")
            try:
                cls.get_db_client(connection_name)
            except KeyError:
                raise ConfigurationError(
                    'Unknown connection "{}" for app "{}"'.format(connection_name, app_name))

            app_models: List[Type[Model]] = []
            for module in app_config["models"]:
                app_models += cls._discover_models(module, app_name)

            for model in app_models:
                model._meta.connection_name = connection_name

            cls._app_models_map[app_name] = {model.__name__: model for model in app_models}

    @classmethod
    def _init_models(cls) -> None:
        models_list = []
        for app_name, _app_models_map in cls._app_models_map.items():
            for model in _app_models_map.values():
                if not model._meta._inited:
                    field_objects = list(model._meta.fields_map.values())
                    for field in field_objects:
                        if isinstance(field, RelationField) and not field.auto_created:
                            field.create_relation()

                    model._meta._inited = True
                    models_list.append(model)

        for model in models_list:
            model._meta.finalize_model()

    @classmethod
    def _get_config_from_config_file(cls, config_file: str) -> dict:
        _, extension = os.path.splitext(config_file)
        if extension in (".yml", ".yaml"):
            import yaml  # pylint: disable=C0415

            with open(config_file, "r") as f:
                config = yaml.safe_load(f)

        elif extension == ".json":
            with open(config_file, "r") as f:
                config = json.load(f)
        else:
            raise ConfigurationError(
                f"Unknown config extension {extension}, only .yml and .json are supported"
            )
        return config

    @classmethod
    def get_models_for_connection(cls, connection_name) -> List[Type[Model]]:
        return [model
            for models_map in cls._app_models_map.values()
            for model in models_map.values()
            if model._meta.connection_name == connection_name
        ]

    @classmethod
    async def init(
        cls,
        config: Optional[dict] = None,
        config_file: Optional[str] = None,
        _create_db: bool = False,
        db_url: Optional[str] = None,
        modules: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        """
        Sets up Tortoise-ORM.

        You can configure using only one of ``config``, ``config_file``
        and ``(db_url, modules)``.

        Parameters
        ----------
        :param config:
            Dict containing config:

            Example
            -------

            .. code-block:: python3

                {
                    'connections': {
                        # Dict format for connection
                        'default': {
                            'engine': 'tortoise.backends.asyncpg',
                            'credentials': {
                                'host': 'localhost',
                                'port': '5432',
                                'user': 'tortoise',
                                'password': 'qwerty123',
                                'database': 'test',
                            }
                        },
                        # Using a DB_URL string
                        'default': 'postgres://postgres:qwerty123@localhost:5432/events'
                    },
                    'apps': {
                        'models': {
                            'models': ['__main__'],
                            # If no default_connection specified, defaults to 'default'
                            'default_connection': 'default',
                        }
                    }
                }

        :param config_file:
            Path to .json or .yml (if PyYAML installed) file containing config with
            same format as above.

        :param _create_db:
            If ``True`` tries to create database for specified connections,
            could be used for testing purposes.

        :param db_url:
            Use a DB_URL string. See :ref:`db_url`

        :param modules:
            Dictionary of ``key``: [``list_of_modules``] that defined "apps" and modules that
            should be discovered for models.

        Raises
        ------
        ConfigurationError
            For any configuration error
        """

        if int(bool(config) + bool(config_file) + bool(db_url)) != 1:
            raise ConfigurationError(
                'You should init either from "config", "config_file" or "db_url"'
            )

        if config_file:
            config = cls._get_config_from_config_file(config_file)

        if db_url:
            if not modules:
                raise ConfigurationError('You must specify "db_url" and "modules" together')
            config = generate_config(db_url, modules)

        if "connections" not in config:
            raise ConfigurationError('Config must define "connections" section')

        if "apps" not in config:
            raise ConfigurationError('Config must define "apps" section')

        if cls._inited:
            await cls.close_connections()
            await cls._reset_apps()

        connections_config = config["connections"]  # type: ignore
        apps_config = config["apps"]  # type: ignore
        logger.info(
            "Tortoise-ORM startup\n    connections: %s\n    apps: %s",
            str(obscure_password(connections_config)),
            str(apps_config),
        )

        await cls._init_connections(connections_config, _create_db)
        cls._init_apps(apps_config)
        cls._init_models()
        cls._inited = True

    @classmethod
    async def close_connections(cls) -> None:
        """
        Close all connections cleanly.

        It is required for this to be called on exit,
        else your event loop may never complete
        as it is waiting for the connections to die.
        """
        for db_client in cls._db_client_map.values():
            await db_client.close()

        cls._db_client_map = {}
        logger.info("Tortoise-ORM shutdown")

    @classmethod
    async def _reset_apps(cls) -> None:
        for models_map in cls._app_models_map.values():
            for model in models_map.values():
                model._meta.connection_name = None

        cls._app_models_map.clear()
        cls._current_transaction_map.clear()

    @classmethod
    async def generate_schemas(cls, safe: bool = True) -> None:
        """
        Generate schemas according to models provided to ``.init()`` method.
        Will fail if schemas already exists, so it's not recommended to be used as part
        of application workflow

        Parameters
        ----------
        safe:
            When set to true, creates the table only when it does not already exist.
        """
        if not cls._inited:
            raise ConfigurationError("You have to call .init() first before generating schemas")
        for db_client in cls._db_client_map.values():
            await db_client.generate_schema(safe)

    @classmethod
    async def _drop_databases(cls) -> None:
        """
        Tries to drop all databases provided in config passed to ``.init()`` method.
        Normally should be used only for testing purposes.
        """
        if not cls._inited:
            raise ConfigurationError("You have to call .init() first before deleting schemas")
        for db_client in cls._db_client_map.values():
            await db_client.close()
            await db_client.db_delete()

        cls._db_client_map = {}
        await cls._reset_apps()


def run_async(coro: Coroutine) -> None:
    """
    Simple async runner that cleans up DB connections on exit.
    This is meant for simple scripts.

    Usage::

        from tortoise import Tortoise, run_async

        async def do_stuff():
            await Tortoise.init(
                db_url='sqlite://db.sqlite3',
                models={'models': ['app.models']}
            )

            ...

        run_async(do_stuff())
    """
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(Tortoise.close_connections())


__version__ = "0.15.9-sina"
