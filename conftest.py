
import asyncio
import logging.config
import os
import pytest

from tortoise.contrib.test import finalizer, initializer

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{asctime} {levelname} [{name}:{lineno}] {message}',
            # 'format': '{asctime} {levelname} [{name}.{module}.{funcName}:{lineno}] {message}',
            'style': '{',
        },
        'simple': {
            'format': '{asctime} {levelname} {message}',
            'style': '{',
        },
    },
    'filters': {
    },
    'handlers': {
        'null': {
            'class': 'logging.NullHandler',
        },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'loggers': {
        'tortoise': {
            'level': 'DEBUG',
        },
        'aiosqlite': {
            'level': 'INFO',
        },
        'asyncio': {
            'level': 'INFO',
        }
    },
    'root': {
        'handlers': ['console', ],
        'level': 'DEBUG',
    },
}


@pytest.yield_fixture(scope='session')
def event_loop(request):
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests(request, event_loop):
    # def _finalizer():
    #     finalizer(event_loop)

    try:
        logging.config.dictConfig(LOGGING)
    except AttributeError as e:
        print(e)

    # request.addfinalizer(_finalizer)

    db_url = os.environ.get("TORTOISE_TEST_DB", "sqlite://:memory:")
    modules = str(os.environ.get("TORTOISE_TEST_MODULES", "tests.testmodels")).split(",")
    if not modules:
        raise Exception("TORTOISE_TEST_MODULES env var not defined")

    # initializer(modules, db_url=db_url, loop=event_loop)
    await initializer(modules, db_url=db_url)

    yield True

    await finalizer()
