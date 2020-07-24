
import asyncio
import logging.config
import os
import pytest

from tortoise.contrib.test import TortoiseTestModelsTestCase, TortoiseBaseTestCase

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

try:
    logging.config.dictConfig(LOGGING)
except AttributeError as e:
    print(e)

logger = logging.getLogger("tortoise.test")


@pytest.fixture(scope='session')
def event_loop(request):
    loop = asyncio.get_event_loop()

    #
    # We talk through all collected tests and add event_loop
    # property to each of them
    #
    # See here for details: https://docs.pytest.org/en/stable/example/special.html
    #
    # session = request.node
    # for item in session.items:
    #     cls = item.getparent(pytest.Class)
    #     if not hasattr(cls, event_loop):
    #         cls.event_loop = loop
    #

    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests(request):
    logger.debug("initialize_tests")

    TortoiseBaseTestCase.tortoise_test_db = os.environ.get("TORTOISE_TEST_DB", "sqlite://:memory:")
    await TortoiseTestModelsTestCase.initialize()
    yield True
    await TortoiseTestModelsTestCase.finalize()
