
import asyncio
import logging.config
import os
import pytest

from tortoise.contrib.test import TruncationTestCase, SimpleTestCase, TestEventLoopPolicy

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


@pytest.fixture(scope='session')
def event_loop(request):
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
def initialize_tests(request, event_loop):
    try:
        logging.config.dictConfig(LOGGING)
    except AttributeError as e:
        print(e)

    SimpleTestCase.tortoise_test_db = os.environ.get("TORTOISE_TEST_DB", "sqlite://:memory:")

    # original_policy = asyncio.get_event_loop_policy()
    # asyncio.set_event_loop_policy(TestEventLoopPolicy(event_loop))

    event_loop.run_until_complete(TruncationTestCase.initialize())
    yield True
    event_loop.run_until_complete(TruncationTestCase.finalize())

    # asyncio.set_event_loop_policy(original_policy)
