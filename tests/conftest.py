import asyncio

import pytest

from mela.factories.core.connection import connect
from mela.settings import URLConnectionParams


@pytest.fixture(scope='session')
def default_connection_params():
    default_url = 'amqp://user:bitnami@localhost:5672'
    return URLConnectionParams(url=default_url)


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope='module')
def connection_factory(default_connection_params):
    async def factory(name='test', connection_params=None, mode='r'):
        if connection_params is None:
            connection_params = default_connection_params
        return await connect(name, connection_params, mode)
    return factory
