import asyncio

import pytest
from aiormq.exceptions import PublishError
from pamqp import commands as spec

from mela.factories import consumer
from mela.factories import publisher
from mela.processor import Processor
from mela.settings import ConsumerParams
from mela.settings import ExchangeParams
from mela.settings import PublisherParams
from mela.settings import QueueParams


@pytest.fixture
def exchange_params_factory(request):
    default_exchange_name = request.node.originalname + '-x'

    def factory(**kwargs):
        kwargs.setdefault('name', default_exchange_name)
        kwargs.setdefault('durable', False)
        return ExchangeParams(**kwargs)
    return factory


@pytest.fixture
def default_exchange_params(exchange_params_factory):
    return exchange_params_factory()


@pytest.fixture
def publisher_params_factory(request, default_exchange_params, default_connection_params):
    def factory(routing_key, **kwargs):
        kwargs.setdefault('exchange', default_exchange_params)
        kwargs.setdefault('connection', default_connection_params)
        kwargs.setdefault('name', request.node.name)
        kwargs['routing_key'] = routing_key
        return PublisherParams(**kwargs)
    return factory


@pytest.fixture
def default_publisher_params(request, publisher_params_factory):
    return publisher_params_factory(request.param)


@pytest.fixture
def queue_params_factory(request):
    default_queue_name = request.node.originalname + '-q'

    def factory(**kwargs):
        kwargs.setdefault('name', default_queue_name)
        kwargs.setdefault('durable', False)
        return QueueParams(**kwargs)
    return factory


@pytest.fixture
def default_queue_params(queue_params_factory):
    return queue_params_factory()


@pytest.fixture
def consumer_params_factory(
        request,
        default_exchange_params,
        default_connection_params,
        default_queue_params,
):
    def factory(routing_key, **kwargs):
        kwargs.setdefault('exchange', default_exchange_params)
        kwargs.setdefault('connection', default_connection_params)
        kwargs.setdefault('name', request.node.name)
        kwargs.setdefault('queue', default_queue_params)
        kwargs['routing_key'] = routing_key
        return ConsumerParams(**kwargs)
    return factory


@pytest.fixture
def default_consumer_params(request, consumer_params_factory):
    return consumer_params_factory(request.param)


async def test_connection_is_set(connection_factory):
    connection = await connection_factory()
    await connection.ready()


@pytest.mark.parametrize("default_publisher_params", ['absolutely_unroutable', ''], indirect=True)
async def test_publish_unroutable_message(default_publisher_params):
    publisher_ = await publisher(
        default_publisher_params,
    )
    with pytest.raises(PublishError):
        await publisher_.publish({'lol': 'wut'})


async def test_publish_unroutable_message_should_be_skipped(publisher_params_factory):
    publisher_ = await publisher(
        publisher_params_factory('absolutely_unroutable', skip_unroutables=True),
    )
    assert await publisher_.publish({'lol': 'wut'}) is None


async def test_publish_routable_message(publisher_params_factory, default_queue_params):
    publisher_ = await publisher(
        publisher_params_factory(
            'somehow_routable',
            queue=default_queue_params,
        ),
    )
    resp = await publisher_.publish({'lol': 'wut'})
    assert isinstance(resp, spec.Basic.Ack)


@pytest.mark.parametrize(
    'default_publisher_params,default_consumer_params',
    [
        ('route', 'route'),
        ('other_route', 'other_route'),
    ],
    indirect=True)
async def test_consumer_straight(default_consumer_params, default_publisher_params):
    consumer_ = await consumer(default_consumer_params)
    publisher_ = await publisher(default_publisher_params)
    send_body = {'lol': 'wut'}
    called_once = False

    async def callback(body, message):
        assert body == send_body
        nonlocal called_once
        if called_once:
            raise AssertionError
        called_once = True
        await consumer_.cancel()
    consumer_.set_processor(Processor(callback))
    asyncio.create_task(consumer_.consume())

    await publisher_.publish(send_body)
    await publisher_.publish(send_body)

    await asyncio.sleep(0.01)

    assert called_once is True
