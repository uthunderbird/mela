from typing import Dict

from ..components import Consumer
from ..factories.core.connection import connect
from ..factories.core.exchange import declare_exchange
from ..factories.core.queue import declare_queue
from ..settings import AbstractConnectionParams
from ..settings import ConsumerParams
from ..settings import ExchangeParams
from ..settings import QueueParams


consumers: Dict[str, Consumer] = {}


async def consumer(settings: ConsumerParams) -> Consumer:
    assert settings.name
    if settings.name not in consumers:
        assert isinstance(settings.connection, AbstractConnectionParams)
        connection = await connect(settings.name, settings.connection, 'r')
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=settings.prefetch_count)
        assert isinstance(settings.queue, QueueParams)
        queue = await declare_queue(settings.queue, channel)
        assert isinstance(settings.exchange, ExchangeParams)
        exchange = await declare_exchange(settings.exchange, channel)
        await queue.bind(exchange, routing_key=settings.routing_key)
        instance = Consumer(**settings.get_params_dict(), queue=queue)
        consumers[settings.name] = instance
    return consumers[settings.name]


async def anonymous_consumer(settings: ConsumerParams) -> Consumer:
    assert isinstance(settings.connection, AbstractConnectionParams)
    assert settings.name
    connection = await connect(settings.name, settings.connection, 'r')
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=settings.prefetch_count)
    settings.queue = QueueParams(name="", durable=False, auto_delete=True, exclusive=True)
    queue = await declare_queue(settings.queue, channel)
    assert isinstance(settings.exchange, ExchangeParams)
    exchange = await declare_exchange(settings.exchange, channel)
    await queue.bind(exchange, routing_key=queue.name)
    instance = Consumer(**settings.get_params_dict(), queue=queue)
    return instance
