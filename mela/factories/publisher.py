from typing import Dict

from ..components import Publisher
from ..factories.core.connection import connect
from ..factories.core.exchange import declare_exchange
from ..factories.core.queue import declare_queue
from ..settings import AbstractConnectionParams
from ..settings import ExchangeParams
from ..settings import PublisherParams
from ..settings import QueueParams


publishers: Dict[str, Publisher] = {}


async def publisher(settings: PublisherParams) -> Publisher:
    assert settings.name
    if settings.name not in publishers:
        assert isinstance(settings.connection, AbstractConnectionParams)
        connection = await connect(settings.name, settings.connection, 'w')
        channel = await connection.channel(publisher_confirms=(not settings.skip_unroutables))
        assert isinstance(settings.exchange, ExchangeParams)
        exchange = await declare_exchange(settings.exchange, channel)
        if settings.queue:
            async with connection.channel() as temp_channel:
                assert isinstance(settings.queue, QueueParams)
                queue = await declare_queue(settings.queue, temp_channel)
                await queue.bind(exchange, routing_key=settings.routing_key)
        instance: Publisher = Publisher(**settings.get_params_dict(), exchange=exchange)
        publishers[settings.name] = instance
    return publishers[settings.name]
