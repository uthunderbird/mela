from aio_pika.abc import AbstractChannel
from aio_pika.abc import AbstractQueue

from ...factories.core.exchange import declare_exchange
from ...settings import ExchangeParams
from ...settings import QueueParams


async def declare_queue(settings: QueueParams, channel: AbstractChannel) -> AbstractQueue:
    if settings.dead_letter_exchange:
        assert isinstance(settings.dead_letter_exchange, ExchangeParams)
        await declare_exchange(settings.dead_letter_exchange, channel)
    return await channel.declare_queue(**settings.get_params_dict())
