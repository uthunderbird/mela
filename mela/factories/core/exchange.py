from aio_pika.abc import AbstractChannel
from aio_pika.abc import AbstractExchange

from ...settings import ExchangeParams


async def declare_exchange(settings: ExchangeParams, channel: AbstractChannel) -> AbstractExchange:
    return await channel.declare_exchange(**settings.get_params_dict())
