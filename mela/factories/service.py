from ..components.service import Service
from ..settings import ConsumerParams
from ..settings import PublisherParams
from ..settings import ServiceParams
from .consumer import consumer
from .publisher import publisher


async def service(settings: ServiceParams) -> 'Service':
    assert isinstance(settings.consumer, ConsumerParams)
    assert isinstance(settings.publisher, PublisherParams)
    assert settings.name
    publisher_instance = await publisher(settings.publisher)
    consumer_instance = await consumer(settings.consumer)
    instance = Service(settings.name, publisher=publisher_instance, consumer=consumer_instance)
    return instance
