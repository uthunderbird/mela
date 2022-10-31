from ..components.rpc import RPC
from ..components.rpc import RPCClient
from ..settings import ConsumerParams
from ..settings import PublisherParams
from ..settings import RPCParams
from .consumer import anonymous_consumer
from .consumer import consumer
from .publisher import publisher


async def service(settings: RPCParams) -> 'RPC':
    assert isinstance(settings.worker, ConsumerParams)
    assert isinstance(settings.request_publisher, PublisherParams)
    assert isinstance(settings.response_publisher, PublisherParams)
    assert settings.name
    worker_instance = await consumer(settings.worker)
    response_publisher_instance = await publisher(settings.response_publisher)
    instance = RPC(
        settings.name,
        log_level=settings.log_level,
        worker=worker_instance,
        response_publisher=response_publisher_instance,
    )
    client_instance = await client(settings)
    instance.client = client_instance
    return instance


async def client(settings: RPCParams) -> 'RPCClient':
    assert isinstance(settings.request_publisher, PublisherParams)
    assert isinstance(settings.response_publisher, PublisherParams)
    assert settings.name
    settings.request_publisher.skip_unroutables = True
    request_publisher_instance = await publisher(settings.request_publisher)

    consumer_params = ConsumerParams(
        name=settings.name + '_client',
        connection=settings.connection,
        exchange=settings.response_exchange,
        routing_key='',
        queue='',
    )

    response_consumer_instance = await anonymous_consumer(consumer_params)
    instance = RPCClient(
        settings.name,
        log_level=settings.log_level,
        request_publisher=request_publisher_instance,
        response_consumer=response_consumer_instance,
    )
    await instance.consume()
    return instance
