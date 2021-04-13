from typing import Optional, Callable, Union

import logging

import asyncio
import aio_pika
import aiormq
import envyaml
import inspect

try:
    import ujson as json
except ImportError:
    import json

logging.basicConfig(format='%(name)s\t%(asctime)s\t%(levelname)s\t%(message)s', level=logging.INFO)


class Configurable:

    def __init__(self, app: 'Mela', name=None):
        self.config: Optional[dict] = None
        self.name = name
        self.app = app

    def configure(self, config: dict):
        self.config = config
        self.on_config_update()
        self.ensure_configured()

    def configure_from_yaml(self, filename):
        env = envyaml.EnvYAML(filename)
        self.configure(dict(env))

    def read_config_yaml(self, filename):
        """
        DEPRECATED
        Will be removed in v.1.1

        Only for legacy usage
        """
        logging.warning("DEPRECATION: `Configurable.read_config_yaml` will be "
                        "replaced with `Configurable.configure_from_yaml` in v1.1")
        self.configure_from_yaml(filename)

    def on_config_update(self):
        pass

    def is_configured(self):
        return self.config is not None

    def ensure_configured(self):
        if not self.is_configured():
            raise Exception(f"No config provided for {self}")


class Loggable(Configurable):

    def __init__(self, app: 'Mela', name=None):
        super(Loggable, self).__init__(app, name=name)
        self.log = None
        self.set_logger(logging.getLogger(self.name))

    def set_logger(self, logger):
        self.log = logger
        self.log.setLevel(logging.INFO)

    def on_config_update(self):
        super().on_config_update()
        if 'log' in self.config:
            if 'name' in self.config['log']:
                self.log = logging.getLogger(self.config['log']['name'])


class Mela(Loggable):

    def __init__(self, name):
        super(Mela, self).__init__(self, name)
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self._runnables = []
        self._connection_registry = ConnectionRegistry(self, 'connections')
        self._results = None
        self.services = {}
        self.publishers = {}
        self.consumers = {}

    def on_config_update(self):
        self.config.setdefault('publishers', {})
        self.config.setdefault('producers', {})  # TODO DEPRECATION remove in v1.1
        self.config.setdefault('consumers', {})
        self.config.setdefault('services', {})
        self.config.setdefault('connections', {})
        if 'connection' in self.config and not self.config['connections']:
            self.config['connections'] = {'default': self.config['connection']}

    def publisher(self, name="default", options=None) -> 'MelaPublisher':
        if options is None:
            options = {}
        if options:
            self.log.warning("DEPRECATION: `Mela.publisher` will not "
                             "take options since v1.1. Use config file.")
        self.ensure_configured()
        if name not in self.config['publishers'] and name not in self.config['producers']:
            # TODO DEPRECATION v1.1 remove `producers`
            if 'publisher' in self.config:
                name = 'default'
                self.config['publishers'][name] = self.config['publisher']
            else:
                raise KeyError(f"No config found for publisher {name}")
        publisher = MelaPublisher(self, name)
        publisher.configure({**(self.config['publishers'].get(name) or {}), **(self.config['producers'].get(name) or {}), **options})
        self.register(publisher)
        return publisher

    def producer(self, name, options=None):
        """
        DEPRECATED
        Will be removed in v.1.1

        Only for legacy usage
        """
        self.log.warning("DEPRECATION: `Mela.producer` is replaced with `Mela.publisher`")

        def decorator(*args, **kwargs):
            return self.publisher(name, options)
        return decorator

    def consumer(self, name):
        def decorator(func):
            consumer = MelaConsumer(self, name)
            consumer.configure(self.config['consumers'][name])
            consumer.set_processor(func)
            self.register(consumer)
            return consumer

        return decorator

    def service(self, name, producer_name=None, options_consumer=None, options_publisher=None):
        if options_publisher is None:
            options_publisher = {}
        if options_consumer is None:
            options_consumer = {}
        if producer_name:
            self.log.warning("DEPRECATION: `Mela.service` will use only one argument, service name. "
                             "Update your config format.")
        if options_consumer or options_publisher:
            self.log.warning("DEPRECATION: `Mela.service` will not "
                             "take options since v1.1. Use config file.")

        def decorator(func):
            service = MelaService(self, name)
            if producer_name:
                self.config['services'][name] = {}
                self.config['services'][name]['consumer'] = {**self.config['consumers'][name], **options_consumer}
                self.config['services'][name]['publisher'] = {**self.config['producers'][producer_name],
                                                              **options_publisher}
            service.configure(self.config['services'][name])
            service.set_processor(func)
            self.register(service)
            return service

        return decorator

    def register(self, other):
        if isinstance(other, MelaService):
            self.services[other.name] = other
        elif isinstance(other, MelaConsumer):
            self.consumers[other.name] = other
        elif isinstance(other, MelaPublisher):
            self.publishers[other.name] = other
        self._runnables.append(other)

    @property
    def connections(self):
        return self._connection_registry

    def run(self, coro=None):
        for runnable in self._runnables:
            self.loop.create_task(runnable.run())

        self.log.info("Running app...")

        if coro:
            self.loop.run_until_complete(coro)
        else:
            self.loop.run_forever()


class Connectable(Loggable):
    CONNECTION_MODE = 'read'

    def __init__(self, app, name):
        super().__init__(app, name)
        self.loop = app.loop
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None

    async def ensure_connection(self):
        if self.connection is None:
            try:
                self.connection = await self.app.connections.get(self.config.get('connection', "default"),
                                                                 mode=self.CONNECTION_MODE)
            except Exception as e:
                self.log.exception("Unhandled exception while connecting: ")

    async def ensure_channel(self):
        await self.ensure_connection()
        if self.channel is None:
            try:
                self.channel = await self.connection.channel()
            except Exception as e:
                self.log.warning("Error while creating channel")
                self.log.warning(e.__class__.__name__, e.args)
        elif self.channel.is_closed:
            await self.channel.reopen()


class ConnectionRegistry(Loggable):
    connection_lock = asyncio.Lock()

    def __init__(self, app, name):
        super().__init__(app, name)
        self._registry = {}
        self.loop = app.loop

    async def get(self, name, mode='read'):
        await self.connection_lock.acquire()
        connection_name = f"{name}_{mode}"
        if connection_name not in self._registry:
            self.log.debug(f"Making new connection {name} with mode {mode}")
            connection = await self.make_connection(name, mode)
            self._registry[connection_name] = connection
        self.connection_lock.release()
        return self._registry[connection_name]

    async def make_connection(self, name, mode):
        config = self.app.config['connections'].get(name, {})
        if 'url' not in config and 'host' not in config:
            raise KeyError(f"Connection {name} is not configured")
        if 'username' in config and 'login' not in config:
            config.setdefault('login', config['username'])
        connection: Optional[aio_pika.RobustConnection] = None
        while connection is None:
            try:
                connection = await aio_pika.connect_robust(
                    **config,
                    loop=self.loop,
                    client_properties={
                        'connection_name': f"{self.app.name}_{name}_{mode}"
                    }
                )
                # await connection.connected.wait()
            except ConnectionError:
                self.log.warning("Connection refused, trying again")
            except aiormq.exceptions.IncompatibleProtocolError:
                self.log.warning("aiormq.exceptions.IncompatibleProtocolError")
            except Exception as e:
                self.log.warning("Unhandled exception while connecting: %s" % e.__class__.__name__)
            finally:
                await asyncio.sleep(1)
        return connection


class MelaPublisher(Connectable):
    CONNECTION_MODE = "write"

    def __init__(self, app: Mela, name):
        self.name = name
        self.log = logging.getLogger(app.name + '.' + self.name)
        super(MelaPublisher, self).__init__(app, self.name)
        self.exchange: Optional[aio_pika.RobustExchange] = None
        self.queue = asyncio.Queue(maxsize=1)
        self.default_routing_key = None
        self.decode = self.decode
        self.publishing_params = {}
        self.permanent_publishing_options = {}
        self.permanent_publishing_options.setdefault('content_type', "application/json")
        self.permanent_publishing_options.setdefault('content_encoding', "UTF-8")

    def __call__(self, *args, **kwargs):
        """
        THIS METHOD WILL BE DEPRECATED IN v1.1. DO NOT USE IT.
        """
        self.loop.create_task(self.publish(*args, **kwargs))

    def on_config_update(self):
        super().on_config_update()
        self.config.setdefault('connection', "default")
        self.config.setdefault('skip_unroutables', False)
        if 'routing_key' not in self.config:
            raise KeyError(f"Routing key is not set for publisher {self.name}")
        self.default_routing_key = self.config['routing_key']
        if 'exchange' not in self.config:
            raise KeyError(f"Exchange is not set for publisher {self.name}")

    @staticmethod
    def decode(message):
        return bytes(json.dumps(message), encoding='utf-8')

    def set_decoder(self, func):
        self.decode = func

    async def ensure_exchange(self):
        await self.ensure_channel()
        try:
            self.exchange = await self.channel.declare_exchange(self.config['exchange'], durable=True)
        except Exception as e:
            self.log.warning("Error while declaring exchange")
            self.log.warning(e.__class__.__name__, e.args)

    async def publish(
            self,
            message: Union[dict, list, int, str, float],
            routing_key: Optional[str] = None,
            **options
    ):
        routing_key = routing_key or self.default_routing_key or ''
        options = {**self.permanent_publishing_options, **options}
        options.setdefault('content_type', "application/json")
        options.setdefault('content_encoding', "UTF-8")
        kwargs = {
            'message': aio_pika.Message(self.decode(message), **options),
            'routing_key': routing_key,
            **self.publishing_params
        }
        await self.queue.put(kwargs)

    async def prepare(self):
        self.log.info("preparing publisher")
        await self.ensure_connection()
        await self.ensure_exchange()

    async def publish_direct(
            self,
            message: Union[dict, list, int, str, float],
            routing_key: Optional[str] = None,
            **options
    ):
        self.log.info("Going to directly publish message")
        routing_key = routing_key or self.default_routing_key or ''
        options = {**self.permanent_publishing_options, **options}
        return await self.exchange.publish(
            aio_pika.Message(
                self.decode(message),
                **options
            ),
            routing_key=routing_key,
            **self.publishing_params
        )

    async def _publish(self, **kwargs):
        is_published = False
        resp = None
        while not is_published:
            try:
                resp: Optional[aiormq.types.ConfirmationFrameType, aiormq.types.DeliveredMessage] = \
                    await self.exchange.publish(**kwargs)
                is_published = isinstance(resp,
                                          aiormq.spec.Basic.Ack)  # or isinstance(resp, aiormq.types.DeliveredMessage)
                if not is_published:
                    error_message = f"Delivery response is {resp.delivery.__class__.__name__}." \
                                    f"It looks like message is unroutable"
                    if self.config['skip_unroutables']:
                        self.log.warning(error_message)
                        is_published = True
                    else:

                        raise TypeError(error_message)
            except aiormq.exceptions.ChannelInvalidStateError:
                await self.ensure_exchange()
            except Exception as e:
                self.log.exception("Unhandled exception: ")
        return resp

    async def wait(self):
        await self.queue.join()

    async def run(self):
        await self.ensure_connection()
        async with self.connection:
            await self.ensure_exchange()
            while True:
                message = await self.queue.get()
                self.log.debug(f"Got message should be published by {self.name}")
                await self._publish(**message)
                self.queue.task_done()


class MelaConsumer(Connectable):

    def __init__(self, app, name):
        super().__init__(app, name)
        self.connection = None
        self.connection_established = asyncio.Event()
        self.log = logging.Logger("MelaConsumer", level=logging.INFO)
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.exchange: Optional[aio_pika.RobustExchange] = None
        self.queue: Optional[aio_pika.RobustQueue] = None
        self.encode: Optional[Callable] = self.encode
        self.process: Optional[Callable] = self.process
        self.on_message_processed: Optional[Callable] = self.on_message_processed
        self.queue_iterator: Optional[aio_pika.queue.QueueIterator] = None

    def on_config_update(self):
        super().on_config_update()
        if 'queue' not in self.config:
            raise KeyError(f"No queue found in config for {self.name}")
        if 'exchange' not in self.config:
            raise KeyError(f"No exchange found in config for {self.name}")
        if 'routing_key' not in self.config:
            raise KeyError(f"No routing key found in config for {self.name}")
        self.config.setdefault('prefetch_count', 1)

    async def ensure_channel(self):
        await super().ensure_channel()
        await self.channel.set_qos(prefetch_count=self.config['prefetch_count'])

    async def ensure_exchange(self):
        if self.exchange is None:
            await self.ensure_channel()
            try:
                self.exchange = await self.channel.declare_exchange(self.config['exchange'], durable=True)
            except Exception as e:
                self.log.warning("Error while declaring exchange")
                self.log.warning(e.__class__.__name__, e.args)

    async def ensure_queue(self):
        if self.queue is None:
            await self.ensure_channel()
            try:
                args = {}
                if 'dead_letter_exchange' in self.config:
                    await self.channel.declare_exchange(self.config['dead_letter_exchange'], durable=True)
                    args['x-dead-letter-exchange'] = self.config['dead_letter_exchange']
                    if 'dead_letter_routing_key' in self.config and self.config['dead_letter_routing_key']:
                        args['x-dead-letter-routing-key'] = self.config['dead_letter_routing_key']
                self.queue = await self.channel.declare_queue(self.config['queue'], durable=True, arguments=args)
            except Exception as e:
                self.log.warning("Error while declaring queue")
                self.log.warning(e.__class__.__name__, e.args)

    async def ensure_binding(self):
        await self.ensure_exchange()
        await self.ensure_queue()
        try:
            await self.queue.bind(self.config['exchange'], routing_key=self.config['routing_key'])
        except Exception as e:
            self.log.warning("Error while declaring queue")
            self.log.warning(e.__class__.__name__, e.args)

    async def run(self):
        await self.ensure_connection()

        async with self.connection:
            await self.ensure_binding()

            async with self.queue.iterator() as queue_iter:
                self.queue_iterator = queue_iter
                async for message in queue_iter:  # type: aio_pika.IncomingMessage
                    async with message.process(ignore_processed=True):
                        body = self.encode(message.body)
                        if inspect.iscoroutinefunction(self.process):
                            resp = await self.process(body, message)
                        else:
                            resp = self.process(body, message)
                        await self.on_message_processed(resp, message)

    @staticmethod
    async def on_message_processed(response, message):
        pass

    @staticmethod
    def encode(message_body: bytes):
        return json.loads(message_body)

    @staticmethod
    async def process(body, message):
        raise NotImplementedError

    def set_processor(self, func):
        self.process = func

    def set_on_message_processed(self, func):
        self.on_message_processed = func

    def set_encoder(self, func):
        self.encode = func

    async def cancel(self):
        await self.queue_iterator.close()


class MelaService(Loggable):

    async def __response_processor_for_generator(self, response):
        for obj in response:
            await self.publish(obj)

    async def __response_processor_for_async_generator(self, response):
        async for obj in response:
            await self.publish(obj)

    async def __response_processor_for_function(self, response):
        await self.publish(response)

    def __init__(self, app, name):
        consumer_name = name + '_consumer'
        publisher_name = name + '_publisher'
        super().__init__(app, name)
        self.publisher = MelaPublisher(app, publisher_name)
        self.consumer = MelaConsumer(app, consumer_name)
        self.response_processor = None

    async def publish(self, message):
        if isinstance(message, tuple):
            await self.publisher.publish(message[0], **message[1])
        else:
            await self.publisher.publish(message)

    async def on_message_processed(self, response, message: aio_pika.IncomingMessage):
        if response is not None:
            await self.response_processor(response)
            await self.publisher.wait()
        if not message.processed:
            message.ack()

    def on_config_update(self):
        connection_name = self.config.get("connection", "default")
        if isinstance(self.config['publisher'], str):
            self.config['publisher'] = self.app.config['publishers'][self.config['publisher']]
        if isinstance(self.config['consumer'], str):
            self.config['consumer'] = self.app.config['consumers'][self.config['consumer']]
        self.config['publisher'].setdefault('connection', connection_name)
        self.config['consumer'].setdefault('connection', connection_name)
        self.publisher.configure(self.config['publisher'])
        self.consumer.configure(self.config['consumer'])
        self.consumer.set_on_message_processed(self.on_message_processed)

    def set_processor(self, func):
        self.consumer.set_processor(func)
        if inspect.isgeneratorfunction(self.consumer.process):
            self.response_processor = self.__response_processor_for_generator
        elif inspect.isasyncgenfunction(self.consumer.process):
            self.response_processor = self.__response_processor_for_async_generator
        else:
            self.response_processor = self.__response_processor_for_function

    async def run(self):
        self.log.info(f"Connecting {self.name} service")
        self.app.loop.create_task(self.publisher.run())
        self.app.loop.create_task(self.consumer.run())

    async def cancel(self):
        await self.consumer.cancel()
