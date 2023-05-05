from aio_pika import IncomingMessage

import typing


class Processor:

    def __init__(
            self,
            callback: typing.Callable[[...], typing.Awaitable],
            deserializer: typing.Callable[[bytes], typing.Any] = None,
            serializer: typing.Callable[[typing.Any], bytes] = None,
    ):
        self._callback = callback

    async def process(self, message: IncomingMessage):
        deserialized_body = message.body.decode()

