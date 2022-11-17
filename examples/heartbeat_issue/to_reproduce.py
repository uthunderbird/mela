import asyncio
import logging

import aio_pika

logging.getLogger().setLevel('INFO')


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://admin:admin@127.0.0.1/",
    )

    async with connection:
        routing_key = "botmanager-q"

        channel = await connection.channel()

        while True:
            await asyncio.sleep(3)
            logging.info(f"channel.is_initialized: {channel.is_initialized}")
            logging.info(f"channel.is_closed: {channel.is_closed}")
            # The script fails if below code is commented
            # while channel.is_closed:
            #     logging.info("Waiting channel restores")
            #     await asyncio.sleep(0.01)
            await channel.default_exchange.publish(
                aio_pika.Message(body=f"Hello {routing_key}".encode()),
                routing_key=routing_key,
            )
            logging.info("Published")


if __name__ == "__main__":
    asyncio.run(main())
