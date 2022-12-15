import asyncio
import logging
import time

from aiormq.exceptions import ChannelInvalidStateError
from mela import Mela
from mela.components import RPCClient
from mela.components import NackMessageError
from pydantic import BaseModel

from botmanagerclient import BotManagerClient
from botmanagerclient import BotManagerClientTimeoutError
from botmanagerclient import BotNotFoundError
from mitmproxy_port_pool import Pool
from schemas import Bot

message_consumer = Mela(__name__)


class Task(BaseModel):
    id: int


mitmproxy_port_pool = Pool(2)


@message_consumer.consumer('feed_scroller')
async def consume(
    body: Task,
    bot_manager_client: RPCClient = 'botmanager',
):
    logging.info(f"Got task with id {body.id}")

    botmanager = BotManagerClient(
        rpc_client=bot_manager_client,
        request_timeout=2,
    )
    bot = await get_bot(bot_id=body.id, botmanager=botmanager)

    with await mitmproxy_port_pool.acquire() as port:
        logging.info(f"Port offset is {port.offset}")
        final_cookies = await process(bot)
        logging.debug("Going to save cookies")

    await botmanager.save_bot_cookies(bot_id=bot.id, cookies=final_cookies)
    logging.debug("Saved successfully")


async def get_bot(bot_id: int, botmanager: BotManagerClient):
    try:
        bot = await botmanager.get_bot_by_id(bot_id=bot_id)
        logging.debug("Account and cookies are extracted")
        return bot
    except BotManagerClientTimeoutError:
        raise NackMessageError("BotManager died due timeout", requeue=True)
    except BotNotFoundError:
        raise NackMessageError(f"Bot `{bot_id}` not found", requeue=False)
    except ChannelInvalidStateError:
        logging.exception("ChannelInvalidState")
        raise NackMessageError("Channel invalid state", requeue=True)


async def process(
    bot: Bot,
):
    logging.info(f"Got bot with id `{bot.id}`")

    for _ in range(20):
        logging.info(f"do something blocking for bot {bot.id}")
        time.sleep(10)
        logging.info(f"do something non-blocking for bot {bot.id}")
        await asyncio.sleep(10)

    return [{}]

if __name__ == '__main__':
    message_consumer.run()
