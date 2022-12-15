import asyncio
import logging
import typing

import aio_pika

from mela import Mela

from schemas import Bot

app = Mela(__name__)


async def fetch(url):
    # let's pretend we're asynchronously fetching url here and return its body
    await asyncio.sleep(1)
    return url


@app.rpc_service("fetcher")
async def fetcher(url: str):
    return {"fetched": await fetch(url)}


bots = {1: Bot(id=1, username='Biba'), 2: Bot(id=2, username='Boba')}


def create_bot(bot_id, bot_username):
    bots[bot_id] = Bot(id=bot_id, username=bot_username)


def get_bot(bot_id: int) -> Bot:
    return bots[bot_id]


def save_bot_cookies(bot_id: int, cookies: typing.List[dict]):
    bot: Bot = get_bot(bot_id)
    bot.cookies = cookies


@app.rpc_service("botmanager")
async def botmanager(request, message: aio_pika.IncomingMessage):
    logging.info(f"Got message: {request}")
    if message.headers['request'] == 'get_bot_by_id':
        bot: Bot = get_bot(request['bot_id'])
        resp = {'result': bot.dict(), 'status': "OK"}
    elif message.headers['request'] == 'save_bot_cookies':
        save_bot_cookies(request['bot_id'], request['cookies'])
        resp = {'result': None, 'status': "OK"}
    else:
        resp = {'result': None, 'status': "ERROR_UNKNOWN_METHOD"}
    logging.info(f"Response is {resp}")
    return resp

if __name__ == '__main__':
    app.run()
