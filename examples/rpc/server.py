import asyncio
import aio_pika

from mela import Mela

app = Mela(__name__)


async def fetch(url):
    # let's pretend we're asynchronously fetching url here and return its body
    await asyncio.sleep(1)
    return url


@app.rpc_service("fetcher")
async def fetcher(link, message):
    return await fetch(link)


bots = {}


def create_bot(bot_id, bot_username, bot_password):
    bots[bot_id] = {'username': bot_username, 'password': bot_password}


def get_bot(bot_id):
    return bots[bot_id]


@app.rpc_server("bot_manager")
async def fetcher(body, message: aio_pika.Message):
    if message.headers['method'] == 'create_bot':
        create_bot(**body)
        return {'result': None, 'status': "OK"}
    elif message.headers['method'] == 'get_bot':
        return {'result': get_bot(**body), 'status': "OK"}
    else:
        return {'result': None, 'status': "ERROR_UNKNOWN_METHOD"}

if __name__ == '__main__':
    app.run()
