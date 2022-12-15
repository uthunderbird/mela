import asyncio

from mela import Mela
from aio_pika import IncomingMessage
from mela.components.rpc import RPCClient
from pydantic import BaseModel

app = Mela(__name__)


class Document(BaseModel):
    id: int


@app.consumer("printer")
async def printer(message: IncomingMessage, body: Document, rpc_client: RPCClient = 'fetcher'):
    print(body)
    resp = await rpc_client.call(body)
    print(resp)
    await asyncio.sleep(900)
    print("finished")
    new_resp = await rpc_client.call(body)
    print(new_resp)
    await message.ack()


@app.rpc_service('fetcher-service')
async def fetcher(id: int):
    await asyncio.sleep(1)
    return {"status": "OK", 'id': id}

if __name__ == '__main__':
    app.run()
