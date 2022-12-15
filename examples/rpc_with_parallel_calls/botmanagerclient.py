import asyncio

import mela

from examples.rpc_with_parallel_calls.schemas import Bot


class BotManagerClientTimeoutError(Exception):
    """RPC timeout error
    Raises whenever RPC response delay was held
    for too long.
    RPC timeout might be set up in .env file.
    """


class BotNotFoundError(Exception):
    """Bot was not found
    Raises whenever Bot Manager Reader responses
    with no bot"""


class BotManagerClient:
    def __init__(
        self,
        *,
        rpc_client: mela.components.rpc.RPCClient,
        request_timeout: int | None = None,
    ):
        self._rpc_client = rpc_client
        self._request_timeout = request_timeout

    async def _call_rpc(self, body, headers):
        try:
            result = await asyncio.wait_for(
                self._rpc_client.call(body=body, headers=headers),
                timeout=self._request_timeout,
            )

            return result['result']
        except asyncio.TimeoutError as e:
            raise BotManagerClientTimeoutError from e

    async def get_bot_by_id(self, *, bot_id: int) -> Bot:
        result = await self._call_rpc(
            body={'bot_id': bot_id},
            headers={'request': 'get_bot_by_id'},
        )

        if result is None:
            raise BotNotFoundError

        return Bot(**result)

    async def save_bot_cookies(self, *, bot_id: int, cookies: list[dict]):
        await self._call_rpc(
            body={
                'bot_id': bot_id,
                'cookies': cookies,
            },
            headers={'request': 'save_bot_cookies'},
        )
