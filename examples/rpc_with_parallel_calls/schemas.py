import typing

import pydantic


class Bot(pydantic.BaseModel):
    id: int
    username: str | None
    phone_number: str | None
    cookies: typing.List[dict] | None
    proxy_url: str | None
    status: str = 'ACTIVE'


class BotRequest(pydantic.BaseModel):
    id: int = pydantic.Field(alias="bot_id")


class BotSaveCookiesRequest(pydantic.BaseModel):
    id: int = pydantic.Field(alias="bot_id")
    cookies: typing.List[dict]
