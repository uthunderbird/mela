from typing import Union, Dict, Any

from pydantic import Field, Extra
from yarl import URL

from mela.component import BaseModelWithLogLevel


class URLConnectionConfiguration(BaseModelWithLogLevel):
    url: URL
    timeout: Union[float, int] = Field(120, alias='connTimeout')  # DEPRECATED alias
    client_properties: Dict = {}
    heartbeat: int = None

    class Config:
        extra = Extra.forbid
        arbitrary_types_allowed = True


class HostConnectionConfiguration(BaseModelWithLogLevel):
    host: str
    port: int = 5672
    login: str = Field(alias='username')
    password: str
    virtualhost: str = '/'
    ssl: bool = False
    ssl_options = dict
    timeout: Union[float, int] = Field(120, alias='connTimeout')  # DEPRECATED alias
    client_properties: Dict = dict
    heartbeat: int = None

    class Config:
        extra = Extra.forbid


from aio_pika import RobustConnection


class MelaConnection(RobustConnection):

    def __init__(self, url: URL, **kwargs: Any):
        super().__init__(url, **kwargs)
