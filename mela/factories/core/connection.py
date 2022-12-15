from typing import Dict
from typing import Literal
from typing import Union

from aio_pika import connect_robust
from aio_pika.abc import AbstractConnection

from ...settings import AbstractConnectionParams


connections: Dict[str, AbstractConnection] = {}


def generate_anonymous_connection_name(component_name: str) -> str:
    # TODO
    name = component_name
    return name


async def close_all_connections():
    for connection_name, connection in connections.items():
        await connection.close()


async def connect(
    component_name: str,
    connection_settings: Union[AbstractConnectionParams],
    mode: Literal['r', 'w'],
) -> AbstractConnection:
    if connection_settings.name is None:
        connection_settings.name = generate_anonymous_connection_name(component_name)
    full_connection_name = component_name + '_' + mode
    connection_settings.name = full_connection_name
    if full_connection_name not in connections:
        params = connection_settings.get_params_dict()
        connection = await connect_robust(**params)
        connections[full_connection_name] = connection
    return connections[full_connection_name]
