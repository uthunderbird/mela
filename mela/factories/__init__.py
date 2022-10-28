from typing import Any
from typing import Dict

from .consumer import consumer
from .publisher import publisher
from .rpc import client as rpc_client
from .rpc import service as rpc_service
from .service import service


factory_dict: Dict[str, Any] = {
    'consumer': consumer,
    'publisher': publisher,
    'service': service,
    'rpc_service': rpc_service,
    'rpc_client': rpc_client,
}

__all__ = ['consumer', 'publisher', 'service', 'rpc_service', 'rpc_client', 'factory_dict']
