from typing import Any
from typing import Dict

from .consumer import consumer
from .publisher import publisher
from .service import service


factory_dict: Dict[str, Any] = {
    'consumer': consumer,
    'publisher': publisher,
    'service': service,
}

__all__ = ['consumer', 'publisher', 'service', 'factory_dict']
