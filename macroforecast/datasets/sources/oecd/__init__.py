"""OECD SDMX client package.

Re-exports the public OECD API so that ``from ..sources.oecd import OECDClient``
(and the higher-level ``macroforecast.datasets`` re-exports) keep working
after the split into submodules.
"""
# Importation des éléments d'intérêt du sous-package
from .formats import OECDResponseFormat
from .endpoints import (
    OECDEndpointBuilder,
    OECDEndpointBuilderV1,
    OECDEndpointBuilderV2,
)
from .queries import OECDQueryRequest
from .client import OECDClient

# Réexport des éléments d'intérêt du sous-package
__all__ = [
    "OECDResponseFormat",
    "OECDEndpointBuilder",
    "OECDEndpointBuilderV1",
    "OECDEndpointBuilderV2",
    "OECDQueryRequest",
    "OECDClient",
]
