"""Eurostat SDMX client package.

Re-exports the public Eurostat API so that
``from ..sources.eurostat import EurostatClient`` (and the higher-level
``macroforecast.datasets`` re-exports) keep working after the split into
submodules.
"""
# Importation des éléments d'intérêt du sous-package
from .formats import EurostatResponseFormat
from .endpoints import (
    EurostatEndpointBuilderV30,
    EurostatEndpointBuilderV21,
)
from .queries import (
    EurostatQueryRequest,
    EurostatQueryRequestV30,
    EurostatQueryRequestV21,
)
from .client import EurostatClient

# Réexport des éléments d'intérêt du sous-package
__all__ = [
    "EurostatResponseFormat",
    "EurostatEndpointBuilderV30",
    "EurostatEndpointBuilderV21",
    "EurostatQueryRequest",
    "EurostatQueryRequestV30",
    "EurostatQueryRequestV21",
    "EurostatClient",
]
