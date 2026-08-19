"""UN Comtrade client package.

Re-exports the public Comtrade API so that ``from ..sources.comtrade import
ComtradeClient`` (and the higher-level ``macroforecast.datasets`` re-exports)
work consistently with the SDMX provider packages.
"""
# Importation des éléments d'intérêt du sous-package
from .formats import ComtradeResponseFormat
from .queries import ComtradeQueryRequest
from .client import ComtradeClient

# Réexport des éléments d'intérêt du sous-package
__all__ = [
    "ComtradeResponseFormat",
    "ComtradeQueryRequest",
    "ComtradeClient",
]
