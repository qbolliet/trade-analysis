"""Kedro pipelines of the trade vulnerability project, one sub-package per block.

Each sub-package exposes ``create_pipeline()``: thin nodes calling the step
functions of :mod:`kedro_pipeline.steps`. The registry
(:mod:`kedro_pipeline.pipeline_registry`) sums them into ``__default__`` and
filters the two cadences, ``daily`` and ``weekly``, by tag.
"""
# Importation des modules
import warnings

# Noms de datasets pointés (« eurostat.comext », « state.baci »…) : convention du
# catalogue du projet, sans espace de noms Kedro ; l'avertissement émis à chaque nœud
# n'apporte donc rien
warnings.filterwarnings("ignore", message="One or more dataset names contain '.'")
