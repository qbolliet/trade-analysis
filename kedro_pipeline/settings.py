"""Kedro project settings for the trade vulnerability pipeline.

``CONF_SOURCE`` and ``CONFIG_LOADER_ARGS`` are also the single source of the
arguments of :func:`kedro_pipeline.config.load_parameters`, which reproduces the
Kedro configuration merge for the transitional scripts.
"""
# Importation des modules
from kedro.config import OmegaConfigLoader

from kedro_pipeline.hooks import TradeRunHooks

# Source de configuration : dossier existant `config/` (et non `conf/`)
CONF_SOURCE = "config"

# Chargeur de configuration et environnements :
# - base : paramètres de production, versionnés ;
# - local (environnement par défaut) : surcharges de poste, non versionnées ;
# - cloud : surcharges d'exécution dans Argo ; demo : périmètre de présentation.
# Fusion « soft » des paramètres : un environnement ne surcharge que quelques clés
# d'un bloc (fusion récursive des mappings, remplacement des listes). Les noms
# d'expériences MLflow des scripts transitoires vivent hors des paramètres
# (`experiments*`), en attendant la configuration de kedro-mlflow.
CONFIG_LOADER_CLASS = OmegaConfigLoader
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "merge_strategy": {"parameters": "soft"},
    "config_patterns": {
        "argo": ["argo*", "argo*/**"],
        "mlflow": ["mlflow*", "mlflow*/**"],
        "experiments": ["experiments*", "experiments*/**"],
    },
}

# Hooks projet : les hooks kedro-mlflow et argo-kedro sont auto-enregistrés par entry points
HOOKS = (TradeRunHooks(),)

# Hooks de plugins désactivés tant que leur fichier de configuration n'existe pas, car
# chacun fait échouer la création de toute session Kedro sans lui :
# - kedro-mlflow sans config/base/mlflow.yml configure un magasin fichier `./mlruns`, que
#   MLflow 3 refuse (MlflowException) ;
# - argo-kedro sans config/base/argo.yml valide une configuration vide (namespace,
#   machines, runner obligatoires).
# À retirer avec l'ajout de mlflow.yml et d'argo.yml. Noms de distribution tels que
# déclarés dans les métadonnées des paquets.
DISABLE_HOOKS_FOR_PLUGINS = ("kedro_mlflow", "argo-kedro")
