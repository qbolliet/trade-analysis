"""Kedro project settings for the trade vulnerability pipeline.

``CONF_SOURCE`` and ``CONFIG_LOADER_ARGS`` are also the single source of the
arguments of :func:`kedro_pipeline.config.load_parameters`, which reproduces the
Kedro configuration merge for the transitional scripts.
"""
# Importation des modules
from kedro.config import OmegaConfigLoader

from kedro_pipeline.config import resolve_test_root, resolve_tracking_env
from kedro_pipeline.hooks import TradeRunHooks
from kedro_pipeline.mlflow_hook import GuardedMlflowHook

# Source de configuration : dossier existant `config/` (et non `conf/`)
CONF_SOURCE = "config"

# Chargeur de configuration et environnements :
# - base : paramètres de production, versionnés ;
# - local (environnement par défaut) : surcharges de poste, non versionnées ;
# - cloud : surcharges d'exécution dans Argo ; demo : périmètre de présentation ;
# - test : exécution locale complète sur données simulées.
# Fusion « soft » des paramètres : un environnement ne surcharge que quelques clés
# d'un bloc (fusion récursive des mappings, remplacement des listes). Les noms
# d'expériences MLflow des scripts transitoires vivent hors des paramètres
# (`experiments*`).
# Résolveurs (Kedro réserve `oc.env` aux credentials : aucun secret ne transite par les
# paramètres) :
# - `trade.env` : seules variables d'environnement lisibles ailleurs, celles du suivi
#   MLflow lues par `mlflow.yml` (URI du serveur, expérience, identifiant du workflow),
#   posées dans les pods par le rendu Argo ;
# - `trade.test_root` : racine temporaire (TRADE_TEST_ROOT) de l'environnement test.
CONFIG_LOADER_CLASS = OmegaConfigLoader
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "merge_strategy": {"parameters": "soft"},
    "custom_resolvers": {"trade.env": resolve_tracking_env, "trade.test_root": resolve_test_root},
    "config_patterns": {
        "argo": ["argo*", "argo*/**"],
        "mlflow": ["mlflow*", "mlflow*/**"],
        "experiments": ["experiments*", "experiments*/**"],
    },
}

# Hooks projet. Ordre significatif : pluggy appelle d'abord le hook enregistré en
# dernier, si bien que le hook MLflow ouvre le run de la tâche avant que TradeRunHooks
# ne l'étiquette (TradeRunHooks.before_pipeline_run est en outre déclaré `trylast`)
HOOKS = (TradeRunHooks(), GuardedMlflowHook())

# Hooks de plugins désactivés :
# - kedro-mlflow : remplacé par GuardedMlflowHook (même comportement, mais aucune
#   erreur de suivi n'interrompt le calcul : serveur injoignable → avertissement) ;
# - argo-kedro : sans config/base/argo.yml, il valide une configuration vide (namespace,
#   machines, runner obligatoires) et fait échouer toute session ; à réactiver avec
#   l'ajout d'argo.yml. Noms de distribution tels que déclarés dans les métadonnées.
DISABLE_HOOKS_FOR_PLUGINS = ("kedro_mlflow", "argo-kedro")
