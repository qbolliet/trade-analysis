"""Script de redressement BACI des flux de commerce international.

Lit les déclarations brutes COMTRADE depuis un catalogue DuckLake, applique la
méthodologie de reconstruction BACI du CEPII (conversion en tonnes, estimation et
retrait des coûts de fret, évaluation de la qualité des déclarants, réconciliation
pondérée des flux miroirs, réallocation des zones non spécifiées) et écrit les flux
réconciliés dans ce même catalogue.

La séparation des rôles est stricte : ``macroforecast.trade.processing.baci`` ne
contient que la méthodologie (dataframes, noms de colonnes et paramètres en
entrée) ; le présent script assume tout l'I/O — chargement de la configuration
YAML, construction de la ``BaciConfig``, lecture des fichiers Excel CEPII,
lecture de la table de faits COMTRADE et écriture du résultat. Tous les chemins
et identifiants proviennent de ``config/baci.yaml`` (jamais écrits en dur).

Source (COMTRADE) et résultat (BACI) vivent dans deux schémas d'un même
catalogue DuckLake adossé à Postgres, avec des données Parquet sur S3 — même
principe que ``compute_trade_vulnerabilities.py`` — d'où l'usage d'un unique
``DuckLakeConnector.from_postgres`` pour les deux lectures/écritures. Peut être
ordonnancé (Argo, cron) ou intégré comme nœud Kedro via la fonction exportée.
"""
# Importation des modules
# Modules de base
import logging
import os
import re
from dataclasses import fields, replace
from typing import Dict, Optional, Sequence
import yaml

# Modules de manipulation de données
import duckdb
import pandas as pd

# Module de gestion de la connexion à la base de données
from dt_ducklake_manager import (
    DatabaseUpdater,
    DuckLakeTablesBuilder,
)
# Module de chargement de données
from macroforecast.storage2 import Loader
from macroforecast.datasets.core.download import _schema_name

# Module d'implémentation du traitement BACI
from macroforecast.trade.processing import required_columns, run_baci
from macroforecast.trade.processing import BaciConfig, ComtradeSchema, DEFAULT_CONFIG
# Module de connexion à la base de données
from macroforecast.storage2 import DuckLakeConnector

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de la table de faits DuckLake (convention dt_ducklake_manager)
_FACT_TABLE = "fact_table"

# Clé YAML portant les conventions de schéma des sources (sous-section de PARAMETERS)
_SCHEMA_KEY = "SCHEMA"


# Fonction de chargement de la configuration associée à la base comtrade
def load_comtrade_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load configuration from file.

    Args:
        config_path: Path to config file. If None, uses default location or
            the BACI_CONFIG_PATH environment variable.

    Returns:
        dict: Configuration dictionary.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        # Priorité 1 : variable d'environnement (pour flexibilité Kubernetes)
        config_path = os.environ.get("COMTRADE_CONFIG_PATH", "config/datasets/comtrade.yaml")

    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de chargement de la configuration
def load_baci_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load configuration from file.

    Args:
        config_path: Path to config file. If None, uses default location or
            the BACI_CONFIG_PATH environment variable.

    Returns:
        dict: Configuration dictionary.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        # Priorité 1 : variable d'environnement (pour flexibilité Kubernetes)
        config_path = os.environ.get("BACI_CONFIG_PATH", "config/baci.yaml")

    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de lecture de la table de faits COMTRADE (schéma source du catalogue partagé)
def _read_comtrade_fact_table(
    conn: duckdb.DuckDBPyConnection,
    source_schema: str,
    columns: Sequence[str],
) -> pd.DataFrame:
    """Read selected columns of the COMTRADE fact table, read-only.

    Source and result live in two schemas of the same DuckLake catalog (cf.
    module docstring), so a plain schema-qualified ``SELECT`` on the shared
    connection is enough — no separate ``ATTACH`` is required.

    Args:
        conn: Open DuckLake connection (result-schema-bound connector).
        source_schema: Schema holding the COMTRADE ``fact_table``.
        columns: Columns to project.

    Returns:
        A pandas DataFrame of the projected fact table.
    """
    # Construction de la clause de projection
    col_list = ", ".join(f'"{c}"' for c in columns)
    return conn.execute(
        f"SELECT {col_list} FROM {source_schema}.{_FACT_TABLE}"
    ).df()


# Fonction de vérification de l'existence de la table de faits résultat
def _fact_table_exists(
    conn: duckdb.DuckDBPyConnection, catalog_alias: str, schema: str
) -> bool:
    """Return whether ``{schema}.fact_table`` exists in the attached catalog.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias of the attached catalog.
        schema: Target schema.

    Returns:
        ``True`` if the fact table already exists.
    """
    row = conn.execute(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        [catalog_alias, schema, _FACT_TABLE],
    ).fetchone()
    return bool(row and row[0] > 0)


# Fonction de création/mise à jour de la table de faits résultat
def _write_result(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    df_result: pd.DataFrame,
    primary_keys: Sequence[str],
    result_schema: str,
) -> bool:
    """Create or upsert the reconciled table into the result schema.

    Mirrors :func:`macroforecast.trade.vulnerabilities.runner._write_result`:
    builds the schema on first encounter, upserts by primary key afterwards.

    Args:
        conn: Open DuckLake connection (result-schema-bound connector).
        catalog_alias: Alias of the attached catalog.
        df_result: Reconciled flows to persist.
        primary_keys: Primary-key columns.
        result_schema: Target schema in the shared catalog.

    Returns:
        ``True`` if the schema was created, ``False`` if it was upserted.

    Raises:
        ValueError: If the update operation reports failure.
    """
    # Distinction création / mise à jour selon l'existence de la fact table
    # Mise à jour de la base de données
    if _fact_table_exists(conn, catalog_alias, result_schema):
        # Initialisation de l'updater
        updater = DatabaseUpdater(
            connection=conn, categorical_threshold=None, schema=result_schema
        )
        # Application de la mise à jour à la base de données
        success = updater.update_database(
            df_result, use_transaction=True, compact_after_update=True
        )
        # Vérification de la bonne exécution de l'opération
        if not success:
            raise ValueError("DatabaseUpdater reported failure for result table")

        # Logging
        logger.info("Upserted %d rows into '%s'", len(df_result), result_schema)

        return False
    
    # Première construction : métadonnées + fact table
    builder = DuckLakeTablesBuilder(
        df_result,
        categorical_threshold=None,
        primary_keys=list(primary_keys),
        connection=conn,
        schema=result_schema,
    )
    builder.build_schema()

    # Logging
    logger.info(
        "Created schema '%s' with %d rows (primary keys: %s)",
        result_schema, len(df_result), list(primary_keys),
    )
    return True


# Fonction de construction des conventions de schéma des sources
def comtrade_schema_from_params(params: Optional[Dict]) -> ComtradeSchema:
    """Build a ``ComtradeSchema`` from the YAML ``PARAMETERS.SCHEMA`` sub-section.

    Generic construction: every key matching a ``ComtradeSchema`` field name
    overrides the dataclass default; unknown keys are ignored with a warning.

    Args:
        params: The ``PARAMETERS.SCHEMA`` mapping of ``config/baci.yaml`` (or
            ``None``, meaning the default COMTRADE/CEPII conventions).

    Returns:
        A ``ComtradeSchema`` reflecting the configured overrides.
    """
    # Aucune surcharge : conventions de schéma par défaut
    if not params:
        return DEFAULT_CONFIG.schema

    # Surcharge générique champ à champ (noms de colonnes et codes de flux)
    valid = {f.name for f in fields(ComtradeSchema)}
    overrides: Dict[str, object] = {}
    for key, value in params.items():
        if key not in valid:
            logger.warning("Champ de schéma BACI inconnu ignoré : %s", key)
            continue
        overrides[key] = value

    return replace(DEFAULT_CONFIG.schema, **overrides)


# Fonction de construction de la configuration méthodologique BACI
def baci_config_from_params(params: Optional[Dict]) -> BaciConfig:
    """Build a ``BaciConfig`` from the YAML ``parameters`` section.

    Generic construction: every key matching a ``BaciConfig`` field name
    overrides the dataclass default; unknown keys are ignored with a warning.
    YAML lists are coerced to the tuple types expected by the frozen dataclass
    (including nested pairs such as ``excluded_pairs``). The nested ``SCHEMA``
    sub-section carries the source column conventions and is delegated to
    :func:`comtrade_schema_from_params`.

    Args:
        params: The ``parameters`` mapping of ``config/baci.yaml`` (or ``None``).

    Returns:
        A ``BaciConfig`` reflecting the configured overrides.
    """
    # Aucune surcharge : configuration par défaut
    if not params:
        return DEFAULT_CONFIG

    # Conventions de schéma issues de la sous-section dédiée
    schema = comtrade_schema_from_params(params.get(_SCHEMA_KEY))

    # Surcharge générique champ à champ, avec coercition listes → tuples
    valid = {f.name for f in fields(BaciConfig)} - {"schema"}
    overrides: Dict[str, object] = {}
    for key, value in params.items():
        # Sous-section de schéma déjà traitée
        if key == _SCHEMA_KEY:
            continue
        if key not in valid:
            logger.warning("Paramètre BACI inconnu ignoré : %s", key)
            continue
        default = getattr(DEFAULT_CONFIG, key)
        if isinstance(default, tuple) and isinstance(value, (list, tuple)):
            value = tuple(
                tuple(v) if isinstance(v, (list, tuple)) else v for v in value
            )
        overrides[key] = value

    return replace(DEFAULT_CONFIG, schema=schema, **overrides)


# Fonction principale de redressement BACI
def main() -> None:
    """CLI entry point for the BACI reconstruction script.

    Reads every parameter from the YAML configuration (``BACI_CONFIG_PATH`` or
    the default ``config/baci.yaml``); the "Areas NES" reallocation step can be
    disabled by setting ``apply_nes: false`` at the root of that file.
    """
    # Chargement des configurations (chemins, identifiants et paramètres méthodologiques)
    comtrade_config = load_comtrade_config()
    baci_config = load_baci_config()

    # Construction des parmaètres de modélisation
    baci_parameters_config = baci_config_from_params(baci_config.get("PARAMETERS"))

    # Spécification du dataflow téléchargé auquel on souhaite appliquer la méthodologie BACI
    DATAFLOW = "C_A_HS"

    # paths = config["paths"]
    # bucket = config["BUCKET"]
    # # Activation de l'étape de réallocation des zones "Areas NES"
    # apply_nes = config.get("apply_nes", True)

    # Lecture des fichiers Excel CEPII
    excel_loader = Loader()
    df_dist = excel_loader.load(
        filepath=baci_config['PATHS']["DIST_CEPII"],
        bucket=baci_config['BUCKET']
    )
    df_geo = excel_loader.load(
        filepath=baci_config['PATHS']["GEO_CEPII"], 
        bucket=baci_config['BUCKET']
    )

    # Initialisation du connecteur au catalogue
    connector = DuckLakeConnector.from_postgres(
        data_path=f"s3://{comtrade_config['DOWNLOADS'][DATAFLOW]['BUCKET']}/{comtrade_config['DOWNLOADS'][DATAFLOW]['PATHS']['DATA_PATH']}",
        dbname=comtrade_config["DOWNLOADS"]["DBNAME"],
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        admin_dbname=os.environ["PGDATABASE"],
        catalog_alias=comtrade_config["DOWNLOADS"]["CATALOG_ALIAS"],
        schema=_schema_name(baci_config["PATHS"]["RESULT_SCHEMA"]),
        s3_endpoint=os.environ["AWS_S3_ENDPOINT"],
        s3_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        s3_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        s3_session_token=os.environ["AWS_SESSION_TOKEN"],
    )

    # Etablissement d'une connexion
    conn = connector.connect()
    try:
        # Lecture de la table de faits COMTRADE (schéma source du même catalogue)
        df_comtrade = _read_comtrade_fact_table(
            conn=conn,
            source_schema=_schema_name(DATAFLOW),  # re.sub(r"[^a-zA-Z0-9]", "", DATAFLOW),
            columns=required_columns(baci_parameters_config)
        )

        # Application de la méthodologie sur les jeux de données chargés
        df_reconciled, report = run_baci(
            df_comtrade=df_comtrade,
            df_dist=df_dist,
            df_geo=df_geo,
            config=baci_parameters_config,
            apply_nes=(len(baci_config['PARAMETERS']["nes_partner_codes"]) > 0)
        )

        # Exportation des données pour test
        df_reconciled.to_excel("df_reconciled.xlsx", index=False)
    finally:
        conn.close()

    # Logging
    logger.info("Redressement BACI terminé : %s", report)


# Exécution du script principal
if __name__ == "__main__":
    main()