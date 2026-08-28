"""Script de redressement BACI multi-millésimes des flux de commerce international.

Variante de ``scripts/process_baci.py`` qui exécute la méthodologie BACI une
fois par millésime de nomenclature HS configuré (typiquement HS2022, HS2017,
HS2012) plutôt qu'une seule fois sur la nomenclature courante. Pour chaque
millésime cible ``V`` :

1. les déclarations COMTRADE d'année ``>= START_YEAR[V]`` sont sélectionnées ;
2. toutes les nomenclatures présentes dans cette tranche sont harmonisées vers
   ``V`` par ``macroforecast.trade.processing.classification.HsHarmonizer`` ;
3. la méthodologie BACI (``run_baci``) est appliquée à la tranche harmonisée ;
4. le résultat est écrit dans un schéma DuckLake dédié au millésime
   (``baci_hs2022``, ``baci_hs2017``, …) du même catalogue.

D'où la propriété visée : HS2022 ne porte que les années 2022 et suivantes,
tandis que HS2017 porte 2017 et suivantes, les données postérieures à 2022
étant reversées vers HS2017 par les tables de passage UNSD. Un schéma par
millésime plutôt qu'une colonne de millésime dans une table unique : les
millésimes se recouvrent (une même année figure dans plusieurs cibles), et les
mélanger inviterait au double compte.

Comme ``process_baci.py``, ce script assume tout l'I/O — chargement de la
configuration YAML, construction de la ``BaciConfig``, lecture des fichiers
Excel CEPII, lecture de la table de faits COMTRADE et écriture des résultats —
tandis que le package (``macroforecast.trade.processing``) ne contient que la
méthodologie. Les tables de correspondance HS sont téléchargées via
``UNSDClient`` puis mises en cache côté script (Parquet + registre JSON) :
elles sont invariantes une fois publiées, l'absence de fichier en cache est
donc le seul déclencheur de téléchargement (pas de vérification de fraîcheur
distante), sauf ``FORCE_REFRESH`` explicite en configuration.

L'échec d'un millésime n'interrompt pas les autres : chaque échec est capturé
et journalisé individuellement, et le script ne sort en erreur qu'en fin de
parcours si au moins un millésime a échoué.

Chaque millésime effectivement réécrit voit sa date de traitement consignée dans
le registre JSON ``PATHS.LAST_PROCESSING_PATH`` (même principe que le
``LAST_DOWNLOAD_PATH`` du téléchargement). C'est la seule chose que
``scripts/compute_network_vulnerabilities.py`` lit de ce script : le couplage
reste faible, aucun état en mémoire n'étant partagé.
"""
# Importation des modules
# Modules de base
import hashlib
import logging
import os
from pathlib import Path
from dataclasses import fields, replace
from datetime import datetime, timezone
from typing import Dict, Optional, Sequence, Set, Tuple
import yaml

# Modules de manipulation de données
from botocore.exceptions import ClientError
import duckdb
import pandas as pd

# Module de gestion de la connexion à la base de données
from dt_ducklake_manager import DuckLakeConnector
# Modules de chargement/sauvegarde de données (xls/parquet, puis json)
from macroforecast.storage2 import Loader as TableLoader, Saver as TableSaver
# Helpers DuckLake partagés (création puis upsert de la table de faits)
from macroforecast.storage2.tables import FACT_TABLE as _FACT_TABLE, write_dataframe
from macroforecast.storage import Loader as JsonLoader, Saver as JsonSaver
from macroforecast.datasets.core.download import _schema_name

# Module client des tables de correspondance de nomenclatures UNSD
from macroforecast.datasets import UNSDClient

# Module d'implémentation du traitement BACI
from macroforecast.trade.processing import required_columns, run_baci
from macroforecast.trade.processing import BaciConfig, ComtradeSchema, DEFAULT_CONFIG, BaciReport
from macroforecast.trade.processing import HsHarmonizer, resolve_vintage
# Module de suivi d'exécution (MLflow optionnel)
from macroforecast.tracking import get_tracker


# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé YAML portant les conventions de schéma des sources (sous-section de PARAMETERS)
_SCHEMA_KEY = "SCHEMA"

# Nom du fichier de registre des téléchargements de tables de correspondance
_REGISTRY_FILE = "unsd_correspondance_tables.json"

# Clé racine du registre JSON des dates de dernier traitement BACI, lu par
# scripts/compute_network_vulnerabilities.py pour ne recalculer que les
# millésimes réécrits depuis son dernier passage
_PROCESSING_ROOT = "BACI"


# ──────────────────────────────────────────────────────────────────────
# Plomberie dupliquée de scripts/process_baci.py (chargement config, lecture
# de la table de faits, écriture du résultat) : script autonome, sans
# dépendance croisée entre scripts.
# ──────────────────────────────────────────────────────────────────────

# Fonction de chargement de la configuration associée à la base comtrade
def load_comtrade_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load configuration from file.

    Args:
        config_path: Path to config file. If None, uses default location or
            the COMTRADE_CONFIG_PATH environment variable.

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


# ──────────────────────────────────────────────────────────────────────
# Cache des tables de correspondance HS (Parquet + registre JSON), logique
# propre à ce script : le package ne fait que convertir (HsHarmonizer), ni le
# téléchargement ni le cache n'y vivent.
# ──────────────────────────────────────────────────────────────────────

# Fonction de calcul d'une somme de contrôle du contenu d'une table
def _checksum(df_table: pd.DataFrame) -> str:
    """Compute a content hash of a table, to detect drift in a cached artefact.

    Args:
        df_table: Table to hash.

    Returns:
        Hex-encoded SHA-256 digest of the table's content.
    """
    hashed = pd.util.hash_pandas_object(df_table, index=False)
    return hashlib.sha256(hashed.to_numpy().tobytes()).hexdigest()


# Fonction de construction du chemin de cache d'une paire de millésimes
def _cached_table_path(concordance_path: str, source: str, target: str) -> str:
    """Build the Parquet cache path of a source/target vintage pair.

    Args:
        concordance_path: Root directory of the concordance cache.
        source: Source classification (e.g. ``"HS2022"``).
        target: Target classification (e.g. ``"HS2017"``).

    Returns:
        The Parquet cache path, e.g. ``"{concordance_path}/HS2022-HS2017.parquet"``.
    """
    return f"{concordance_path.rstrip('/')}/{source}-{target}.parquet"


# Fonction de construction du chemin du registre des téléchargements
def _registry_path(concordance_path: str) -> str:
    """Build the JSON registry path of the concordance cache.

    Args:
        concordance_path: Root directory of the concordance cache.

    Returns:
        The registry path, e.g. ``"{concordance_path}/registry.json"``.
    """
    return f"{concordance_path.rstrip('/')}/{_REGISTRY_FILE}"


# Fonction de lecture non bloquante d'une table mise en cache
def _load_cached_table(
    loader: TableLoader, path: str, bucket: Optional[str]
) -> Optional[pd.DataFrame]:
    """Read a cached Parquet table, or ``None`` when it is absent.

    Args:
        loader: Table loader (local or S3, dispatched on ``bucket``).
        path: Cache path (local path or S3 key).
        bucket: S3 bucket name, or ``None`` for a local cache.

    Returns:
        The cached table, or ``None`` when no cache file exists yet.
    """
    try:
        return loader.load(path, bucket=bucket)
    except (FileNotFoundError, ClientError):
        return None


# Fonction de chargement des tables de correspondance nécessaires, avec cache
def _ensure_concordances(
    pairs: Sequence[Tuple[str, str]],
    client: UNSDClient,
    loader: TableLoader,
    saver: TableSaver,
    concordance_path: str,
    bucket: Optional[str],
    force_refresh: bool = False,
) -> Dict[Tuple[str, str], pd.DataFrame]:
    """Load cached HS concordance tables, downloading only the missing ones.

    The correspondence tables are invariant once UNSD publishes them: the
    absence of a cached Parquet file is the only trigger for a download (no
    remote freshness check), unless ``force_refresh`` is set. One normalised
    table is cached per pair (``{concordance_path}/{source}-{target}.parquet``),
    alongside a JSON registry (``{concordance_path}/registry.json``) recording
    the download date, source URL, row count and content checksum of each pair
    actually downloaded — same principle as ``LAST_DOWNLOAD_PATH`` in
    ``download_comtrade.py``.

    Args:
        pairs: Distinct ``(source, target)`` vintage pairs to resolve (UNSD
            identifiers, e.g. ``("HS2022", "HS2017")``).
        client: UNSD correspondence-table client.
        loader: Table loader (local or S3) used to read cached Parquet tables.
        saver: Table saver (local or S3) used to write cached Parquet tables.
        concordance_path: Root directory of the concordance cache.
        bucket: S3 bucket name, or ``None`` for a local cache.
        force_refresh: When ``True``, re-download every pair even if already
            cached.

    Returns:
        Mapping ``(source, target) -> normalised concordance table``, one
        entry per requested pair.
    """
    # Registre des téléchargements (date, URL, volumétrie, somme de contrôle)
    registry_path = _registry_path(concordance_path)
    # Instances réutilisables : la connexion S3 paresseuse est ainsi établie une
    # seule fois et partagée par la lecture initiale et les écritures successives
    json_saver = JsonSaver()
    # Registre absent : premier téléchargement des tables de correspondance
    registry = JsonLoader().load(registry_path, bucket=bucket, missing_ok=True) or {}

    # Catalogue des tables déclarées (URL source de chaque paire)
    df_catalogue = client.list_available_tables().set_index(
        ["source_classification", "target_classification"]
    )

    concordances: Dict[Tuple[str, str], pd.DataFrame] = {}
    for source, target in pairs:
        key = f"{source}-{target}"
        table_path = _cached_table_path(concordance_path, source, target)

        # Cache existant : aucune vérification de fraîcheur distante
        df_table = None if force_refresh else _load_cached_table(loader, table_path, bucket)

        if df_table is None:
            # Logging
            logger.info("Téléchargement de la table de correspondance %s", key)
            df_table = client.get_correspondence(source, target, kind="conversion")
            saver.save(table_path, df_table, bucket=bucket, index=False)

            # Mise à jour du registre uniquement pour les paires téléchargées
            registry[key] = {
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "source_url": str(df_catalogue.loc[(source, target), "url"]),
                "n_rows": int(len(df_table)),
                "checksum": _checksum(df_table),
            }
            json_saver.save(
                registry_path, registry, bucket=bucket, indent=2, ensure_ascii=False
            )

        concordances[(source, target)] = df_table

    return concordances


# ──────────────────────────────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────────────────────────────

# Fonction principale de redressement BACI multi-millésimes
def main() -> None:
    """CLI entry point for the multi-vintage BACI reconstruction script.

    Reads every parameter from the YAML configuration (``BACI_CONFIG_PATH`` or
    the default ``config/baci.yaml``), including the HS vintages to reconstruct
    (``CLASSIFICATIONS.TARGETS``). Each target is processed independently: a
    failure on one vintage is logged and does not prevent the others from
    running, but the script exits with an error once every target has been
    attempted if at least one failed.

    Raises:
        RuntimeError: If at least one vintage failed, once every vintage has
            been attempted.
    """
    # Chargement des configurations (chemins, identifiants et paramètres méthodologiques)
    comtrade_config = load_comtrade_config()
    baci_config = load_baci_config()

    # Construction des paramètres de modélisation
    baci_parameters_config = baci_config_from_params(baci_config.get("PARAMETERS"))
    # Activation de l'étape de réallocation des zones "Areas NES" (clé racine,
    # distincte de PARAMETERS.nes_partner_codes qui ne fait que déclarer les
    # codes éligibles)
    baci_parameters_config = replace(
        baci_parameters_config, apply_nes=bool(baci_config.get("APPLY_NES", True))
    )
    schema = baci_parameters_config.schema

    # Configuration du suivi d'exécution : sans URI (ou sans MLflow installé,
    # ou serveur injoignable), get_tracker retourne un tracker inerte et
    # l'exécution est strictement inchangée
    mlflow_config = baci_config.get("MLFLOW") or {}
    log_artifacts = bool(mlflow_config.get("LOG_ARTIFACTS", True))

    # Configuration des millésimes cibles et du cache de correspondance
    classifications_config = baci_config["CLASSIFICATIONS"]
    targets_config: Dict[str, Dict] = classifications_config["TARGETS"]
    concordance_path = classifications_config["CONCORDANCE_PATH"]
    force_refresh = classifications_config.get("FORCE_REFRESH", False)
    bucket = baci_config["BUCKET"]

    # Spécification du dataflow téléchargé auquel on souhaite appliquer la méthodologie BACI
    DATAFLOW = "C_A_HS"

    # Instant de référence capturé avant le traitement : la date consignée
    # correspond au début du redressement, jamais à sa fin, pour ne pas masquer
    # une mise à jour COMTRADE survenue pendant l'exécution
    processed_at = datetime.now(timezone.utc)

    # Lecture des fichiers Excel CEPII
    table_loader = TableLoader()
    table_saver = TableSaver()
    df_dist = table_loader.load(baci_config['PATHS']["DIST_CEPII"], bucket=bucket)
    df_geo = table_loader.load(baci_config['PATHS']["GEO_CEPII"], bucket=bucket)

    # Initialisation du connecteur au catalogue (schéma par défaut : la source,
    # les schémas résultat étant adressés explicitement à l'écriture)
    connector = DuckLakeConnector.from_postgres(
        data_path=f"s3://{comtrade_config['DOWNLOADS'][DATAFLOW]['BUCKET']}/{comtrade_config['DOWNLOADS'][DATAFLOW]['PATHS']['DATA_PATH']}",
        dbname=comtrade_config["DOWNLOADS"]["DBNAME"],
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        admin_dbname=os.environ["PGDATABASE"],
        catalog_alias=comtrade_config["DOWNLOADS"]["CATALOG_ALIAS"],
        schema=_schema_name(DATAFLOW),
        s3_endpoint=os.environ["AWS_S3_ENDPOINT"],
        s3_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        s3_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        s3_session_token=os.environ["AWS_SESSION_TOKEN"],
    )

    # Etablissement d'une connexion
    conn = connector.connect()
    try:
        # Lecture unique de la table de faits COMTRADE, colonnes requises par
        # run_baci plus la colonne de classification (absente de
        # required_columns, indispensable à l'harmonisation des nomenclatures)
        columns = list(
            dict.fromkeys(required_columns(baci_parameters_config) + [schema.classification_col])
        )
        df_comtrade = _read_comtrade_fact_table(
            conn=conn,
            source_schema=_schema_name(DATAFLOW),
            columns=columns,
        )
        years = df_comtrade[schema.period_col].astype(str).str[:4].astype(int)

        # Passe 1 : tranche temporelle par millésime cible et paires de
        # correspondance nécessaires (millésimes présents dans chaque tranche,
        # hors le millésime cible lui-même)
        slices: Dict[str, pd.DataFrame] = {}
        pairs: Set[Tuple[str, str]] = set()
        for label, target_cfg in targets_config.items():
            df_slice = df_comtrade[years >= target_cfg["START_YEAR"]]
            slices[label] = df_slice
            codes_present = df_slice[schema.classification_col].dropna().unique()
            for code in codes_present:
                source_label = f"HS{resolve_vintage(code)}"
                if source_label != label:
                    pairs.add((source_label, label))

        # Résolution des tables de correspondance nécessaires (cache Parquet)
        client = UNSDClient()
        try:
            concordances = _ensure_concordances(
                sorted(pairs),
                client=client,
                loader=table_loader,
                saver=table_saver,
                concordance_path=concordance_path,
                bucket=bucket,
                force_refresh=force_refresh,
            )
        finally:
            client.close()

        # Passe 2 : harmonisation puis redressement BACI, par millésime cible.
        # L'échec d'un millésime n'interrompt pas les autres.
        reports: Dict[str, BaciReport] = {}
        failures: Dict[str, Exception] = {}
        # Entrées de registre des millésimes effectivement réécrits
        processed: Dict[str, Dict[str, object]] = {}
        for label, target_cfg in targets_config.items():
            try:
                harmonizer = HsHarmonizer(
                    concordances,
                    target_vintage=label,
                    classification_col=schema.classification_col,
                    product_col=schema.product_col,
                    period_col=schema.period_col,
                    value_cols=(schema.value_col, schema.cif_value_col, schema.fob_value_col),
                    weight_cols=(schema.netwgt_col,),
                    qty_col=schema.qty_col,
                    qty_unit_col=schema.qty_unit_col,
                )
                df_harmonised = harmonizer.fit_transform(slices[label])

                # Un run par millésime : l'échec de l'un n'emporte pas les autres
                tracker = get_tracker(
                    tracking_uri=mlflow_config.get("TRACKING_URI"),
                    experiment=mlflow_config.get("EXPERIMENT", "baci"),
                    run_name=f"baci-{label}-{datetime.now():%Y%m%d-%H%M}",
                    tags={"vintage": label},
                )
                with tracker:
                    # Application de la méthodologie sur la tranche harmonisée
                    df_reconciled, report = run_baci(
                        df_comtrade=df_harmonised,
                        df_dist=df_dist,
                        df_geo=df_geo,
                        config=baci_parameters_config,
                        tracker=tracker,
                        log_artifacts=log_artifacts,
                    )

                    # Écriture du résultat dans le schéma dédié au millésime
                    report.created = write_dataframe(
                        conn,
                        df_reconciled,
                        baci_parameters_config.primary_keys,
                        catalog_alias=connector.catalog_alias,
                        schema=_schema_name(target_cfg["RESULT_SCHEMA"]),
                        label=label,
                    )
                    reports[label] = report
                    # Traçage du millésime réécrit, indexé par son schéma
                    # résultat — identité non ambiguë de ce qui a été produit
                    result_schema = _schema_name(target_cfg["RESULT_SCHEMA"])
                    processed[result_schema] = {
                        "vintage": label,
                        "result_schema": result_schema,
                        "last_processed": processed_at.isoformat(),
                        "n_rows": int(len(df_reconciled)),
                    }

                    # Envoi des métriques du redressement et de l'harmonisation
                    tracker.log_metrics(report.to_metrics())
                    tracker.log_metrics(harmonizer.report_.to_metrics())
                    tracker.set_tags(
                        {
                            "result_schema": _schema_name(target_cfg["RESULT_SCHEMA"]),
                            "created": str(report.created),
                        }
                    )
                    # Répartition des relations de nomenclature : mesure de la
                    # perte d'information à la conversion
                    if log_artifacts:
                        tracker.log_dict(
                            harmonizer.report_.relationship_distribution,
                            "classification/relationship_distribution.json",
                        )

                # Logging
                logger.info("Redressement BACI terminé pour %s : %s", label, report)
            except Exception as exc:
                # Journalisation de l'échec, poursuite avec les autres millésimes
                logger.exception("Échec du redressement BACI pour le millésime %s", label)
                failures[label] = exc
    finally:
        conn.close()

    # Registre des dates de traitement : écrit après succès de l'écriture des
    # millésimes concernés, jamais avant — une date avancée à tort ferait
    # silencieusement sauter le recalcul des vulnérabilités de réseau
    if processed:
        last_processing_path = Path(baci_config["PATHS"]["LAST_PROCESSING_PATH"])
        # Fusion avec le registre existant : seuls les millésimes traités bougent
        registry = (
            JsonLoader().load(last_processing_path, bucket=bucket, missing_ok=True) or {}
        ).get(_PROCESSING_ROOT, {})
        registry.update(processed)
        # Écriture du registre mis à jour
        JsonSaver().save(
            last_processing_path,
            {_PROCESSING_ROOT: registry},
            bucket=bucket,
            indent=2,
            ensure_ascii=False,
        )
        # Logging
        logger.info(
            "%d millésime(s) consigné(s) dans le registre de traitement '%s'",
            len(processed),
            baci_config["PATHS"]["LAST_PROCESSING_PATH"],
        )

    # Échec global si au moins un millésime a échoué, une fois tous tentés
    if failures:
        raise RuntimeError(
            f"{len(failures)} millésime(s) en échec sur {len(targets_config)} : "
            f"{sorted(failures)}"
        ) from next(iter(failures.values()))


# Exécution du script principal
if __name__ == "__main__":
    main()
