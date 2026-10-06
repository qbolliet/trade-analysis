"""Script de calcul/mise à jour des scores synthétiques de vulnérabilité.

Combine les deux familles d'indicateurs déjà calculées — indices partenaires
(`vulnerabilities.indicators`, produits par `compute_trade_vulnerabilities.py`)
et indices de réseau (`vulnerabilities.network_indicators`, produits par
`compute_network_vulnerabilities.py`) — en une table de scores synthétiques :
une ligne par cellule `reporter x product` d'un contexte
`(freq, flow, indicators, TIME_PERIOD)` et par méthode d'agrégation, avec trois
paires `(score, rang)`, une par niveau de comparaison (`by_product`,
`by_reporter`, `global`, cf. D-09). La méthodologie multicritère est portée par
`macroforecast.trade.aggregation.run_synthesis`, fonction pure : ce script fait
tout l'I/O (lecture DuckLake des sources, jointure SQL, écriture des schémas
résultat, registre de fraîcheur, suivi MLflow).

Place dans le pipeline (ordre Argo) : après
`compute_trade_vulnerabilities.py` et `compute_network_vulnerabilities.py`
(dépendances directes, couplage faible : seuls leurs registres JSON de fraîcheur
sont lus), avant `compute_synthesis_coherence.py`. Deux schémas résultat dans le
catalogue `vulnerabilities` : `synthesis` pour les scores,
`synthesis_diagnostics` pour les diagnostics d'ajustement de la famille `fit`
(même table longue que le script de cohérence).

Fraîcheur. Les registres amont des vulnérabilités partenaires et de
réseau sont indexés par unité de travail sans période : on ne peut pas savoir
quelles périodes ont bougé. L'unité de fraîcheur de la synthèse est donc unique
(`global`) : tous les contextes sélectionnés par `FILTERS` sont recalculés, ou
aucun. Ils le sont quand la synthèse n'a jamais tourné, quand un calcul amont est
postérieur au watermark enregistré, quand l'empreinte globale (liste complète des
méthodes, configuration, sources et filtres) change, ou en cas de forçage
(`FORCE` historique du YAML ou `runtime.FORCE_STEPS=synthesis`). Le registre
(fragment unique au chemin `PATHS.LAST_COMPUTATION_PATH`) consigne aussi les
raisons des unités amont recalculées depuis (cascade). La date écrite est
capturée avant le calcul, jamais après, pour ne pas rater une mise à jour
concurrente.

Erreurs. Comme `compute_network_vulnerabilities.py` : l'échec d'un contexte
n'interrompt pas les autres, chaque échec est capturé et journalisé, seuls les
contextes réussis sont écrits, et le script ne sort en erreur qu'en fin de
parcours.

Le suivi d'exécution MLflow est piloté par le bloc `SYNTHESIS.MLFLOW` de
`config/synthesis.yaml` : sans `TRACKING_URI` (ou sans serveur joignable),
`get_tracker` retourne un objet nul et l'exécution est strictement inchangée. Un
seul run par exécution.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import fields, replace
from datetime import datetime
from contextlib import nullcontext
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple
import yaml

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.storage.json import Loader, Saver
# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    DuckLakeLocation,
    build_connector,
    compute_write_options,
    pg_credentials_from_env,
    s3_credentials_from_env,
)
# Module d'écriture des tables de faits DuckLake (upsert par clé primaire)
from statflows.storage.ducklake.tables import FACT_TABLE, write_dataframe
# Module d'utilitaires de téléchargement (instants, parsing ISO, noms de schéma)
from statflows.core.download import _now, _parse_iso, _schema_name
# Registres de fraîcheur v2 (unité globale, empreinte, forçage, cascade)
from kedro_pipeline.io.freshness import (
    ForceSpec,
    FreshnessRegistry,
    LegacySource,
    RegistryEntry,
    Unit,
    UnitPlan,
    UpstreamSummary,
    adopt_legacy_flag,
    fingerprint,
    legacy_entry,
    plan_metrics,
    summarize_upstream,
    units_to_compute,
)
# Registres amont (partenaires et réseau), lus seulement
from scripts.compute_trade_vulnerabilities import (
    load_flows,
    partner_classification,
    partner_registry,
    vulnerability_config_from_params,
)
from scripts.compute_network_vulnerabilities import network_registry
# Paramètres d'exécution partagés (nomenclatures, forçage ponctuel)
from scripts.download_comtrade import load_runtime_config

# Module de manipulation de données
import pandas as pd

# Module de suivi d'exécution (MLflow optionnel, objet nul par défaut)
from macroforecast.tracking import CapturingTracker, get_tracker, rekey_metrics
from macroforecast.tracking.figures import key_figures_synthesis, sections_synthesis
from macroforecast.tracking.report import Units
from scripts._run_report import RunScope, guarded_run, run_name
# Méthodologie de synthèse multiniveau (fonction pure)
from macroforecast.trade.methodology import methodology_params
from macroforecast.trade.vulnerabilities import flow_code_map
from macroforecast.trade.aggregation import (
    LevelReport,
    SynthesisConfig,
    SynthesisReport,
    method_spec_from_mapping,
    run_synthesis,
)

# Configuration de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)
# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé racine du registre JSON des dates de dernier calcul de synthèse, tenu ici
_REGISTRY_ROOT = "SYNTHESIS"
# Clé racine du registre des vulnérabilités partenaires (cf. compute_trade_vulnerabilities.py)
_PARTNERS_ROOT = "VULNERABILITIES"
# Clé racine du registre des vulnérabilités de réseau (cf. compute_network_vulnerabilities.py)
_NETWORK_ROOT = "NETWORK_VULNERABILITIES"
# Colonne temporelle de la grille partenaires (clé de contexte) sur laquelle
# porte `FILTERS.LAST_N_PERIODS` — fait de schéma source, pas un paramètre
# méthodologique
_PERIOD_COLUMN = "TIME_PERIOD"
# Colonne du flux de la grille partenaires (clé de contexte) sur laquelle porte
# le prédicat généré depuis `FLOWS` — fait de schéma source
_FLOW_COLUMN = "flow"


# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────

# Fonction de chargement de la configuration de synthèse
def load_synthesis_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the synthesis configuration from file.

    Args:
        config_path: Path to config file. If ``None``, uses the
            ``SYNTHESIS_CONFIG_PATH`` environment variable, then the default
            ``config/synthesis.yaml``.

    Returns:
        dict: Configuration dictionary (blocks ``SYNTHESIS`` and ``COHERENCE``).
    """
    # Détermination du chemin de configuration
    if config_path is None:
        config_path = os.environ.get(
            "SYNTHESIS_CONFIG_PATH", "config/synthesis.yaml"
        )
    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de chargement de la configuration dédiée au calcul des vulnérabilités
def load_vulnerability_config(config_path: Optional[os.PathLike] = None) -> dict:
    """Load the vulnerability-computation configuration from file.

    Read for three things only: the catalog identity shared by the two upstream
    families (``VULNERABILITIES.DBNAME`` / ``VULNERABILITIES.CATALOG_ALIAS``, the
    same catalog the synthesis writes into), and the paths of the two upstream
    freshness registries (partners and network). No methodological parameter of
    the metric computation is used here.

    Args:
        config_path: Path to config file. If ``None``, uses the
            ``VULNERABILITIES_CONFIG_PATH`` environment variable, then the
            default ``config/vulnerabilities.yaml``.

    Returns:
        dict: Configuration dictionary.
    """
    # Détermination du chemin de configuration
    if config_path is None:
        config_path = os.environ.get(
            "VULNERABILITIES_CONFIG_PATH", "config/vulnerabilities.yaml"
        )
    # Chargement du fichier
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


# Fonction de construction de la configuration méthodologique de la synthèse
def synthesis_config_from_params(params: Optional[Mapping[str, Any]]) -> SynthesisConfig:
    """Build a ``SynthesisConfig`` from the YAML ``PARAMETERS`` section.

    Generic construction, twin of ``network_config_from_params`` /
    ``vulnerability_config_from_params`` of the sibling scripts: every key
    matching a ``SynthesisConfig`` field name overrides the dataclass default;
    unknown keys are dropped with a warning. YAML lists are coerced to the tuple
    types the frozen dataclass expects, nested pairs included (``polarities``).
    The ``methods`` list is routed through
    :func:`~macroforecast.trade.aggregation.method_spec_from_mapping`, one
    ``MethodSpec`` per entry.

    Args:
        params: The ``SYNTHESIS.PARAMETERS`` mapping of
            ``config/synthesis.yaml`` (or ``None``, meaning the defaults of
            :class:`~macroforecast.trade.aggregation.SynthesisConfig`).

    Returns:
        A ``SynthesisConfig`` reflecting the configured overrides.

    Examples:
        >>> config = synthesis_config_from_params({"levels": ["global"]})
        >>> config.levels
        ('global',)
    """
    # Aucune surcharge : configuration par défaut
    default = SynthesisConfig()
    if not params:
        return default

    # Surcharge générique champ à champ
    valid = {field.name for field in fields(SynthesisConfig)}
    overrides: Dict[str, Any] = {}
    for key, value in params.items():
        if key not in valid:
            # Logging
            logger.warning(f"Paramètre de synthèse inconnu ignoré : {key}")
            continue
        # Liste de méthodes : une spécification gelée par entrée
        if key == "methods":
            overrides[key] = tuple(
                method_spec_from_mapping(entry) for entry in value
            )
            continue
        # Coercition listes → tuples pour les champs tuple du dataclass gelé,
        # paires imbriquées comprises (polarities)
        current = getattr(default, key)
        if isinstance(current, tuple) and isinstance(value, (list, tuple)):
            value = tuple(
                tuple(item) if isinstance(item, (list, tuple)) else item
                for item in value
            )
        overrides[key] = value

    return replace(default, **overrides)


# Fonction de validation du contexte de comparaison au regard des sens synthétisés
def validate_flow_context(config: SynthesisConfig, flows: Sequence[str]) -> None:
    """Check that import and export cells can never be compared together.

    The synthesis compares cells within a context only (``context_columns``):
    with more than one direction, the flow column must be part of the context,
    otherwise an importer's and an exporter's scores would be ranked against
    each other. The polarities need no such care: a more concentrated cell is
    a more vulnerable one in both directions (suppliers at the import, outlets
    at the export), for the partner and the network metrics alike.

    Args:
        config: Synthesis configuration.
        flows: Directions synthesised.

    Raises:
        ValueError: If more than one direction is synthesised and the flow
            column is not a context column.

    Examples:
        >>> validate_flow_context(SynthesisConfig(context_columns=("flow",)), ["import", "export"])
        >>> validate_flow_context(SynthesisConfig(context_columns=("freq",)), ["import"])
    """
    if len(flows) > 1 and _FLOW_COLUMN not in config.context_columns:
        raise ValueError(
            f"FLOWS={list(flows)} requires '{_FLOW_COLUMN}' in context_columns "
            f"(got {list(config.context_columns)}): import and export cells must "
            "never be compared together."
        )


# Fonction de lecture des sens synthétisés et de leurs codes
def load_synthesis_flows(
    synthesis_block: Mapping[str, Any],
    vulnerability_config: Mapping[str, Any],
    config: SynthesisConfig,
) -> Tuple[Tuple[str, ...], List[int]]:
    """Read the synthesised directions and translate them into flow codes.

    Args:
        synthesis_block: ``SYNTHESIS`` block of ``config/synthesis.yaml``
            (``FLOWS`` key; the import direction alone when absent).
        vulnerability_config: Whole ``config/vulnerabilities.yaml``: the codes
            are those of the partner ``PARAMETERS`` (``import_flow`` /
            ``export_flow``), never literals of this script.
        config: Synthesis configuration (context columns checked).

    Returns:
        ``(flows, flow_codes)``.

    Raises:
        ValueError: If ``FLOWS`` is invalid, or names several directions while
            the flow column is not a context column.

    Examples:
        >>> load_synthesis_flows({"FLOWS": ["import", "export"]}, {}, SynthesisConfig())
        (('import', 'export'), [1, 2])
    """
    flows = load_flows(synthesis_block)
    validate_flow_context(config, flows)
    codes = flow_code_map(
        vulnerability_config_from_params(vulnerability_config.get("PARAMETERS"))
    )
    return flows, [codes[flow] for flow in flows]


# ──────────────────────────────────────────────────────────────────────
# Construction de la requête source (fonction pure, testable sans base)
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction de la requête DuckDB combinant les sources
def build_source_query(
    sources: Sequence[Mapping[str, Any]],
    filters: Mapping[str, Any],
    catalog_alias: str,
    flow_codes: Optional[Sequence[int]] = None,
) -> str:
    """Build the single DuckDB query reading and joining the source tables (S-2.3).

    Pure function, no database connection: the query text is entirely a
    function of the ``SOURCES`` / ``FILTERS`` configuration blocks and of the
    catalog alias.

    The first source is the **grid**: it is selected whole (``alias.*``), so
    every context and cell key survives without this builder having to know the
    methodology configuration. Every other source is ``LEFT JOIN``-ed, its
    ``JOIN.ON`` expressions and its ``JOIN.WHERE`` predicate conjoined in the
    ``ON`` clause (so unmatched grid rows are kept, with ``NULL`` on the joined
    columns), and only its listed ``COLUMNS`` projected, prefixed by its alias.
    ``FILTERS.WHERE`` is appended as-is, then, when ``flow_codes`` is given,
    the predicate ``<grid alias>."flow" IN (...)`` generated from the
    synthesised directions; ``FILTERS.LAST_N_PERIODS`` becomes a sub-query
    restricting the grid to its most recent distinct periods (omitted when
    ``None``). Without ``flow_codes`` the query is exactly the one of the
    configuration alone.

    Args:
        sources: The ``SOURCES`` list; the first entry is the grid. Each entry
            carries ``SCHEMA``, ``ALIAS`` and, for joined sources, ``COLUMNS``
            and a ``JOIN`` mapping (``ON`` list, optional ``WHERE`` string).
        filters: The ``FILTERS`` mapping (``WHERE`` string, ``LAST_N_PERIODS``
            integer or ``None``).
        catalog_alias: DuckLake catalog alias the fact tables live in.
        flow_codes: Flow codes of the synthesised directions (see
            :func:`load_synthesis_flows`); ``None`` adds no flow predicate.

    Returns:
        The SQL query as a string.

    Raises:
        ValueError: If ``sources`` is empty or a joined source carries no
            ``ON`` / ``WHERE`` condition.

    Examples:
        >>> query = build_source_query(
        ...     [{"SCHEMA": "indicators", "ALIAS": "p", "COLUMNS": ["HHI"]}],
        ...     {"WHERE": "p.freq = 'A'", "LAST_N_PERIODS": None},
        ...     "vulnerabilities",
        ... )
        >>> query.splitlines()[0]
        'SELECT p.*'
        >>> build_source_query(
        ...     [{"SCHEMA": "indicators", "ALIAS": "p"}], {}, "v", flow_codes=[1, 2]
        ... ).splitlines()[-1]
        'WHERE p."flow" IN (1, 2)'
    """
    if not sources:
        raise ValueError("`SOURCES` doit contenir au moins la grille.")
    grid = sources[0]
    joined = list(sources[1:])

    # Nom pleinement qualifié de la table de faits d'un schéma
    def _table(schema: str) -> str:
        """Return the fully qualified fact-table name of ``schema``."""
        return f'"{catalog_alias}"."{schema}"."{FACT_TABLE}"'

    # Projection : grille entière, puis colonnes listées des sources jointes
    projection: List[str] = [f'{grid["ALIAS"]}.*']
    for source in joined:
        projection.extend(
            f'{source["ALIAS"]}."{column}"'
            for column in source.get("COLUMNS", ())
        )

    lines: List[str] = [f'SELECT {", ".join(projection)}']
    lines.append(f'FROM {_table(grid["SCHEMA"])} AS {grid["ALIAS"]}')

    # Jointures gauches : ON = conjonction des expressions JOIN.ON et de JOIN.WHERE
    for source in joined:
        join = source.get("JOIN") or {}
        conditions: List[str] = list(join.get("ON") or [])
        if join.get("WHERE"):
            conditions.append(join["WHERE"])
        if not conditions:
            raise ValueError(
                f"La source jointe {source['ALIAS']!r} n'a aucune condition "
                f"de jointure (JOIN.ON / JOIN.WHERE)."
            )
        lines.append(f'LEFT JOIN {_table(source["SCHEMA"])} AS {source["ALIAS"]}')
        lines.append(f'  ON {conditions[0]}')
        lines.extend(f'  AND {condition}' for condition in conditions[1:])

    # Clause WHERE : prédicat des FILTERS puis, optionnellement, restriction aux
    # N dernières périodes distinctes de la grille
    where_parts: List[str] = []
    if filters.get("WHERE"):
        where_parts.append(filters["WHERE"])
    if flow_codes is not None:
        codes = ", ".join(str(int(code)) for code in flow_codes)
        where_parts.append(f'{grid["ALIAS"]}."{_FLOW_COLUMN}" IN ({codes})')
    last_n_periods = filters.get("LAST_N_PERIODS")
    if last_n_periods is not None:
        where_parts.append(
            f'{grid["ALIAS"]}."{_PERIOD_COLUMN}" IN (\n'
            f'    SELECT DISTINCT "{_PERIOD_COLUMN}"\n'
            f'    FROM {_table(grid["SCHEMA"])}\n'
            f'    ORDER BY 1 DESC\n'
            f'    LIMIT {int(last_n_periods)}\n'
            f'  )'
        )
    if where_parts:
        lines.append(f'WHERE {where_parts[0]}')
        lines.extend(f'  AND {part}' for part in where_parts[1:])

    return "\n".join(lines)


# Fonction de lecture de la table source combinée
def read_source_metrics(
    conn: Any, query: str, string_columns: Sequence[str] = ()
) -> pd.DataFrame:
    """Run the source query and return its result as a pandas DataFrame.

    Args:
        conn: Open DuckLake / DuckDB connection positioned on the catalog.
        query: Query built by :func:`build_source_query`.
        string_columns: Identifier columns to cast to text. The aggregated
            levels write the ``"ALL"`` sentinel into ``reporter`` / ``product``,
            which fails when the source stores them as integers (Comext product
            codes are read as ``BIGINT``). Absent columns are ignored.

    Returns:
        The joined metric table, one row per grid cell.

    Examples:
        >>> import duckdb
        >>> conn = duckdb.connect()
        >>> frame = read_source_metrics(
        ...     conn, "SELECT 28444190::BIGINT AS product", ["product"]
        ... )
        >>> frame["product"].tolist()
        ['28444190']
    """
    # Exécution de la requête et matérialisation en pandas
    df = conn.execute(query).df()
    # Colonnes identifiantes en texte, pour cohabiter avec la valeur « ALL »
    present = [column for column in string_columns if column in df.columns]
    return df.astype({column: str for column in present}) if present else df


# ──────────────────────────────────────────────────────────────────────
# Registres de fraîcheur : amonts (lecture seule) et synthèse (lecture/écriture)
# ──────────────────────────────────────────────────────────────────────

# Générateur des registres de fraîcheur amont (partenaires et réseau)
def iter_upstream_registries(
    vulnerability_config: Mapping[str, Any],
) -> Iterator[Tuple[Path, Optional[str], str]]:
    """Yield ``(path, bucket, root)`` for each upstream freshness registry.

    The partners family may hold several dataflow blocks under
    ``VULNERABILITIES``; every mapping carrying a
    ``PATHS.LAST_COMPUTATION_PATH`` is a registry. The network family holds a
    single block under ``NETWORK_VULNERABILITIES``.

    Args:
        vulnerability_config: Parsed ``config/vulnerabilities.yaml``.

    Yields:
        Tuples ``(registry_path, bucket, registry_root_key)``.
    """
    # Famille partenaires : un bloc par dataflow sous VULNERABILITIES
    partners = vulnerability_config.get(_PARTNERS_ROOT) or {}
    for block in partners.values():
        if not isinstance(block, Mapping):
            continue
        path = (block.get("PATHS") or {}).get("LAST_COMPUTATION_PATH")
        if path:
            yield Path(path), block.get("BUCKET"), _PARTNERS_ROOT

    # Famille réseau : bloc unique sous NETWORK_VULNERABILITIES
    network = vulnerability_config.get(_NETWORK_ROOT) or {}
    path = (network.get("PATHS") or {}).get("LAST_COMPUTATION_PATH")
    if path:
        yield Path(path), network.get("BUCKET"), _NETWORK_ROOT


# Fonction de lecture de l'instant de calcul amont le plus récent
def load_max_upstream_computation(
    vulnerability_config: Mapping[str, Any],
    loader: Loader,
) -> Optional[datetime]:
    """Read every upstream registry and return the most recent ``last_computed``.

    Args:
        vulnerability_config: Parsed ``config/vulnerabilities.yaml``.
        loader: ``Loader`` instance.

    Returns:
        The latest upstream computation instant (UTC-aware), or ``None`` when no
        upstream registry holds a single dated entry yet.
    """
    # Parcours des registres amont, collecte des instants exploitables
    instants: List[datetime] = []
    for path, bucket, root in iter_upstream_registries(vulnerability_config):
        registry = (
            loader.load(path, bucket=bucket, missing_ok=True) or {}
        ).get(root, {})
        for entry in registry.values():
            if not isinstance(entry, Mapping):
                continue
            when = _parse_iso(entry.get("last_computed"))
            if when is not None:
                instants.append(when)
    return max(instants) if instants else None


# Fonction de lecture de l'instant de dernier calcul de synthèse
def load_synthesis_computation_date(
    last_computation_path: Path,
    loader: Loader,
    bucket: Optional[str],
) -> Optional[datetime]:
    """Read the synthesis registry (empty if it does not exist yet).

    Args:
        last_computation_path: Path to the synthesis registry.
        loader: ``Loader`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.

    Returns:
        The last synthesis computation instant (UTC-aware), or ``None``.
    """
    # Lecture de l'entrée globale (racine "SYNTHESIS")
    entry = (
        loader.load(last_computation_path, bucket=bucket, missing_ok=True) or {}
    ).get(_REGISTRY_ROOT, {})
    return _parse_iso(entry.get("last_computed")) if isinstance(entry, Mapping) else None


# Fonction d'écriture de l'entrée de registre de synthèse
def save_synthesis_computation_date(
    last_computation_path: Path,
    entry: Mapping[str, Any],
    saver: Saver,
    bucket: Optional[str],
) -> None:
    """Persist the synthesis registry entry (single global entry, S-2.1 v1).

    Args:
        last_computation_path: Path to the synthesis registry.
        entry: Registry payload (``last_computed``, ``n_cells``, ``n_contexts``,
            ``methods``).
        saver: ``Saver`` instance.
        bucket: S3 bucket holding the registry, or ``None`` for a local path.
    """
    # Écriture de l'entrée sous la racine "SYNTHESIS" (aucune fusion : entrée unique)
    saver.save(
        last_computation_path,
        {_REGISTRY_ROOT: dict(entry)},
        bucket=bucket,
        indent=2,
        ensure_ascii=False,
    )
    # Logging
    logger.info(
        f"Registre de synthèse mis à jour dans '{last_computation_path}'"
    )


# ──────────────────────────────────────────────────────────────────────
# Règle de fraîcheur
# ──────────────────────────────────────────────────────────────────────

# Fonction de décision de recalcul des contextes
def contexts_to_recompute(
    last_upstream: Optional[datetime],
    last_synthesis: Optional[datetime],
    force: bool,
) -> bool:
    """Decide whether the selected contexts must be recomputed (S-2.1 v1).

    Since neither upstream registry is indexed by period, the granularity of
    the decision is the whole selection: either every context selected by
    ``FILTERS`` is recomputed, or none is.

    Args:
        last_upstream: Most recent upstream computation instant, or ``None``
            when no upstream registry holds a dated entry.
        last_synthesis: Last synthesis computation instant, or ``None`` when the
            synthesis has never run.
        force: The ``SYNTHESIS.FORCE`` flag.

    Returns:
        ``True`` when ``force`` is set, when the synthesis never ran while an
        upstream instant exists, or when the most recent upstream instant is
        strictly after the last synthesis; ``False`` otherwise (nothing
        upstream, or the synthesis is already up to date).

    Examples:
        >>> from datetime import datetime, timezone
        >>> old = datetime(2026, 1, 1, tzinfo=timezone.utc)
        >>> new = datetime(2026, 6, 1, tzinfo=timezone.utc)
        >>> contexts_to_recompute(new, old, False)
        True
        >>> contexts_to_recompute(old, new, False)
        False
        >>> contexts_to_recompute(None, None, True)
        True
    """
    # Recalcul forcé
    if force:
        return True
    # Rien en amont : rien à synthétiser
    if last_upstream is None:
        return False
    # Jamais synthétisé alors qu'un amont existe
    if last_synthesis is None:
        return True
    # Amont strictement plus récent que la dernière synthèse
    return last_upstream > last_synthesis


# ──────────────────────────────────────────────────────────────────────
# Registre de fraîcheur v2 : unité globale, empreinte globale
# ──────────────────────────────────────────────────────────────────────

# Nom de l'étape (forçage FORCE_STEPS, champ « step » du registre)
STEP = "synthesis"
# Unité unique : toute la sélection de contextes est recalculée d'un bloc
GLOBAL_UNIT = Unit.of(scope="global")
# Champs de SynthesisConfig exclus de l'empreinte : options d'artefacts seulement
_SYNTHESIS_FINGERPRINT_EXCLUDED = frozenset({"artifact_top_n"})


# Fonction de calcul de l'empreinte globale de la synthèse
def synthesis_requested(
    config: SynthesisConfig,
    sources: Optional[Sequence[Mapping[str, Any]]] = None,
    filters: Optional[Mapping[str, Any]] = None,
    flows: Optional[Sequence[str]] = None,
) -> Dict[str, str]:
    """Current methodological fingerprint of the synthesis (a single, global one).

    Digests the complete list of methods (name, kind, parameters, metrics,
    levels…), the result-shaping fields of the configuration and the source
    selection (``SOURCES`` / ``FILTERS`` / ``FLOWS``): adding or changing a
    method, or changing the selected contexts, makes the whole synthesis stale. A fix in
    the implementation of a method is signalled by invalidating the recorded
    fingerprint (``scripts/invalidate_freshness.py --step synthesis``).

    Args:
        config: Synthesis configuration.
        sources: ``SYNTHESIS.SOURCES`` block.
        filters: ``SYNTHESIS.FILTERS`` block.
        flows: ``SYNTHESIS.FLOWS`` directions (``None`` leaves them out of the
            digest).

    Returns:
        ``{"synthesis": fingerprint}``.

    Examples:
        >>> list(synthesis_requested(SynthesisConfig()))
        ['synthesis']
    """
    params = methodology_params(config, _SYNTHESIS_FINGERPRINT_EXCLUDED)
    params["sources"] = list(sources or [])
    params["filters"] = dict(filters or {})
    if flows is not None:
        params["flows"] = list(flows)
    return {STEP: fingerprint(STEP, params)}


# Fabrique du lecteur d'un registre global v1 (fichier unique, une entrée)
def global_legacy_parser(root: str):
    """Build the parser of a version-1 global registry (``{root: {...}}``).

    Args:
        root: Root key of the version-1 document (``SYNTHESIS``, ``COHERENCE``).

    Returns:
        Function turning the document into at most one legacy entry of the
        global unit.
    """
    def parse(data: Mapping[str, Any]) -> Iterator[RegistryEntry]:
        entry = data.get(root)
        if isinstance(entry, Mapping) and entry.get("last_computed"):
            extra = {k: v for k, v in entry.items() if k != "last_computed"}
            yield legacy_entry(GLOBAL_UNIT, entry["last_computed"], **extra)

    return parse


# Fonction de construction d'un registre global à fragment unique
def global_registry(
    path: Any,
    bucket: Optional[str],
    step: str,
    root: str,
    *,
    loader: Optional[Loader] = None,
    saver: Optional[Saver] = None,
) -> FreshnessRegistry:
    """Build a single-fragment registry, reading a version-1 file at the same path.

    Args:
        path: Registry path (``PATHS.LAST_COMPUTATION_PATH``).
        bucket: S3 bucket, or ``None`` for a local file.
        step: Step name.
        root: Root key of the version-1 document.
        loader: JSON loader (a fresh one by default).
        saver: JSON saver (a fresh one by default).

    Returns:
        The registry.
    """
    return FreshnessRegistry(
        path,
        bucket,
        step,
        shard_of=lambda unit: unit.key,
        legacy=LegacySource(path, bucket, global_legacy_parser(root)),
        loader=loader,
        saver=saver,
    )


# Fonction de construction des registres amont (partenaires et réseau)
def upstream_registries(
    vulnerability_config: Mapping[str, Any],
    classification: str,
) -> List[FreshnessRegistry]:
    """Freshness registries of the partner and network steps, read only.

    Args:
        vulnerability_config: Parsed ``config/vulnerabilities.yaml``.
        classification: Classification label of the partner units.

    Returns:
        One registry per partner dataflow block carrying a ``STATE``, plus the
        network registry when configured.
    """
    registries: List[FreshnessRegistry] = []
    for block in (vulnerability_config.get(_PARTNERS_ROOT) or {}).values():
        if isinstance(block, Mapping) and (block.get("STATE") or {}).get("PATH_TEMPLATE"):
            registries.append(partner_registry(block, classification))
    network = vulnerability_config.get(_NETWORK_ROOT) or {}
    if (network.get("STATE") or {}).get("PATH_TEMPLATE"):
        registries.append(network_registry(network))
    return registries


# Fonction de résumé des registres amont
def summarize_registries(
    registries: Iterable[FreshnessRegistry],
    since: Optional[datetime],
) -> UpstreamSummary:
    """Summarise every upstream registry since the last downstream computation.

    Args:
        registries: Upstream registries.
        since: ``upstream_watermark`` of the downstream entry (``None`` when
            it was never computed).

    Returns:
        The upstream summary: watermark (most recent upstream computation) and
        reasons of the units computed since, for the cascade.
    """
    return summarize_upstream(
        (entry for registry in registries for entry in registry.iter_entries()), since
    )


# Fonction de décision de l'unité globale (synthèse ou cohérence)
def plan_global_unit(
    registry: FreshnessRegistry,
    watermark: Optional[datetime],
    requested: Mapping[str, str],
    force: ForceSpec,
    *,
    step: str,
    yaml_force: bool = False,
    adopt_legacy_fingerprints: bool = False,
) -> Dict[Unit, UnitPlan]:
    """Decide whether the whole selection of contexts must be recomputed.

    Same priorities as every step (first > forced > new_data > fingerprint)
    on the single global unit. The historical ``FORCE`` flag of the YAML is
    kept and is equivalent to listing the step in ``FORCE_STEPS``. Nothing is
    computed while no upstream exists, unless forced.

    Args:
        registry: Single-fragment registry of the step.
        watermark: Most recent upstream computation (``None``: no upstream).
        requested: Current fingerprint.
        force: One-off forcing.
        step: Step name (``synthesis`` or ``coherence``).
        yaml_force: The historical ``FORCE`` flag of the step block.
        adopt_legacy_fingerprints: Deployment migration flag.

    Returns:
        ``{GLOBAL_UNIT: plan}`` or an empty mapping.

    Examples:
        >>> from kedro_pipeline.io.freshness import _EmptyLoader
        >>> registry = FreshnessRegistry("s.json", None, "synthesis", lambda u: "global",
        ...                              loader=_EmptyLoader())
        >>> plan_global_unit(registry, None, {"synthesis": "x"}, ForceSpec(), step="synthesis")
        {}
        >>> plan_global_unit(registry, None, {"synthesis": "x"}, ForceSpec(), step="synthesis",
        ...                  yaml_force=True)[GLOBAL_UNIT].reason
        'first'
    """
    if yaml_force:
        force = replace(force, steps=force.steps | {step})
    # Rien en amont : rien à calculer, sauf forçage
    if watermark is None and not force.covers(step, GLOBAL_UNIT, requested):
        return {}
    return units_to_compute(
        [GLOBAL_UNIT], registry, {GLOBAL_UNIT: watermark}, requested, force,
        step=step, adopt_legacy_fingerprints=adopt_legacy_fingerprints,
    )


# Fonction de construction des tags de fraîcheur d'une exécution
def freshness_tags(
    plan: UnitPlan,
    force: ForceSpec,
    requested: Mapping[str, str],
    step: str,
) -> Dict[str, str]:
    """Run tags of a freshness decision.

    Args:
        plan: Plan of the global unit.
        force: One-off forcing.
        requested: Current fingerprint.
        step: Step name.

    Returns:
        ``freshness_reason``, plus ``forced`` (description of the forcing)
        when the step is forced.

    Examples:
        >>> freshness_tags(UnitPlan("forced", frozenset()), ForceSpec(steps=frozenset({"synthesis"})),
        ...                {"synthesis": "x"}, "synthesis")
        {'freshness_reason': 'forced', 'forced': 'steps=synthesis'}
    """
    tags = {"freshness_reason": plan.reason}
    if plan.reason == "forced" or force.forces_step(step, requested):
        tags["forced"] = force.describe() or f"steps={step}"
    return tags


# Fonction de construction de l'entrée de l'unité globale calculée
def global_entry(
    plan: UnitPlan,
    computed_at: datetime,
    summary: UpstreamSummary,
    requested: Mapping[str, str],
    **counters: Any,
) -> RegistryEntry:
    """Entry recorded once the selection of contexts was recomputed.

    Args:
        plan: Plan of the computation (its reason cascades downstream).
        computed_at: Instant captured before the computation started.
        summary: Upstream summary taken into account (its watermark and the
            upstream reasons are recorded for the downstream steps).
        requested: Current fingerprint.
        **counters: Extra fields (``n_cells``, ``n_contexts``…).

    Returns:
        The registry entry.
    """
    return RegistryEntry(
        unit=GLOBAL_UNIT,
        last_computed=computed_at,
        upstream_watermark=summary.watermark,
        fingerprints=dict(requested),
        reason=plan.reason,
        extra={**counters, "upstream_reasons": summary.to_json()},
    )


# ──────────────────────────────────────────────────────────────────────
# Agrégation des rapports (un rapport par contexte -> un rapport d'exécution)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'agrégation des rapports de contexte en un rapport d'exécution
def _aggregate_reports(reports: Sequence[SynthesisReport]) -> SynthesisReport:
    """Merge the per-context :class:`SynthesisReport` into a run-level one.

    Counters are summed; per-method timings are accumulated. The median group
    size is left at ``NaN`` (dropped by ``to_metrics``): a run-level median
    cannot be recovered from per-context medians.

    Args:
        reports: One report per successfully synthesised context.

    Returns:
        A single :class:`SynthesisReport` describing the whole run.
    """
    total = SynthesisReport()
    for report in reports:
        total.n_contexts += report.n_contexts
        total.n_cells += report.n_cells
        for level, level_report in report.levels.items():
            merged = total.levels.setdefault(level, LevelReport())
            merged.n_groups += level_report.n_groups
            merged.n_cells_scored += level_report.n_cells_scored
            merged.n_methods_skipped += level_report.n_methods_skipped
            for name, seconds in level_report.seconds.items():
                merged.seconds[name] = merged.seconds.get(name, 0.0) + seconds
    return total


# ──────────────────────────────────────────────────────────────────────
# Connecteurs DuckLake
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction d'un connecteur DuckLake sur le catalogue des vulnérabilités
def _result_connector(
    vulnerability_config: Mapping[str, Any],
    bucket: str,
    data_path: str,
    schema: str,
) -> Any:
    """Build a DuckLake connector on the shared ``vulnerabilities`` catalog.

    Args:
        vulnerability_config: Parsed ``config/vulnerabilities.yaml`` (catalog
            identity: ``DBNAME`` / ``CATALOG_ALIAS``).
        bucket: S3 bucket holding the schema's data.
        data_path: Data path of the schema's fact table, under ``bucket``.
        schema: Result schema the connector is positioned on.

    Returns:
        An unconnected ``dt_ducklake_manager.DuckLakeConnector``.
    """
    catalog = vulnerability_config[_PARTNERS_ROOT]
    return build_connector(
        DuckLakeLocation(
            dbname=catalog["DBNAME"],
            catalog_alias=catalog["CATALOG_ALIAS"],
            schema=schema,
            bucket=bucket,
            data_path=data_path,
        ),
        pg=pg_credentials_from_env(),
        s3=s3_credentials_from_env(),
    )


# ──────────────────────────────────────────────────────────────────────
# Orchestration lecture -> run_synthesis -> écriture (S-2.3, S-2.4, S-2.6)
# ──────────────────────────────────────────────────────────────────────

# Fonction d'exécution de la partie « lecture -> run_synthesis -> écriture »
def run_from_connections(
    scores_conn: Any,
    diagnostics_conn: Any,
    query: str,
    config: SynthesisConfig,
    *,
    catalog_alias: str,
    result_schema: str,
    diagnostics_schema: str,
    tracker: Any = None,
    log_artifacts: bool = True,
    scope: Optional[RunScope] = None,
    freshness_metrics: Optional[Mapping[str, float]] = None,
    freshness_tags: Optional[Mapping[str, str]] = None,
) -> Tuple[List[SynthesisReport], Dict[str, Exception], bool, int]:
    """Read the source query, run the synthesis per context, and write both schemas.

    Isolates the DB-bound core of :func:`main` — the S-2.3 read, the
    context-by-context call to ``run_synthesis`` and the S-2.4 / S-2.6 writes —
    from connection setup (``kedro_pipeline.io.ducklake.build_connector``, environment
    variables) and freshness bookkeeping, so it is callable on any pair of
    already-open connections, tests included. As in :func:`main`, the failure
    of one context does not interrupt the others.

    Args:
        scores_conn: Open connection positioned to read the source tables and
            write the ``result_schema`` fact table.
        diagnostics_conn: Open connection to write the ``diagnostics_schema``
            fact table (``fit`` family).
        query: Source query built by :func:`build_source_query`.
        config: Synthesis methodological configuration.
        catalog_alias: DuckLake catalog alias both connections are attached to.
        result_schema: Target schema of the scores.
        diagnostics_schema: Target schema of the fit diagnostics.
        tracker: Experiment tracker; the null tracker by default.
        log_artifacts: Whether to log the S-2.7 artifacts to the tracker.
        scope: Run-report scope. When given, the run report (checks, key figures,
            sections) is built and published inside the tracker's run, after the
            metrics and before returning, and an uncaught exception publishes the
            reduced failure description.
        freshness_metrics: Metrics of the freshness decision
            (``freshness/*``), logged in the run.
        freshness_tags: Tags of the freshness decision (``forced``…), set on
            the run.

    Returns:
        Tuple ``(reports, failures, created_any, n_contexts)``: one
        :class:`~macroforecast.trade.aggregation.SynthesisReport` per
        successfully synthesised context, the per-context exceptions keyed by
        their string representation, whether either schema was created on
        this call, and the number of contexts attempted.
    """
    if tracker is None:
        from macroforecast.tracking import NULL_TRACKER

        tracker = NULL_TRACKER
    # Enregistrement de ce qui est journalisé : source des contrôles et du rapport de run
    tracker = CapturingTracker(tracker)

    context_columns = list(config.context_columns)
    scores_keys = [
        *context_columns, config.reporter_col, config.product_col, "method"
    ]
    diagnostics_keys = [
        *context_columns,
        "level",
        config.reporter_col,
        config.product_col,
        "family",
        "statistic",
        "item_a",
        "item_b",
    ]

    reports: List[SynthesisReport] = []
    failures: Dict[str, Exception] = {}
    created_any = False
    n_contexts = 0

    with tracker, (guarded_run(scope, tracker) if scope is not None else nullcontext()):
        # Décision de fraîcheur de l'exécution (métriques et tag de forçage)
        if freshness_metrics:
            tracker.log_metrics(dict(freshness_metrics))
        if freshness_tags:
            tracker.set_tags(dict(freshness_tags))
        # Lecture de la table source combinée (une seule requête)
        df_source = read_source_metrics(
            scores_conn, query, (config.reporter_col, config.product_col)
        )
        logger.info(
            f"{len(df_source)} ligne(s) source lue(s), "
            f"{df_source[context_columns].drop_duplicates().shape[0]} contexte(s)."
        )

        # Un contexte après l'autre : l'échec de l'un n'emporte pas les autres
        for context_value, df_context in df_source.groupby(
            context_columns, sort=False, observed=True
        ):
            n_contexts += 1
            context = (
                context_value
                if isinstance(context_value, tuple)
                else (context_value,)
            )
            try:
                # Calcul pur des scores et des diagnostics d'ajustement
                df_scores, df_fit, report = run_synthesis(
                    df_context,
                    config,
                    tracker=tracker,
                    log_artifacts=log_artifacts,
                )

                # Options d'écriture communes aux deux schémas : unité = contexte
                write_options = compute_write_options(
                    f"compute_synthetic_scores {'/'.join(str(value) for value in context)}"
                )

                # Écriture des scores (schéma « synthesis »)
                created_scores = write_dataframe(
                    scores_conn,
                    df_scores,
                    scores_keys,
                    catalog_alias=catalog_alias,
                    schema=result_schema,
                    **write_options,
                )
                # Écriture des diagnostics « fit » (schéma « synthesis_diagnostics »,
                # même table longue que le script de cohérence)
                created_diag = write_dataframe(
                    diagnostics_conn,
                    df_fit,
                    diagnostics_keys,
                    catalog_alias=catalog_alias,
                    schema=diagnostics_schema,
                    **write_options,
                )
                created_any = created_any or created_scores or created_diag
                reports.append(report)

                # Logging
                logger.info(
                    f"Contexte {context} synthétisé : {report.n_cells} "
                    f"cellule(s), {len(config.methods)} méthode(s)."
                )
            except Exception as exc:
                # Journalisation de l'échec, poursuite avec les autres contextes
                logger.exception(
                    f"Échec de la synthèse pour le contexte {context}"
                )
                failures[str(context)] = exc

        # Métriques et tags de l'exécution : un rapport agrégé sur tous
        # les contextes réussis
        aggregate = _aggregate_reports(reports)
        aggregate.created = created_any
        tracker.log_metrics(rekey_metrics(aggregate.to_metrics()))
        tracker.set_tags(
            {
                "result_schema": result_schema,
                "n_contexts": str(len(reports)),
                "created": str(created_any),
            }
        )

        # Rapport de run : contrôles, chiffres clés, sections, publiés avant toute sortie
        # en erreur (les contextes en échec sont listés, ils ne l'interrompent pas)
        if scope is not None:
            scope.step = "rapport de run"
            scope.publish(
                tracker,
                scope.build(
                    metrics=tracker.metrics,
                    units=Units(
                        planned=n_contexts,
                        succeeded=len(reports),
                        failed=len(failures),
                        planned_label=f"{n_contexts} contextes",
                    ),
                    failures={
                        unit: f"{type(exc).__name__}: {exc}" for unit, exc in failures.items()
                    },
                    key_figures=key_figures_synthesis,
                    sections=lambda m: sections_synthesis(m, tracker.tables),
                ),
            )

    return reports, failures, created_any, n_contexts


# ──────────────────────────────────────────────────────────────────────
# Point d'entrée
# ──────────────────────────────────────────────────────────────────────

# Nœud du rapport de run (clé de config/tracking.yaml)
NODE = "compute_synthetic_scores"


# Fonction principale de calcul des scores synthétiques
def main() -> None:
    """CLI entry point for the incremental synthetic-score computation.

    Raises:
        RuntimeError: If at least one context failed, once every context has
            been attempted.
    """
    # Chargement des configurations : synthèse (méthodologie, sources, filtres)
    # et vulnérabilités (identité du catalogue, registres amont)
    synthesis_file = load_synthesis_config()
    vulnerability_config = load_vulnerability_config()
    synthesis_config = synthesis_file["SYNTHESIS"]
    coherence_config = synthesis_file["COHERENCE"]

    # Construction de la configuration méthodologique
    parameters = synthesis_config.get("PARAMETERS") or {}
    config = synthesis_config_from_params(parameters)
    # Sens synthétisés et leurs codes (échec explicite si le flux n'est pas une
    # clé de contexte alors que plusieurs sens sont demandés)
    flows, flow_codes = load_synthesis_flows(synthesis_config, vulnerability_config, config)

    # Options de suivi d'exécution (un seul run par exécution, D-14)
    mlflow_config = synthesis_config.get("MLFLOW") or {}
    log_artifacts = bool(mlflow_config.get("LOG_ARTIFACTS", True))
    tracker = get_tracker(
        tracking_uri=mlflow_config.get("TRACKING_URI"),
        experiment=mlflow_config.get("EXPERIMENT", "trade-03-vulnerabilities"),
        run_name=run_name(f"vulnerabilities-synthesis-{datetime.now():%Y%m%d-%H%M}", NODE),
    )
    scope = RunScope(NODE)

    # Identité du catalogue partagé et schémas résultat
    catalog = vulnerability_config[_PARTNERS_ROOT]
    catalog_alias = catalog["CATALOG_ALIAS"]
    result_schema = _schema_name(synthesis_config["RESULT_SCHEMA"])
    diagnostics_schema = _schema_name(coherence_config["RESULT_SCHEMA"])

    # Fraîcheur : registre à fragment unique (le registre v1 au même chemin est
    # relu pour la migration), empreinte globale des méthodes et des sources
    runtime_config = load_runtime_config()
    registry = global_registry(
        synthesis_config["PATHS"]["LAST_COMPUTATION_PATH"],
        synthesis_config["BUCKET"],
        STEP,
        _REGISTRY_ROOT,
    )
    requested = synthesis_requested(
        config, synthesis_config["SOURCES"], synthesis_config.get("FILTERS") or {}, flows
    )
    force = ForceSpec.from_runtime(runtime_config)
    # Amont : registres partenaires et réseau, résumés depuis le dernier calcul
    # (watermark et raisons des unités recalculées depuis, pour la cascade)
    previous = registry.get(GLOBAL_UNIT)
    summary = summarize_registries(
        upstream_registries(
            vulnerability_config,
            partner_classification(runtime_config["NOMENCLATURES"]["HS"]),
        ),
        previous.upstream_watermark if previous is not None else None,
    )
    plans = plan_global_unit(
        registry, summary.watermark, requested, force,
        step=STEP,
        yaml_force=bool(synthesis_config.get("FORCE", False)),
        adopt_legacy_fingerprints=adopt_legacy_flag(synthesis_config),
    )

    # Sortie anticipée : amont inchangé, empreinte inchangée, aucun forçage
    if not plans:
        registry.save()
        logger.info(
            "Synthèse à jour (amont : "
            f"{summary.to_json()}), rien à recalculer."
        )
        return
    plan = plans[GLOBAL_UNIT]
    # Logging
    logger.info(f"Synthèse à recalculer ({plan.reason}) ; amont : {summary.to_json()}")

    # Instant de référence capturé avant le calcul : la date enregistrée
    # correspond au début du traitement, jamais après, pour ne pas rater une
    # mise à jour survenue pendant le calcul
    computed_at = _now()

    # Requête source : combinaison SQL des familles partenaires et réseau (S-2.3)
    query = build_source_query(
        synthesis_config["SOURCES"],
        synthesis_config.get("FILTERS") or {},
        catalog_alias,
        flow_codes,
    )
    # Logging
    logger.info(f"Requête source :\n{query}")

    # Connecteurs DuckLake : un par schéma résultat (chemins de données distincts),
    # tous deux sur le catalogue partagé « vulnerabilities »
    scores_connector = _result_connector(
        vulnerability_config,
        bucket=synthesis_config["BUCKET"],
        data_path=synthesis_config["PATHS"]["DATA_PATH"],
        schema=result_schema,
    )
    diagnostics_connector = _result_connector(
        vulnerability_config,
        bucket=synthesis_config["BUCKET"],
        data_path=coherence_config["PATHS"]["DATA_PATH"],
        schema=diagnostics_schema,
    )

    # Ouverture des connexions : leur cycle de vie appartient au script, le
    # runner ne les ouvre ni ne les ferme (`run_from_connections` orchestre
    # lecture / calcul / écriture sur des connexions déjà ouvertes)
    scores_conn = scores_connector.connect()
    try:
        diagnostics_conn = diagnostics_connector.connect()
        try:
            reports, failures, created_any, n_contexts = run_from_connections(
                scores_conn,
                diagnostics_conn,
                query,
                config,
                catalog_alias=catalog_alias,
                result_schema=result_schema,
                diagnostics_schema=diagnostics_schema,
                tracker=tracker,
                log_artifacts=log_artifacts,
                scope=scope,
                freshness_metrics=plan_metrics(plans, n_candidates=1),
                freshness_tags=freshness_tags(plan, force, requested, STEP),
            )
        finally:
            diagnostics_conn.close()
    finally:
        scores_conn.close()

    # Mise à jour du registre de synthèse, uniquement si au moins un contexte a
    # réussi (cohérence : jamais de date avancée à tort)
    if reports:
        registry.upsert(
            global_entry(
                plan, computed_at, summary, requested,
                n_cells=int(sum(report.n_cells for report in reports)),
                n_contexts=int(len(reports)),
                methods=[spec.name for spec in config.methods],
            )
        )
        registry.save()

    # Échec global si au moins un contexte a échoué, une fois tous tentés
    if failures:
        raise RuntimeError(
            f"{len(failures)} contexte(s) en échec sur {n_contexts} : "
            f"{sorted(failures)}"
        ) from next(iter(failures.values()))


# Exécution du script principal
if __name__ == "__main__":
    main()
