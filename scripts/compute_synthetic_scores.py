"""Script de calcul/mise à jour des scores synthétiques de vulnérabilité.

Combine les deux familles d'indicateurs déjà calculées — indices partenaires
(`vulnerabilities.indicators`, produits par `compute_trade_vulnerabilities.py`)
et indices de réseau (`vulnerabilities.network_indicators`, produits par
`compute_network_vulnerabilities.py`) — en une table de scores synthétiques :
une ligne par cellule `reporter x product` d'un contexte
`(hs_vintage, freq, flow, indicators, TIME_PERIOD)` et par méthode d'agrégation,
avec trois paires `(score, rang)`, une par niveau de comparaison (`by_product`,
`by_reporter`, `global`). La méthodologie multicritère est portée par
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

Fraîcheur, par contexte et par méthode. L'unité de fraîcheur est le contexte ;
le registre (`STATE.PATH_TEMPLATE`, un fichier par période) porte pour chaque
contexte une empreinte par méthode, une pour le consensus et une pour la
sélection des entrées. Un contexte est recalculé :

* entièrement, s'il n'a jamais été calculé, s'il entre dans le périmètre d'un
  forçage (`FORCE` du YAML ou `runtime.FORCE_STEPS`), si un changement amont
  touchant toutes ses périodes est survenu depuis (couple partenaire calculé pour
  la première fois, méthodologie partenaire modifiée ou forcée, réseau de son
  millésime recalculé), ou si les métriques partenaires ont de nouvelles données
  et que sa période est l'une des `RECENT_PERIODS` plus récentes ;
* pour les seules méthodes dont l'empreinte a changé ou a été invalidée, plus le
  consensus, nourri des scores déjà en base des autres méthodes : ajouter ou
  corriger une méthode ne recalcule qu'elle, sur tout l'historique.

L'exécution enchaîne trois temps : planification (requête des contextes de la
grille, sans lecture des métriques), calcul pur de chaque contexte, écriture par
lots de `WRITE_BATCH_CONTEXTS` contextes (lignes des méthodes recalculées
remplacées dans une transaction par table), le registre avançant lot par lot.
Le budget `MAX_CONTEXTS_PER_RUN` (null en régime nominal) ne sert qu'au
rattrapage. `--cadence-check` (ou `CADENCE_CHECK=1`) fait sortir le script sans
rien calculer si la dernière exécution date de moins de
`CADENCE.MIN_INTERVAL_DAYS` jours. La date écrite est capturée avant le calcul,
jamais après, pour ne pas rater une mise à jour concurrente.

Erreurs. Comme `compute_network_vulnerabilities.py` : l'échec d'un contexte
n'interrompt pas les autres, chaque échec est capturé et journalisé, seuls les
contextes réussis sont écrits et enregistrés, et le script ne sort en erreur
qu'en fin de parcours.

Le suivi d'exécution MLflow est piloté par le bloc `SYNTHESIS.MLFLOW` de
`config/synthesis.yaml` : sans `TRACKING_URI` (ou sans serveur joignable),
`get_tracker` retourne un objet nul et l'exécution est strictement inchangée. Un
seul run par exécution.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from collections import Counter
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timedelta
from contextlib import nullcontext
import argparse
import logging
import numbers
import os
from pathlib import Path
import re
import time
from functools import partial
from typing import (
    Any,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)
import yaml

# Modules de chargement/sauvegarde JSON (local ou S3), même brique que le téléchargement
from statflows.storage.json import Loader, Saver
# Fabrique de connecteur DuckLake (seul point de lecture des identifiants)
from kedro_pipeline.io.ducklake import (
    BorrowedReader,
    ConnectionReader,
    DuckLakeLocation,
    DuckLakeTable,
    build_connector,
    pg_credentials_from_env,
    s3_credentials_from_env,
    workflow_run_id,
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
# Macros SQL de nomenclature et millésime en vigueur (référentiel des millésimes)
from kedro_pipeline.config import nomenclature_macros_sql, vintage_in_force

# Modules de manipulation de données
import numpy as np
import pandas as pd

# Module de suivi d'exécution (MLflow optionnel, objet nul par défaut)
from macroforecast.tracking import (
    ArtifactCollector,
    CapturingTracker,
    RecordingTracker,
    get_tracker,
    rekey_metrics,
)
# Parallélisme intra-pod (résolution de n_jobs, carte parallèle, erreurs transmissibles)
from kedro_pipeline.parallel import parallel_map, resolve_n_jobs, serialisable_exception
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
from macroforecast.trade.aggregation.methods import (
    DRAW_PARAMETERS,
    SEED_PARAMETERS,
    MethodSpec,
    method_metrics,
    registry_entry,
    resolve_normalization,
)
from macroforecast.trade.aggregation.synthesis import CONSENSUS_PREFIX, FIT_FAMILY

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
# Colonnes de nomenclature de la grille partenaires — faits de schéma source :
# drapeau des lignes en vigueur (prédicat généré depuis `VINTAGES`) et millésime
# SH de rattachement (clé de contexte : deux millésimes ne sont jamais comparés)
_IN_FORCE_COLUMN = "in_force"
_VINTAGE_COLUMN = "hs_vintage"
# Valeurs admises de `SYNTHESIS.VINTAGES`
VINTAGE_MODES: Tuple[str, ...] = ("in_force", "all")
# Colonne de la table des scores portant le nom de la méthode (ou pseudo-méthode)
_METHOD_COLUMN = "method"
# Colonnes de la table longue des diagnostics : famille et objet de la statistique
_FAMILY_COLUMN = "family"
_ITEM_A_COLUMN = "item_a"
# Raisons de calcul d'une unité partenaire valant changement de toutes ses
# périodes : couple jamais calculé (tout son historique arrive d'un coup),
# changement de méthodologie, forçage
_FULL_CHANGE_REASONS = frozenset({"first", "fingerprint", "forced"})


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


# Fonction de lecture du périmètre de nomenclature synthétisé
def load_synthesis_vintages(
    synthesis_block: Mapping[str, Any], config: SynthesisConfig
) -> str:
    """Read which partner rows the synthesis scores, by nomenclature.

    ``"in_force"`` keeps the rows of the nomenclature in force (codes as
    declared, the dashboard's yearly view); ``"all"`` also scores the
    historical rows (flows of later years converted into an older HS vintage),
    which then requires the HS vintage among the context columns: a row in
    force and a historical row of the same period must never be ranked
    together.

    Args:
        synthesis_block: ``SYNTHESIS`` block (``VINTAGES`` key, ``"in_force"``
            when absent).
        config: Synthesis configuration (context columns checked).

    Returns:
        ``"in_force"`` or ``"all"``.

    Raises:
        ValueError: If the value is unknown, or ``"all"`` without the HS
            vintage in the context columns.

    Examples:
        >>> load_synthesis_vintages({}, SynthesisConfig())
        'in_force'
        >>> load_synthesis_vintages({"VINTAGES": "all"},
        ...                         SynthesisConfig(context_columns=("hs_vintage", "flow")))
        'all'
    """
    vintages = synthesis_block.get("VINTAGES", "in_force")
    if vintages not in VINTAGE_MODES:
        raise ValueError(f"SYNTHESIS.VINTAGES must be one of {VINTAGE_MODES}, got {vintages!r}")
    if vintages == "all" and _VINTAGE_COLUMN not in config.context_columns:
        raise ValueError(
            f"SYNTHESIS.VINTAGES='all' requires '{_VINTAGE_COLUMN}' in context_columns "
            f"(got {list(config.context_columns)}): rows of two vintages must never be "
            "compared together."
        )
    return vintages


# ──────────────────────────────────────────────────────────────────────
# Construction de la requête source (fonction pure, testable sans base)
# ──────────────────────────────────────────────────────────────────────

# Fonction de construction de la requête DuckDB combinant les sources
def build_source_query(
    sources: Sequence[Mapping[str, Any]],
    filters: Mapping[str, Any],
    catalog_alias: str,
    flow_codes: Optional[Sequence[int]] = None,
    vintages: Optional[str] = None,
    contexts: Optional[Sequence[Sequence[Any]]] = None,
    context_columns: Optional[Sequence[str]] = None,
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
    synthesised directions, and, when ``vintages`` is ``"in_force"``, the
    predicate ``<grid alias>."in_force" = true`` keeping the rows of the
    nomenclature in force; ``FILTERS.LAST_N_PERIODS`` becomes a sub-query
    restricting the grid to its most recent distinct periods (omitted when
    ``None``). Without ``flow_codes`` nor ``vintages`` the query is exactly the
    one of the configuration alone. The join conditions come from the
    configuration only (``JOIN.ON``): the network rows are matched on the HS
    vintage of each partner row there.

    Args:
        sources: The ``SOURCES`` list; the first entry is the grid. Each entry
            carries ``SCHEMA``, ``ALIAS`` and, for joined sources, ``COLUMNS``
            and a ``JOIN`` mapping (``ON`` list, optional ``WHERE`` string).
        filters: The ``FILTERS`` mapping (``WHERE`` string, ``LAST_N_PERIODS``
            integer or ``None``).
        catalog_alias: DuckLake catalog alias the fact tables live in.
        flow_codes: Flow codes of the synthesised directions (see
            :func:`load_synthesis_flows`); ``None`` adds no flow predicate.
        vintages: ``"in_force"`` (rows in force only), ``"all"`` or ``None``
            (no predicate on the nomenclature; see
            :func:`load_synthesis_vintages`).
        contexts: Context tuples the read is restricted to (a row-value
            ``IN`` predicate on the grid, see :func:`context_in_predicate`),
            so that an incremental run reads its contexts batch by batch;
            ``None`` adds no predicate.
        context_columns: Ordered context key columns, required with
            ``contexts``.

    Returns:
        The SQL query as a string.

    Raises:
        ValueError: If ``sources`` is empty, if a joined source carries no
            ``ON`` / ``WHERE`` condition, or if ``contexts`` is given without
            ``context_columns``.

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
        >>> build_source_query(
        ...     [{"SCHEMA": "indicators", "ALIAS": "p"}], {}, "v", vintages="in_force"
        ... ).splitlines()[-1]
        'WHERE p."in_force" = true'
        >>> build_source_query(
        ...     [{"SCHEMA": "indicators", "ALIAS": "p"}], {}, "v",
        ...     contexts=[("A",)], context_columns=["freq"],
        ... ).splitlines()[-3:]
        ['WHERE (p."freq") IN (', "    ('A')", '  )']
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
    # N dernières périodes distinctes de la grille et aux contextes demandés
    where_parts = _grid_where_parts(grid, filters, catalog_alias, flow_codes, vintages)
    if contexts is not None:
        if context_columns is None:
            raise ValueError("`contexts` requires `context_columns`.")
        where_parts.append(context_in_predicate(context_columns, contexts, grid["ALIAS"]))
    if where_parts:
        lines.append(f'WHERE {where_parts[0]}')
        lines.extend(f'  AND {part}' for part in where_parts[1:])

    return "\n".join(lines)


# Fonction de construction des prédicats de la grille (FILTERS, flux, nomenclature)
def _grid_where_parts(
    grid: Mapping[str, Any],
    filters: Mapping[str, Any],
    catalog_alias: str,
    flow_codes: Optional[Sequence[int]],
    vintages: Optional[str],
) -> List[str]:
    """Build the predicates on the grid shared by the source and context queries.

    Args:
        grid: First entry of ``SOURCES`` (``SCHEMA``, ``ALIAS``).
        filters: The ``FILTERS`` mapping (``WHERE``, ``LAST_N_PERIODS``).
        catalog_alias: DuckLake catalog alias.
        flow_codes: Flow codes of the synthesised directions, or ``None``.
        vintages: ``"in_force"``, ``"all"`` or ``None``.

    Returns:
        The predicates, in order: ``FILTERS.WHERE``, the flow list, the
        in-force flag, the restriction to the last periods.
    """
    alias = grid["ALIAS"]
    table = f'"{catalog_alias}"."{grid["SCHEMA"]}"."{FACT_TABLE}"'
    where_parts: List[str] = []
    if filters.get("WHERE"):
        where_parts.append(filters["WHERE"])
    if flow_codes is not None:
        codes = ", ".join(str(int(code)) for code in flow_codes)
        where_parts.append(f'{alias}."{_FLOW_COLUMN}" IN ({codes})')
    if vintages == "in_force":
        where_parts.append(f'{alias}."{_IN_FORCE_COLUMN}" = true')
    last_n_periods = filters.get("LAST_N_PERIODS")
    if last_n_periods is not None:
        where_parts.append(
            f'{alias}."{_PERIOD_COLUMN}" IN (\n'
            f'    SELECT DISTINCT "{_PERIOD_COLUMN}"\n'
            f'    FROM {table}\n'
            f'    ORDER BY 1 DESC\n'
            f'    LIMIT {int(last_n_periods)}\n'
            f'  )'
        )
    return where_parts


# Fonction de rendu d'un littéral SQL à partir d'une valeur scalaire
def _sql_literal(value: Any) -> str:
    """Render one scalar as a DuckDB SQL literal.

    Strings are single-quoted with quote doubling; booleans become ``TRUE`` /
    ``FALSE``; integers and floats are emitted verbatim; anything else falls
    back to its quoted string form. ``numpy`` scalars are handled through the
    ``numbers`` ABCs.

    Args:
        value: Scalar drawn from a context tuple.

    Returns:
        The SQL literal.

    Examples:
        >>> _sql_literal("VALUE_IN_EUROS")
        "'VALUE_IN_EUROS'"
        >>> _sql_literal(1)
        '1'
        >>> _sql_literal("d'Ivoire")
        "'d''Ivoire'"
    """
    # Chaîne : guillemets simples, apostrophes doublées
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    # Booléen avant l'entier (bool est sous-classe de int)
    if isinstance(value, (bool, np.bool_)):
        return "TRUE" if value else "FALSE"
    # Entier puis réel, via les ABC `numbers` (couvre les scalaires numpy)
    if isinstance(value, numbers.Integral):
        return str(int(value))
    if isinstance(value, numbers.Real):
        return repr(float(value))
    # Repli : forme chaîne échappée
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


# Fonction de construction d'un prédicat d'appartenance à une liste de contextes
def context_in_predicate(
    context_columns: Sequence[str],
    contexts: Sequence[Sequence[Any]],
    alias: Optional[str] = None,
) -> str:
    """Build the row-value ``IN`` predicate selecting a list of contexts.

    An empty list yields ``FALSE`` rather than no predicate, so that a query
    restricted to no context never scans the whole table.

    Args:
        context_columns: Ordered context key columns.
        contexts: Context tuples, each in the order of ``context_columns``.
        alias: Table alias qualifying the columns, or ``None``.

    Returns:
        The SQL predicate.

    Examples:
        >>> print(context_in_predicate(["freq", "flow"], [("A", 1), ("A", 2)]))
        ("freq", "flow") IN (
            ('A', 1),
            ('A', 2)
          )
        >>> context_in_predicate(["freq"], [], alias="p")
        'FALSE'
    """
    if not contexts:
        return "FALSE"
    prefix = f"{alias}." if alias else ""
    columns = ", ".join(f'{prefix}"{column}"' for column in context_columns)
    tuples = ",\n    ".join(
        "(" + ", ".join(_sql_literal(value) for value in context) + ")"
        for context in contexts
    )
    return f"({columns}) IN (\n    {tuples}\n  )"


# Fonction de construction de la requête des contextes de la grille
def build_contexts_query(
    sources: Sequence[Mapping[str, Any]],
    filters: Mapping[str, Any],
    catalog_alias: str,
    context_columns: Sequence[str],
    flow_codes: Optional[Sequence[int]] = None,
    vintages: Optional[str] = None,
) -> str:
    """Build the query listing the distinct contexts of the filtered grid.

    Same predicates as :func:`build_source_query` (``FILTERS``, flows, in-force
    flag, last periods), on the grid alone: no join, no metric column is read.
    ``FILTERS.WHERE`` is a predicate on the grid alias by contract.

    Args:
        sources: The ``SOURCES`` list; only the grid (first entry) is read.
        filters: The ``FILTERS`` mapping.
        catalog_alias: DuckLake catalog alias.
        context_columns: Ordered context key columns.
        flow_codes: Flow codes of the synthesised directions, or ``None``.
        vintages: ``"in_force"``, ``"all"`` or ``None``.

    Returns:
        The SQL query, one row per distinct context.

    Raises:
        ValueError: If ``sources`` is empty.

    Examples:
        >>> print(build_contexts_query(
        ...     [{"SCHEMA": "indicators", "ALIAS": "p"}], {}, "v", ["freq", "flow"],
        ...     flow_codes=[1], vintages="in_force",
        ... ))
        SELECT DISTINCT p."freq", p."flow"
        FROM "v"."indicators"."fact_table" AS p
        WHERE p."flow" IN (1)
          AND p."in_force" = true
    """
    if not sources:
        raise ValueError("`SOURCES` doit contenir au moins la grille.")
    grid = sources[0]
    alias = grid["ALIAS"]
    columns = ", ".join(f'{alias}."{column}"' for column in context_columns)
    lines = [
        f"SELECT DISTINCT {columns}",
        f'FROM "{catalog_alias}"."{grid["SCHEMA"]}"."{FACT_TABLE}" AS {alias}',
    ]
    where_parts = _grid_where_parts(grid, filters, catalog_alias, flow_codes, vintages)
    if where_parts:
        lines.append(f"WHERE {where_parts[0]}")
        lines.extend(f"  AND {part}" for part in where_parts[1:])
    return "\n".join(lines)


# Fonction de lecture des contextes de la grille
def read_contexts(conn: Any, query: str) -> List[Tuple[Any, ...]]:
    """Run the context query and return the typed context tuples.

    Args:
        conn: Open DuckLake / DuckDB connection.
        query: Query built by :func:`build_contexts_query`.

    Returns:
        One tuple per distinct context, values typed as stored (they are
        reused as SQL literals to read and replace the rows of the context).
    """
    return [tuple(row) for row in conn.execute(query).fetchall()]


# Fonction de construction de la requête de lecture des scores
def build_scores_query(
    catalog_alias: str,
    schema: str,
    context_columns: Sequence[str],
    contexts: Sequence[Sequence[Any]],
    methods: Optional[Sequence[str]] = None,
) -> str:
    """Build the query reading the score table, restricted to ``contexts``.

    Pure function, no database connection. The table is read whole
    (``SELECT *``) — the runner projects the columns it needs — and filtered by
    a row-value ``IN`` list on the context key, so a single query covers every
    selected context. An empty ``contexts`` yields a ``WHERE FALSE`` guard
    rather than an unfiltered scan.

    Args:
        catalog_alias: DuckLake catalog alias the fact table lives in.
        schema: Result schema of the scores (``SYNTHESIS.RESULT_SCHEMA``).
        context_columns: Ordered context key columns.
        contexts: Context tuples to keep, each in the order of
            ``context_columns``.
        methods: Values of the ``method`` column to keep (the stored scores of
            the methods a partial recomputation leaves out); ``None`` keeps
            every method.

    Returns:
        The SQL query as a string.

    Examples:
        >>> print(build_scores_query(
        ...     "vulnerabilities", "synthesis", ["freq", "flow"],
        ...     [("A", 1), ("A", 2)],
        ... ))
        SELECT * FROM "vulnerabilities"."synthesis"."fact_table"
        WHERE ("freq", "flow") IN (
            ('A', 1),
            ('A', 2)
          )
        >>> build_scores_query("v", "s", ["freq"], [("A",)], methods=["mpi"]).splitlines()[-1]
        '  AND "method" IN (\\'mpi\\')'
    """
    table = f'"{catalog_alias}"."{schema}"."{FACT_TABLE}"'
    base = f"SELECT * FROM {table}"
    # Aucun contexte : garde-fou explicite plutôt qu'un balayage complet
    if not contexts:
        return f"{base}\nWHERE FALSE"
    query = f"{base}\nWHERE {context_in_predicate(context_columns, contexts)}"
    if methods is not None:
        query += f'\n  AND "{_METHOD_COLUMN}" IN ({_sql_list(methods)})'
    return query


# Fonction de rendu d'une liste de littéraux SQL
def _sql_list(values: Iterable[Any]) -> str:
    """Render values as a comma-separated list of SQL literals (``FALSE``-safe).

    Args:
        values: Scalars to render.

    Returns:
        The literals joined by ``", "``; ``NULL`` for an empty list, which an
        ``IN`` never matches.
    """
    rendered = [_sql_literal(value) for value in values]
    return ", ".join(rendered) if rendered else "NULL"


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
    vintages: Optional[str] = None,
) -> Dict[str, str]:
    """Current methodological fingerprint of the synthesis (a single, global one).

    Digests the complete list of methods (name, kind, parameters, metrics,
    levels…), the result-shaping fields of the configuration and the source
    selection (``SOURCES`` / ``FILTERS`` / ``FLOWS`` / ``VINTAGES``): adding or changing a
    method, or changing the selected contexts, makes the whole synthesis stale. A fix in
    the implementation of a method is signalled by invalidating the recorded
    fingerprint (``scripts/invalidate_freshness.py --step synthesis``).

    Args:
        config: Synthesis configuration.
        sources: ``SYNTHESIS.SOURCES`` block.
        filters: ``SYNTHESIS.FILTERS`` block.
        flows: ``SYNTHESIS.FLOWS`` directions (``None`` leaves them out of the
            digest).
        vintages: ``SYNTHESIS.VINTAGES`` (``None`` leaves it out of the digest).

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
    if vintages is not None:
        params["vintages"] = vintages
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
# Fraîcheur par contexte et par méthode : empreintes
# ──────────────────────────────────────────────────────────────────────

# Empreinte de la sélection des entrées (sources, filtres, sens, nomenclatures,
# clés) : périmée, elle rend toutes les méthodes de tous les contextes périmées.
# Elle porte le nom de l'étape, que l'invalidation `--metrics synthesis` désigne
INPUTS_FINGERPRINT = STEP
# Empreinte des pseudo-méthodes de consensus (règles et liste des méthodes classées)
CONSENSUS_FINGERPRINT = "consensus"


# Fonction de description des paramètres méthodologiques d'une méthode
def method_fingerprint_params(spec: MethodSpec, config: SynthesisConfig) -> Dict[str, Any]:
    """Parameters digested into the fingerprint of one synthesis method.

    The fingerprint of a method changes, and the method alone (plus the
    consensus) is recomputed on every context, when one of these parameters
    changes:

    * the entry of the YAML ``methods`` list: ``kind``, ``params``, and the
      **resolved** ``metrics`` (``None`` resolves to every configured metric),
      ``levels`` (restricted to the configured levels), ``normalization`` (the
      method override, else the configuration default when the ``kind``
      admits it, else the default of the ``kind``) and ``min_group_size`` (the
      method override, else the larger of the ``kind`` default and the
      configuration floor);
    * the configuration fields every method depends on: ``winsorize_quantile``,
      ``rank_ties`` and the polarities of the method's own metrics;
    * ``random_state``, for the methods drawing at random (``smaa``,
      ``cone_quantile``, ``kantorovich``) and for those bootstrapped;
    * ``smaa_n_draws`` for ``smaa`` and ``cone_quantile`` (shared draws),
      ``smaa_k`` for ``smaa``, ``ot_dimension_limit`` for ``kantorovich``;
    * ``bootstrap_n``, ``bootstrap_ci`` and ``bootstrap_levels`` when the
      method is listed in ``bootstrap_methods``.

    The name of the method is the key of the fingerprint, not a parameter. A
    corrected implementation is signalled by invalidating the fingerprint
    (``invalidate-freshness-script --step synthesis --metrics <method>``), the
    code itself never being digested.

    Args:
        spec: Configured method.
        config: Synthesis configuration.

    Returns:
        JSON-ready mapping of the parameters.

    Raises:
        ValueError: If the ``kind`` is unknown or a requested metric or
            normalisation is not available.

    Examples:
        >>> params = method_fingerprint_params(
        ...     MethodSpec(name="mpi", kind="mpi"), SynthesisConfig(metric_columns=("HHI",)))
        >>> params["metrics"], params["normalization"], "random_state" in params
        (['HHI'], 'minmax', False)
    """
    entry = registry_entry(spec.kind)
    metrics = method_metrics(spec, config)
    params = methodology_params(spec, excluded={"name"})
    params.update(
        {
            "metrics": list(metrics),
            "levels": [
                level for level in config.levels
                if spec.levels is None or level in spec.levels
            ],
            "normalization": resolve_normalization(spec, config),
            "min_group_size": (
                spec.min_group_size
                if spec.min_group_size is not None
                else max(entry.min_group_size, config.min_group_size)
            ),
            "winsorize_quantile": config.winsorize_quantile,
            "rank_ties": config.rank_ties,
            "polarities": {
                name: sign for name, sign in config.polarities if name in metrics
            },
        }
    )
    bootstrapped = spec.name in config.bootstrap_methods
    if spec.kind in SEED_PARAMETERS or bootstrapped:
        params["random_state"] = config.random_state
    if spec.kind in DRAW_PARAMETERS:
        params["smaa_n_draws"] = config.smaa_n_draws
    if spec.kind == "smaa":
        params["smaa_k"] = config.smaa_k
    if spec.kind == "kantorovich":
        params["ot_dimension_limit"] = config.ot_dimension_limit
    if bootstrapped:
        params["bootstrap"] = {
            "n": config.bootstrap_n,
            "ci": config.bootstrap_ci,
            "levels": list(config.bootstrap_levels),
        }
    return params


# Fonction de calcul des empreintes de la synthèse par contexte
def synthesis_context_requested(
    config: SynthesisConfig,
    sources: Optional[Sequence[Mapping[str, Any]]] = None,
    filters: Optional[Mapping[str, Any]] = None,
    flows: Optional[Sequence[str]] = None,
    vintages: Optional[str] = None,
) -> Dict[str, str]:
    """Current fingerprints of a synthesis context: one per method, plus two.

    * ``<method name>``: :func:`method_fingerprint_params` of the method;
    * ``consensus``: the consensus rules, ``consensus_top_n``, ``rank_ties``,
      the levels and the sorted list of the configured method names (adding
      or removing a method changes the consensus); absent when no consensus
      is configured;
    * ``synthesis``: the input selection (``SOURCES``, ``FILTERS``, ``FLOWS``,
      ``VINTAGES``, context, reporter and product columns); when it changes,
      every method of every context is recomputed.

    Args:
        config: Synthesis configuration.
        sources: ``SYNTHESIS.SOURCES`` block.
        filters: ``SYNTHESIS.FILTERS`` block.
        flows: ``SYNTHESIS.FLOWS`` directions.
        vintages: ``SYNTHESIS.VINTAGES``.

    Returns:
        Mapping fingerprint name -> fingerprint.

    Raises:
        ValueError: If two configured methods share a name, or a name collides
            with ``consensus`` or ``synthesis``.

    Examples:
        >>> sorted(synthesis_context_requested(SynthesisConfig(
        ...     metric_columns=("HHI",), methods=(MethodSpec(name="mpi", kind="mpi"),))))
        ['consensus', 'mpi', 'synthesis']
    """
    names = [spec.name for spec in config.methods]
    reserved = {INPUTS_FINGERPRINT, CONSENSUS_FINGERPRINT}
    if len(set(names)) != len(names) or reserved & set(names):
        raise ValueError(
            f"Method names must be unique and differ from {sorted(reserved)}: {names}"
        )
    requested = {spec.name: fingerprint(spec.name, method_fingerprint_params(spec, config))
                 for spec in config.methods}
    if config.consensus:
        requested[CONSENSUS_FINGERPRINT] = fingerprint(
            CONSENSUS_FINGERPRINT,
            {
                "rules": list(config.consensus),
                "top_n": config.consensus_top_n,
                "rank_ties": config.rank_ties,
                "levels": list(config.levels),
                "methods": sorted(names),
            },
        )
    requested[INPUTS_FINGERPRINT] = fingerprint(
        INPUTS_FINGERPRINT,
        {
            "sources": list(sources or []),
            "filters": dict(filters or {}),
            "flows": list(flows) if flows is not None else None,
            "vintages": vintages,
            "context_columns": list(config.context_columns),
            "reporter_col": config.reporter_col,
            "product_col": config.product_col,
        },
    )
    return requested


# Fonction d'extension des noms périmés d'un contexte
def expand_synthesis_names(
    names: Collection[str], requested: Mapping[str, str], method_names: Sequence[str]
) -> FrozenSet[str]:
    """Complete the stale names of a context with the names they make stale.

    A stale input selection makes every name stale; a recomputed method makes
    the consensus stale, since the consensus ranks every method.

    Args:
        names: Stale fingerprint names of the context.
        requested: Current fingerprints.
        method_names: Configured method names.

    Returns:
        The names to recompute.

    Examples:
        >>> requested = {"mpi": "a", "bod": "b", "consensus": "c", "synthesis": "d"}
        >>> sorted(expand_synthesis_names({"mpi"}, requested, ["mpi", "bod"]))
        ['consensus', 'mpi']
        >>> len(expand_synthesis_names({"synthesis"}, requested, ["mpi", "bod"]))
        4
    """
    expanded = set(names)
    if INPUTS_FINGERPRINT in expanded:
        return frozenset(requested)
    if expanded & set(method_names) and CONSENSUS_FINGERPRINT in requested:
        expanded.add(CONSENSUS_FINGERPRINT)
    return frozenset(expanded)


# Fonction de sélection des méthodes à ajuster pour un plan
def plan_methods(
    names: Collection[str], method_names: Sequence[str]
) -> Optional[Tuple[str, ...]]:
    """Return the methods a context plan fits, ``None`` meaning all of them.

    Args:
        names: Names of the plan (see :func:`expand_synthesis_names`).
        method_names: Configured method names, in configuration order.

    Returns:
        ``None`` when every configured method is stale, else the stale
        methods in configuration order (possibly empty: consensus alone).

    Examples:
        >>> plan_methods({"mpi", "bod", "consensus"}, ["mpi", "bod"]) is None
        True
        >>> plan_methods({"consensus"}, ["mpi", "bod"])
        ()
    """
    if set(method_names) <= set(names):
        return None
    return tuple(name for name in method_names if name in names)


# ──────────────────────────────────────────────────────────────────────
# Fraîcheur par contexte : registre, réglages, amont
# ──────────────────────────────────────────────────────────────────────

# Réglages de l'exécution incrémentale d'une étape par contexte
@dataclass(frozen=True)
class IncrementalSettings:
    """Run settings of a per-context step (synthesis or coherence).

    Attributes:
        recent_periods: Number of most recent periods recomputed when the
            partner metrics have new data (the incremental download only
            brings back the last observations); ``None`` treats every period
            as recent.
        max_contexts: Catch-up budget: maximum number of contexts computed in
            one run, the most recent periods first, the others left to the
            next runs; ``None`` (nominal regime) computes every stale context.
        write_batch_contexts: Number of contexts read, written and recorded
            together.
        min_interval_days: Minimum interval between two runs under
            ``--cadence-check``.
        period_column: Context column holding the period.
        n_jobs: Number of worker processes computing the contexts.
        sequential_methods: Methods computed in the parent process once the
            parallel phase is over (memory-hungry ones such as the optimal
            transport score), for the same contexts.

    Examples:
        >>> IncrementalSettings().max_contexts is None
        True
    """
    recent_periods: Optional[int] = None
    max_contexts: Optional[int] = None
    write_batch_contexts: int = 50
    min_interval_days: float = 6.0
    period_column: str = _PERIOD_COLUMN
    n_jobs: int = 1
    sequential_methods: Tuple[str, ...] = ()


# Fonction de lecture de la profondeur de l'incrémental Eurostat
def default_recent_periods(eurostat_config: Optional[Mapping[str, Any]]) -> Optional[int]:
    """Number of periods the incremental partner download brings back.

    Args:
        eurostat_config: Parsed Eurostat download configuration, or ``None``.

    Returns:
        The largest ``N_LAST_OBSERVATIONS`` found in it, or ``None`` when none
        is set (every period then counts as recent, the conservative choice).

    Examples:
        >>> default_recent_periods({"DOWNLOADS": {"DS": {"N_LAST_OBSERVATIONS": 10}}})
        10
        >>> default_recent_periods(None) is None
        True
    """
    found: List[int] = []

    # Parcours récursif : la clé vit sous le bloc de chaque dataflow
    def walk(node: Any) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                if key == "N_LAST_OBSERVATIONS" and value is not None:
                    found.append(int(value))
                else:
                    walk(value)

    walk(eurostat_config or {})
    return max(found) if found else None


# Fonction de lecture des réglages incrémentaux d'un bloc de configuration
def incremental_settings(
    block: Mapping[str, Any], eurostat_config: Optional[Mapping[str, Any]] = None
) -> IncrementalSettings:
    """Read the incremental settings of a ``SYNTHESIS`` or ``COHERENCE`` block.

    Args:
        block: Configuration block (keys ``RECENT_PERIODS``,
            ``MAX_CONTEXTS_PER_RUN``, ``WRITE_BATCH_CONTEXTS``,
            ``CADENCE.MIN_INTERVAL_DAYS``, ``N_JOBS`` and
            ``SEQUENTIAL_METHODS``). ``N_JOBS`` null resolves to the CPU of the
            pod (see :func:`~kedro_pipeline.parallel.resolve_n_jobs`).
        eurostat_config: Eurostat download configuration, the default of
            ``RECENT_PERIODS``.

    Returns:
        The settings.

    Raises:
        ValueError: If a count is not a positive integer.

    Examples:
        >>> incremental_settings({"MAX_CONTEXTS_PER_RUN": 2, "RECENT_PERIODS": 3}).max_contexts
        2
    """
    recent = block.get("RECENT_PERIODS")
    if recent is None:
        recent = default_recent_periods(eurostat_config)
    budget = block.get("MAX_CONTEXTS_PER_RUN")
    batch = block.get("WRITE_BATCH_CONTEXTS") or IncrementalSettings.write_batch_contexts
    interval = (block.get("CADENCE") or {}).get("MIN_INTERVAL_DAYS")
    for name, value in (("RECENT_PERIODS", recent), ("MAX_CONTEXTS_PER_RUN", budget),
                        ("WRITE_BATCH_CONTEXTS", batch)):
        if value is not None and int(value) < 1:
            raise ValueError(f"{name} must be a positive integer, got {value!r}")
    return IncrementalSettings(
        recent_periods=int(recent) if recent is not None else None,
        max_contexts=int(budget) if budget is not None else None,
        write_batch_contexts=int(batch),
        min_interval_days=(
            float(interval) if interval is not None else IncrementalSettings.min_interval_days
        ),
        n_jobs=resolve_n_jobs(block.get("N_JOBS")),
        sequential_methods=tuple(block.get("SEQUENTIAL_METHODS") or ()),
    )


# Fonction de construction de l'unité de fraîcheur d'un contexte
def context_unit(context_columns: Sequence[str], values: Any) -> Unit:
    """Return the freshness unit of a context.

    Args:
        context_columns: Ordered context key columns.
        values: Context values (a tuple, or a scalar for a single column).

    Returns:
        The unit, one dimension per context column, values as text.

    Examples:
        >>> context_unit(["freq", "flow"], ("A", 1)).key
        'A|1'
    """
    values = values if isinstance(values, tuple) else (values,)
    return Unit.from_mapping(dict(zip(context_columns, values)))


# Fonction de construction du registre par contexte d'une étape
def context_registry(
    block: Mapping[str, Any],
    bucket: Optional[str],
    step: str,
    context_columns: Sequence[str],
    *,
    loader: Optional[Loader] = None,
    saver: Optional[Saver] = None,
) -> FreshnessRegistry:
    """Build the per-context freshness registry of the synthesis or the coherence.

    One entry per context; the fragment is set by ``STATE.PATH_TEMPLATE``,
    whose fields must be context columns (one file per period, e.g.
    ``trade/state/synthesis/{TIME_PERIOD}.json``).

    Args:
        block: ``SYNTHESIS`` or ``COHERENCE`` block (``STATE.PATH_TEMPLATE``).
        bucket: S3 bucket, or ``None`` for local storage.
        step: Step name.
        context_columns: Ordered context key columns.
        loader: JSON loader (a fresh one by default).
        saver: JSON saver (a fresh one by default).

    Returns:
        The registry.

    Raises:
        KeyError: If the block has no ``STATE.PATH_TEMPLATE``.
        ValueError: If the template names a field that is not a context column.
    """
    template = str(block["STATE"]["PATH_TEMPLATE"])
    fields_named = re.findall(r"\{([^{}]*)\}", template)
    unknown = sorted(set(fields_named) - set(context_columns))
    if unknown:
        raise ValueError(
            f"STATE.PATH_TEMPLATE of step '{step}' names {unknown}, which are not "
            f"context columns {list(context_columns)}"
        )

    # Libellé de fragment : valeurs des champs du modèle (la période par défaut)
    def shard_of(unit: Unit) -> str:
        return "|".join(str(unit.get(name)) for name in fields_named) or unit.key

    return FreshnessRegistry(
        template, bucket, step, shard_of=shard_of, loader=loader, saver=saver
    )


# Marques amont d'une étape par contexte, par pertinence de millésime
@dataclass(frozen=True)
class UpstreamMarks:
    """Most recent upstream computations, by the contexts they concern.

    A context of the synthesis reads the partner rows of its HS vintage
    (``hs_vintage``) and period, joined to the network rows of the same
    vintage. A partner unit in force (classification = label of the in-force
    units) concerns the contexts in force; a historical partner unit of
    vintage ``V`` concerns the historical contexts of ``V``; a network unit of
    vintage ``V`` concerns every context of ``V``. A context is in force when
    its ``hs_vintage`` is the vintage in force in its year.

    Two families of marks, each the most recent ``last_computed`` per
    relevance key:

    * ``full``: changes that may alter **every period** of the contexts
      concerned — a partner unit computed for the first time (a reporter x
      product pair newly downloaded brings its whole history), recomputed
      after a change of methodology or forced, and any network unit (a BACI
      re-estimation rewrites every year of its vintage);
    * ``partner``: any partner computation, including ``new_data``, which
      only alters the most recent periods (the incremental download brings
      back the last observations only).

    Attributes:
        watermark: Most recent upstream computation, recorded on the context
            entries.
        full: Relevance key -> most recent complete change.
        partner: Relevance key -> most recent partner computation.
        nomenclatures: HS vintage -> entry year, to tell a context in force
            from a historical one; ``None`` makes every key relevant.
        vintage_column: Context column holding the HS vintage.
        period_column: Context column holding the period.

    Examples:
        >>> from kedro_pipeline.io.freshness import parse_instant
        >>> t = parse_instant("2026-01-01")
        >>> marks = UpstreamMarks.from_entries(
        ...     [], [RegistryEntry(Unit.of(vintage="HS2017"), t, reason="new_data")],
        ...     partner_label="HS2022", nomenclatures={"HS2017": 2017, "HS2022": 2022})
        >>> marks.full_mark(Unit.of(hs_vintage="HS2017", TIME_PERIOD="2019")) == t
        True
        >>> marks.full_mark(Unit.of(hs_vintage="HS2022", TIME_PERIOD="2023")) is None
        True
    """
    watermark: Optional[datetime]
    full: Mapping[Tuple[str, str], datetime]
    partner: Mapping[Tuple[str, str], datetime]
    nomenclatures: Optional[Mapping[str, int]] = None
    vintage_column: str = _VINTAGE_COLUMN
    period_column: str = _PERIOD_COLUMN

    # Construction à partir des entrées des registres amont
    @classmethod
    def from_entries(
        cls,
        partner_entries: Iterable[RegistryEntry],
        network_entries: Iterable[RegistryEntry],
        *,
        partner_label: Optional[str],
        nomenclatures: Optional[Mapping[str, int]],
        vintage_column: str = _VINTAGE_COLUMN,
        period_column: str = _PERIOD_COLUMN,
    ) -> "UpstreamMarks":
        """Summarise the partner and network registries into relevance marks.

        Args:
            partner_entries: Entries of the partner registries.
            network_entries: Entries of the network registry.
            partner_label: Classification of the partner units in force (the
                most recent vintage label); ``None`` treats every partner
                unit as in force.
            nomenclatures: HS vintage -> entry year.
            vintage_column: Context column holding the HS vintage.
            period_column: Context column holding the period.

        Returns:
            The marks.
        """
        full: Dict[Tuple[str, str], datetime] = {}
        partner: Dict[Tuple[str, str], datetime] = {}
        watermark: Optional[datetime] = None

        # Conservation du maximum par clé
        def keep(marks: Dict[Tuple[str, str], datetime], key: Tuple[str, str], when: datetime) -> None:
            if key not in marks or when > marks[key]:
                marks[key] = when

        for entry in partner_entries:
            if entry.last_computed is None:
                continue
            classification = entry.unit.get("classification")
            in_force = partner_label is None or classification in (None, partner_label)
            key = ("partners", "in_force" if in_force else str(classification))
            keep(partner, key, entry.last_computed)
            if entry.reason in _FULL_CHANGE_REASONS:
                keep(full, key, entry.last_computed)
            watermark = entry.last_computed if watermark is None else max(watermark, entry.last_computed)
        for entry in network_entries:
            if entry.last_computed is None:
                continue
            keep(full, ("network", str(entry.unit.get("vintage"))), entry.last_computed)
            watermark = entry.last_computed if watermark is None else max(watermark, entry.last_computed)
        return cls(watermark, full, partner, nomenclatures, vintage_column, period_column)

    # Clés de pertinence d'un contexte
    def relevance_keys(self, unit: Unit) -> Optional[Tuple[Tuple[str, str], ...]]:
        """Relevance keys of a context; ``None`` when every key is relevant.

        Args:
            unit: Context unit.

        Returns:
            The partner key (in force or historical vintage) and the network
            key of its vintage, or ``None`` when the context carries no HS
            vintage or its period cannot be placed in the nomenclatures.
        """
        vintage = unit.get(self.vintage_column)
        period = unit.get(self.period_column)
        if vintage is None or period is None or not self.nomenclatures:
            return None
        try:
            in_force = vintage == vintage_in_force(int(str(period)[:4]), self.nomenclatures)
        except ValueError:
            return None
        partner_key = ("partners", "in_force" if in_force else vintage)
        return (partner_key, ("network", vintage))

    # Maximum des marques pertinentes d'une famille
    def _mark(self, marks: Mapping[Tuple[str, str], datetime], unit: Unit) -> Optional[datetime]:
        keys = self.relevance_keys(unit)
        values = list(marks.values()) if keys is None else [marks[k] for k in keys if k in marks]
        return max(values) if values else None

    # Dernier changement complet pertinent pour un contexte
    def full_mark(self, unit: Unit) -> Optional[datetime]:
        """Most recent upstream change altering every period of the context."""
        return self._mark(self.full, unit)

    # Dernier calcul partenaire pertinent pour un contexte
    def partner_mark(self, unit: Unit) -> Optional[datetime]:
        """Most recent partner computation concerning the context."""
        return self._mark(self.partner, unit)


# Fonction de lecture des registres amont, par famille
def upstream_registry_groups(
    vulnerability_config: Mapping[str, Any],
    classification: str,
) -> Tuple[List[FreshnessRegistry], List[FreshnessRegistry]]:
    """Freshness registries of the partner and network steps, read only, by family.

    Args:
        vulnerability_config: Parsed ``config/vulnerabilities.yaml``.
        classification: Classification label of the partner units in force.

    Returns:
        ``(partner_registries, network_registries)``.
    """
    partners: List[FreshnessRegistry] = []
    for block in (vulnerability_config.get(_PARTNERS_ROOT) or {}).values():
        if isinstance(block, Mapping) and (block.get("STATE") or {}).get("PATH_TEMPLATE"):
            partners.append(partner_registry(block, classification))
    network = vulnerability_config.get(_NETWORK_ROOT) or {}
    networks = [network_registry(network)] if (network.get("STATE") or {}).get("PATH_TEMPLATE") else []
    return partners, networks


# ──────────────────────────────────────────────────────────────────────
# Fraîcheur par contexte : planification (fonctions pures)
# ──────────────────────────────────────────────────────────────────────

# Fonction de sélection des périodes récentes de la grille
def recent_period_values(
    units: Iterable[Unit], period_column: str, recent_periods: Optional[int]
) -> Optional[FrozenSet[str]]:
    """Return the ``recent_periods`` most recent periods among the contexts.

    Args:
        units: Planned context units.
        period_column: Context column holding the period.
        recent_periods: Number of periods; ``None`` means every period.

    Returns:
        The recent periods, or ``None`` when every period is recent.

    Examples:
        >>> units = [Unit.of(TIME_PERIOD=p) for p in ("2021", "2023", "2022")]
        >>> sorted(recent_period_values(units, "TIME_PERIOD", 2))
        ['2022', '2023']
    """
    if recent_periods is None:
        return None
    periods = sorted({str(unit.get(period_column)) for unit in units}, reverse=True)
    return frozenset(periods[: int(recent_periods)])


# Fonction de décision des contextes de synthèse à (re)calculer
def plan_synthesis_contexts(
    units: Sequence[Unit],
    registry: FreshnessRegistry,
    requested: Mapping[str, str],
    marks: UpstreamMarks,
    force: ForceSpec,
    *,
    method_names: Sequence[str],
    recent_periods: Optional[int],
    period_column: str = _PERIOD_COLUMN,
    adopt_legacy_fingerprints: bool = False,
) -> Dict[Unit, UnitPlan]:
    """Decide, per context, why it is recomputed and which methods.

    A context gets a single plan, the first matching reason winning:

    * ``first``: never computed → every method and the consensus;
    * ``forced``: within the forcing scope → the forced methods (and the
      consensus), or all of them;
    * ``new_data``: a relevant upstream change altering every period happened
      since the context was computed (see :class:`UpstreamMarks`), or the
      partner metrics changed and the context is one of the
      ``recent_periods`` most recent periods → every method;
    * ``fingerprint``: the methods whose fingerprint differs or is missing
      (added, modified or invalidated), plus the consensus; a stale input
      selection makes every method stale.

    Args:
        units: Contexts of the filtered grid.
        registry: Per-context registry of the synthesis.
        requested: Current fingerprints (:func:`synthesis_context_requested`).
        marks: Upstream marks.
        force: One-off forcing.
        method_names: Configured method names.
        recent_periods: Number of recent periods recomputed on partner new
            data (``None``: every period).
        period_column: Context column holding the period.
        adopt_legacy_fingerprints: Deployment migration flag.

    Returns:
        Mapping ``unit -> UnitPlan`` of the stale contexts.
    """
    recent = recent_period_values(units, period_column, recent_periods)

    # Règle « nouvelles données » : changement complet pertinent, ou données
    # partenaires nouvelles sur une période récente
    def is_new_data(unit: Unit, entry: RegistryEntry, _watermark: Optional[datetime]) -> bool:
        since = entry.upstream_watermark
        full = marks.full_mark(unit)
        if full is not None and (since is None or full > since):
            return True
        partner = marks.partner_mark(unit)
        if partner is not None and (since is None or partner > since):
            return recent is None or str(unit.get(period_column)) in recent
        return False

    plans = units_to_compute(
        units, registry, {}, requested, force, step=STEP,
        is_new_data=is_new_data, adopt_legacy_fingerprints=adopt_legacy_fingerprints,
    )
    return {
        unit: UnitPlan(plan.reason, expand_synthesis_names(plan.names, requested, method_names))
        for unit, plan in plans.items()
    }


# Fonction d'ordonnancement et de troncature des contextes planifiés
def select_contexts(
    plans: Mapping[Unit, UnitPlan],
    order: Sequence[Unit],
    period_column: str,
    max_contexts: Optional[int],
) -> Tuple[List[Unit], int]:
    """Order the stale contexts by decreasing period and apply the budget.

    Args:
        plans: Plans of the stale contexts.
        order: Every context, in grid order (ties keep this order).
        period_column: Context column holding the period.
        max_contexts: Budget; ``None`` keeps every stale context.

    Returns:
        ``(selected, backlog)``: the contexts computed in this run, most
        recent periods first, and the number left to the next runs.

    Examples:
        >>> units = [Unit.of(TIME_PERIOD=p) for p in ("2021", "2023", "2022")]
        >>> plans = {unit: UnitPlan("first", frozenset()) for unit in units}
        >>> selected, backlog = select_contexts(plans, units, "TIME_PERIOD", 2)
        >>> [unit.key for unit in selected], backlog
        (['2023', '2022'], 1)
    """
    stale = [unit for unit in order if unit in plans]
    stale.sort(key=lambda unit: str(unit.get(period_column) or ""), reverse=True)
    if max_contexts is None:
        return stale, 0
    return stale[: int(max_contexts)], max(0, len(stale) - int(max_contexts))


# Fonction de lecture de l'instant du dernier calcul d'une étape
def last_computation(registry: FreshnessRegistry) -> Optional[datetime]:
    """Most recent ``last_computed`` of a registry (``None`` when empty).

    Args:
        registry: Registry of the step.

    Returns:
        The instant.
    """
    instants = [entry.last_computed for entry in registry.iter_entries() if entry.last_computed]
    return max(instants) if instants else None


# Fonction de lecture de la demande de contrôle de cadence
def cadence_check_requested(flag: bool = False, environ: Optional[Mapping[str, str]] = None) -> bool:
    """Tell whether the run must honour the minimum interval between two runs.

    Args:
        flag: The ``--cadence-check`` command-line flag.
        environ: Environment (``os.environ`` by default); ``CADENCE_CHECK``
            set to ``1`` / ``true`` / ``yes`` / ``on`` requests the check.

    Returns:
        Whether the cadence is checked.

    Examples:
        >>> cadence_check_requested(False, {"CADENCE_CHECK": "1"})
        True
        >>> cadence_check_requested(False, {})
        False
    """
    environ = os.environ if environ is None else environ
    value = (environ.get("CADENCE_CHECK") or "").strip().lower()
    return bool(flag) or value in {"1", "true", "yes", "on"}


# Fonction de décision de saut par cadence
def skipped_by_cadence(
    last_computed: Optional[datetime],
    now: datetime,
    min_interval_days: float,
    forced: bool,
) -> bool:
    """Tell whether a run is skipped because the previous one is too recent.

    Useful while a daily workflow still calls the weekly steps: the run exits
    at once when the step ran less than ``min_interval_days`` ago, unless a
    recomputation is forced. In production, the weekly ``CronWorkflow``
    carries the cadence and the check is not requested.

    Args:
        last_computed: Most recent computation of the step (``None``: never).
        now: Current instant.
        min_interval_days: Minimum interval, in days.
        forced: Whether a recomputation of the step is forced.

    Returns:
        Whether the run is skipped.

    Examples:
        >>> from kedro_pipeline.io.freshness import parse_instant
        >>> now = parse_instant("2026-10-06")
        >>> skipped_by_cadence(parse_instant("2026-10-03"), now, 6, False)
        True
        >>> skipped_by_cadence(parse_instant("2026-10-03"), now, 6, True)
        False
        >>> skipped_by_cadence(None, now, 6, False)
        False
    """
    if forced or last_computed is None:
        return False
    return now - last_computed < timedelta(days=float(min_interval_days))


# Fonction de découpage d'une séquence en lots
def chunks(items: Sequence[Any], size: int) -> Iterator[List[Any]]:
    """Split a sequence into consecutive batches of at most ``size`` items.

    Args:
        items: Items to split.
        size: Batch size (at least 1).

    Yields:
        The batches.

    Examples:
        >>> list(chunks([1, 2, 3], 2))
        [[1, 2], [3]]
    """
    size = max(1, int(size))
    for start in range(0, len(items), size):
        yield list(items[start:start + size])


# Fonction de construction des tags de fraîcheur d'une exécution par contexte
def context_freshness_tags(
    plans: Mapping[Unit, UnitPlan], force: ForceSpec, requested: Collection[str], step: str
) -> Dict[str, str]:
    """Run tags of a per-context freshness decision.

    Args:
        plans: Plans of the stale contexts.
        force: One-off forcing.
        requested: Current fingerprint names.
        step: Step name.

    Returns:
        ``freshness_reasons`` (count per reason), plus ``forced`` when the
        step is forced.

    Examples:
        >>> context_freshness_tags({Unit.of(p="1"): UnitPlan("first", frozenset())},
        ...                        ForceSpec(), ["x"], "synthesis")
        {'freshness_reasons': 'first=1'}
    """
    counts = Counter(plan.reason for plan in plans.values())
    tags = {"freshness_reasons": ";".join(f"{reason}={counts[reason]}" for reason in sorted(counts))}
    if force.forces_step(step, requested):
        tags["forced"] = force.describe() or f"steps={step}"
    return tags


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
# Calcul d'un contexte (fonction pure) et écriture par lots
# ──────────────────────────────────────────────────────────────────────

# Résultat du calcul d'un contexte, prêt à être écrit
@dataclass
class ContextResult:
    """Outcome of the synthesis of one context, before any write.

    Attributes:
        unit: Freshness unit of the context.
        context: Typed context values (SQL literals of the replacement).
        methods: Methods fitted, ``None`` meaning every configured method.
        scores: Score rows of the fitted methods and of the consensus
            (one row per cell and per method, with a (score, rank) pair per level).
        fit: ``fit``-family diagnostics of the fitted methods (long
            diagnostic table).
        report: Synthesis report of the context.
        with_consensus: Whether the rows of the consensus pseudo-methods are
            part of ``scores`` (``False`` for the parallel phase of a context
            whose consensus waits for its sequential methods).
    """
    unit: Unit
    context: Tuple[Any, ...]
    methods: Optional[Tuple[str, ...]]
    scores: pd.DataFrame
    fit: pd.DataFrame
    report: SynthesisReport
    with_consensus: bool = True


# Fonction de calcul d'un contexte (aucune lecture ni écriture)
def compute_synthesis_context(
    df_context: pd.DataFrame,
    config: SynthesisConfig,
    *,
    methods: Optional[Sequence[str]] = None,
    df_existing_scores: Optional[pd.DataFrame] = None,
    tracker: Any = None,
    log_artifacts: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, SynthesisReport]:
    """Synthesise one context: pure computation, no read, no write.

    The middle step of an incremental run (planning → computation of each
    context → batched writes): it depends on its arguments only, so the
    contexts of a run can be computed in separate processes, the parent
    process remaining the only writer.

    Args:
        df_context: Metric rows of a single context.
        config: Synthesis configuration.
        methods: Methods to fit (``None``: all; empty: consensus alone).
        df_existing_scores: Stored scores of the methods left out, fed to the
            consensus (see :func:`~macroforecast.trade.aggregation.run_synthesis`).
        tracker: Experiment tracker receiving the artifacts (null by default).
        log_artifacts: Whether to log the artifacts.

    Returns:
        ``(df_scores, df_fit, report)`` of the context.
    """
    from macroforecast.tracking import NULL_TRACKER

    return run_synthesis(
        df_context,
        config,
        methods=methods,
        df_existing_scores=df_existing_scores,
        tracker=tracker if tracker is not None else NULL_TRACKER,
        log_artifacts=log_artifacts,
    )


# Fonction de construction du prédicat de remplacement d'une table longue
def replacement_predicate(
    context_columns: Sequence[str],
    groups: Mapping[Tuple[str, ...], Sequence[Sequence[Any]]],
    column: str,
    restriction: Optional[str] = None,
) -> Optional[str]:
    """Build the condition selecting the long-table rows a batch replaces.

    The rows of a context are replaced for the names it recomputed only (the
    stored rows of the other methods stay); contexts recomputing the same
    names are grouped into one row-value ``IN`` list.

    Args:
        context_columns: Ordered context key columns.
        groups: Recomputed names -> contexts recomputing exactly them.
        column: Column holding the name (``method`` or ``item_a``).
        restriction: Extra predicate (``"family" = 'fit'``), or ``None``.

    Returns:
        The SQL condition, or ``None`` when no row is to be replaced.

    Examples:
        >>> print(replacement_predicate(["freq"], {("mpi",): [("A",)]}, "method"))
        (("freq") IN (
            ('A')
          ) AND "method" IN ('mpi'))
    """
    parts = [
        f'({context_in_predicate(context_columns, contexts)} AND "{column}" IN ({_sql_list(names)}))'
        for names, contexts in groups.items()
        if names and contexts
    ]
    if not parts:
        return None
    predicate = " OR ".join(parts)
    return f"{restriction} AND ({predicate})" if restriction else predicate


# Fonction d'écriture d'un lot de contextes calculés
def write_synthesis_batch(
    results: Sequence[ContextResult],
    config: SynthesisConfig,
    *,
    scores_table: DuckLakeTable,
    diagnostics_table: DuckLakeTable,
    run_id: Optional[str] = None,
    commit_message: Optional[str] = None,
) -> bool:
    """Write the scores and fit diagnostics of a batch of contexts.

    Each table is written in one transaction: the stored rows of the
    recomputed methods (and of the consensus) of every context of the batch
    are deleted, then the new rows are upserted. The rows are thus
    **replaced**, not merged: a diagnostic emitted by the previous fit and not
    by this one (a method skipped then, scored now) disappears. The rows of
    the methods not recomputed are left untouched.

    Args:
        results: Computed contexts.
        config: Synthesis configuration.
        scores_table: Handle of the score table.
        diagnostics_table: Handle of the long diagnostic table.
        run_id: Run identifier recorded on the snapshots.
        commit_message: Commit message recorded on the snapshots.

    Returns:
        Whether one of the two tables was created by this write.
    """
    if not results:
        return False
    context_columns = list(config.context_columns)
    scores_keys = [*context_columns, config.reporter_col, config.product_col, _METHOD_COLUMN]
    diagnostics_keys = [
        *context_columns, "level", config.reporter_col, config.product_col,
        _FAMILY_COLUMN, "statistic", _ITEM_A_COLUMN, "item_b",
    ]
    all_methods = tuple(spec.name for spec in config.methods)
    consensus = tuple(f"{CONSENSUS_PREFIX}{rule}" for rule in config.consensus)

    # Contextes regroupés par ensemble de noms recalculés
    score_groups: Dict[Tuple[str, ...], List[Tuple[Any, ...]]] = {}
    fit_groups: Dict[Tuple[str, ...], List[Tuple[Any, ...]]] = {}
    for result in results:
        methods = all_methods if result.methods is None else tuple(result.methods)
        names = methods + consensus if result.with_consensus else methods
        score_groups.setdefault(names, []).append(result.context)
        fit_groups.setdefault(methods, []).append(result.context)

    df_scores = pd.concat([result.scores for result in results], ignore_index=True)
    created = scores_table.upsert_many(
        [df_scores], scores_keys,
        delete_where=replacement_predicate(context_columns, score_groups, _METHOD_COLUMN),
        run_id=run_id, commit_message=commit_message,
    )
    fits = [result.fit for result in results if not result.fit.empty]
    fit_where = replacement_predicate(
        context_columns, fit_groups, _ITEM_A_COLUMN,
        restriction=f'"{_FAMILY_COLUMN}" = {_sql_literal(FIT_FAMILY)}',
    )
    if fits or (fit_where is not None and diagnostics_table.exists()):
        created_fit = diagnostics_table.upsert_many(
            [pd.concat(fits, ignore_index=True)] if fits else [], diagnostics_keys,
            delete_where=fit_where, run_id=run_id, commit_message=commit_message,
        )
        created = created or created_fit
    return created


# Fonction de construction de l'entrée de registre d'un contexte calculé
def context_entry(
    unit: Unit,
    plan: UnitPlan,
    computed_at: datetime,
    watermark: Optional[datetime],
    requested: Mapping[str, str],
    previous: Optional[RegistryEntry],
    **counters: Any,
) -> RegistryEntry:
    """Registry entry of a context once its plan was computed and written.

    The fingerprints of the recomputed names are set to their current value;
    those of the names left untouched keep their recorded value.

    Args:
        unit: Context unit.
        plan: Plan computed (its reason cascades to the coherence).
        computed_at: Instant captured before the run started.
        watermark: Upstream instant taken into account.
        requested: Current fingerprints.
        previous: Previous entry of the context, or ``None``.
        **counters: Extra fields (``n_cells``, ``methods``…).

    Returns:
        The entry.
    """
    fingerprints = dict(previous.fingerprints) if previous is not None else {}
    fingerprints.update({name: requested[name] for name in plan.names if name in requested})
    return RegistryEntry(
        unit=unit,
        last_computed=computed_at,
        upstream_watermark=watermark,
        fingerprints=fingerprints,
        reason=plan.reason,
        extra=dict(counters),
    )


# ──────────────────────────────────────────────────────────────────────
# Orchestration lecture -> run_synthesis -> écriture (scores et diagnostics)
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

    Complete (non incremental) synthesis of every context of ``query``, with
    every configured method; the incremental runs go through
    :func:`run_incremental_synthesis`. Isolates the DB-bound core from
    connection setup and freshness bookkeeping, so it is callable on any pair
    of already-open connections, tests included. The failure of one context
    does not interrupt the others. Each context is written as soon as it is
    computed, its rows replacing the stored rows of its methods.

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
        log_artifacts: Whether to log the synthesis artifacts (top cells,
            weights, automatic selections, transport reports) to the tracker.
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
    scores_table = DuckLakeTable(scores_conn, catalog_alias, result_schema)
    diagnostics_table = DuckLakeTable(diagnostics_conn, catalog_alias, diagnostics_schema)

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
                df_scores, df_fit, report = compute_synthesis_context(
                    df_context, config, tracker=tracker, log_artifacts=log_artifacts,
                )
                # Écriture : les lignes du contexte remplacent celles de ses méthodes
                created = write_synthesis_batch(
                    [ContextResult(
                        unit=context_unit(context_columns, context), context=context,
                        methods=None, scores=df_scores, fit=df_fit, report=report,
                    )],
                    config,
                    scores_table=scores_table,
                    diagnostics_table=diagnostics_table,
                    run_id=workflow_run_id(),
                    commit_message=(
                        f"compute_synthetic_scores {'/'.join(str(value) for value in context)}"
                    ),
                )
                created_any = created_any or created
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

        _publish_run(
            tracker, scope, reports, failures, created_any, n_contexts,
            result_schema=result_schema,
        )

    return reports, failures, created_any, n_contexts


# Fonction de publication des métriques et du rapport d'une exécution
def _publish_run(
    tracker: Any,
    scope: Optional[RunScope],
    reports: Sequence[SynthesisReport],
    failures: Mapping[str, Exception],
    created_any: bool,
    n_contexts: int,
    *,
    result_schema: str,
) -> None:
    """Log the aggregated metrics and tags of a run, then publish its report.

    Args:
        tracker: Capturing tracker of the run.
        scope: Run-report scope, or ``None``.
        reports: One report per synthesised context.
        failures: Per-context exceptions.
        created_any: Whether a schema was created.
        n_contexts: Number of contexts attempted.
        result_schema: Target schema of the scores.
    """
    # Métriques et tags de l'exécution : un rapport agrégé sur tous les contextes réussis
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


# Résultat d'une exécution incrémentale
@dataclass
class IncrementalOutcome:
    """Outcome of an incremental run (synthesis or coherence).

    Attributes:
        reports: One report per context computed and written.
        failures: Per-context exceptions, keyed by context key.
        created_any: Whether a result schema was created.
        plans: Plans of every stale context (computed or left to the budget).
        computed: Contexts computed and written, in order.
        backlog: Stale contexts left to the next runs by the budget.
    """
    reports: List[Any] = field(default_factory=list)
    failures: Dict[str, Exception] = field(default_factory=dict)
    created_any: bool = False
    plans: Dict[Unit, UnitPlan] = field(default_factory=dict)
    computed: List[Unit] = field(default_factory=list)
    backlog: int = 0


# Fonction d'exécution incrémentale de la synthèse (planification, calcul, écriture)
def run_incremental_synthesis(
    scores_conn: Any,
    diagnostics_conn: Any,
    config: SynthesisConfig,
    *,
    sources: Sequence[Mapping[str, Any]],
    filters: Mapping[str, Any],
    catalog_alias: str,
    result_schema: str,
    diagnostics_schema: str,
    registry: FreshnessRegistry,
    requested: Mapping[str, str],
    marks: UpstreamMarks,
    force: ForceSpec,
    settings: IncrementalSettings,
    flow_codes: Optional[Sequence[int]] = None,
    vintages: Optional[str] = None,
    adopt_legacy_fingerprints: bool = False,
    computed_at: Optional[datetime] = None,
    run_id: Optional[str] = None,
    tracker: Any = None,
    log_artifacts: bool = True,
    scope: Optional[RunScope] = None,
    reader: Any = None,
) -> IncrementalOutcome:
    """Synthesise the stale contexts, by context and by method.

    Three separate steps, so that the middle one can be parallelised:

    1. **planning**: the distinct contexts of the filtered grid (one SQL
       query, no metric read), the plan of each one
       (:func:`plan_synthesis_contexts`), the order (most recent periods
       first) and the catch-up budget;
    2. **computation** of each context (:func:`compute_context_task`), in
       ``settings.n_jobs`` worker processes, each with a read connection of
       its own, restricted to the methods of its plan, the consensus being fed
       with the stored scores of the other methods; the methods of
       ``settings.sequential_methods`` are computed afterwards in this
       process, for the same contexts, the consensus coming last;
    3. **writes** by batches of ``settings.write_batch_contexts`` contexts
       (:func:`write_synthesis_batch`), then the registry entries of the batch
       (the registry advances batch by batch: an interrupted run keeps what it
       wrote).

    The metric rows are read context by context, never as a whole. The failure of one context does not interrupt the
    others; a failed write fails the contexts of its batch.

    Args:
        scores_conn: Open connection reading the sources and writing the scores.
        diagnostics_conn: Open connection writing the fit diagnostics.
        config: Synthesis configuration.
        sources: ``SYNTHESIS.SOURCES`` block.
        filters: ``SYNTHESIS.FILTERS`` block.
        catalog_alias: DuckLake catalog alias.
        result_schema: Target schema of the scores.
        diagnostics_schema: Target schema of the fit diagnostics.
        registry: Per-context registry of the synthesis.
        requested: Current fingerprints (:func:`synthesis_context_requested`).
        marks: Upstream marks.
        force: One-off forcing.
        settings: Incremental settings.
        flow_codes: Flow codes of the synthesised directions.
        vintages: ``SYNTHESIS.VINTAGES``.
        adopt_legacy_fingerprints: Deployment migration flag.
        computed_at: Instant recorded as ``last_computed`` (captured before the
            run by default).
        run_id: Run identifier recorded on the snapshots.
        tracker: Experiment tracker; the null tracker by default.
        log_artifacts: Whether to log the synthesis artifacts.
        scope: Run-report scope, or ``None``.
        reader: Opens the read connection of a worker (a
            :class:`~kedro_pipeline.io.ducklake.ConnectionReader`). ``None``
            lends ``scores_conn``, which only works with one process.

    Returns:
        The outcome of the run.

    Raises:
        ValueError: If several processes are requested without a ``reader``.
    """
    if tracker is None:
        from macroforecast.tracking import NULL_TRACKER

        tracker = NULL_TRACKER
    tracker = CapturingTracker(tracker)
    computed_at = computed_at or _now()
    if reader is None:
        if settings.n_jobs > 1:
            raise ValueError("n_jobs > 1 requires a `reader` (connector factory), not a shared connection.")
        reader = BorrowedReader(scores_conn)
    context_columns = list(config.context_columns)
    method_names = [spec.name for spec in config.methods]
    scores_table = DuckLakeTable(scores_conn, catalog_alias, result_schema)
    diagnostics_table = DuckLakeTable(diagnostics_conn, catalog_alias, diagnostics_schema)
    outcome = IncrementalOutcome()

    with tracker, (guarded_run(scope, tracker) if scope is not None else nullcontext()):
        # 1. Planification : contextes de la grille, plans, ordre et budget
        contexts = read_contexts(
            scores_conn,
            build_contexts_query(sources, filters, catalog_alias, context_columns, flow_codes, vintages),
        )
        typed = {context_unit(context_columns, context): context for context in contexts}
        units = list(typed)
        outcome.plans = plan_synthesis_contexts(
            units, registry, requested, marks, force,
            method_names=method_names,
            recent_periods=settings.recent_periods,
            period_column=settings.period_column,
            adopt_legacy_fingerprints=adopt_legacy_fingerprints,
        )
        selected, outcome.backlog = select_contexts(
            outcome.plans, units, settings.period_column, settings.max_contexts
        )
        tracker.log_metrics(
            {
                **plan_metrics(outcome.plans, n_candidates=len(units)),
                "freshness/skipped_by_cadence": 0.0,
                f"{STEP}/contexts_planned": float(len(outcome.plans)),
                f"{STEP}/contexts_run": float(len(selected)),
                f"{STEP}/contexts_budget_left": float(outcome.backlog),
            }
        )
        tracker.set_tags(context_freshness_tags(outcome.plans, force, requested, STEP))
        # Logging
        logger.info(
            f"{len(units)} contexte(s) dans la grille, {len(outcome.plans)} périmé(s), "
            f"{len(selected)} calculé(s) dans cette exécution, {outcome.backlog} reporté(s)."
        )

        # 2-3. Calcul dans les workers, écriture par lots dans ce processus
        artifacts = ArtifactCollector()
        wall_started = time.perf_counter()
        cpu_seconds = _synthesise_contexts(
            selected, outcome,
            scores_conn=scores_conn, reader=reader, config=config, typed=typed,
            sources=sources, filters=filters, catalog_alias=catalog_alias,
            flow_codes=flow_codes, vintages=vintages, result_schema=result_schema,
            scores_table=scores_table, diagnostics_table=diagnostics_table,
            registry=registry, requested=requested, marks=marks,
            computed_at=computed_at, run_id=run_id, settings=settings,
            log_artifacts=log_artifacts, artifacts=artifacts,
        )
        # Artefacts des workers rejoués dans le run, comme un calcul séquentiel
        artifacts.replay(tracker)
        tracker.log_metrics(
            {
                "timing/wall_seconds": time.perf_counter() - wall_started,
                "timing/cpu_seconds_sum": cpu_seconds,
                "parallel/n_jobs": float(settings.n_jobs),
            }
        )

        # Rapport de run seulement si un contexte était périmé : une exécution
        # sans rien à recalculer ne doit pas lever le contrôle « contextes calculés »
        if outcome.plans:
            _publish_run(
                tracker, scope, outcome.reports, outcome.failures, outcome.created_any,
                len(selected), result_schema=result_schema,
            )
    # Entrées de registre déjà écrites lot par lot ; écriture de garde des fragments
    # restés modifiés
    registry.save()
    return outcome


# Tâche de calcul d'un contexte, envoyée à un worker
@dataclass(frozen=True)
class SynthesisTask:
    """Everything a worker needs to synthesise one context.

    The task carries no connection: the worker opens its own through
    ``reader``, reads the rows of its context, computes and returns the result.
    It never writes.

    Attributes:
        rank: Position of the context in the run plan.
        unit: Freshness unit of the context.
        context: Typed context values.
        config: Synthesis configuration (without consensus when
            ``with_consensus`` is false).
        methods: Methods to fit (``None``: all; empty: consensus alone).
        with_consensus: Whether the consensus is computed by this task.
        kept_methods: Stored methods whose scores feed the consensus; empty
            when the consensus is not computed or every method is fitted.
        reader: Opens the read connection (see
            :class:`~kedro_pipeline.io.ducklake.ConnectionReader`).
        sources: ``SYNTHESIS.SOURCES`` block.
        filters: ``SYNTHESIS.FILTERS`` block.
        catalog_alias: DuckLake catalog alias.
        result_schema: Schema of the scores.
        flow_codes: Flow codes of the synthesised directions.
        vintages: ``SYNTHESIS.VINTAGES``.
        log_artifacts: Whether the artifacts are recorded for the parent.
    """
    rank: int
    unit: Unit
    context: Tuple[Any, ...]
    config: SynthesisConfig
    methods: Optional[Tuple[str, ...]]
    with_consensus: bool
    kept_methods: Tuple[str, ...]
    reader: Any
    sources: Sequence[Mapping[str, Any]]
    filters: Mapping[str, Any]
    catalog_alias: str
    result_schema: str
    flow_codes: Optional[Sequence[int]]
    vintages: Optional[str]
    log_artifacts: bool


# Résultat d'une tâche de calcul, renvoyé au processus parent
@dataclass
class TaskOutcome:
    """Outcome of one :class:`SynthesisTask`, picklable.

    Attributes:
        task_rank: Rank of the context in the plan.
        unit: Freshness unit of the context.
        result: Computed context, ``None`` on failure or when the context
            vanished from the grid.
        recorded: Tracker calls recorded by the worker.
        cpu_seconds: CPU time spent by the task.
        error: Serialisable exception of a failed context.
        missing: Whether the context was absent from the read.
    """
    task_rank: int
    unit: Unit
    result: Optional[ContextResult] = None
    recorded: Optional[RecordingTracker] = None
    cpu_seconds: float = 0.0
    error: Optional[BaseException] = None
    missing: bool = False


# Fonction de calcul d'un contexte dans un worker (lecture, calcul, aucune écriture)
def compute_context_task(task: SynthesisTask) -> TaskOutcome:
    """Read and synthesise one context with a connection of its own.

    Module-level function so that it can be sent to a worker process. Errors
    are returned, never raised: the failure of a context must not stop the
    others.

    Args:
        task: Context to compute.

    Returns:
        The outcome of the task.
    """
    started = time.process_time()
    outcome = TaskOutcome(task_rank=task.rank, unit=task.unit)
    context_columns = list(task.config.context_columns)
    cell_columns = (task.config.reporter_col, task.config.product_col)
    try:
        with task.reader() as conn:
            df_context = read_source_metrics(
                conn,
                build_source_query(
                    task.sources, task.filters, task.catalog_alias, task.flow_codes,
                    task.vintages, contexts=[task.context], context_columns=context_columns,
                ),
                cell_columns,
            )
            if df_context.empty:
                # Contexte disparu de la grille entre la planification et la lecture
                outcome.missing = True
                return outcome
            # Scores stockés des méthodes non recalculées, pour le seul consensus
            df_existing: Optional[pd.DataFrame] = None
            if task.kept_methods and DuckLakeTable(
                conn, task.catalog_alias, task.result_schema
            ).exists():
                df_existing = read_source_metrics(
                    conn,
                    build_scores_query(
                        task.catalog_alias, task.result_schema, context_columns,
                        [task.context], methods=task.kept_methods,
                    ),
                    cell_columns,
                )
        recorder = RecordingTracker() if task.log_artifacts else None
        df_scores, df_fit, report = compute_synthesis_context(
            df_context, task.config, methods=task.methods, df_existing_scores=df_existing,
            tracker=recorder, log_artifacts=task.log_artifacts,
        )
        outcome.result = ContextResult(
            unit=task.unit, context=task.context, methods=task.methods,
            scores=df_scores, fit=df_fit, report=report, with_consensus=task.with_consensus,
        )
        outcome.recorded = recorder
    except Exception as exc:
        # Journalisation côté worker ; l'exception transmissible part au parent
        logger.exception(f"Échec de la synthèse pour le contexte {task.unit}")
        outcome.error = serialisable_exception(exc)
    outcome.cpu_seconds = time.process_time() - started
    return outcome


# Fonction de fusion des rapports des deux phases d'un même contexte
def _merge_phase_reports(first: SynthesisReport, second: SynthesisReport) -> SynthesisReport:
    """Merge the reports of the parallel and the sequential phase of a context.

    The context and its cells are counted once (by the first phase); the
    cells scored, the skipped methods and the timings of the second phase are
    added.

    Args:
        first: Report of the parallel phase.
        second: Report of the sequential phase.

    Returns:
        The merged report (``first``, updated in place).
    """
    for level, level_report in second.levels.items():
        merged = first.levels.setdefault(level, LevelReport())
        merged.n_cells_scored += level_report.n_cells_scored
        merged.n_methods_skipped += level_report.n_methods_skipped
        for name, seconds in level_report.seconds.items():
            merged.seconds[name] = merged.seconds.get(name, 0.0) + seconds
    return first


# Fonction de calcul parallèle puis d'écriture des contextes sélectionnés
def _synthesise_contexts(
    selected: Sequence[Unit],
    outcome: IncrementalOutcome,
    *,
    scores_conn: Any,
    reader: Any,
    config: SynthesisConfig,
    typed: Mapping[Unit, Tuple[Any, ...]],
    sources: Sequence[Mapping[str, Any]],
    filters: Mapping[str, Any],
    catalog_alias: str,
    flow_codes: Optional[Sequence[int]],
    vintages: Optional[str],
    result_schema: str,
    scores_table: DuckLakeTable,
    diagnostics_table: DuckLakeTable,
    registry: FreshnessRegistry,
    requested: Mapping[str, str],
    marks: UpstreamMarks,
    computed_at: datetime,
    run_id: Optional[str],
    settings: IncrementalSettings,
    log_artifacts: bool,
    artifacts: ArtifactCollector,
) -> float:
    """Compute the selected contexts in worker processes and write them.

    The workers read and compute; this (parent) process is the only writer. The
    results are written by batches of ``settings.write_batch_contexts``
    contexts, in arrival order, the registry advancing batch by batch.

    A context whose plan contains a sequential method is handled in two
    phases. The parallel phase fits its other methods and writes them without
    consensus; the registry does not advance. Once every context went through
    the parallel phase, the parent fits the sequential methods of those
    contexts, one at a time, with the stored scores of the other methods: the
    consensus is thus computed last, once all the methods are available, and
    written once.

    Args:
        selected: Contexts to compute, in plan order.
        outcome: Outcome of the run, updated in place.
        scores_conn: Parent connection (sequential phase).
        reader: Reader opening the connection of a worker.
        config: Synthesis configuration.
        typed: Typed context values, by unit.
        sources: ``SYNTHESIS.SOURCES`` block.
        filters: ``SYNTHESIS.FILTERS`` block.
        catalog_alias: DuckLake catalog alias.
        flow_codes: Flow codes of the synthesised directions.
        vintages: ``SYNTHESIS.VINTAGES``.
        result_schema: Schema of the scores.
        scores_table: Handle of the score table.
        diagnostics_table: Handle of the diagnostic table.
        registry: Per-context registry of the synthesis.
        requested: Current fingerprints.
        marks: Upstream marks.
        computed_at: Instant recorded as ``last_computed``.
        run_id: Run identifier recorded on the snapshots.
        settings: Incremental settings (``n_jobs``, ``sequential_methods``,
            batch size).
        log_artifacts: Whether the artifacts are recorded.
        artifacts: Collector of the artifacts recorded by the workers.

    Returns:
        The CPU seconds spent by the tasks (workers and sequential phase).

    Raises:
        ValueError: If ``sequential_methods`` names an unconfigured method.
    """
    method_names = [spec.name for spec in config.methods]
    sequential = set(settings.sequential_methods)
    unknown = sequential - set(method_names)
    if unknown:
        raise ValueError(
            f"SEQUENTIAL_METHODS names method(s) {sorted(unknown)} absent from the configuration."
        )
    plans = outcome.plans
    methods_of = {unit: plan_methods(plans[unit].names, method_names) for unit in selected}
    no_consensus = replace(config, consensus=())
    cpu_seconds = 0.0

    # Construction de la tâche d'un contexte pour une phase donnée
    def make_task(rank: int, unit: Unit, methods: Optional[Tuple[str, ...]],
                  with_consensus: bool, task_reader: Any) -> SynthesisTask:
        fitted = set(method_names if methods is None else methods)
        # Scores stockés des méthodes laissées de côté : seul le consensus les lit
        kept = tuple(name for name in method_names if name not in fitted) if with_consensus else ()
        return SynthesisTask(
            rank=rank, unit=unit, context=typed[unit],
            config=config if with_consensus else no_consensus,
            methods=methods, with_consensus=with_consensus, kept_methods=kept,
            reader=task_reader, sources=sources, filters=filters, catalog_alias=catalog_alias,
            result_schema=result_schema, flow_codes=flow_codes, vintages=vintages,
            log_artifacts=log_artifacts,
        )

    # Phase parallèle : méthodes du plan hors méthodes séquentielles
    tasks: List[SynthesisTask] = []
    deferred: Dict[Unit, Tuple[int, Tuple[str, ...]]] = {}
    for rank, unit in enumerate(selected):
        planned = tuple(method_names) if methods_of[unit] is None else methods_of[unit]
        sequential_part = tuple(name for name in planned if name in sequential)
        if not sequential_part:
            tasks.append(make_task(rank, unit, methods_of[unit], True, reader))
            continue
        deferred[unit] = (rank, sequential_part)
        parallel_part = tuple(name for name in planned if name not in sequential)
        if parallel_part:
            tasks.append(make_task(rank, unit, parallel_part, False, reader))

    first_phase_reports: Dict[Unit, SynthesisReport] = {}

    # Écriture d'un lot de résultats, puis avancement du registre des contextes terminés
    def flush(results: List[ContextResult], final: bool) -> None:
        if not results:
            return
        try:
            created = write_synthesis_batch(
                results, config,
                scores_table=scores_table, diagnostics_table=diagnostics_table,
                run_id=run_id,
                commit_message=f"{NODE} {len(results)} contexte(s) "
                               f"{results[0].unit.key} .. {results[-1].unit.key}",
            )
        except Exception as exc:
            logger.exception(f"Échec de l'écriture d'un lot de {len(results)} contexte(s)")
            for result in results:
                outcome.failures[result.unit.key] = exc
                deferred.pop(result.unit, None)
            return
        outcome.created_any = outcome.created_any or created
        for result in results:
            report = result.report
            if not final and result.unit in deferred:
                # Phase parallèle d'un contexte à méthode séquentielle : pas terminé
                first_phase_reports[result.unit] = report
                continue
            if result.unit in first_phase_reports:
                report = _merge_phase_reports(first_phase_reports.pop(result.unit), report)
            planned = methods_of[result.unit]
            registry.upsert(
                context_entry(
                    result.unit, plans[result.unit], computed_at, marks.watermark, requested,
                    registry.get(result.unit),
                    n_cells=int(report.n_cells),
                    methods=list(planned) if planned is not None else "all",
                )
            )
            outcome.reports.append(report)
            outcome.computed.append(result.unit)
        registry.save()
        # Logging
        logger.info(f"Lot de {len(results)} contexte(s) écrit et enregistré.")

    # Calcul d'une phase, écriture par lots dans l'ordre d'arrivée
    def run_phase(phase_tasks: Sequence[SynthesisTask], n_jobs: int, final: bool) -> None:
        nonlocal cpu_seconds
        pending: List[ContextResult] = []
        for _, task_outcome in parallel_map(compute_context_task, phase_tasks, n_jobs):
            cpu_seconds += task_outcome.cpu_seconds
            unit = task_outcome.unit
            if task_outcome.error is not None:
                outcome.failures[unit.key] = task_outcome.error
                deferred.pop(unit, None)
                continue
            if task_outcome.missing:
                logger.warning(f"Contexte {unit} absent de la lecture des métriques, ignoré.")
                deferred.pop(unit, None)
                continue
            if task_outcome.recorded is not None:
                artifacts.add(task_outcome.task_rank, task_outcome.recorded)
            pending.append(task_outcome.result)
            if len(pending) >= settings.write_batch_contexts:
                flush(pending, final)
                pending = []
        flush(pending, final)

    run_phase(tasks, settings.n_jobs, final=False)

    # Phase séquentielle : méthodes séquentielles puis consensus, dans le parent
    remaining = sorted(
        ((rank, unit, part) for unit, (rank, part) in deferred.items()),
        key=lambda item: item[0],
    )
    if remaining:
        borrowed = BorrowedReader(scores_conn)
        run_phase(
            [make_task(rank, unit, part, True, borrowed) for rank, unit, part in remaining],
            1, final=True,
        )
    return cpu_seconds


# ──────────────────────────────────────────────────────────────────────
# Point d'entrée
# ──────────────────────────────────────────────────────────────────────

# Nœud du rapport de run (clé de config/tracking.yaml)
NODE = "compute_synthetic_scores"


# Fonction de lecture des arguments de la ligne de commande
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse the command line of the per-context steps (synthesis, coherence).

    Args:
        argv: Arguments (``sys.argv[1:]`` when ``None``).

    Returns:
        Namespace with ``cadence_check``.

    Examples:
        >>> parse_args(["--cadence-check"]).cadence_check
        True
    """
    parser = argparse.ArgumentParser(
        description="Incremental per-context step of the synthesis (scores or coherence)."
    )
    parser.add_argument(
        "--cadence-check", action="store_true",
        help="exit at once (code 0) when the step ran less than "
             "CADENCE.MIN_INTERVAL_DAYS ago and no recomputation is forced "
             "(also requested by CADENCE_CHECK=1)",
    )
    return parser.parse_args(argv)


# Fonction de lecture optionnelle de la configuration de téléchargement Eurostat
def _optional_eurostat_config() -> Optional[Mapping[str, Any]]:
    """Read the Eurostat download configuration, ``None`` when absent.

    It only provides the default of ``RECENT_PERIODS`` (depth of the
    incremental download).
    """
    from scripts.compute_trade_vulnerabilities import load_eurostat_config

    try:
        return load_eurostat_config()
    except FileNotFoundError:
        return None


# Fonction de journalisation d'une exécution sautée par cadence
def log_cadence_skip(tracker: Any, step: str, last: Optional[datetime]) -> None:
    """Log the metric and tag of a run skipped by the cadence check.

    Args:
        tracker: Experiment tracker of the run.
        step: Step name.
        last: Most recent computation of the step.
    """
    with tracker:
        tracker.log_metrics({"freshness/skipped_by_cadence": 1.0})
        tracker.set_tags({"freshness_reasons": "cadence"})
    # Logging
    logger.info(f"Étape '{step}' calculée le {last} : exécution sautée (cadence).")


# Fonction principale de calcul des scores synthétiques
def main(argv: Optional[Sequence[str]] = None) -> None:
    """CLI entry point for the incremental synthetic-score computation.

    Args:
        argv: Command-line arguments (``sys.argv[1:]`` when ``None``).

    Raises:
        RuntimeError: If at least one context failed, once every context has
            been attempted.
    """
    args = parse_args(argv)
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
    # Nomenclatures synthétisées (en vigueur seulement, ou aussi les historiques)
    vintages = load_synthesis_vintages(synthesis_config, config)
    sources = synthesis_config["SOURCES"]
    filters = synthesis_config.get("FILTERS") or {}
    settings = incremental_settings(synthesis_config, _optional_eurostat_config())

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

    # Fraîcheur : registre par contexte (fragment = période), empreintes par
    # méthode, forçage (FORCE historique du YAML équivalent à FORCE_STEPS)
    runtime_config = load_runtime_config()
    nomenclatures = runtime_config["NOMENCLATURES"]["HS"]
    registry = context_registry(
        synthesis_config, synthesis_config["BUCKET"], STEP, config.context_columns
    )
    requested = synthesis_context_requested(config, sources, filters, flows, vintages)
    force = ForceSpec.from_runtime(runtime_config)
    if synthesis_config.get("FORCE", False):
        force = replace(force, steps=force.steps | {STEP})

    # Cadence : sortie immédiate si la dernière exécution est trop récente
    if cadence_check_requested(args.cadence_check):
        last = last_computation(registry)
        if skipped_by_cadence(last, _now(), settings.min_interval_days,
                              force.forces_step(STEP, requested)):
            log_cadence_skip(tracker, STEP, last)
            return

    # Amont : marques des registres partenaires et réseau, par millésime
    classification = partner_classification(nomenclatures)
    partner_registries, network_registries = upstream_registry_groups(
        vulnerability_config, classification
    )
    marks = UpstreamMarks.from_entries(
        (entry for registry_ in partner_registries for entry in registry_.iter_entries()),
        (entry for registry_ in network_registries for entry in registry_.iter_entries()),
        partner_label=classification,
        nomenclatures=nomenclatures,
    )

    # Instant de référence capturé avant le calcul : la date enregistrée
    # correspond au début du traitement, jamais après, pour ne pas rater une
    # mise à jour survenue pendant le calcul
    computed_at = _now()

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

    # Macros de nomenclature de la session (product_code des conditions de
    # jointure : zéro initial des codes stockés en entiers)
    macros = nomenclature_macros_sql(nomenclatures)
    # Lecteur des workers : chacun ouvre sa propre connexion (jamais partagée)
    reader = ConnectionReader(
        partial(
            _result_connector, vulnerability_config,
            bucket=synthesis_config["BUCKET"],
            data_path=synthesis_config["PATHS"]["DATA_PATH"],
            schema=result_schema,
        ),
        macros,
    )

    # Ouverture des connexions : leur cycle de vie appartient au script
    scores_conn = scores_connector.connect()
    try:
        for statement in macros:
            scores_conn.execute(statement)
        diagnostics_conn = diagnostics_connector.connect()
        try:
            outcome = run_incremental_synthesis(
                scores_conn,
                diagnostics_conn,
                config,
                sources=sources,
                filters=filters,
                catalog_alias=catalog_alias,
                result_schema=result_schema,
                diagnostics_schema=diagnostics_schema,
                registry=registry,
                requested=requested,
                marks=marks,
                force=force,
                settings=settings,
                flow_codes=flow_codes,
                vintages=vintages,
                computed_at=computed_at,
                run_id=workflow_run_id(),
                tracker=tracker,
                log_artifacts=log_artifacts,
                scope=scope,
                reader=reader,
            )
        finally:
            diagnostics_conn.close()
    finally:
        scores_conn.close()

    # Échec global si au moins un contexte a échoué, une fois tous tentés
    if outcome.failures:
        raise RuntimeError(
            f"{len(outcome.failures)} contexte(s) en échec sur {len(outcome.computed) + len(outcome.failures)} : "
            f"{sorted(outcome.failures)}"
        ) from next(iter(outcome.failures.values()))


# Exécution du script principal
if __name__ == "__main__":
    main()
