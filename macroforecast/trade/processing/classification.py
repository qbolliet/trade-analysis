"""Harmonisation of HS product codes onto a single classification vintage.

COMTRADE declarations carry the HS vintage in force at the time of the
declaration (``H4`` for HS2012, ``H5`` for HS2017, ``H6`` for HS2022), so any
panel spanning more than a few years mixes several nomenclatures. Comparing a
2019 flow to a 2023 flow on the same six-digit code is only meaningful once both
sit on the same vintage classification system : :func:`~macroforecast.trade.processing.baci.run_baci`
therefore refuses a heterogeneous input (see
:func:`~macroforecast.trade.processing.baci._assert_homogeneous_classification`)
and expects the caller to have run :class:`HsHarmonizer` upstream, on the raw
fact table.

**Direction of the conversion.** Only the *downward* direction is unambiguous:
UNSD publishes, for each pair of vintages, a *Conversion* table that is a genuine
function from the recent codes to the older ones (one target per source), whereas
the upward direction is a one-to-many relation. Hence the default target — the
oldest vintage present at ``fit`` time — and the explicit refusal of an upward
target.

**Why the re-aggregation is the delicate part.** Downward conversions are mostly
``n:1``: several HS2022 codes collapse onto a single HS2017 code. Once remapped,
the key ``(reporter, partner, flow, product, period, …)`` is no longer unique, so
the table *must* be re-aggregated — and the rule differs across measures. Values
and weights are additive, quantities are not: they are summed per unit code, and
the unit carrying the largest cumulated quantity is kept. That rule lives in
:func:`~macroforecast.trade.processing.aggregation.aggregate_measures`, shared
with the mirror-flow construction of ``baci.py``.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from collections import Counter, deque
from dataclasses import dataclass, field
import logging
from typing import Dict, List, Literal, Mapping, Optional, Sequence, Tuple
# Modules de manipulation de données
import pandas as pd
# Modules du package
from ...tracking import flatten_metrics
from .aggregation import aggregate_measures
from ...datasets.sources.unsd.formats import vintage_year
from ...datasets.sources.unsd.parsing import PARAMETERS, normalise_codes

# Initialisation du logger
logger = logging.getLogger(__name__)

# Correspondance des codes de nomenclature COMTRADE vers leur millésime. Lue au
# catalogue UNSD plutôt que redéclarée ici : c'est la même convention qui nomme
# les tables de correspondance téléchargées.
HS_VINTAGE_YEARS: Dict[str, str] = {
    code: str(year) for code, year in PARAMETERS["CLASSIFICATION_VINTAGES"].items()
}

# Politiques de traitement des codes non appariés
UNMAPPED_POLICIES: Tuple[str, ...] = ("raise", "drop", "keep")

# Colonnes du schéma canonique des tables de correspondance (cf. unsd.parsing)
_SOURCE_CODE, _TARGET_CODE = "source_code", "target_code"
_RELATIONSHIP = "relationship"

# Colonnes de travail de la jointure de remappage (préfixées pour ne pas
# entrer en collision avec une colonne de la table)
_JOIN_VINTAGE, _JOIN_SOURCE, _JOIN_TARGET = "_hs_vintage", "_hs_source", "_hs_target"


# ──────────────────────────────────────────────────────────────────────
# Résolution des millésimes
# ──────────────────────────────────────────────────────────────────────

# Fonction de résolution d'une étiquette de nomenclature vers son millésime
def resolve_vintage(label: str) -> str:
    """Resolve a classification label to its four-digit vintage.

    The four-digit year is the pivot form of this module: it is what pairs the
    COMTRADE labels (``"H5"``) with the correspondence-table labels
    (``"HS2017"``), and what orders the vintages so the oldest one can be
    elected as the default target.

    Args:
        label: Classification label, in any of the three spellings met in
            practice: COMTRADE code (``"H5"``), UNSD identifier (``"HS2017"``,
            ``"HS 2017"``) or bare vintage (``"2017"``).

    Returns:
        The four-digit vintage as a string.

    Raises:
        ValueError: If the label matches none of the three spellings.

    Examples:
        >>> resolve_vintage("H5")
        '2017'
        >>> resolve_vintage("HS2022")
        '2022'
        >>> resolve_vintage("2012")
        '2012'
    """
    # Normalisation de l'étiquette (casse et espaces indifférents)
    normalised = str(label).strip().upper()

    # Millésime nu
    if normalised.isdigit() and len(normalised) == 4:
        return normalised

    # Identifiant UNSD ("HS2017", "HS 2017")
    try:
        return vintage_year(normalised)
    except ValueError:
        pass

    # Code de nomenclature COMTRADE ("H5")
    if normalised in HS_VINTAGE_YEARS:
        return HS_VINTAGE_YEARS[normalised]

    raise ValueError(
        f"Could not resolve classification label {label!r} to an HS vintage. "
        f"Expected a COMTRADE code among {sorted(HS_VINTAGE_YEARS)}, a UNSD "
        f"identifier such as 'HS2017', or a bare four-digit vintage."
    )


# Fonction d'indexation des tables de correspondance par couple de millésimes
def _index_concordances(
    concordances: Mapping[Tuple[str, str], pd.DataFrame],
) -> Dict[Tuple[str, str], pd.DataFrame]:
    """Re-key a concordance mapping by resolved ``(source year, target year)``.

    Args:
        concordances: Mapping ``(source, target) -> table``, keyed by any of the
            label spellings accepted by :func:`resolve_vintage`.

    Returns:
        The same tables, keyed by their pair of four-digit vintages.

    Raises:
        ValueError: If a key cannot be resolved.
    """
    # Réindexation sur la forme pivot
    indexed: Dict[Tuple[str, str], pd.DataFrame] = {}
    for (source, target), df_table in concordances.items():
        pair = (resolve_vintage(source), resolve_vintage(target))
        # Doublon de couple : deux étiquettes distinctes du même millésime
        if pair in indexed:
            # Logging
            logger.warning(
                f"Concordance pair {pair} declared twice in the mapping "
                f"(duplicate key {(source, target)!r}): keeping the first table."
            )
            continue
        indexed[pair] = df_table
    return indexed


# Fonction de construction d'un dictionnaire de conversion depuis une table
def _map_from_table(df_table: pd.DataFrame, source: str, target: str) -> Dict[str, str]:
    """Turn one normalised correspondence table into a code-to-code mapping.

    Args:
        df_table: Correspondence table, canonical UNSD schema.
        source: Source classification label, used in error messages.
        target: Target classification label, used in error messages.

    Returns:
        Mapping ``source code -> target code``.

    Raises:
        ValueError: If the canonical columns are missing, or if a source code
            carries several distinct target codes — the signature of a
            *Correlation* sheet used where a *Conversion* sheet is required.
    """
    # Vérification du schéma canonique
    missing = [col for col in (_SOURCE_CODE, _TARGET_CODE) if col not in df_table.columns]
    if missing:
        raise ValueError(
            f"Concordance table {source!r} -> {target!r} is missing the canonical "
            f"columns {missing}. Available columns: {list(df_table.columns)}."
        )

    # Normalisation des codes des deux côtés (zéros de tête préservés)
    df_pairs = pd.DataFrame(
        {
            _SOURCE_CODE: normalise_codes(df_table[_SOURCE_CODE]),
            _TARGET_CODE: normalise_codes(df_table[_TARGET_CODE]),
        }
    ).dropna()
    df_pairs = df_pairs.drop_duplicates()

    # Contrôle de fonctionnalité : un seul code cible par code source
    ambiguous = df_pairs.loc[df_pairs[_SOURCE_CODE].duplicated(keep=False), _SOURCE_CODE]
    # Message d'erreur si présence de duplicats
    if not ambiguous.empty:
        raise ValueError(
            f"Concordance table {source!r} -> {target!r} is not a function: "
            f"{ambiguous.nunique()} source codes carry several target codes "
            f"(e.g. {sorted(ambiguous.unique())[:5]}). Supply the *Conversion* "
            f"sheet, not the *Correlation* one — the latter is a relation and "
            f"cannot drive a conversion."
        )

    return dict(zip(df_pairs[_SOURCE_CODE], df_pairs[_TARGET_CODE]))


# Fonction de recherche d'un chemin de conversion par millésimes intermédiaires
def _find_chain(
    indexed: Mapping[Tuple[str, str], pd.DataFrame], source_year: str, target_year: str
) -> List[str]:
    """Find the shortest chain of available tables from one vintage to another.

    Args:
        indexed: Concordance tables keyed by ``(source year, target year)``.
        source_year: Four-digit vintage to start from.
        target_year: Four-digit vintage to reach.

    Returns:
        The vintages of the path, from ``source_year`` to ``target_year``
        included; empty when no path exists.
    """
    # Parcours en largeur du graphe des paires disponibles
    queue: deque = deque([[source_year]])
    visited = {source_year}
    while queue:
        path = queue.popleft()
        for pair_source, pair_target in indexed:
            if pair_source != path[-1] or pair_target in visited:
                continue
            extended = path + [pair_target]
            if pair_target == target_year:
                return extended
            visited.add(pair_target)
            queue.append(extended)
    return []


# Fonction de construction du dictionnaire de conversion d'un couple de millésimes
def build_conversion_map(
    concordances: Mapping[Tuple[str, str], pd.DataFrame],
    source: str,
    target: str,
) -> Dict[str, str]:
    """Build the code-to-code conversion map between two HS vintages.

    The **direct** table is used whenever it is available, which is the normal
    case: UNSD publishes all 21 downward combinations. Chaining through
    intermediate vintages is a fallback only — every hop drops the codes that
    fell out at the previous one and compounds the ambiguity — and is reported
    as a warning.

    Args:
        concordances: Mapping ``(source, target) -> normalised table``, provided
            by the caller. Keys accept any spelling resolved by
            :func:`resolve_vintage`.
        source: Source classification to convert from (e.g. ``"H6"``).
        target: Target classification to converge to (e.g. ``"H5"``).

    Returns:
        Mapping ``source code -> target code``, six-digit zero-padded on both
        sides.

    Raises:
        ValueError: If no direct table and no chain connects the two vintages,
            or if a table used is not a function.

    Examples:
        >>> df_table = pd.DataFrame(
        ...     {
        ...         "source_classification": ["HS2022"] * 2,
        ...         "source_code": ["010121", "010129"],
        ...         "target_classification": ["HS2017"] * 2,
        ...         "target_code": ["010121", "010121"],
        ...         "relationship": [pd.NA, pd.NA],
        ...     }
        ... )
        >>> build_conversion_map({("HS2022", "HS2017"): df_table}, "H6", "H5")
        {'010121': '010121', '010129': '010121'}
    """
    # Résolution des millésimes et indexation des tables
    source_year, target_year = resolve_vintage(source), resolve_vintage(target)
    indexed = _index_concordances(concordances)

    # Cas nominal : table directe
    if (source_year, target_year) in indexed:
        return _map_from_table(
            indexed[(source_year, target_year)], source=source, target=target
        )

    # Repli : chaînage par millésimes intermédiaires
    path = _find_chain(indexed, source_year=source_year, target_year=target_year)
    # Message d'erreur si aucun chemin valide
    if not path:
        raise ValueError(
            f"No concordance table connects {source!r} to {target!r}. Available "
            f"pairs: {sorted(indexed)}. The 21 downward combinations are "
            f"published by UNSD: a missing pair usually means the caller did "
            f"not load it."
        )
    # Logging
    logger.warning(
        f"No direct concordance table from {source!r} to {target!r}: falling "
        f"back on the chain {' -> '.join(path)}. Chaining compounds the "
        f"ambiguity of each hop and drops the codes lost along the way."
    )

    # Composition successive des dictionnaires le long du chemin
    conversion_map = _map_from_table(
        indexed[(path[0], path[1])], source=path[0], target=path[1]
    )
    for hop_source, hop_target in zip(path[1:-1], path[2:]):
        hop_map = _map_from_table(
            indexed[(hop_source, hop_target)], source=hop_source, target=hop_target
        )
        conversion_map = {
            code: hop_map[intermediate]
            for code, intermediate in conversion_map.items()
            if intermediate in hop_map
        }
    return conversion_map


# ──────────────────────────────────────────────────────────────────────
# Rapport d'harmonisation
# ──────────────────────────────────────────────────────────────────────

# Rapport d'exécution de l'harmonisation des nomenclatures
@dataclass
class HsHarmonizationReport:
    """Summary of one HS harmonisation run.

    Diagnostics are returned as data rather than logged, so the caller can push
    them to MLflow or persist them next to the result.

    Attributes:
        n_input_rows: Rows handed to :meth:`HsHarmonizer.transform`.
        n_output_rows: Rows produced after remapping and re-aggregation.
        target_vintage: Vintage every observation was converged to.
        vintages_converted: Vintages actually converted (the target itself
            excluded).
        n_codes_mapped: Distinct ``(vintage, code)`` pairs of the fitted
            mapping.
        n_codes_unmapped: Distinct ``(vintage, code)`` pairs found at ``fit``
            time with no counterpart in the concordance tables.
        share_value_unmapped: Share of the leading value column carried by the
            unmapped rows, measured **before** ``on_unmapped`` is applied.
        share_rows_collapsed: Share of the retained rows absorbed by the
            re-aggregation, ``1 - n_output_rows / n_retained_rows``. Rows
            discarded by ``on_unmapped="drop"`` are excluded from the
            denominator, so the indicator measures ``n:1`` collapsing only.
        relationship_distribution: Counts of the ``1:1`` / ``1:n`` / ``n:1`` /
            ``n:n`` annotations of the tables used, restricted to the source
            codes present in the data. **Empty when the caller supplies
            *Conversion* sheets**, which carry no annotation; fill it by
            supplying *Correlation* sheets instead, whose distribution measures
            the information loss of the conversion.
    """
    n_input_rows: int = 0
    n_output_rows: int = 0
    target_vintage: Optional[str] = None
    vintages_converted: Tuple[str, ...] = ()
    n_codes_mapped: int = 0
    n_codes_unmapped: int = 0
    share_value_unmapped: float = float("nan")
    share_rows_collapsed: float = float("nan")
    relationship_distribution: Dict[str, int] = field(default_factory=dict)

    # Mise en forme des métriques (mêmes conventions que BaciReport.to_metrics)
    def to_metrics(self, prefix: str = "hs") -> Dict[str, float]:
        """Flatten the numeric fields into a dotted metric mapping.

        ``relationship_distribution`` is excluded: being a distribution over
        relationship labels, it belongs to a JSON artifact rather than to the
        metrics. ``NaN`` and infinite values are dropped, MLflow rejecting them.

        Args:
            prefix: Prefix prepended to every metric name.

        Returns:
            Mapping of dotted metric names to finite floats.

        Examples:
            >>> HsHarmonizationReport(n_input_rows=5).to_metrics()["hs.n_input_rows"]
            5.0
        """
        # Construction des métriques
        metrics = flatten_metrics(self, prefix=prefix)
        # Retrait de la distribution des relations, remontée en artefact
        excluded = f"{prefix}.relationship_distribution." if prefix else "relationship_distribution."
        return {
            name: value for name, value in metrics.items() if not name.startswith(excluded)
        }


# ──────────────────────────────────────────────────────────────────────
# Harmonisation des nomenclatures
# ──────────────────────────────────────────────────────────────────────

# Transformateur d'harmonisation des nomenclatures HS
class HsHarmonizer:
    """Harmonise HS product codes of a trade dataset onto a single vintage.

    Remaps every observation onto ``target_vintage`` using the correspondence
    tables supplied by the caller, then re-aggregates the table — mandatory,
    since downward conversions are mostly ``n:1`` and destroy the uniqueness of
    the key. Values and weights are summed; the quantity is summed per unit code
    and keeps the unit carrying the largest cumulated quantity.

    The aggregation key is *every* column that is not a measure, so any extra
    dimension of the source table (customs regime, mode of transport, …) is
    preserved and the collapsing is limited to genuine collisions.

    Args:
        concordances: Mapping ``(source_vintage, target_vintage) -> DataFrame``
            of normalised conversion tables, provided by the caller — this class
            downloads nothing. Keys accept any spelling resolved by
            :func:`resolve_vintage`.
        target_vintage: Vintage to converge to. ``None`` (default) picks the
            oldest vintage present at ``fit`` time — the only direction in which
            the conversion is unambiguous.
        classification_col: Column holding the HS vintage of each observation.
        product_col: Column holding the six-digit product code.
        period_col: Column holding the period; part of the aggregation key, and
            reported per vintage at ``fit`` time.
        value_cols: Value columns re-aggregated by summation.
        weight_cols: Weight columns re-aggregated by summation.
        qty_col: Quantity column, handled apart (units are not additive).
        qty_unit_col: Quantity-unit column driving the quantity aggregation.
        on_unmapped: What to do with codes carrying no counterpart in the
            concordance tables — ``"raise"`` (default), ``"drop"`` (rows
            discarded, their value lost and measured by
            ``report_.share_value_unmapped``) or ``"keep"`` (rows left untouched,
            **including their original vintage**, so the output stays
            heterogeneous and ``run_baci`` refuses it: ``"keep"`` is a
            diagnostic mode, not a production one).

    Attributes:
        target_vintage_: Vintage every mapped observation was converged to.
        vintages_present_: Vintage labels found at ``fit`` time, oldest first.
        mapping_: Mapping ``(vintage label, source code) -> target code``.
            Observations already at the target vintage are **not** materialised
            here: the identity is applied by rule at ``transform`` time, so a
            target-vintage code unseen at ``fit`` time is not wrongly reported
            as unmapped.
        unmapped_codes_: Sorted ``(vintage label, code)`` pairs present in the
            fitted data with no counterpart in the concordance tables.
        report_: :class:`HsHarmonizationReport`; its structural fields are set
            by :meth:`fit` and its volumetric ones refreshed by every
            :meth:`transform`.

    Raises:
        ValueError: On an unknown ``on_unmapped`` policy, a missing column, an
            upward target vintage, a quantity column without its unit column,
            or — under ``on_unmapped="raise"`` — an unmapped code.

    Examples:
        >>> df_table = pd.DataFrame(
        ...     {
        ...         "source_code": ["010121", "010129"],
        ...         "target_code": ["010121", "010121"],
        ...     }
        ... )
        >>> df_data = pd.DataFrame(
        ...     {
        ...         "classificationCode": ["H6", "H6", "H5"],
        ...         "cmdCode": ["010121", "010129", "010121"],
        ...         "period": ["2023", "2023", "2019"],
        ...         "primaryValue": [10.0, 20.0, 7.0],
        ...     }
        ... )
        >>> harmonizer = HsHarmonizer({("HS2022", "HS2017"): df_table})
        >>> df_out = harmonizer.fit_transform(df_data)
        >>> df_out.to_dict("records")
        [{'classificationCode': 'H5', 'cmdCode': '010121', 'period': '2019', \
'primaryValue': 7.0}, {'classificationCode': 'H5', 'cmdCode': '010121', \
'period': '2023', 'primaryValue': 30.0}]
        >>> harmonizer.target_vintage_
        'H5'
    """

    # Initialisation
    def __init__(
        self,
        concordances: Mapping[Tuple[str, str], pd.DataFrame],
        *,
        target_vintage: Optional[str] = None,
        classification_col: str = "classificationCode",
        product_col: str = "cmdCode",
        period_col: str = "period",
        value_cols: Sequence[str] = ("primaryValue", "cifvalue", "fobvalue"),
        weight_cols: Sequence[str] = ("netWgt",),
        qty_col: str = "qty",
        qty_unit_col: str = "qtyUnitCode",
        on_unmapped: Literal["raise", "drop", "keep"] = "raise",
    ) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.concordances = concordances
        self.target_vintage = target_vintage
        self.classification_col = classification_col
        self.product_col = product_col
        self.period_col = period_col
        self.value_cols = value_cols
        self.weight_cols = weight_cols
        self.qty_col = qty_col
        self.qty_unit_col = qty_unit_col
        self.on_unmapped = on_unmapped

    # Méthode auxiliaire : contrôle des colonnes exploitables du jeu de données
    def _check_columns(self, df_data: pd.DataFrame) -> None:
        """Check that the frame carries the columns the transformation needs.

        Args:
            df_data: Frame to check.

        Returns:
            None.

        Raises:
            ValueError: If an identifying column is missing, or if the quantity
                column is present without its unit column.
        """
        # Colonnes identifiantes, indispensables au remappage
        missing = [
            col
            for col in (self.classification_col, self.product_col)
            if col not in df_data.columns
        ]
        # message d'erreur si des colonnes sont manquantes
        if missing:
            raise ValueError(
                f"Columns {missing} are absent from df_data. Available columns: "
                f"{list(df_data.columns)}."
            )

        # Quantité sans unité : agrégation impossible sans risque de bug silencieux
        if self.qty_col in df_data.columns and self.qty_unit_col not in df_data.columns:
            raise ValueError(
                f"Column {self.qty_col!r} is present but {self.qty_unit_col!r} is "
                f"not: quantities cannot be re-aggregated without their unit "
                f"code, and summing heterogeneous units would be a silent bug. "
                f"Project the unit column, or drop the quantity column."
            )

        # Période : simple avertissement, la colonne n'est qu'une dimension de clé
        if self.period_col not in df_data.columns:
            # Logging
            logger.warning(
                f"Column {self.period_col!r} is absent from df_data: the "
                f"harmonisation runs without it, but the aggregation key then "
                f"carries no time dimension."
            )

    # Méthode auxiliaire : mesures effectivement présentes dans le jeu de données
    def _measure_columns(self, df_data: pd.DataFrame) -> List[str]:
        """Return the additive measure columns present in the frame.

        Args:
            df_data: Frame to inspect.

        Returns:
            Ordered, de-duplicated list of value and weight columns present.
        """
        # Ordre stable, sans doublon (une colonne peut figurer dans les deux listes)
        return list(
            dict.fromkeys(
                col
                for col in list(self.value_cols) + list(self.weight_cols)
                if col in df_data.columns
            )
        )

    # Méthode auxiliaire : distribution des relations d'une table directe
    def _relationship_counts(
        self,
        indexed: Mapping[Tuple[str, str], pd.DataFrame],
        source_year: str,
        target_year: str,
        codes_present: Sequence[str],
    ) -> Counter:
        """Count the relationship annotations of a direct table, if any.

        Args:
            indexed: Concordance tables keyed by ``(source year, target year)``.
            source_year: Four-digit vintage of the source.
            target_year: Four-digit vintage of the target.
            codes_present: Source codes present in the data, the only ones whose
                relationship is informative here.

        Returns:
            Counter of the ``1:1`` / ``1:n`` / ``n:1`` / ``n:n`` annotations;
            empty on a *Conversion* sheet, which carries none.
        """
        # Table directe uniquement : un chaînage n'a pas de relation propre
        df_table = indexed.get((source_year, target_year))
        if df_table is None or _RELATIONSHIP not in df_table.columns:
            return Counter()

        # Restriction aux codes sources présents dans les données
        relationships = df_table.loc[
            normalise_codes(df_table[_SOURCE_CODE]).isin(list(codes_present)),
            _RELATIONSHIP,
        ].dropna()
        return Counter(relationships.astype(str).tolist())

    # Détermination du millésime cible et du dictionnaire de conversion
    def fit(self, df_data: pd.DataFrame) -> "HsHarmonizer":
        """Elect the target vintage and build the conversion mapping.

        Args:
            df_data: Trade declarations carrying a classification column.

        Returns:
            The fitted harmoniser (``self``).

        Raises:
            ValueError: On an unknown ``on_unmapped`` policy, a missing column,
                an empty classification column, a target vintage older than
                nothing present (upward conversion), or — under
                ``on_unmapped="raise"`` — the presence of unmapped codes.
        """
        # Vérification des arguments et des colonnes
        if self.on_unmapped not in UNMAPPED_POLICIES:
            raise ValueError(
                f"Unsupported on_unmapped policy {self.on_unmapped!r}. "
                f"Expected one of {list(UNMAPPED_POLICIES)}."
            )
        self._check_columns(df_data)

        # Millésimes présents, du plus ancien au plus récent
        labels = df_data[self.classification_col].dropna().unique().tolist()
        if not labels:
            raise ValueError(
                f"Column {self.classification_col!r} carries no classification "
                f"label: nothing to harmonise."
            )
        self.vintages_present_ = tuple(sorted(labels, key=resolve_vintage))

        # Millésime cible : celui demandé, ou le plus ancien présent
        target = (
            self.target_vintage
            if self.target_vintage is not None
            else self.vintages_present_[0]
        )
        target_year = resolve_vintage(target)

        # Refus des conversions ascendantes : la relation cible → source est
        # une correspondance 1:n, donc non fonctionnelle
        older = [
            label
            for label in self.vintages_present_
            if resolve_vintage(label) < target_year
        ]
        if older:
            raise ValueError(
                f"Target vintage {target!r} ({target_year}) is more recent than "
                f"the vintages {older} present in df_data. Only downward "
                f"conversions are unambiguous: pick a target at most as old as "
                f"the oldest vintage present."
            )
        self.target_vintage_ = target

        # Journalisation de la couverture temporelle de chaque millésime présent
        if self.period_col in df_data.columns:
            for label in self.vintages_present_:
                periods = df_data.loc[
                    df_data[self.classification_col] == label, self.period_col
                ]
                if not periods.empty:
                    # Logging
                    logger.info(
                        f"Vintage {label!r}: {len(periods)} rows over periods "
                        f"{periods.min()}-{periods.max()}."
                    )

        # Construction du dictionnaire de conversion, millésime par millésime
        indexed = _index_concordances(self.concordances)
        codes = normalise_codes(df_data[self.product_col])
        mapping: Dict[Tuple[str, str], str] = {}
        unmapped: List[Tuple[str, str]] = []
        converted: List[str] = []
        relationships: Counter = Counter()

        # Parcours des millésimes
        for label in self.vintages_present_:
            source_year = resolve_vintage(label)
            # Millésime déjà cible : identité appliquée par règle dans transform
            if source_year == target_year:
                continue
            converted.append(label)

            conversion_map = build_conversion_map(self.concordances, label, target)
            codes_present = (
                codes[df_data[self.classification_col] == label].dropna().unique()
            )
            for code in codes_present:
                target_code = conversion_map.get(code)
                if target_code is None:
                    unmapped.append((label, str(code)))
                else:
                    mapping[(label, str(code))] = target_code

            relationships += self._relationship_counts(
                indexed,
                source_year=source_year,
                target_year=target_year,
                codes_present=codes_present,
            )

        # Mise à jour des attributs ajustés
        self.mapping_ = mapping
        self.unmapped_codes_ = tuple(sorted(unmapped))
        self.report_ = HsHarmonizationReport(
            target_vintage=self.target_vintage_,
            vintages_converted=tuple(converted),
            n_codes_mapped=len(mapping),
            n_codes_unmapped=len(self.unmapped_codes_),
            relationship_distribution=dict(relationships),
        )

        # Échec au plus tôt : inutile de remapper si la politique est "raise"
        if self.on_unmapped == "raise" and self.unmapped_codes_:
            raise ValueError(
                f"{len(self.unmapped_codes_)} product codes have no counterpart "
                f"in the concordance tables towards {self.target_vintage_!r} "
                f"(e.g. {list(self.unmapped_codes_[:5])}). Use "
                f"on_unmapped='drop' to discard them or 'keep' to leave them "
                f"untouched."
            )

        # Logging
        logger.info(
            f"Harmonisation fitted on {self.target_vintage_!r}: "
            f"{len(converted)} vintages to convert, {len(mapping)} codes mapped, "
            f"{len(self.unmapped_codes_)} unmapped."
        )
        return self

    # Remappage des codes produits puis ré-agrégation
    def transform(self, df_data: pd.DataFrame) -> pd.DataFrame:
        """Remap product codes onto the target vintage and re-aggregate.

        The product column comes out as six-digit zero-padded text, the spelling
        of the correspondence tables. ``zfill`` never truncates: a longer code
        (a national tariff line, say) passes through untouched and simply comes
        out unmapped.

        Args:
            df_data: Trade declarations to harmonise.

        Returns:
            The harmonised table, one row per distinct combination of the
            non-measure columns, in the input column order.

        Raises:
            ValueError: If :meth:`fit` has not been called, if a column is
                missing, or — under ``on_unmapped="raise"`` — if a code carries
                no counterpart.
        """
        # Vérification de l'ajustement préalable
        if not hasattr(self, "target_vintage_"):
            raise ValueError(
                "HsHarmonizer is not fitted yet: call fit or fit_transform "
                "before transform."
            )
        self._check_columns(df_data)

        # Copie indépendante, index remis à plat pour la jointure de remappage
        df_out = df_data.reset_index(drop=True).copy()
        n_input_rows = len(df_out)
        target_year = resolve_vintage(self.target_vintage_)

        # Codes normalisés et millésimes de chaque ligne
        vintages = df_out[self.classification_col]
        codes = normalise_codes(df_out[self.product_col])
        is_target = vintages.map(
            {
                label: resolve_vintage(label) == target_year
                for label in vintages.dropna().unique()
            }
        ).fillna(False)

        # Remappage par jointure gauche : la table de faits COMTRADE se compte en
        # millions de lignes, une recherche ligne à ligne y serait prohibitive
        df_mapping = pd.DataFrame(
            [
                (label, code, target_code)
                for (label, code), target_code in self.mapping_.items()
            ],
            columns=[_JOIN_VINTAGE, _JOIN_SOURCE, _JOIN_TARGET],
        )
        df_keys = pd.DataFrame({_JOIN_VINTAGE: vintages, _JOIN_SOURCE: codes})
        if df_mapping.empty:
            mapped_codes = pd.Series(pd.NA, index=df_out.index, dtype="string")
        else:
            df_mapping[_JOIN_SOURCE] = df_mapping[_JOIN_SOURCE].astype("string")
            mapped_codes = df_keys.merge(
                df_mapping, on=[_JOIN_VINTAGE, _JOIN_SOURCE], how="left"
            )[_JOIN_TARGET].astype("string")
            mapped_codes.index = df_out.index

        # Identité pour les lignes déjà au millésime cible
        resolved_codes = mapped_codes.where(~is_target, codes)
        is_mapped = resolved_codes.notna()

        # Part de valeur non appariée, mesurée avant application de la politique
        value_columns = [col for col in self.value_cols if col in df_out.columns]
        share_value_unmapped = float("nan")
        if value_columns:
            values = pd.to_numeric(df_out[value_columns[0]], errors="coerce")
            total = values.sum()
            if total:
                share_value_unmapped = float(values[~is_mapped].sum() / total)

        # Application de la politique de traitement des codes non appariés
        if (~is_mapped).any():
            faulty = sorted(set(zip(vintages[~is_mapped], codes[~is_mapped])))
            if self.on_unmapped == "raise":
                raise ValueError(
                    f"{len(faulty)} product codes have no counterpart in the "
                    f"concordance tables towards {self.target_vintage_!r} "
                    f"(e.g. {faulty[:5]}). Use on_unmapped='drop' or 'keep'."
                )
            logger.warning(
                f"{int((~is_mapped).sum())} rows carry a code with no "
                f"counterpart towards {self.target_vintage_!r} "
                f"({share_value_unmapped:.2%} of {value_columns[0] if value_columns else 'value'}): "
                f"policy {self.on_unmapped!r} applied."
            )

        # Écriture des codes et du millésime. Une ligne non appariée conservée
        # garde son millésime d'origine : la sortie reste hétérogène et
        # run_baci la refusera bruyamment plutôt que de la compter à tort.
        df_out[self.product_col] = resolved_codes.where(is_mapped, codes)
        df_out[self.classification_col] = vintages.where(
            ~is_mapped, self.target_vintage_
        )
        if self.on_unmapped == "drop":
            df_out = df_out[is_mapped].reset_index(drop=True)
        n_retained_rows = len(df_out)

        # Ré-agrégation : la conversion descendante étant majoritairement n:1,
        # la clé n'est plus unique. Toute colonne non mesurée est une dimension.
        measure_cols = self._measure_columns(df_out)
        has_qty = (
            self.qty_col in df_out.columns and self.qty_unit_col in df_out.columns
        )
        keys = [
            col
            for col in df_out.columns
            if col not in measure_cols
            and not (has_qty and col in (self.qty_col, self.qty_unit_col))
        ]
        df_out = aggregate_measures(
            df_out,
            keys,
            sum_cols=measure_cols,
            qty_col=self.qty_col if has_qty else None,
            qty_unit_col=self.qty_unit_col if has_qty else None,
            # Les manquants d'une colonne de clé ne doivent pas faire disparaître
            # de lignes : la valeur agrégée doit être conservée
            dropna=False,
        )
        # Restitution de l'ordre des colonnes d'entrée
        df_out = df_out[[col for col in df_data.columns if col in df_out.columns]]

        # Mise à jour des champs volumétriques du rapport
        self.report_.n_input_rows = n_input_rows
        self.report_.n_output_rows = len(df_out)
        self.report_.share_value_unmapped = share_value_unmapped
        self.report_.share_rows_collapsed = (
            1.0 - len(df_out) / n_retained_rows if n_retained_rows else float("nan")
        )

        # Logging
        logger.info(
            f"Harmonisation towards {self.target_vintage_!r}: {n_input_rows} rows "
            f"in, {len(df_out)} rows out "
            f"({self.report_.share_rows_collapsed:.2%} collapsed)."
        )
        return df_out.reset_index(drop=True)

    # Ajustement et transformation en une passe
    def fit_transform(self, df_data: pd.DataFrame) -> pd.DataFrame:
        """Fit on ``df_data`` then harmonise it — the nominal path.

        Args:
            df_data: Trade declarations to harmonise.

        Returns:
            The harmonised table, as :meth:`transform` returns it.
        """
        return self.fit(df_data).transform(df_data)
