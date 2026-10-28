"""Trade-graph primitives of the network vulnerability metrics.

Turns a table of reconciled bilateral flows into one **undirected weighted graph
per output cell** (``nomenclature x product x year``) and measures its topology:
weighted local clustering (Barrat), diameter, and the structural counts feeding
the diagnostics.

Notes:
    Unlike the rest of the sub-package (backend-agnostic via narwhals), the
    per-graph computations run on **dense NumPy matrices**, ``scipy`` being used
    only to label the connected components. This is a deliberate,
    measured choice rather than a shortcut:

    * the workload is not *one large graph* but tens of thousands of small ones
      — roughly ``n_products x n_years`` per HS vintage — over an invariant node
      set of about 200 countries. What dominates is the per-graph overhead, not
      the asymptotics;
    * a dense ``200 x 200`` adjacency matrix weighs 320 kB, so a whole graph fits
      in cache and ``A @ A`` costs about 0.1 ms in BLAS;
    * benchmarked on this shape, ``networkx`` (one Python object per node and
      edge) is one to two orders of magnitude slower, and the libraries built for
      millions of edges (``scikit-network``, ``networkit``, ``graph-tool``) bring
      nothing at 200 nodes;
    * the diameter by boolean transitive closure measures about 3 ms per full
      200-node graph, against 20–27 ms for
      ``scipy.sparse.csgraph.shortest_path(method="D", unweighted=True)``, for
      results verified identical.

    The boundary is crossed once, at the entry of :func:`compute_graph_features`,
    and crossed back through ``nw.from_dict(..., backend=...)``: the **caller's
    backend is preserved**, exactly as it is by the pure-narwhals metrics.

    Two things keep the scan tractable, both found by profiling rather than by
    intuition, and both worth preserving:

    * every matrix is restricted to the countries actually present in its cell
      (usually a few dozen, not 200), the local renumbering going through a
      reused lookup buffer rather than a per-cell sort;
    * the country codes and the group boundaries are obtained **without sorting
      strings** — the former by a narwhals join (see :func:`_encode_countries`),
      the latter by comparing consecutive rows of an already-sorted table.
      Factorising a million-odd Python strings with ``np.unique`` used to cost
      more than every graph of the pass put together.

    Order of magnitude, measured end to end on a dense synthetic vintage (900
    cells, a million edges, 100 countries and 500 links per graph): about 20 ms
    per cell for the six metrics and the diagnostics, the real HS6 networks being
    sparser than that.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Sequence, Tuple
# Modules de manipulation de données
import narwhals as nw
import numpy as np
# Modules de calcul sur graphes creux
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components

# Initialisation du logger
logger = logging.getLogger(__name__)

# Traits topologiques restituables par une passe de graphe
CLUSTERING_W = "clustering_w"
DIAMETER = "diameter"
N_NODES = "n_nodes"
N_EDGES = "n_edges"
N_COMPONENTS = "n_components"
DENSITY = "density"

# Traits structurels, mesurés à chaque passe (coût négligeable) et alimentant
# systématiquement le rapport, qu'ils soient demandés en sortie ou non
STRUCTURAL_FEATURES: Tuple[str, ...] = (N_NODES, N_EDGES, N_COMPONENTS, DENSITY)
# Traits coûteux, calculés uniquement s'ils sont demandés
TOPOLOGICAL_FEATURES: Tuple[str, ...] = (CLUSTERING_W, DIAMETER)
# Ensemble des traits admis
GRAPH_FEATURES: Tuple[str, ...] = STRUCTURAL_FEATURES + TOPOLOGICAL_FEATURES

# Valeur des traits non calculables (graphe dégénéré)
_NAN = float("nan")

# Colonnes de travail de l'encodage des pays (jamais rendues à l'appelant)
_COUNTRY = "_graph_country"
_COUNTRY_CODE = "_graph_country_code"
_EXPORTER_CODE = "_graph_exporter_code"
_IMPORTER_CODE = "_graph_importer_code"


# ──────────────────────────────────────────────────────────────────────
# Diagnostics de la passe de graphe
# ──────────────────────────────────────────────────────────────────────

# Diagnostics de la construction des graphes
@dataclass
class GraphReport:
    """Diagnostics of one graph-building pass.

    Attributes:
        n_graphs: Number of graphs built, i.e. of output cells.
        median_n_nodes: Median number of countries per graph.
        median_n_edges: Median number of undirected trade links per graph.
        median_density: Median density, ``2 m / (n (n - 1))``.
        share_graphs_disconnected: Share of graphs made of more than one
            connected component. The diameter being reported on the largest
            component only, this is what says how representative it is.
        share_graphs_below_min_nodes: Share of graphs too small for the
            topological metrics, left undefined rather than computed on a
            degenerate graph.
        share_rows_dropped_non_positive: Share of input rows dropped because
            their value was null or non-positive, or an endpoint missing — such
            a row carries no edge.
    """
    n_graphs: int = 0
    median_n_nodes: float = _NAN
    median_n_edges: float = _NAN
    median_density: float = _NAN
    share_graphs_disconnected: float = _NAN
    share_graphs_below_min_nodes: float = _NAN
    share_rows_dropped_non_positive: float = _NAN


# ──────────────────────────────────────────────────────────────────────
# Mesures topologiques d'un graphe
# ──────────────────────────────────────────────────────────────────────

# Coefficient de clustering local moyen pondéré (Barrat)
def weighted_clustering(adjacency: np.ndarray, weights: np.ndarray) -> float:
    """Average the weighted local clustering coefficient over a graph.

    Implements the measure ::

        CC_i^w = 1 / (k_i (k_i - 1)) * Σ_{j,k} (1 / <w_i>)
                 * (w_ij + w_ik) / 2 * a_ij a_ik a_jk

    which is the Barrat coefficient: with ``s_i = k_i <w_i>`` the strength of
    node ``i``, the prefactor ``1 / (k_i (k_i - 1) <w_i>)`` is exactly
    ``1 / (s_i (k_i - 1))``. And since the constraint ``a_ij a_ik a_jk`` is
    symmetric in ``j`` and ``k``, the two halves of ``(w_ij + w_ik) / 2``
    contribute equally, so the numerator collapses to::

        Σ_j w_ij a_ij * (number of common neighbours of i and j)
        = row-sum of (W ∘ (A @ A))

    which is what is computed — one matrix product instead of a triple loop.

    Nodes of degree below 2 have no pair of neighbours to close a triangle with
    and are excluded from the average rather than counted as zero.

    Args:
        adjacency: Symmetric boolean adjacency matrix, zero diagonal.
        weights: Symmetric weight matrix, zero wherever ``adjacency`` is false.

    Returns:
        The mean of ``CC_i^w`` over the nodes of degree at least 2, ``NaN``
        when no node qualifies.

    Examples:
        >>> import numpy as np
        >>> A = np.array([[0, 1, 1], [1, 0, 1], [1, 1, 0]], dtype=bool)
        >>> W = 2.0 * A
        >>> float(weighted_clustering(A, W))  # triangle complet
        1.0
        >>> A = np.array([[0, 1, 1], [1, 0, 0], [1, 0, 0]], dtype=bool)
        >>> float(weighted_clustering(A, 1.0 * A))  # étoile : aucun triangle
        0.0
    """
    # Matrice binaire flottante : le produit passe par BLAS
    binary = adjacency.astype(np.float64)
    # Degré et force de chaque nœud
    degree = binary.sum(axis=1)
    strength = weights.sum(axis=1)

    # Numérateur : somme sur j de w_ij x (nombre de voisins communs de i et j)
    numerator = (weights * (binary @ binary)).sum(axis=1)
    # Dénominateur de Barrat : s_i (k_i - 1)
    denominator = strength * (degree - 1.0)

    # Nœuds dont le coefficient est défini (au moins deux voisins)
    valid = (degree >= 2) & (denominator > 0)
    if not valid.any():
        return _NAN
    return float((numerator[valid] / denominator[valid]).mean())


# Diamètre du réseau (plus grande composante connexe)
def diameter(adjacency: np.ndarray) -> float:
    """Measure the diameter of a graph, on its largest connected component.

    The trade network of a product is regularly disconnected — a handful of
    countries trading a niche good among themselves, apart from the main
    component — which would make the diameter of the whole graph infinite and
    therefore useless. The reported value is thus that of the **largest
    connected component**; the number of components travels alongside it, in
    :attr:`GraphReport.share_graphs_disconnected`, so that a diameter measured
    on a fragmented graph is never read as if it described the whole.

    Computed by boolean transitive closure: ``R`` starts as "reachable in at
    most one step" and is multiplied by the adjacency until it saturates, the
    number of iterations being the diameter. On a component of a few dozen
    nodes this converges in three to six products, far cheaper than an
    all-pairs shortest-path.

    Args:
        adjacency: Symmetric boolean adjacency matrix, zero diagonal.

    Returns:
        The diameter of the largest connected component, ``0`` when that
        component is a single node, ``NaN`` on an empty graph.

    Examples:
        >>> import numpy as np
        >>> path = np.array([[0, 1, 0], [1, 0, 1], [0, 1, 0]], dtype=bool)
        >>> float(diameter(path))  # chaîne 0-1-2
        2.0
        >>> triangle = np.array([[0, 1, 1], [1, 0, 1], [1, 1, 0]], dtype=bool)
        >>> float(diameter(triangle))
        1.0
    """
    # Graphe vide : aucun diamètre définissable
    n_nodes = adjacency.shape[0]
    if n_nodes == 0:
        return _NAN

    # Étiquetage des composantes, puis restriction à la plus grande
    _, labels = connected_components(csr_matrix(adjacency), directed=False)
    largest = np.flatnonzero(labels == np.bincount(labels).argmax())
    # Composante réduite à un nœud isolé : diamètre nul
    if largest.size < 2:
        return 0.0
    component = adjacency[np.ix_(largest, largest)]

    # Fermeture transitive booléenne : R = A ∪ I, puis R ← R ∪ (R x A)
    binary = component.astype(np.float32)
    reachable = component | np.eye(largest.size, dtype=bool)
    distance = 1
    # La composante étant connexe, la saturation est atteinte en au plus n - 1
    # produits ; la borne protège d'une boucle infinie sur une entrée aberrante.
    while not reachable.all() and distance < largest.size:
        extended = ((reachable.astype(np.float32) @ binary) > 0) | reachable
        # Point fixe atteint sans saturation : composante en réalité disjointe
        if np.array_equal(extended, reachable):
            return float("inf")
        reachable = extended
        distance += 1
    return float(distance)


# ──────────────────────────────────────────────────────────────────────
# Balayage des cellules : une passe, un graphe par cellule
# ──────────────────────────────────────────────────────────────────────

# Fonction auxiliaire : bornes des groupes d'une table triée sur ses clés
def _group_starts(key_values: Sequence[np.ndarray], n_rows: int) -> np.ndarray:
    """Locate the first row of each group in a table sorted on its keys.

    Compares consecutive values directly rather than factorising the key
    columns: the table being sorted, a group boundary is exactly a row differing
    from its predecessor. That is a linear scan, where factorising would sort
    each key column all over again — measurably the dominant cost of the pass
    when the keys are strings.

    Args:
        key_values: One value array per key column, all of length ``n_rows``, in
            the sort order of the table.
        n_rows: Number of rows of the table.

    Returns:
        Indices of the first row of each group, ascending.

    Examples:
        >>> import numpy as np
        >>> _group_starts([np.array(["a", "a", "b", "b", "b"])], 5).tolist()
        [0, 2]
    """
    # Table vide : aucun groupe
    if n_rows == 0:
        return np.empty(0, dtype=np.int64)
    # Changement de clé d'une ligne à la suivante (le tri garantit la contiguïté)
    changed = np.zeros(n_rows, dtype=bool)
    changed[0] = True
    for values in key_values:
        changed[1:] |= values[1:] != values[:-1]
    return np.flatnonzero(changed)


# Fonction auxiliaire : matrice de poids non orientée d'une cellule
def _cell_matrix(
    exporter_codes: np.ndarray,
    importer_codes: np.ndarray,
    values: np.ndarray,
    *,
    lookup: np.ndarray,
    present_flags: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """Build the undirected weight and adjacency matrices of one cell.

    The graph is undirected and its weight is the **total trade between the two
    countries**, ``w_ij = value(i -> j) + value(j -> i)``: the measures of the
    note (clustering, diameter) are undirected, and keeping only one direction
    would make the topology depend on which partner happens to declare.

    The matrix is restricted to the countries present in the cell, so its size
    follows the product rather than the 200-odd countries of the world.

    Args:
        exporter_codes: Global integer code of the exporter of each row.
        importer_codes: Global integer code of the importer of each row.
        values: Flow value of each row (strictly positive).
        lookup: Scratch buffer of one slot per country, reused from cell to
            cell, receiving the local index of each country present.
        present_flags: Scratch boolean buffer of the same length, reset to
            ``False`` on the way out so the next cell finds it clean.

    Returns:
        Tuple ``(weights, adjacency)``: the symmetric weight matrix and its
        boolean support, both with a zero diagonal.
    """
    # Ré-indexation locale par table de correspondance : les codes pays étant de
    # petits entiers, marquer puis renuméroter les pays présents est linéaire, là
    # où un ``np.unique`` par cellule retrierait toutes ses arêtes
    present_flags[exporter_codes] = True
    present_flags[importer_codes] = True
    present = np.flatnonzero(present_flags)
    n_nodes = present.size
    lookup[present] = np.arange(n_nodes)
    origin = lookup[exporter_codes]
    destination = lookup[importer_codes]

    # Accumulation des valeurs orientées : un comptage pondéré sur l'indice
    # aplati cumule en une passe les arêtes déclarées plusieurs fois
    weights = np.bincount(
        origin * n_nodes + destination, weights=values, minlength=n_nodes * n_nodes
    ).reshape(n_nodes, n_nodes)
    # Symétrisation : w_ij = v(i -> j) + v(j -> i)
    weights = weights + weights.T
    # Remise à zéro du tampon, au prix des seuls pays présents dans la cellule
    present_flags[present] = False
    # Auto-échanges : absents de BACI, neutralisés par précaution (ils fausseraient
    # degrés et triangles)
    np.fill_diagonal(weights, 0.0)
    return weights, weights > 0


# Fonction auxiliaire : encodage entier des pays des deux extrémités des arêtes
def _encode_countries(
    data: nw.DataFrame,
    *,
    exporter_col: str,
    importer_col: str,
) -> Tuple[nw.DataFrame, int]:
    """Give every country an integer code, shared by both edge endpoints.

    Done **in narwhals**, by joining a directory of the distinct countries, and
    not by a NumPy ``unique`` over the two columns: factorising a million-odd
    Python strings by sorting them is by far the most expensive operation of the
    whole scan, whereas every backend has native machinery for exactly this —
    measured four times faster on pandas and twenty times on PyArrow. The rule
    of the sub-package holds here too: dataframe work belongs to narwhals, only
    the graph work goes to NumPy.

    Args:
        data: Narwhals frame of bilateral flows.
        exporter_col: Column holding the exporting country.
        importer_col: Column holding the importing country.

    Returns:
        Tuple ``(frame, n_countries)``: the frame with two added integer-code
        columns, and the size of the country directory. Row order is not
        preserved — the caller sorts afterwards anyway.
    """
    # Répertoire des pays : union des deux extrémités, dédoublonnée et numérotée
    countries = (
        nw.concat(
            [
                data.select(nw.col(exporter_col).alias(_COUNTRY)),
                data.select(nw.col(importer_col).alias(_COUNTRY)),
            ],
            how="vertical",
        )
        .unique()
        .sort(_COUNTRY)
        .with_row_index(_COUNTRY_CODE)
    )

    # Jointure du code sur chaque extrémité
    for column, alias in ((exporter_col, _EXPORTER_CODE), (importer_col, _IMPORTER_CODE)):
        data = data.join(
            countries.rename({_COUNTRY: column, _COUNTRY_CODE: alias}),
            on=column,
            how="left",
        )
    return data, len(countries)


# Fonction auxiliaire : extraction en NumPy d'une colonne narwhals
def _column(data: nw.DataFrame, name: str) -> np.ndarray:
    """Materialise one narwhals column as a NumPy array.

    Args:
        data: Source frame.
        name: Column to extract.

    Returns:
        The column as a NumPy array.
    """
    return data.get_column(name).to_numpy()


# Fonction de calcul des traits topologiques, une cellule à la fois
def compute_graph_features(
    data: nw.DataFrame,
    *,
    keys: Sequence[str],
    exporter_col: str,
    importer_col: str,
    value_col: str,
    features: Sequence[str] = (),
    min_nodes: int = 3,
) -> Tuple[nw.DataFrame, GraphReport]:
    """Build one trade graph per cell and measure the requested features.

    Single scan of the table: it is sorted on ``keys`` so that the rows of a
    cell are contiguous, materialised once in NumPy, then walked group by group.
    The structural counts (:data:`STRUCTURAL_FEATURES`) are measured on every
    pass — they cost a few array reductions and always feed the report — while
    the expensive ones (:data:`TOPOLOGICAL_FEATURES`) are computed only when
    asked for, which is what lets a metric pay for its own measure and nothing
    else.

    Args:
        data: Narwhals frame of bilateral flows.
        keys: Columns identifying an output cell.
        exporter_col: Column holding the exporting country.
        importer_col: Column holding the importing country.
        value_col: Column holding the flow value (edge weight). Rows carrying a
            null or non-positive value, or a missing endpoint, are dropped: they
            describe no edge.
        features: Features to return as columns, among :data:`GRAPH_FEATURES`.
            An empty sequence returns the keys alone — useful to obtain the
            report without paying for any topological measure.
        min_nodes: Minimum number of countries below which the topological
            features are left ``NaN`` rather than computed on a degenerate
            graph.

    Returns:
        Tuple ``(frame, report)``: a narwhals frame — in the **caller's
        backend** — holding ``keys`` plus one column per requested feature, and
        the :class:`GraphReport` of the pass.

    Raises:
        ValueError: If ``features`` names something outside
            :data:`GRAPH_FEATURES`.

    Examples:
        >>> import narwhals as nw, pandas as pd
        >>> df = pd.DataFrame({
        ...     "product": ["01"] * 6,
        ...     "exporter": ["FR", "DE", "FR", "IT", "DE", "IT"],
        ...     "importer": ["DE", "IT", "IT", "FR", "FR", "DE"],
        ...     "value": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        ... })
        >>> frame, report = compute_graph_features(
        ...     nw.from_native(df, eager_only=True),
        ...     keys=["product"], exporter_col="exporter",
        ...     importer_col="importer", value_col="value",
        ...     features=["clustering_w", "diameter"],
        ... )
        >>> frame.to_native().to_dict("records")
        [{'product': '01', 'clustering_w': 1.0, 'diameter': 1.0}]
        >>> report.n_graphs, report.median_n_nodes
        (1, 3.0)
    """
    # Validation des traits demandés (échec explicite plutôt que colonne absente)
    unknown = sorted(set(features) - set(GRAPH_FEATURES))
    if unknown:
        raise ValueError(
            f"Unknown graph feature(s) {unknown}. Available: {sorted(GRAPH_FEATURES)}."
        )
    requested = list(dict.fromkeys(features))
    want_clustering = CLUSTERING_W in requested
    want_diameter = DIAMETER in requested

    # Clés de la grille
    key_cols = list(keys)

    # Retrait des arêtes inexistantes : une valeur nulle ou négative ne décrit
    # aucun échange, et une extrémité manquante ne décrit aucun lien — les deux
    # fausseraient poids comme degrés
    n_input = len(data)
    data = data.filter(nw.col(value_col) > 0).drop_nulls(
        subset=[exporter_col, importer_col]
    )
    share_dropped = (n_input - len(data)) / n_input if n_input else _NAN

    # Table vide : grille vide au bon schéma, rapport neutre
    if len(data) == 0:
        empty: Dict[str, Any] = {col: np.empty(0, dtype=object) for col in key_cols}
        empty.update({name: np.empty(0, dtype=np.float64) for name in requested})
        return (
            nw.from_dict(empty, backend=nw.get_native_namespace(data)),
            GraphReport(share_rows_dropped_non_positive=share_dropped),
        )

    # Encodage entier des pays, puis tri sur les clés : condition de contiguïté
    # du balayage (la jointure d'encodage ne préserve pas l'ordre, le tri suit)
    data, n_countries = _encode_countries(
        data, exporter_col=exporter_col, importer_col=importer_col
    )
    data = data.sort(*key_cols)
    n_rows = len(data)

    # Matérialisation NumPy (unique franchissement de la frontière narwhals)
    key_values = [_column(data, col) for col in key_cols]
    values = _column(data, value_col).astype(np.float64)
    exporter_codes = _column(data, _EXPORTER_CODE)
    importer_codes = _column(data, _IMPORTER_CODE)

    # Tampons de ré-indexation locale, alloués une fois pour tout le balayage
    lookup = np.zeros(n_countries, dtype=np.int64)
    present_flags = np.zeros(n_countries, dtype=bool)

    # Bornes des cellules
    starts = _group_starts(key_values, n_rows)
    ends = np.append(starts[1:], n_rows)
    n_graphs = starts.size

    # Accumulateurs des traits, cellule par cellule
    measured: Dict[str, np.ndarray] = {
        name: np.full(n_graphs, _NAN, dtype=np.float64)
        for name in dict.fromkeys(list(STRUCTURAL_FEATURES) + requested)
    }

    # Balayage : un graphe par cellule
    for position, (start, end) in enumerate(zip(starts, ends)):
        weights, adjacency = _cell_matrix(
            exporter_codes[start:end],
            importer_codes[start:end],
            values[start:end],
            lookup=lookup,
            present_flags=present_flags,
        )
        n_nodes = adjacency.shape[0]

        # Traits structurels : quelques réductions, toujours mesurés
        n_edges = float(adjacency.sum()) / 2.0
        measured[N_NODES][position] = float(n_nodes)
        measured[N_EDGES][position] = n_edges
        measured[DENSITY][position] = (
            2.0 * n_edges / (n_nodes * (n_nodes - 1)) if n_nodes > 1 else _NAN
        )
        measured[N_COMPONENTS][position] = float(
            connected_components(csr_matrix(adjacency), directed=False)[0]
        )

        # Traits topologiques : réservés aux graphes exploitables
        if n_nodes < min_nodes:
            continue
        if want_clustering:
            measured[CLUSTERING_W][position] = weighted_clustering(adjacency, weights)
        if want_diameter:
            measured[DIAMETER][position] = diameter(adjacency)

    # Diagnostics de la passe
    n_components = measured[N_COMPONENTS]
    report = GraphReport(
        n_graphs=int(n_graphs),
        median_n_nodes=float(np.median(measured[N_NODES])),
        median_n_edges=float(np.median(measured[N_EDGES])),
        median_density=float(np.nanmedian(measured[DENSITY])),
        share_graphs_disconnected=float((n_components > 1).mean()),
        share_graphs_below_min_nodes=float((measured[N_NODES] < min_nodes).mean()),
        share_rows_dropped_non_positive=share_dropped,
    )

    # Logging
    logger.info(
        "compute_graph_features: %d graphes, %.0f nœuds médians, %.1f%% déconnectés",
        report.n_graphs,
        report.median_n_nodes,
        100.0 * report.share_graphs_disconnected,
    )

    # Retour dans le backend de l'appelant : les clés reprennent la valeur de la
    # première ligne de chaque cellule (identique sur toute la cellule par
    # construction du tri)
    output: Dict[str, Any] = {
        col: values_of_col[starts] for col, values_of_col in zip(key_cols, key_values)
    }
    output.update({name: measured[name] for name in requested})
    return nw.from_dict(output, backend=nw.get_native_namespace(data)), report
