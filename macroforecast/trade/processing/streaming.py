"""Sufficient-statistic accumulators for estimations pooled over many data chunks.

Every estimator of this module consumes its data **chunk by chunk** and keeps only
statistics that are additive over any partition of the observations: feeding the
same observations as one chunk or as many chunks of any size and order yields the
same estimate, up to floating-point rounding. Memory is therefore bounded by one
chunk plus the (small) statistics, whatever the total number of observations.

The accumulators follow the scikit-learn ``partial_fit`` convention:
``partial_fit(...)`` returns ``self`` and folds one more chunk into the state;
``finalize()`` (or ``solve()``) derives the estimate from the state. No method reads
or writes any file.

* :class:`WelfordGroupStats` — count, mean and standard deviation per group key,
  merged with Chan's parallel formula (numerically stable, unlike raw ``Σx``/``Σx²``).
* :class:`WeightedLeastSquaresAccumulator` — weighted least squares from the
  triangular factor ``R`` of ``[√w·X | √w·y]``, updated by successive QR
  decompositions (TSQR). ``RᵀR`` holds the cross-products ``A = Σ w x xᵀ``,
  ``b = Σ w x y`` and ``c = Σ w y²``; keeping them in factored form preserves the
  accuracy of a direct SVD-based solve, which the normal equations lose on
  ill-conditioned designs (relative error ``κ(X)²·ε`` instead of ``κ(X)·ε``).
* :class:`CookFilter` — Cook's distance of each observation from a first fit.
* :class:`AbsorbedWLSAccumulator` — weighted least squares on one-hot factor
  designs with one absorbed group dimension (within transformation), with the
  heteroskedasticity-robust covariance computed in a second pass.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
from dataclasses import dataclass, field
from typing import Dict, Hashable, List, Mapping, Optional, Sequence
# Modules de manipulation de données
import numpy as np
import pandas as pd

# Taille par défaut des sous-blocs de lignes d'une mise à jour QR : borne la mémoire
# de la matrice empilée sans dégrader le débit des routines LAPACK
_DEFAULT_BLOCK_ROWS = 262_144

# Seuil relatif des valeurs singulières conservées par la pseudo-inverse : celui de
# statsmodels (pinv_extended), dont la solution est reproduite à l'identique
_PINV_RCOND = 1e-15

# Seuil relatif de la norme d'une colonne démoyennée en deçà duquel elle est réputée
# entièrement absorbée par les effets de groupe
_ABSORBED_TOL = 1e-8


# ──────────────────────────────────────────────────────────────────────
# Moments par groupe
# ──────────────────────────────────────────────────────────────────────

# Accumulateur des moments d'ordre 1 et 2 par clé de groupe
class WelfordGroupStats:
    """Count, mean and standard deviation per group, accumulated chunk by chunk.

    The state holds, per group key, the count ``n``, the mean ``x̄`` and the sum of
    squared deviations ``M2 = Σ (x - x̄)²``. Two partial states ``a`` and ``b`` of a
    same group merge with Chan's parallel formula::

        n   = n_a + n_b
        δ   = x̄_b - x̄_a
        x̄   = x̄_a + δ · n_b / n
        M2  = M2_a + M2_b + δ² · n_a · n_b / n

    so that any partition of the observations yields the same moments. Unlike the
    raw sums ``Σx`` and ``Σx²`` (both still exposed), this form does not cancel
    catastrophically when the mean is large compared with the dispersion. Missing
    values are skipped, as in a pandas ``groupby``.

    Args:
        ddof: Delta degrees of freedom of the standard deviation
            (``std = √(M2 / (n - ddof))``); ``1`` reproduces pandas' sample
            standard deviation, ``NaN`` when ``n <= ddof``.

    Attributes:
        stats_: Frame indexed by the group keys with columns ``count``, ``mean``,
            ``std``, ``sum`` and ``sum_sq``, sorted by key; set by
            :meth:`finalize`.

    Examples:
        >>> df = pd.DataFrame({"k": ["a", "a", "b", "a"], "x": [1.0, 2.0, 5.0, 3.0]})
        >>> acc = WelfordGroupStats()
        >>> acc = acc.partial_fit(df.iloc[:2], by=["k"], value="x")
        >>> acc = acc.partial_fit(df.iloc[2:], by=["k"], value="x").finalize()
        >>> acc.stats_[["count", "mean", "std"]].to_dict("index")
        {'a': {'count': 3, 'mean': 2.0, 'std': 1.0}, 'b': {'count': 1, 'mean': 5.0, 'std': nan}}
    """

    # Initialisation
    def __init__(self, *, ddof: int = 1) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.ddof = ddof

    # Moments d'une tranche, par groupe
    @staticmethod
    def _chunk_moments(df_chunk: pd.DataFrame, by: Sequence[str], value: str) -> pd.DataFrame:
        """Return ``n``, mean and ``M2`` per group of one chunk (two-pass, exact).

        Args:
            df_chunk: Chunk of observations.
            by: Group-key columns.
            value: Value column.

        Returns:
            Frame indexed by the group keys with columns ``n``, ``mean``, ``m2``.
        """
        # Observations exploitables : valeur renseignée, clé complète
        df_valid = df_chunk.loc[df_chunk[value].notna(), list(by) + [value]]
        if df_valid.empty:
            return pd.DataFrame(columns=["n", "mean", "m2"], dtype="float64")
        grouped = df_valid.groupby(list(by), sort=False)[value]
        n = grouped.count()
        mean = grouped.mean()
        # Écarts à la moyenne du groupe (deuxième passage) : M2 sans annulation
        deviation = df_valid[value] - grouped.transform("mean")
        m2 = (deviation ** 2).groupby([df_valid[col] for col in by], sort=False).sum()
        return pd.DataFrame({"n": n.astype("float64"), "mean": mean, "m2": m2})

    # Fusion de deux états partiels (formule de Chan)
    @staticmethod
    def _merge(df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame:
        """Merge two partial states with Chan's parallel formula.

        Args:
            df_a: First state (``n``, ``mean``, ``m2`` by key).
            df_b: Second state, same layout.

        Returns:
            The merged state.
        """
        if df_a.empty:
            return df_b.copy()
        if df_b.empty:
            return df_a.copy()
        # Alignement sur l'union des clés ; une clé absente d'un état compte pour n = 0
        df_a, df_b = df_a.align(df_b, join="outer")
        n_a = df_a["n"].fillna(0.0)
        n_b = df_b["n"].fillna(0.0)
        mean_a = df_a["mean"].fillna(0.0)
        mean_b = df_b["mean"].fillna(0.0)
        n = n_a + n_b
        delta = mean_b - mean_a
        mean = mean_a + delta * n_b / n
        m2 = df_a["m2"].fillna(0.0) + df_b["m2"].fillna(0.0) + delta ** 2 * n_a * n_b / n
        # Clé présente d'un seul côté : état repris tel quel (évite tout arrondi)
        only_a, only_b = n_b == 0, n_a == 0
        mean = mean.where(~only_a, df_a["mean"]).where(~only_b, df_b["mean"])
        m2 = m2.where(~only_a, df_a["m2"]).where(~only_b, df_b["m2"])
        return pd.DataFrame({"n": n, "mean": mean, "m2": m2})

    # Ajout d'une tranche d'observations
    def partial_fit(
        self, df_chunk: pd.DataFrame, *, by: Sequence[str], value: str
    ) -> "WelfordGroupStats":
        """Fold one chunk of observations into the per-group moments.

        Args:
            df_chunk: Chunk holding the group-key columns and the value column.
            by: Group-key columns (the same at every call).
            value: Value column.

        Returns:
            The accumulator (``self``).

        Raises:
            ValueError: If ``by`` differs from the keys of the previous calls.
        """
        # Vérification de la stabilité des clés entre tranches
        by = list(by)
        if getattr(self, "by_", None) is None:
            self.by_ = by
            self.state_ = pd.DataFrame(columns=["n", "mean", "m2"], dtype="float64")
        elif self.by_ != by:
            raise ValueError(f"Group keys changed between chunks: {self.by_} then {by}.")
        self.state_ = self._merge(self.state_, self._chunk_moments(df_chunk, by, value))
        return self

    # Dérivation des statistiques finales
    def finalize(self) -> "WelfordGroupStats":
        """Derive count, mean, standard deviation and raw sums per group.

        Returns:
            The accumulator (``self``), with ``stats_`` set.
        """
        state = getattr(self, "state_", pd.DataFrame(columns=["n", "mean", "m2"], dtype="float64"))
        n = state["n"]
        with np.errstate(invalid="ignore", divide="ignore"):
            variance = state["m2"] / (n - self.ddof)
        variance = variance.where(n > self.ddof)
        df_stats = pd.DataFrame(
            {
                "count": n.astype("int64"),
                "mean": state["mean"],
                "std": np.sqrt(variance),
                "sum": n * state["mean"],
                "sum_sq": state["m2"] + n * state["mean"] ** 2,
            },
            index=state.index,
        )
        self.stats_ = df_stats.sort_index()
        return self

    # Sérialisation de l'état (reprise d'une passe interrompue)
    def to_frame(self) -> pd.DataFrame:
        """Return the raw state (``n``, ``mean``, ``m2`` by key) as a flat frame.

        Returns:
            Flat frame with the group-key columns followed by ``n``, ``mean``,
            ``m2`` — suitable for persistence by the caller.

        Examples:
            >>> df = pd.DataFrame({"k": ["a", "b"], "x": [1.0, 2.0]})
            >>> WelfordGroupStats().partial_fit(df, by=["k"], value="x").to_frame().columns.tolist()
            ['k', 'n', 'mean', 'm2']
        """
        state = getattr(self, "state_", pd.DataFrame(columns=["n", "mean", "m2"], dtype="float64"))
        df_flat = state.reset_index()
        if getattr(self, "by_", None) is not None and len(state) == 0:
            df_flat = pd.DataFrame(columns=list(self.by_) + ["n", "mean", "m2"])
        return df_flat

    # Restauration d'un état sérialisé
    @classmethod
    def from_frame(
        cls, df_state: pd.DataFrame, *, by: Sequence[str], ddof: int = 1
    ) -> "WelfordGroupStats":
        """Rebuild an accumulator from the frame returned by :meth:`to_frame`.

        Args:
            df_state: Flat state frame.
            by: Group-key columns.
            ddof: Delta degrees of freedom of the standard deviation.

        Returns:
            An accumulator whose state equals the serialised one.

        Examples:
            >>> df = pd.DataFrame({"k": ["a", "a"], "x": [1.0, 3.0]})
            >>> acc = WelfordGroupStats().partial_fit(df, by=["k"], value="x")
            >>> clone = WelfordGroupStats.from_frame(acc.to_frame(), by=["k"]).finalize()
            >>> float(clone.stats_.loc["a", "std"])
            1.4142135623730951
        """
        acc = cls(ddof=ddof)
        acc.by_ = list(by)
        # État vide (aucune observation) : schéma restauré sans clé
        if df_state.empty or not set(by) <= set(df_state.columns):
            acc.state_ = pd.DataFrame(columns=["n", "mean", "m2"], dtype="float64")
            return acc
        state = df_state.set_index(list(by))[["n", "mean", "m2"]].astype("float64")
        acc.state_ = state
        return acc


# ──────────────────────────────────────────────────────────────────────
# Moindres carrés pondérés par QR incrémentale
# ──────────────────────────────────────────────────────────────────────

# Solution d'un ajustement des moindres carrés pondérés
@dataclass
class WlsSolution:
    """Weighted least-squares solution derived from accumulated statistics.

    Every quantity reproduces the ``statsmodels`` WLS fit (``method="pinv"``) on the
    same observations: the solution is the minimum-norm one, so a design column
    without any non-zero value gets a zero coefficient instead of an error.

    Attributes:
        columns: Design columns of the solution, in order.
        params: Coefficients ``β = (XᵀWX)⁺ XᵀWy``.
        cov_unscaled: ``(XᵀWX)⁺`` (``normalized_cov_params`` in statsmodels).
        pinv_r: Pseudo-inverse ``R⁺`` of the triangular factor, so that
            ``(XᵀWX)⁺ = R⁺ R⁺ᵀ``; used for the leverages.
        rss: Weighted residual sum of squares ``Σ w (y - xᵀβ)²``.
        rank: Numerical rank of the design.
        nobs: Number of observations.
        df_resid: Residual degrees of freedom ``nobs - rank``.
        mse: ``rss / df_resid`` (``NaN`` when ``df_resid`` is zero).
        centered_tss: Weighted total sum of squares ``Σ w (y - ȳ_w)²``.
        r_squared: ``1 - rss / centered_tss`` (meaningful when the design holds
            a constant).
        bse: Standard errors ``√diag(mse · (XᵀWX)⁺)``.
    """
    # Initialisation des attributs
    columns: List[str]
    params: np.ndarray
    cov_unscaled: np.ndarray
    pinv_r: np.ndarray
    rss: float
    rank: int
    nobs: int
    df_resid: int
    mse: float
    centered_tss: float
    r_squared: float
    bse: np.ndarray = field(default_factory=lambda: np.empty(0))

    # Coefficients indexés par colonne
    @property
    def params_series(self) -> pd.Series:
        """Coefficients as a Series indexed by column name."""
        return pd.Series(self.params, index=self.columns, dtype="float64")


# Accumulateur des moindres carrés pondérés (facteur triangulaire)
class WeightedLeastSquaresAccumulator:
    """Weighted least squares accumulated chunk by chunk through a QR factor.

    The state is the upper-triangular factor ``R`` of the stacked matrix
    ``[√w·X | √w·y]`` (``p + 1`` columns): appending a chunk replaces ``R`` with the
    ``R`` factor of ``[R ; √w·X_chunk | √w·y_chunk]``, which leaves ``RᵀR`` equal to
    the cross-products of **all** the observations seen so far::

        RᵀR = [[A, b], [bᵀ, c]]   with   A = Σ w x xᵀ,  b = Σ w x y,  c = Σ w y²

    These are the sufficient statistics of the fit, additive over any partition of
    the observations. The factored form is kept because solving the normal
    equations ``A β = b`` squares the condition number of the design: on the BACI
    gravity design (``ln dist`` and ``(ln dist)²`` nearly collinear) the normal
    equations drift by about ``1e-7`` in relative terms from an SVD-based solve,
    whereas ``R`` stays within ``1e-11``.

    Alongside ``R`` the accumulator keeps the number of observations, the number
    of non-zero rows of each column (to spot empty dummies) and the weighted mean
    and centred sum of squares of ``y`` (merged with Chan's formula), needed for the
    ``R²`` of a model with a constant.

    Args:
        columns: Design columns, fixed at construction: every chunk passes a
            matrix with exactly these columns, in this order.
        block_rows: Row-block size of the QR updates (memory bound of the stacked
            matrix; no effect on the result beyond rounding).

    Attributes:
        r_: Current triangular factor, shape ``(p + 1, p + 1)``.
        n_obs_: Number of observations accumulated.
        col_count_: Number of observations with a non-zero value, per column.

    Examples:
        >>> rng = np.random.default_rng(0)
        >>> X = np.column_stack([np.ones(50), rng.normal(size=50)])
        >>> y = X @ np.array([1.0, 2.0]) + rng.normal(scale=0.1, size=50)
        >>> w = rng.uniform(0.5, 1.5, 50)
        >>> acc = WeightedLeastSquaresAccumulator(["const", "x"])
        >>> for part in np.array_split(np.arange(50), 3):
        ...     acc = acc.partial_fit(X[part], y[part], w[part])
        >>> sol = acc.solve()
        >>> beta = np.linalg.solve((X * w[:, None]).T @ X, X.T @ (w * y))
        >>> bool(np.allclose(sol.params, beta, rtol=1e-12))
        True
        >>> bool(np.allclose(acc.A, (X * w[:, None]).T @ X))
        True
    """

    # Initialisation
    def __init__(self, columns: Sequence[str], *, block_rows: int = _DEFAULT_BLOCK_ROWS) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.columns = list(columns)
        self.block_rows = block_rows
        # État vide : facteur nul, aucun effectif
        p = len(self.columns)
        self.r_ = np.zeros((p + 1, p + 1))
        self.n_obs_ = 0
        self.col_count_ = np.zeros(p, dtype="int64")
        self._sum_w = 0.0
        self._mean_y = 0.0
        self._m2_y = 0.0

    # Ajout d'une tranche d'observations
    def partial_fit(
        self, X: np.ndarray, y: np.ndarray, w: np.ndarray
    ) -> "WeightedLeastSquaresAccumulator":
        """Fold one chunk of observations into the triangular factor.

        Args:
            X: Design matrix of the chunk, shape ``(n, p)``, columns ordered as
                ``columns``.
            y: Dependent variable, shape ``(n,)``.
            w: Strictly positive weights, shape ``(n,)``.

        Returns:
            The accumulator (``self``).

        Raises:
            ValueError: If the shapes are inconsistent with ``columns``.
        """
        # Vérification des dimensions
        X = np.asarray(X, dtype="float64")
        y = np.asarray(y, dtype="float64").reshape(-1)
        w = np.asarray(w, dtype="float64").reshape(-1)
        p = len(self.columns)
        if X.ndim != 2 or X.shape[1] != p or len(y) != len(X) or len(w) != len(X):
            raise ValueError(
                f"Expected X of shape (n, {p}) with y and w of length n, got "
                f"X{X.shape}, y({len(y)}), w({len(w)})."
            )
        if len(X) == 0:
            return self

        # Mise à jour par sous-blocs : QR de [R ; √w·X | √w·y]
        for start in range(0, len(X), self.block_rows):
            stop = start + self.block_rows
            sqrt_w = np.sqrt(w[start:stop])
            block = np.column_stack([X[start:stop], y[start:stop]]) * sqrt_w[:, None]
            stacked = np.vstack([self.r_, block])
            r_new = np.linalg.qr(stacked, mode="r")
            # Facteur toujours carré (complété par des lignes nulles si peu d'observations)
            self.r_ = np.zeros((p + 1, p + 1))
            self.r_[: r_new.shape[0]] = r_new

        # Effectifs et colonnes renseignées
        self.n_obs_ += len(X)
        self.col_count_ += (X != 0).sum(axis=0)

        # Moyenne et dispersion pondérées de y (fusion de Chan)
        sum_w = float(w.sum())
        mean_y = float(np.average(y, weights=w))
        m2_y = float(np.sum(w * (y - mean_y) ** 2))
        total = self._sum_w + sum_w
        delta = mean_y - self._mean_y
        self._m2_y = self._m2_y + m2_y + delta ** 2 * self._sum_w * sum_w / total
        self._mean_y = self._mean_y + delta * sum_w / total
        self._sum_w = total
        return self

    # Produits croisés dérivés du facteur
    @property
    def A(self) -> np.ndarray:
        """Cross-product matrix ``Σ w x xᵀ`` (``p × p``)."""
        p = len(self.columns)
        gram = self.r_.T @ self.r_
        return gram[:p, :p]

    @property
    def b(self) -> np.ndarray:
        """Cross-product vector ``Σ w x y`` (``p``)."""
        p = len(self.columns)
        return (self.r_.T @ self.r_)[:p, p]

    @property
    def c(self) -> float:
        """Weighted sum of squares ``Σ w y²``."""
        p = len(self.columns)
        return float((self.r_.T @ self.r_)[p, p])

    # Résolution sur un sous-ensemble de colonnes
    def solve(self, columns: Optional[Sequence[str]] = None) -> WlsSolution:
        """Solve the weighted least squares on all or some of the design columns.

        Selecting columns is exact: since ``√W·X = Q·R``, the factor of the
        sub-design ``√W·X[:, S]`` is the ``R`` factor of ``R[:, S]``. The solution
        mirrors ``statsmodels``' ``WLS(...).fit()`` (pseudo-inverse with relative
        cut-off ``1e-15``, rank from the singular values, ``df_resid = nobs −
        rank``).

        Args:
            columns: Columns to keep, in the order wanted for the solution;
                ``None`` keeps every column.

        Returns:
            The :class:`WlsSolution`.

        Raises:
            KeyError: If a requested column is not a design column.
            ValueError: If no observation was accumulated.
        """
        # Vérification de l'existence d'observations
        if self.n_obs_ == 0:
            raise ValueError("No observation accumulated: nothing to solve.")
        # Sélection des colonnes et re-triangularisation
        names = list(self.columns) if columns is None else list(columns)
        position = {name: i for i, name in enumerate(self.columns)}
        missing = [name for name in names if name not in position]
        if missing:
            raise KeyError(f"Unknown design columns: {missing}")
        selected = [position[name] for name in names] + [len(self.columns)]
        r_sel = np.linalg.qr(self.r_[:, selected], mode="r")
        k = len(names)
        r_full = np.zeros((k + 1, k + 1))
        r_full[: r_sel.shape[0]] = r_sel
        r_xx, z, r_yy = r_full[:k, :k], r_full[:k, k], r_full[k, k]

        # Pseudo-inverse du facteur (seuil relatif de statsmodels)
        u, s, vt = np.linalg.svd(r_xx)
        cutoff = _PINV_RCOND * (s.max() if s.size else 0.0)
        s_inv = np.where(s > cutoff, 1.0 / np.where(s > cutoff, s, 1.0), 0.0)
        pinv_r = (vt.T * s_inv) @ u.T
        params = pinv_r @ z
        cov_unscaled = pinv_r @ pinv_r.T
        rank = int(np.linalg.matrix_rank(np.diag(s))) if s.size else 0

        # Somme des carrés des résidus : composante dans l'espace du facteur plus
        # composante orthogonale portée par le dernier pivot (valable à tout rang)
        rss = float(np.sum((z - r_xx @ params) ** 2) + r_yy ** 2)
        df_resid = int(self.n_obs_ - rank)
        mse = rss / df_resid if df_resid > 0 else float("nan")
        r_squared = 1.0 - rss / self._m2_y if self._m2_y > 0 else float("nan")
        with np.errstate(invalid="ignore"):
            bse = np.sqrt(np.diag(mse * cov_unscaled))
        return WlsSolution(
            columns=names,
            params=params,
            cov_unscaled=cov_unscaled,
            pinv_r=pinv_r,
            rss=rss,
            rank=rank,
            nobs=int(self.n_obs_),
            df_resid=df_resid,
            mse=mse,
            centered_tss=float(self._m2_y),
            r_squared=r_squared,
            bse=bse,
        )


# ──────────────────────────────────────────────────────────────────────
# Distance de Cook
# ──────────────────────────────────────────────────────────────────────

# Filtre des observations influentes au sens de Cook
class CookFilter:
    """Cook's distance of each observation from a first weighted fit.

    Reproduces ``OLSInfluence(sm.OLS(√w·y, √w·X).fit()).cooks_distance[0]``::

        ẽ_i = √w_i (y_i - x_iᵀβ)                     (whitened residual)
        h_ii = x̃_iᵀ (X̃ᵀX̃)⁺ x̃_i = ‖R⁺ᵀ x̃_i‖²        (leverage, x̃ = √w·x)
        D_i = ẽ_i² h_ii / (k · MSE · (1 - h_ii)²)

    where ``k`` is the number of design **columns** (constant included, as
    ``k_vars`` in statsmodels) and ``MSE = RSS / (N - rank)``. An observation is
    kept when ``D_i < cook_factor / N``, ``N`` being the number of observations of
    the first fit; a ``NaN`` distance (``h_ii = 1``) is not kept, the comparison
    being false. The distance depends on the whole sample only through ``β``,
    ``R⁺`` and ``MSE``: one more pass over the chunks evaluates it exactly.

    Args:
        solution: First fit (:meth:`WeightedLeastSquaresAccumulator.solve`).
        cook_factor: Numerator of the cut-off ``cook_factor / N``.
        n_obs: ``N`` of the cut-off; defaults to ``solution.nobs``.
        k_vars: ``k`` of the distance; defaults to the number of columns of
            ``solution``.

    Examples:
        >>> rng = np.random.default_rng(1)
        >>> X = np.column_stack([np.ones(40), rng.normal(size=40)])
        >>> y = X @ np.array([0.5, 1.0]) + rng.normal(scale=0.1, size=40)
        >>> y[3] += 5.0  # observation aberrante
        >>> w = np.ones(40)
        >>> sol = WeightedLeastSquaresAccumulator(["c", "x"]).partial_fit(X, y, w).solve()
        >>> keep = CookFilter(sol, cook_factor=4.0).keep_mask(X, y, w)
        >>> bool(keep[3])
        False
    """

    # Initialisation
    def __init__(
        self,
        solution: WlsSolution,
        *,
        cook_factor: float,
        n_obs: Optional[int] = None,
        k_vars: Optional[int] = None,
    ) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.solution = solution
        self.cook_factor = cook_factor
        self.n_obs = n_obs
        self.k_vars = k_vars

    # Distance de Cook par observation
    def distance(self, X: np.ndarray, y: np.ndarray, w: np.ndarray) -> np.ndarray:
        """Return Cook's distance of each observation of a chunk.

        Args:
            X: Design matrix of the chunk, columns ordered as ``solution.columns``.
            y: Dependent variable.
            w: Weights.

        Returns:
            Array of Cook's distances (``NaN`` where the leverage is one).
        """
        X = np.asarray(X, dtype="float64")
        sqrt_w = np.sqrt(np.asarray(w, dtype="float64"))
        k_vars = self.k_vars if self.k_vars is not None else len(self.solution.columns)
        # Résidus blanchis et leviers sur le modèle transformé par √w
        resid = sqrt_w * (np.asarray(y, dtype="float64") - X @ self.solution.params)
        projected = (X * sqrt_w[:, None]) @ self.solution.pinv_r
        hat = np.sum(projected ** 2, axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            return resid ** 2 * hat / (k_vars * self.solution.mse * (1.0 - hat) ** 2)

    # Masque des observations conservées
    def keep_mask(self, X: np.ndarray, y: np.ndarray, w: np.ndarray) -> np.ndarray:
        """Flag the observations kept by the Cook cut-off.

        Args:
            X: Design matrix of the chunk, columns ordered as ``solution.columns``.
            y: Dependent variable.
            w: Weights.

        Returns:
            Boolean array, ``True`` where ``D_i < cook_factor / N``.
        """
        n_obs = self.n_obs if self.n_obs is not None else self.solution.nobs
        with np.errstate(invalid="ignore"):
            return self.distance(X, y, w) < self.cook_factor / n_obs


# ──────────────────────────────────────────────────────────────────────
# Moindres carrés pondérés à effets de groupe absorbés
# ──────────────────────────────────────────────────────────────────────

# Accumulateur d'une ANOVA pondérée à effets de groupe absorbés
class AbsorbedWLSAccumulator:
    """Weighted least squares with one absorbed group dimension, chunk by chunk.

    The design ``z`` is made of one-hot blocks of categorical **factors** (one
    column per level, columns named ``"<factor>_<level>"`` and fixed at
    construction); the group dimension ``k`` (e.g. the product) is absorbed by the
    within transformation, i.e. by subtracting the **weighted** group means. The
    first pass accumulates, globally::

        G = Σ w z zᵀ,   g = Σ w z y,   q = Σ w y²,   N

    and per group ``k``::

        W_k = Σ w,   s_k = Σ w z,   t_k = Σ w y

    from which the demeaned cross-products follow exactly::

        G̃ = G - Σ_k s_k s_kᵀ / W_k,    g̃ = g - Σ_k s_k t_k / W_k,    β = G̃⁻¹ g̃

    with group means ``m_k = s_k / W_k`` and ``μ_k = t_k / W_k``. All the terms are
    sums over observations, hence additive over any partition — provided ``s_k``
    and ``W_k`` cover every chunk where group ``k`` appears. The
    heteroskedasticity-robust covariance needs the residuals
    ``e = (y - μ_k) - (z - m_k)ᵀβ``, hence a second pass (:meth:`robust_meat`)
    accumulating ``M = Σ (w e)² z̃ z̃ᵀ`` with ``z̃ = z - m_k``; then::

        Cov = G̃⁻¹ M G̃⁻¹      (symmetrised)

    which is exactly ``linearmodels``' ``AbsorbingLS(...).fit()`` default
    (``cov_type="robust"``, ``debiased=False``): weights normalised by their mean
    (a factor that cancels out), within transformation by weighted group means,
    no degrees-of-freedom correction.

    The one-hot blocks are never materialised: the cross-products are weighted
    counts per pair of levels (``np.bincount``), so a chunk of millions of rows
    over hundreds of levels costs a few vectors.

    Args:
        factors: Ordered mapping ``factor name -> levels``; the design columns are
            the levels of each factor, in this order.

    Attributes:
        level_count_: Number of observations per design column (unweighted),
            after the first pass.
        params_: Coefficients on the selected columns (:meth:`solve`).
        columns_: Selected columns (:meth:`solve`).

    Examples:
        >>> df = pd.DataFrame({
        ...     "e": ["a", "b", "a", "b", "a", "b"], "y": [1.0, 2.0, 1.5, 2.5, 0.5, 3.0],
        ...     "w": [1.0, 2.0, 1.0, 1.0, 2.0, 1.0], "k": [1, 1, 2, 2, 3, 3]})
        >>> acc = AbsorbedWLSAccumulator({"e": ["a", "b"]})
        >>> acc = acc.partial_fit(df[["e"]], df["y"], df["w"], df["k"])
        >>> acc = acc.solve(["e_b"])
        >>> acc = acc.robust_meat(df[["e"]], df["y"], df["w"], df["k"])
        >>> acc.params_.round(4).to_dict(), acc.covariance().round(4).to_numpy().tolist()
        ({'e_b': 1.5455}, [[0.0999]])
    """

    # Initialisation
    def __init__(self, factors: Mapping[str, Sequence[Hashable]]) -> None:
        # Initialisation des attributs (stockage tel quel, convention sklearn)
        self.factors = {name: list(levels) for name, levels in factors.items()}
        # Colonnes du design et positions de départ de chaque bloc
        self.columns: List[str] = []
        self._offsets: Dict[str, int] = {}
        for name, levels in self.factors.items():
            self._offsets[name] = len(self.columns)
            self.columns.extend(f"{name}_{level}" for level in levels)
        p = len(self.columns)
        # Statistiques globales
        self._G = np.zeros((p, p))
        self._g = np.zeros(p)
        self._q = 0.0
        self.n_obs_ = 0
        self.level_count_ = np.zeros(p, dtype="int64")
        # Statistiques par groupe (index dynamique des groupes)
        self._groups = pd.Index([], dtype=object)
        self._W = np.zeros(0)
        self._S = np.zeros((0, p))
        self._T = np.zeros(0)
        # Seconde passe
        self._M = np.zeros((p, p))

    # Positions des colonnes du design de chaque observation
    def _column_codes(self, df_factors: pd.DataFrame) -> List[np.ndarray]:
        """Map each factor's labels to design-column positions.

        Args:
            df_factors: One column per factor, holding level labels.

        Returns:
            One integer array per factor (column positions in the design).

        Raises:
            ValueError: If a factor column is missing or holds an unknown level.
        """
        codes = []
        for name, levels in self.factors.items():
            if name not in df_factors.columns:
                raise ValueError(f"Factor column {name!r} is missing from the chunk.")
            code = pd.Categorical(df_factors[name], categories=levels).codes
            if (code < 0).any():
                unknown = sorted(set(df_factors.loc[code < 0, name].astype(str)))[:5]
                raise ValueError(
                    f"Factor {name!r} holds levels outside the fixed design: {unknown}"
                )
            codes.append(code.astype("int64") + self._offsets[name])
        return codes

    # Positions des groupes de chaque observation (index étendu à la volée)
    def _group_codes(self, groups: pd.Series) -> np.ndarray:
        """Map group keys to dense indices, growing the per-group state if needed.

        Args:
            groups: Group key of each observation.

        Returns:
            Integer array of group positions.
        """
        uniques = pd.Index(pd.unique(np.asarray(groups, dtype=object)))
        new = uniques.difference(self._groups, sort=False)
        if len(new):
            p = len(self.columns)
            self._groups = self._groups.append(new)
            self._W = np.concatenate([self._W, np.zeros(len(new))])
            self._T = np.concatenate([self._T, np.zeros(len(new))])
            self._S = np.vstack([self._S, np.zeros((len(new), p))])
        return self._groups.get_indexer(np.asarray(groups, dtype=object))

    # Produits croisés pondérés d'un design à blocs indicateurs
    def _cross(self, codes: List[np.ndarray], weights: np.ndarray) -> np.ndarray:
        """Return ``Σ weight · z zᵀ`` for a one-hot factor design.

        Args:
            codes: Design-column positions of each factor.
            weights: Weight of each observation.

        Returns:
            The ``p × p`` cross-product matrix.
        """
        p = len(self.columns)
        out = np.zeros((p, p))
        for a in range(len(codes)):
            for b in range(a, len(codes)):
                # Effectifs pondérés par paire de modalités (une seule cellule par ligne)
                flat = np.bincount(codes[a] * p + codes[b], weights=weights, minlength=p * p)
                block = flat.reshape(p, p)
                out += block
                if a != b:
                    out += block.T
        return out

    # Sommes pondérées par (groupe, colonne)
    def _group_sums(
        self, codes: List[np.ndarray], group: np.ndarray, weights: np.ndarray
    ) -> np.ndarray:
        """Return ``Σ_{i ∈ k} weight_i z_i`` for every group present.

        Args:
            codes: Design-column positions of each factor.
            group: Group position of each observation.
            weights: Weight of each observation.

        Returns:
            Array ``(n_groups, p)``.
        """
        p = len(self.columns)
        n_groups = len(self._groups)
        out = np.zeros(n_groups * p)
        for code in codes:
            out += np.bincount(group * p + code, weights=weights, minlength=n_groups * p)
        return out.reshape(n_groups, p)

    # Première passe : statistiques globales et par groupe
    def partial_fit(
        self,
        df_factors: pd.DataFrame,
        y: pd.Series,
        w: pd.Series,
        groups: pd.Series,
    ) -> "AbsorbedWLSAccumulator":
        """Fold one chunk into the first-pass statistics.

        Args:
            df_factors: One column per factor, holding level labels.
            y: Dependent variable.
            w: Strictly positive weights.
            groups: Absorbed group key of each observation.

        Returns:
            The accumulator (``self``).

        Raises:
            ValueError: On a missing factor column or an unknown level.
        """
        y = np.asarray(y, dtype="float64")
        w = np.asarray(w, dtype="float64")
        if len(y) == 0:
            return self
        codes = self._column_codes(df_factors)
        group = self._group_codes(groups)
        p = len(self.columns)
        # Statistiques globales
        self._G += self._cross(codes, w)
        for code in codes:
            self._g += np.bincount(code, weights=w * y, minlength=p)
            self.level_count_ += np.bincount(code, minlength=p)
        self._q += float(np.sum(w * y * y))
        self.n_obs_ += len(y)
        # Statistiques par groupe
        n_groups = len(self._groups)
        self._W += np.bincount(group, weights=w, minlength=n_groups)
        self._T += np.bincount(group, weights=w * y, minlength=n_groups)
        self._S += self._group_sums(codes, group, w)
        return self

    # Résolution sur les variables démoyennées
    def solve(self, columns: Optional[Sequence[str]] = None) -> "AbsorbedWLSAccumulator":
        """Solve the within-transformed weighted least squares.

        Args:
            columns: Design columns to keep (e.g. all levels but one reference
                per factor); ``None`` keeps every column.

        Returns:
            The accumulator (``self``), with ``params_``, ``columns_`` and the
            bread ``G̃⁻¹`` set; the second-pass state is reset.

        Raises:
            KeyError: If a requested column is not a design column.
            ValueError: If a selected column is fully absorbed by the group
                effects, or if the demeaned design is singular.
        """
        names = list(self.columns) if columns is None else list(columns)
        position = {name: i for i, name in enumerate(self.columns)}
        missing = [name for name in names if name not in position]
        if missing:
            raise KeyError(f"Unknown design columns: {missing}")
        sel = np.array([position[name] for name in names], dtype="int64")

        # Produits croisés démoyennés (groupes non vides seulement)
        present = self._W > 0
        means = self._S[present] / self._W[present, None]
        g_tilde_full = self._G - self._S[present].T @ means
        g_vec_full = self._g - means.T @ self._T[present]
        g_tilde = g_tilde_full[np.ix_(sel, sel)]
        g_vec = g_vec_full[sel]

        # Colonnes entièrement absorbées : refus, comme linearmodels
        diag_raw = np.diag(self._G)[sel]
        with np.errstate(invalid="ignore", divide="ignore"):
            ratio = np.sqrt(np.clip(np.diag(g_tilde), 0.0, None) / diag_raw)
        absorbed = [names[i] for i in np.flatnonzero(~(ratio > _ABSORBED_TOL))]
        if absorbed:
            raise ValueError(
                f"Columns fully absorbed by the group effects: {absorbed[:5]}. "
                "This model cannot be estimated."
            )
        try:
            params = np.linalg.solve(g_tilde, g_vec)
            bread = np.linalg.inv(g_tilde)
        except np.linalg.LinAlgError as exc:
            raise ValueError(f"Singular demeaned design: {exc}") from exc

        self.columns_ = names
        self._sel = sel
        self.params_ = pd.Series(params, index=names, dtype="float64")
        self.bread_ = bread
        # Coefficients étendus au design complet (zéro hors sélection)
        self._beta_full = np.zeros(len(self.columns))
        self._beta_full[sel] = params
        # Moyennes de groupe conservées pour la seconde passe
        with np.errstate(invalid="ignore", divide="ignore"):
            self._group_mean_y = self._T / self._W
            self._group_mean_fit = (self._S @ self._beta_full) / self._W
        self._M = np.zeros((len(self.columns), len(self.columns)))
        self.meat_n_obs_ = 0
        return self

    # Seconde passe : « viande » de la covariance robuste
    def robust_meat(
        self,
        df_factors: pd.DataFrame,
        y: pd.Series,
        w: pd.Series,
        groups: pd.Series,
    ) -> "AbsorbedWLSAccumulator":
        """Fold one chunk into the robust meat ``M = Σ (w e)² z̃ z̃ᵀ``.

        The residual of each observation is ``e = (y − μ_k) − (zᵀβ − m_kᵀβ)``.
        The demeaned design ``z̃ = z − m_k`` being dense, the meat is expanded by
        group so that only weighted counts are accumulated::

            M = Σ u z zᵀ − Σ_k (a_k m_kᵀ + m_k a_kᵀ) + Σ_k U_k m_k m_kᵀ

        with ``u = (w e)²``, ``a_k = Σ_{i∈k} u_i z_i`` and ``U_k = Σ_{i∈k} u_i``.

        Args:
            df_factors: One column per factor, holding level labels.
            y: Dependent variable.
            w: Weights (the same as in the first pass).
            groups: Absorbed group key of each observation.

        Returns:
            The accumulator (``self``).

        Raises:
            AttributeError: If :meth:`solve` was not called first.
            ValueError: On an unknown level or a group unseen in the first pass.
        """
        if not hasattr(self, "params_"):
            raise AttributeError("Call solve() before accumulating the robust meat.")
        y = np.asarray(y, dtype="float64")
        w = np.asarray(w, dtype="float64")
        if len(y) == 0:
            return self
        codes = self._column_codes(df_factors)
        group = self._groups.get_indexer(np.asarray(groups, dtype=object))
        if (group < 0).any():
            raise ValueError("The second pass holds groups unseen in the first pass.")
        p = len(self.columns)
        # Résidus sur les variables démoyennées
        fit = np.zeros(len(y))
        for code in codes:
            fit += self._beta_full[code]
        resid = (y - self._group_mean_y[group]) - (fit - self._group_mean_fit[group])
        u = (w * resid) ** 2
        # Développement par groupe (aucune matrice dense par observation)
        n_groups = len(self._groups)
        means = np.zeros((n_groups, p))
        present = self._W > 0
        means[present] = self._S[present] / self._W[present, None]
        sums_u = self._group_sums(codes, group, u)
        total_u = np.bincount(group, weights=u, minlength=n_groups)
        cross = sums_u.T @ means
        self._M += self._cross(codes, u) - cross - cross.T + (means.T * total_u) @ means
        self.meat_n_obs_ += len(y)
        return self

    # Covariance robuste des coefficients
    def covariance(self) -> pd.DataFrame:
        """Return the heteroskedasticity-robust covariance ``G̃⁻¹ M G̃⁻¹``.

        Returns:
            Symmetrised covariance indexed by the selected columns.

        Raises:
            AttributeError: If :meth:`solve` was not called first.
        """
        if not hasattr(self, "params_"):
            raise AttributeError("Call solve() before computing the covariance.")
        meat = self._M[np.ix_(self._sel, self._sel)]
        cov = self.bread_ @ meat @ self.bread_
        cov = (cov + cov.T) / 2.0
        return pd.DataFrame(cov, index=self.columns_, columns=self.columns_)

    # Effectifs par modalité, par facteur
    def level_counts(self) -> Dict[str, pd.Series]:
        """Return the number of observations per level, factor by factor.

        Returns:
            Mapping ``factor -> Series(level -> count)``.
        """
        out: Dict[str, pd.Series] = {}
        for name, levels in self.factors.items():
            start = self._offsets[name]
            out[name] = pd.Series(
                self.level_count_[start : start + len(levels)], index=levels, dtype="int64"
            )
        return out
