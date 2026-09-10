# Prompts d'implémentation — synthèse de métriques de vulnérabilité

Liste ordonnée de prompts à exécuter chacun dans une session Claude Code séparée, depuis
la racine du dépôt `trade-analysis`. Chaque prompt est autonome : il rappelle le contexte
indispensable et renvoie aux sections de `AGREGATION_ARCHITECTURE.md` (noté « ARCH »)
qu'il faut lire avant d'agir. Les identifiants `M-xx`, `I-xx`, `A-xx`, `D-xx`, `S-x` sont
ceux d'ARCH.

**Choix du modèle.** *Opus* pour les tâches algorithmiques, mathématiques ou de
conception (plusieurs solutions possibles, propriétés à garantir). *Sonnet* pour les
tâches mécaniques bien spécifiées (tests, scripts calqués sur un patron existant,
documentation de configuration).

**Mode plan.** *Oui* quand des choix d'implémentation restent ouverts et méritent
validation avant écriture ; *non* quand ARCH fixe déjà la solution.

**Conventions communes (à respecter dans chaque prompt).** Elles proviennent de
`CLAUDE.md` et de l'existant :
- commentaires internes en **français**, formulations nominales
  (`# Vérification des arguments`) ; docstrings en **anglais**, style Google, avec
  `Args`, `Returns`, `Raises`, `Examples` (doctests) ;
- annotations de type systématiques ; convention sklearn (`BaseEstimator`, `fit`,
  `predict`, `transform`, attributs ajustés suffixés `_`, `check_array`,
  `check_is_fitted`) ;
- aucune valeur méthodologique codée en dur dans `macroforecast/` : tout paramètre est un
  argument avec un défaut documenté, valorisé par la configuration dans `scripts/` ;
- les runners ne gèrent jamais de connexion ; les scripts font tout l'I/O ;
- dépendances optionnelles importées paresseusement avec `ImportError` explicite ;
- `uv run pytest` doit passer à la fin de chaque prompt ; les doctests du module
  (`uv run pytest --doctest-modules macroforecast/trade/aggregation`) aussi ;
- pas de commit sans demande explicite.

Ordre et dépendances :

```
P01 tests de base
 └─ P02 Pareto ── P03 poids ── P04 fonctions ── P05 transport optimal ── P06 diagnostics
      └──────────────────────────────┬──────────────────────────────────────┘
                                     P07 registre + run_synthesis
                                     P08 run_coherence
                                     P09 script synthèse ── P10 script cohérence ── P11 tests bout en bout
P12 → P13 → P14 → P15 → P16 documentation LaTeX (indépendants du code, après P07 de préférence)
P17 nettoyage, README, API snapshot
```

---

## P01 — Tests de base du module `aggregation`

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Sonnet | non | aucun |

```
Contexte. Le module `macroforecast/trade/aggregation` (préprocessing, dominance de Pareto,
pondérations endogènes, fonctions d'agrégation, transport optimal, diagnostics, runner)
n'a que des doctests. Avant de le corriger et de l'étendre, je veux une suite de tests
unitaires qui fige les comportements CORRECTS et documente, par des tests marqués
`xfail(strict=True)`, les bugs connus qui seront corrigés par les prompts suivants.

Lis d'abord : `AGREGATION_ARCHITECTURE.md` sections 4 (I-01 à I-18) et 10 (S-4), puis
`tests/conftest.py` (fixtures existantes, style) et les fichiers du module.

Tâches.
1. Crée `tests/aggregation/__init__.py` et `tests/aggregation/conftest.py` avec des
   fixtures de matrices synthétiques déterministes (`numpy.random.default_rng(0)`) :
   `X_uniform` (500 x 3), `X_correlated` (300 x 4, un facteur commun + bruit),
   `X_minmax` (min-max de `X_uniform`), `X_with_constant_column`, `df_metrics_toy`
   (DataFrame `id` + 3 métriques, 40 lignes, 3 lignes avec NaN).
2. Écris `test_preprocessing.py`, `test_pareto.py`, `test_weights.py`,
   `test_functions.py`, `test_diagnostics.py` couvrant les comportements corrects
   listés en S-4 ET les propriétés mathématiques suivantes :
   - comptage de dominance : si `x_i` domine `x_k` alors `count[i] >= count[k] + 2` ;
     couches : `layers[i] < layers[k]` ;
   - `pareto_front(X) == ~any(pareto_dominance_matrix(X), axis=0)` ;
   - somme pondérée, moyenne géométrique (données >= 0), TOPSIS : strictement monotones
     sur les paires dominantes (poids strictement positifs) ;
   - MPI : contre-exemple de non-monotonie construit explicitement ;
   - Kendall W = 1 pour des scores identiques, τ_b symétrique, RBO(A, A) = 1 - p^depth
     (formule tronquée), Borda/Copeland sur cas à 3 éléments ;
   - SMAA : `confidence_factor` dans [0, 1], `rank_acceptability` somme à 1 par ligne.
3. Ajoute les tests `xfail(strict=True, reason="I-xx")` suivants (ils DOIVENT échouer
   aujourd'hui) :
   - I-02 : `pareto_front(X_uniform, epsilon=0.2)` contient `argmax(X.sum(axis=1))` ;
   - I-01 : `run_aggregation` avec une polarité -1 rapporte `pareto_front_size` égal au
     front de la matrice orientée ;
   - I-06 : `critic_weights(X_with_constant_column)` ne contient aucun NaN ;
   - I-07 : `geometric_mean_score` sur données centrées-réduites lève `ValueError` ;
   - M-01/I-03 : les normes de `spherical_uniform_grid(4096, 6)` passent un test KS
     contre U[0,1] avec p > 0.01 ;
   - I-04 : `benefit_of_doubt_weights` donne un score > 0 à la ligne
     `[0, 1, 1]` de `[[0,1,1],[0.5,0.5,0.5],[1,0.2,0.3]]`.
4. Vérifie que la suite existante (`tests/`) passe toujours et que les nouveaux tests
   passent (hors xfail). Commande : `uv run pytest tests -q`.

Ne modifie aucun fichier de `macroforecast/`. Rapporte le nombre de tests, les xfail et
toute incohérence découverte qui ne serait pas listée dans ARCH section 4.
```

---

## P02 — Dominance de Pareto : corrections, algorithmes pour grands `n`, `ParetoScorer`

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | oui | P01 |

```
Contexte. Le fichier `macroforecast/trade/aggregation/pareto.py` implémente la dominance
de Pareto par une matrice booléenne n x n (mémoire O(n²), impossible pour n ≈ 2·10^5 au
niveau global du futur pipeline) et une « ε-dominance » incorrecte : la relation
`x_i ≥ x_k - ε` est symétrique pour deux points proches, si bien que le front ε élimine
le maximum global (vérifié : `pareto_front([[3,3],[2,2],[1,1]], epsilon=1.5)` → tout
False). Le runner calcule de plus le front sur la matrice NON orientée (polarités
ignorées).

Lis : `AGREGATION_ARCHITECTURE.md` M-02, M-18, M-19, I-01, I-02, A-07, D-03, S-1.3,
Annexe A ; `pareto.py`, `runner.py`, `preprocessing.py`, `tests/aggregation/test_pareto.py`.

Spécification.
1. `epsilon_pareto_set(X, epsilon, *, sort_key="sum", counts=None) -> bool[n]` :
   sous-ensemble représentatif du front exact au sens de Laumanns et al. (2002).
   Relation `x_i ε-domine x_k ⟺ ∀j, x_ij + ε_j ≥ x_kj`. Algorithme glouton : restreindre
   au front exact ; trier par somme des coordonnées décroissante (départager par
   comptage de dominance si fourni) ; conserver un point s'il n'est ε-dominé par aucun
   point déjà conservé. Propriétés à garantir et tester : résultat ⊆ front exact ;
   `argmax(sum)` toujours conservé ; ε = 0 ⇒ égalité avec le front exact ; monotone
   décroissant en ε (inclusion). `epsilon` : scalaire ou vecteur (d,).
   `pareto_dominance_matrix` perd son argument `epsilon` ; `epsilon_pareto_front` devient
   un alias documenté de `epsilon_pareto_set`.
2. `pareto_front_sweep(X) -> bool[n]` : front exact par balayage (tri par somme
   décroissante, comparaison de chaque point au front courant), résultat identique à la
   version matricielle (test sur aléatoire), complexité O(n log n + n·|F|·d), mémoire O(n).
3. `dominance_count_chunked(X, chunk_size=2048, progress=False) -> int[n]` : comptage
   `#dominés - #dominants` par blocs de lignes contre toutes les colonnes, mémoire
   O(chunk·n), résultat identique à `dominance_count` (test).
4. `non_dominated_sort_sweep(X, max_layers=None)` : épluchage itératif par balayage.
5. `MetricReducer(BaseEstimator, TransformerMixin)` : classification ascendante
   hiérarchique (`scipy.cluster.hierarchy`) sur `1 - |ρ_Spearman|`, `n_groups` ou
   `threshold` (défaut 0.3), `linkage="average"` ; `transform` remplace chaque groupe par
   la moyenne de ses colonnes **rang-normalisées** (ce choix rend la réduction invariante
   par transformation monotone) ; attributs `groups_` (tuples d'indices), `labels_`.
   Documenter la propriété : dominance sur les moyennes ⊇ dominance sur les colonnes.
6. `ParetoScorer(BaseEstimator)` conforme à S-1.3 : `__init__(score="dominance_depth",
   epsilon=None, epsilon_scale="mad", reduce=None, layers=True, chunk_size=2048,
   sort_key="sum", large_n_threshold=20000)` ; `fit` ajuste le réducteur et calcule
   `epsilon_` (multiplicateur × échelle robuste par colonne réduite, ou vecteur absolu) ;
   `front`, `epsilon_front`, `layers`, `dominance_count`, `predict` (profondeur
   normalisée `count/(n-1)`), `alert` (front ε, ou front exact si `epsilon=None`).
   Au-dessus de `large_n_threshold`, utiliser automatiquement le balayage et les blocs.
7. Runner : orienter `X` (`PolarityOrienter(polarity_vector(config))`) en tête de
   `run_aggregation` et `recommended_workflow` et utiliser la matrice orientée pour le
   front, le comptage et `compute_coherence_report`.
8. Retire les `xfail` I-01 et I-02 de `tests/aggregation` et ajoute les tests des
   propriétés ci-dessus (`test_pareto.py`).

Contraintes : aucune valeur codée en dur (seuils en arguments avec défauts) ; docstrings
anglaises Google avec doctests ; commentaires français nominaux. Vérifie avec
`uv run pytest tests -q` et les doctests du module. Rapporte les temps mesurés de
`dominance_count_chunked` pour n = 20 000, d = 6 (données uniformes) et l'estimation
extrapolée pour n = 150 000.
```

---

## P03 — Pondérations endogènes : corrections, méta-pondération, *benefit of the doubt*

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | non | P02 |

```
Contexte. `macroforecast/trade/aggregation/weights.py` et `estimators.py` portent
plusieurs erreurs vérifiées : CRITIC renvoie NaN si une colonne est constante ; la
pondération OCDE-JRC n'applique pas la formule du Handbook (variances avant rotation,
pas de normalisation intra-facteur) ; `pca_weights` suppose des données standardisées
alors que le pipeline par défaut est min-max ; la variante « axe 1 » renvoie une
direction non simpliciale acceptée par TOPSIS et la moyenne géométrique ; le benefit of
the doubt (BoD) avec restrictions de parts attribue un score 0 à tout produit ayant une
coordonnée nulle (sur données min-max, au moins le minimum de chaque colonne), et n'est
pas exposé comme estimateur.

Lis : `AGREGATION_ARCHITECTURE.md` M-03, M-04, M-05, M-12, M-13, I-04, I-05, I-06, A-05,
D-04, D-05, S-1.4, S-1.5 ; `weights.py`, `estimators.py`, `preprocessing.py`,
`tests/aggregation/test_weights.py`.

Spécification.
1. `entropy_weights` : colonne nulle → poids 0 sans avertissement numpy ; docstring
   précisant l'exigence min-max ou rangs.
2. `critic_weights(X, method="pearson"|"spearman", scale="std"|"mad", abs_corr=False)` :
   corrélations NaN remplacées par 0 (colonne constante ⇒ poids 0) ; variante
   `abs_corr=True` utilisant `Σ_k (1 - |r_jk|)`.
3. `pca_weights` : calcul sur la matrice de corrélation (`np.corrcoef`, indépendant de
   l'échelle d'entrée). Variante OCDE (`rotate=True`) conforme au Handbook (Nardo et al.
   2008, d'après Nicoletti et al. 2000) : axes de valeur propre > 1 (repli : premier axe),
   rotation varimax, variances après rotation `V_m = Σ_j ℓ'_jm²`, affectation de chaque
   métrique au facteur de saturation² maximale, poids
   `w_j = (V_m* / Σ V_m) · ℓ'_jm*² / Σ_{k affectés à m*} ℓ'_km*²` (somme à 1 sans
   renormalisation). Test : deux facteurs, quatre métriques de saturation² 0,8 sur le
   premier, une sur le second ⇒ cinq poids égaux à 0,2 (construire des données
   synthétiques réalisant approximativement cette structure, tolérance 0,03).
   Variante simple (`rotate=False`) renommée conceptuellement « direction » : retourne
   les saturations de norme 1, signe fixé par somme positive, et `PcaWeightingReport`
   inchangé.
4. Nouvel estimateur `PcaProjectionScorer(BaseEstimator)` (`estimators.py`) : `fit`
   standardise en interne (moyenne, écart-type mémorisés) et calcule la direction ;
   `predict` projette les données standardisées ; `report_` exposé.
5. `auto_weights(X, *, kmo_min=0.6, bartlett_alpha=0.05, axis1_share_min=0.5,
   redundancy_rho=0.3, max_weight=0.6) -> (w, AutoWeightingReport)` selon les règles
   S-1.5 (dans l'ordre : ACP-OCDE si structure factorielle ; CRITIC si redondance ;
   entropie sinon ; garde de dégénérescence `max w_j ≥ max_weight` passant au candidat
   suivant, `equal` en dernier recours). `AutoWeightingReport(selected, candidates_tried,
   kmo, bartlett_p, axis1_share, mean_abs_rho, max_weight)`. Enregistrer sous la clé
   `"auto"` dans `WEIGHTING_REGISTRY` (retour tuple comme `pca`), et `"pca_oecd"` pour
   la variante rotée.
6. `benefit_of_doubt_weights(X, *, restriction="assurance_region", rho=4.0,
   kappa=0.5, delta=1e-3, restrict_to_front=True)` :
   - validation `X ≥ 0` (ValueError nommant la normalisation attendue) ;
   - `restriction="assurance_region"` : contraintes `w_j ≤ rho·w_k` et `w_k ≤ rho·w_j`
     pour tout j < k (linéaires, réalisables), `rho=None` ⇒ aucune restriction ;
   - `restriction="shares"` : restrictions de parts existantes appliquées sur `X + delta`
     (delta > 0 obligatoire, documenté comme paramètre de sensibilité) ;
   - `restriction=None` : programme sans restriction ;
   - contraintes de non-domination restreintes au **front exact** (`pareto_front`,
     jamais le front ε) ; option `n_jobs` via `joblib` si disponible (import paresseux) ;
   - scores dans [0, 1] ; test : sur `[[0,1,1],[0.5,0.5,0.5],[1,0.2,0.3]]` avec
     `assurance_region`, la première ligne a un score > 0.5 ; avec `rho < ∞`, tout point
     du front a un score 1 seulement s'il est sur l'enveloppe convexe supérieure élargie
     (documenter dans la docstring, pas de test).
7. Nouvel estimateur `BenefitOfDoubtScorer(BaseEstimator)` : `fit` mémorise les lignes
   du front (contraintes) et les paramètres ; `predict(X)` résout un programme par ligne
   contre ces contraintes ; `weights_` (n, d) du dernier `predict`.
8. `WeightedAggregator.fit` : pour les agrégations `weighted_sum`, `geometric_mean`,
   `topsis`, `vikor` (à venir), vérifier que les poids sont ≥ 0 et de somme 1 (tolérance
   1e-8), sinon `ValueError` explicite ; la clé `"pca"` (direction) n'est donc plus
   acceptée dans `WeightedAggregator` (message renvoyant vers `PcaProjectionScorer`).
9. Retirer les `xfail` I-04, I-06 ; compléter `test_weights.py` (règles `auto`, OCDE,
   BoD, validations).

Vérifie avec `uv run pytest tests -q` et les doctests. Rapporte les décisions prises sur
les cas limites (n < d, d = 1, toutes colonnes constantes).
```

---

## P04 — Fonctions d'agrégation : validations et nouvelles méthodes

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | non | P03 |

```
Contexte. `macroforecast/trade/aggregation/functions.py` définit somme pondérée, moyenne
géométrique, MPI, TOPSIS et distance de Mahalanobis à l'anti-idéal. La moyenne
géométrique renvoie NaN sur données négatives sans message ; la distance de Mahalanobis
n'est pas orientée et n'est pas la contrepartie linéaire du score de transport optimal
(voir M-06) ; il manque trois méthodes de la littérature (VIKOR, score de rang moyen,
quantile de cône de Hamel-Kostner) et l'estimateur SMAA n'existe pas comme méthode.

Lis : `AGREGATION_ARCHITECTURE.md` M-06, I-07, I-14, A-01, A-02, A-03, A-04, D-16,
S-1.4 ; `functions.py`, `estimators.py`, `diagnostics.py` (`smaa_rank_acceptability`,
`dirichlet_weights`), `tests/aggregation/test_functions.py`.

Spécification (toutes les fonctions gardent la signature `f(X, weights=None, **params)
-> np.ndarray (n,)`, plus élevé = plus vulnérable).
1. `geometric_mean_score` : `ValueError` si `X < 0` ; `epsilon` conservé.
2. `vikor_score(X, weights, *, v=0.5, robust=False, quantile=0.01)` : pôles `A±`
   (min/max ou quantiles), `S_i`, `R_i`, `Q_i` (Opricovic & Tzeng 2004), score `1 - Q_i` ;
   dénominateurs nuls gardés (`S⁺ = S⁻` ⇒ terme 0).
3. `rank_mean_score(X, weights=None)` : moyenne pondérée (égale par défaut) des rangs
   normalisés `(rg - 1)/(n - 1)` (ex æquo moyennés) ; documenter qu'elle est invariante
   par transformation monotone des colonnes.
4. `whitened_projection_score(X, weights=None, *, covariance_estimator="mcd",
   ideal_quantile=0.99)` : `μ`, `Σ` robustes (registre existant), `v` = direction
   blanchie de `x⁺ - μ` normalisée, score `⟨Σ^{-1/2}(x - μ), v⟩` ; `Σ^{-1/2}` par
   décomposition symétrique (eigh) ; documenter que c'est le score de Kantorovitch
   orienté du cas elliptique (M-06).
5. `mahalanobis_score` : ajouter `center="anti_ideal"|"robust_center"` ; la distance au
   centre robuste est celle qui correspond au rang center-outward elliptique.
6. `cone_quantile_score(X, weights_draws)` : pour une matrice de tirages
   `W (T, d)` sur le simplexe, `F̂_C(x_i) = min_t rang(w_t ᵀ x_i)/n` (rang moyen pour les
   ex æquo) ; retourner aussi la version `max_t` via une fonction sœur
   `cone_quantile_bounds(X, W) -> (lower, upper)` ; vectoriser par blocs de tirages
   (`X @ W_bᵀ` puis `rankdata` par colonne). Propriété testée : monotone faible pour la
   dominance (si `x_i ≻ x_k` alors `F̂_C(x_i) ≥ F̂_C(x_k)`).
7. Estimateurs (`estimators.py`) : `ConeQuantileScorer(n_draws=2000, random_state=0,
   bound="lower")` (tirages Dirichlet dans `fit`, réutilisés dans `predict`) ;
   `SmaaScorer(aggregation="weighted_sum", aggregation_params=None, k=50, n_draws=2000,
   random_state=0)` dont `predict` renvoie le facteur de confiance `p_i` calculé sur `X`
   (les tirages sont fixés dans `fit`) et `result_` le `SmaaResult`.
8. `AGGREGATION_REGISTRY` : ajouter `vikor`, `rank_mean`, `whitened_projection` ;
   `_WEIGHT_FREE_AGGREGATIONS` mis à jour (`whitened_projection`, `mahalanobis`, `mpi`).
9. Tests : monotonie stricte de VIKOR et du rang moyen sur paires dominantes ;
   projection blanchie = projection sur données gaussiennes isotropes (τ > 0.99 avec la
   somme des coordonnées) ; quantile de cône ∈ [0, 1] et monotone ; retirer le `xfail`
   I-07.

Vérifie avec `uv run pytest tests -q` et les doctests.
```

---

## P05 — Score de Kantorovitch orienté : loi de référence, alertes, passage à l'échelle

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | oui | P04 |

```
Contexte. `macroforecast/trade/aggregation/optimal_transport.py` transporte le nuage des
métriques sur une mesure de référence puis projette sur une direction de vulnérabilité
estimée. Trois défauts vérifiés : (1) la grille tire les rayons par `ρ^(1/d)` (uniforme
de Lebesgue sur la boule) alors que le contrôle de convergence teste l'uniformité des
normes sur [0,1] — ce test rejette toujours (p < 1e-200 sur la grille elle-même) ; la
définition canonique des rangs center-outward (Hallin et al. 2021, Chernozhukov et al.
2017) utilise la loi sphérique uniforme (rayon uniforme), pour laquelle les normes sont
uniformes et les régions quantiles ont couverture 1 - α ; (2) `epsilon` est absolu donc
dépendant de l'échelle ; aucun sous-échantillonnage, donc irréalisable pour n ≈ 2·10^5 ;
(3) API non sklearn (`score_samples`, pas `BaseEstimator`), une seule variante de score,
pas de seuil d'alerte. La dépendance `jax`/`ott-jax` est optionnelle (extra
`optimal-transport`, non installée dans `.venv` : installe-la dans un environnement de
travail temporaire pour tester, ou utilise `uv sync --extra optimal-transport`).

Lis : `AGREGATION_ARCHITECTURE.md` M-01, M-06, M-07, M-15, M-17, I-03, I-10, A-04, A-08,
D-06, D-07, D-11, S-1.4, Annexe A ; `optimal_transport.py`, `functions.py`
(`whitened_projection_score`), `runner.py`.

Spécification.
1. `spherical_uniform_grid(n_points, dim, seed=0, radial="uniform")` : directions par
   Sobol + Φ⁻¹ normalisées ; rayons `ρ` uniformes stratifiés (`radial="uniform"`, défaut,
   loi sphérique uniforme U_d) ; `radial="lebesgue"` conserve `ρ^(1/d)` pour
   comparaison. Docstring : sous U_d, `‖u‖ ~ U[0,1]` et `P(‖u‖ ≤ r) = r`.
2. `OrientedKantorovichScorer(BaseEstimator)` avec `__init__(epsilon=0.1,
   n_target=4096, pole_quantile=0.99, seed=0, scale_cost="mean", fit_sample_size=20000,
   batch_size=8192, score="projection", alpha=0.05, theta0_degrees=60.0,
   conformal="split", calibration_fraction=0.5, alert="projected",
   max_iterations=2000, threshold=1e-3)`.
   - `fit(X)` : sous-échantillon aléatoire de taille `fit_sample_size` si `n` dépasse
     (générateur seedé) ; géométrie `PointCloud(X_fit, grid, epsilon=epsilon,
     scale_cost=scale_cost)` ; Sinkhorn ; potentiels duaux ; `converged_` (journalisé en
     avertissement si False, pas d'exception) ; direction `u*` = image normalisée du pôle
     `x⁺` (quantiles marginaux `pole_quantile`) ; sensibilité de la direction :
     `direction_cos_` = cosinus entre `u*(q=0.9)` et `u*(q=pole_quantile)` ; calibration
     du seuil d'alerte : si `conformal="split"`, l'ajustement se fait sur une moitié
     (`calibration_fraction`) et le seuil est le `⌈(1-α)(n_cal+1)⌉`-ième plus petit score
     de non-conformité sur l'autre moitié ; si `conformal="fit"`, quantile empirique sur
     l'échantillon d'ajustement (descriptif). Score de non-conformité : `‖T̄(x)‖`
     (`alert="radial"`) ou `s_proj(x)` (`alert="projected"`).
   - `transport(X)` par lots de `batch_size` ; `ranks(X)` = normes ; `signs(X)` ;
     `predict(X)` selon `score` : `projection` `⟨T̄(x), u*⟩` (défaut), `cone`
     `‖T̄(x)‖ · 1{⟨S(x), u*⟩ ≥ cos θ0}`, `modulated` `‖T̄(x)‖ (1 + ⟨S(x), u*⟩)/2` ;
     `alert(X)` booléen : radial `‖T̄(x)‖ > r̂_α ∧ ⟨S(x), u*⟩ ≥ cos θ0`, projeté
     `s_proj(x) > q̂_{1-α}` ; `threshold_` exposé.
   - `fit_report(X) -> OrientedKantorovichReport(converged, uniformity_ks_statistic,
     uniformity_ks_p_value, direction_cos, alert_threshold, alert_share, n_fit, n_cal)`.
3. `ellipticity_screen(X, scorer, *, covariance_estimator="mcd") -> dict` : `tau_proj` =
   τ_b entre `scorer.predict(X)` et `whitened_projection_score(X)` ; `tau_rank` = τ_b
   entre `scorer.ranks(X)` et la distance de Mahalanobis au centre robuste. Ne plus
   comparer à la distance à l'anti-idéal. Dans `recommended_workflow`, supprimer la
   condition `tau < ellipticity_threshold` : le score est inclus dès que la dépendance
   est disponible et `d ≤ ot_dimension_limit` ; les τ sont rapportés dans
   `AggregationReport` (`ellipticity_tau_proj`, `ellipticity_tau_rank`).
4. Tests (`tests/aggregation/test_optimal_transport.py`, `pytest.importorskip("ott")`) :
   normes ~ U[0,1] (KS p > 0.01) sur une gaussienne 2 000 x 3 ; sur une gaussienne
   isotrope, τ_b(`predict`, somme des coordonnées) > 0.9 ; part d'alertes sur la moitié
   de calibration ≈ α (tolérance 0.02) ; reproductibilité (`seed`) ; sous-échantillonnage
   déclenché (n = 30 000, `fit_sample_size=5000`) sans erreur. Retirer le `xfail` M-01.
   Un test sans `jax` vérifie que `fit` lève `ImportError` avec le nom de l'extra.

Le mode plan est demandé : propose d'abord le découpage `fit` / calibration / lots et le
traitement des points hors support (pôle `x⁺` souvent hors du nuage) avant d'écrire.
Vérifie avec `uv run pytest tests -q` (avec et sans l'extra) et les doctests.
```

---

## P06 — Diagnostics : violations, bootstrap, SMAA, consensus, alignement du runner

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | non | P05 |

```
Contexte. `macroforecast/trade/aggregation/diagnostics.py` implémente le protocole de
comparaison (τ_b, W, RBO, top-k, taux de violation de la dominance, bootstrap, SMAA,
leave-one-metric-out, Borda/Copeland/Kemeny). Problèmes : les ex æquo comptent comme des
violations ; le seuil de litige est absolu (50 rangs) alors que les groupes iront de 27 à
2·10^5 lignes ; le bootstrap classe les produits à l'intérieur du rééchantillon (rangs
non comparables, produits absents) ; SMAA stocke une matrice n x n ; Copeland construit
une matrice n x n ; il manque le τ de Kendall pondéré et le contrôle « rang des membres
du front ». Le runner par défaut winsorise à 0,99 avant min-max, ce qui crée 50 ex æquo
au maximum sur 5 000 produits.

Lis : `AGREGATION_ARCHITECTURE.md` M-08, M-09, M-10, I-08, I-09, I-11, I-17, I-18, A-06,
D-08, D-12 ; `diagnostics.py`, `runner.py`, `preprocessing.py` (`Winsorizer`),
`tests/aggregation/test_diagnostics.py`.

Spécification.
1. `dominance_violation_rate(X, scores) -> ViolationRates(strict, tie, n_pairs)`
   (dataclass ; `strict` = part des paires dominantes avec `s_i < s_k`, `tie` avec
   `s_i == s_k`) ; pour n > `large_n_threshold` (défaut 20 000), estimation sur un
   échantillon aléatoire de paires (`n_pairs_sample`, seed) documentée.
2. `front_rank_summary(front_mask, scores) -> (median_rank, max_rank)`.
3. `weighted_tau_matrix(scores_by_method)` via `scipy.stats.weightedtau`.
4. `compute_coherence_report(..., dispute_fraction=0.01)` : seuil `max(1,
   round(dispute_fraction · n))` ; `violation_rate` devient un mapping vers
   `ViolationRates` ; ajouter `front_rank` (par méthode), `weighted_tau_matrix` ;
   `to_metrics` adapté.
5. `bootstrap_rank_stability(df_data, config, pipeline_factory, *, n_boot=50, ci=0.9,
   random_state=None)` : à chaque tirage, ajustement sur le rééchantillon puis
   `predict` sur la population complète et rangs sur la population complète ; chaque
   identifiant reçoit exactement `n_boot` rangs ; colonnes `rank_median`, `rank_low`,
   `rank_high`, `rank_sd` ; supprimer `n_draws_observed`. Docstring citant
   Goldstein & Spiegelhalter (1996) et Xie, Singh & Zhang (2009) pour l'interprétation.
6. `smaa_rank_acceptability(..., k=50, batch_size=256)` : `rank_acceptability` de forme
   `(n, k)` (rangs 1..k seulement), calcul par blocs de tirages (`X @ W_bᵀ`, `argsort`
   par colonne), `central_weight` et `confidence_factor` inchangés ; `SmaaResult.k`.
7. `copeland_rank(scores_by_method, *, top_n=None)` : si `top_n` est fourni,
   présélection Borda puis Copeland exact sur la présélection, reste dans l'ordre de
   Borda (même contrat que `kemeny_rank`).
8. `Winsorizer(quantile=None)` : identité quand `None` ; `default_pipeline` et
   `recommended_workflow` prennent `winsorize_quantile=None` par défaut ; docstring
   expliquant le coût en ex æquo (M-08).
9. `recommended_workflow` : aligner sur P02–P05 (matrice orientée, `auto` comme
   pondération pivot, τ d'ellipticité rapportés sans porte).
10. Tests : `ViolationRates` sur cas à ex æquo ; bootstrap renvoie `n_boot` rangs par id ;
    SMAA `(n, k)` et somme des lignes ≤ 1 ; Copeland présélectionné = Copeland complet
    sur petit cas ; `Winsorizer(None)` identité.

Vérifie avec `uv run pytest tests -q` et les doctests.
```

---

## P07 — Registre des méthodes et runner de synthèse (`methods.py`, `synthesis.py`)

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | oui | P02 à P06 |

```
Contexte. Le module `macroforecast/trade/aggregation` sait scorer UNE matrice (n, d).
Le pipeline doit produire, pour chaque cellule (reporter x product à contexte fixé :
freq, flow, indicators, TIME_PERIOD), des scores et rangs selon un ensemble de méthodes
configurées, à trois niveaux : `by_product` (un groupe par produit, on ordonne les pays),
`by_reporter` (un groupe par pays, on ordonne les produits), `global` (toutes les
cellules du contexte). Les méthodes à pondération endogène s'ajustent sur chaque groupe.
La sortie est une table longue par méthode avec trois paires (score, rang), les tailles
de groupe, des alertes et des bornes bootstrap. Aucune entrée/sortie DuckLake ici : le
runner est une fonction pure DataFrame → DataFrames + rapport (compatibilité Kedro).

Lis intégralement : `AGREGATION_ARCHITECTURE.md` sections 1, 6 (D-09 à D-17), 7 (S-1.2,
S-1.4, S-1.6), 8 (S-2.2 pour la forme YAML des méthodes, S-2.4, S-2.5.c, S-2.7),
Annexe A. Lis aussi `macroforecast/trade/vulnerabilities/runner.py` et `diagnostics.py`
(patron des rapports `to_metrics`, `log_*_artifacts`) et `macroforecast/tracking/base.py`.

Spécification.
1. `methods.py` : `MethodSpec` (dataclass gelée, S-1.2), `METHOD_REGISTRY` mappant
   `kind` → (fabrique d'estimateur, normalisations admissibles, normalisation par défaut,
   `min_group_size` par défaut, `supports_alert`, `has_fitted_state`) selon le tableau
   S-1.4 ; `build_method(spec, config) -> sklearn.pipeline.Pipeline` assemblant
   `[("orient", PolarityOrienter), ("winsorize", Winsorizer|None), ("scale",
   normaliseur ou passthrough), ("estimate", estimateur)]`, avec `ValueError` si la
   normalisation demandée n'est pas admissible ; `method_spec_from_mapping(mapping)`
   (conversion YAML → `MethodSpec`, listes → tuples, clés inconnues → avertissement).
   `reduce` de Pareto est un mapping converti en `MetricReducer`.
2. `synthesis.py` : `SynthesisConfig` (S-1.2, valeurs par défaut = liste de méthodes de
   S-2.2), `LEVELS = ("by_product", "by_reporter", "global")`, `group_keys(level,
   config)`, `iter_groups(df, config, level)` ; `run_synthesis(df_metrics, config, *,
   tracker=NULL_TRACKER, log_artifacts=True) -> (df_scores, df_fit_diagnostics,
   SynthesisReport)`.
   - `df_scores` : colonnes S-2.4 exactement (contexte + reporter + product + method +
     score_/rank_/n_/alert_/rank_low_/rank_high_ par niveau) ; une ligne par cellule x
     méthode (y compris pseudo-méthodes de consensus `consensus_borda`,
     `consensus_copeland`, et `consensus_kemeny` si dans `config.consensus`) ;
   - valeurs manquantes (D-15) : par méthode et par groupe, seules les lignes complètes
     sur `spec.metrics` (ou toutes les métriques) sont ajustées et scorées ;
   - `min_group_size` : méthode sautée (NaN) et ligne de diagnostic
     `skipped_min_group_size` ;
   - rangs : `rankdata(-score, method=config.rank_ties)`, NaN propagés (D-16) ;
   - bootstrap (D-08) pour `config.bootstrap_methods` aux niveaux
     `config.bootstrap_levels`, via `bootstrap_rank_stability` (P06) ;
   - SMAA et quantile de cône partagent les tirages Dirichlet du groupe
     (`random_state`) ;
   - `df_fit_diagnostics` : format long S-2.6 avec `family="fit"` (poids par méthode et
     métrique, `auto_selected__{scheme}`, `ot_converged`, `ot_ks_p`,
     `ot_alert_threshold`, `ot_alert_share`, `ot_direction_cos_q90_q99`,
     `n_skipped_incomplete`, `skipped_min_group_size`), sentinelles `'ALL'` (groupe
     absent) et `''` (item absent) ;
   - `SynthesisReport` (dataclass) avec `to_metrics(prefix="synthesis")` (S-2.7) ;
   - artefacts via le tracker (`log_table`) : top `artifact_top_n` par niveau et méthode,
     poids, sélections `auto`, rapport OT ;
   - journalisation du temps par méthode et par niveau (dans le rapport) ;
   - le score de Kantorovitch est importé paresseusement ; sans `jax`, la méthode est
     sautée avec une ligne de diagnostic `skipped_missing_dependency` et un avertissement
     unique.
3. Exports dans `aggregation/__init__.py` et `trade/__init__.py`.
4. Tests `tests/aggregation/test_synthesis.py` : table jouet (2 contextes, 3 pays,
   20 produits, 4 métriques dont une avec NaN), configuration réduite (méthodes
   `pareto`, `rank_mean`, `critic_sum`, `auto_sum`, `bod`, `smaa`, `cone_quantile`,
   `mpi`) ; vérifier les colonnes, la cohérence rang/score par groupe, les NaN, le saut
   sous `min_group_size`, l'unicité de la clé primaire, la présence des consensus et des
   diagnostics `fit`, et `to_metrics()` sans NaN.

Le mode plan est demandé : propose d'abord la boucle contexte → niveau → groupe →
méthode, la structure d'accumulation (listes de frames puis `concat`) et la stratégie
de partage des tirages, avant d'écrire. Vérifie avec `uv run pytest tests -q`.
```

---

## P08 — Runner de cohérence (`coherence.py`)

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | oui | P07 |

```
Contexte. Après la synthèse (P07), une seconde étape mesure, par contexte x niveau x
groupe : (a) la cohérence des métriques de vulnérabilité entre elles (corrélations,
structure factorielle, part du front de Pareto) et (b) la cohérence des synthèses entre
elles (τ_b, τ pondéré, RBO, top-k, W, violations de dominance strictes et ex æquo, rang
des membres du front, part de cellules litigieuses, largeur des intervalles bootstrap,
leave-one-metric-out, τ score-métrique, diagnostic d'ellipticité, groupes de méthodes).
Le résultat est une table longue (statistique x niveau x groupe x item_a x item_b →
valeur). Fonction pure DataFrames → DataFrame + rapport.

Lis : `AGREGATION_ARCHITECTURE.md` sections 1, D-02, D-11, S-1.2 (`CoherenceConfig`),
S-1.7, S-2.5 (a, b), S-2.6, S-2.7, Annexe A ; `synthesis.py`, `methods.py`,
`diagnostics.py` (P06), `pareto.py`.

Spécification.
1. `CoherenceConfig` (S-1.2). `run_coherence(df_metrics, df_scores, synthesis_config,
   config, *, tracker=NULL_TRACKER, log_artifacts=True) -> (df_diagnostics,
   CoherenceRunReport)`.
2. Pour chaque contexte, niveau et groupe (mêmes fonctions `iter_groups` que P07) :
   - famille `metrics` : toutes les statistiques du tableau S-2.5.a, calculées sur la
     matrice orientée du groupe restreinte aux lignes complètes ; `kendall_w` sur les
     `d` classements induits par les métriques ; `pareto_front_share` par
     `pareto_front_sweep` ; `pareto_front_share_expected = (ln n)^(d-1)/((d-1)! n)` ;
   - famille `methods` : statistiques du tableau S-2.5.b sur les colonnes
     `score_{niveau}` de `df_scores` (méthodes non NaN sur le groupe ; paires
     ordonnées une seule fois, `item_a < item_b`) ; `violation_*` et `front_*` calculés
     avec la matrice orientée du groupe ; `disputed_share` avec
     `dispute_fraction` ; `rank_interval_width_median` depuis `rank_low_/rank_high_` ;
     `smaa_confidence_top{k}_share` depuis la pseudo-méthode `smaa` ; `cluster_id`
     par `cluster_methods` + `fcluster(t=0.2, criterion="distance")` ;
     `ellipticity_tau_*` : depuis `df_fit_diagnostics` si présent, sinon recalculé si
     la méthode `kantorovich` existe (dépendance optionnelle) ;
   - `lomo` : si `config.lomo`, pour chaque méthode de `lomo_methods` et chaque
     métrique, ré-ajuster via `build_method` sans la métrique et rapporter `lomo_tau`,
     `lomo_topk_overlap` (profondeur `topk_depths[1]`) ; borner le coût (avertissement
     si `n > 20000`).
3. Sortie : colonnes S-2.6 exactement, sentinelles `'ALL'` et `''`, clé primaire
   unique (assert dans le runner). `CoherenceRunReport.to_metrics(prefix="coherence")`
   : par niveau, médianes de `kendall_w`, `disputed_share`, `mean_abs_rho`, et
   `violation_strict` par méthode. Artefacts : matrices τ (`log_table`) par niveau pour
   le premier contexte, tableau LOMO.
4. Tests `tests/aggregation/test_coherence.py` : à partir de la sortie de
   `run_synthesis` sur la table jouet de P07 ; vérifier la présence de chaque statistique
   attendue, les bornes ([−1, 1] pour τ, [0, 1] pour W, RBO, parts), l'unicité de la clé,
   `lomo=True` sur une méthode.

Le mode plan est demandé : propose la factorisation des calculs par groupe (une seule
extraction de matrice orientée par groupe, réutilisée par les deux familles) et le
traitement des groupes trop petits, avant d'écrire. Vérifie avec `uv run pytest tests -q`.
```

---

## P09 — Script `scripts/compute_synthetic_scores.py` et `config/synthesis.yaml`

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Sonnet | non | P07 |

```
Contexte. Le pipeline a des scripts par étape dans `scripts/`, paramétrés par des YAML
dans `config/`, qui lisent et écrivent des tables DuckLake (`fact_table` unique par
schéma) via `statflows`/`dt_ducklake_manager`, tiennent un registre JSON de fraîcheur et
tracent l'exécution dans MLflow (objet nul sans URI). Le runner `run_synthesis`
(`macroforecast/trade/aggregation/synthesis.py`) est une fonction pure : le script fait
tout l'I/O. Il faut écrire le script de calcul des scores synthétiques, calqué sur
`scripts/compute_network_vulnerabilities.py` (structure, registre, MLflow, gestion des
échecs par unité de travail).

Lis : `AGREGATION_ARCHITECTURE.md` sections 2.2, 2.3, 6 (D-01, D-09, D-14), 8 (S-2.1,
S-2.2, S-2.3, S-2.4, S-2.7), 11 ; `scripts/compute_network_vulnerabilities.py`,
`scripts/compute_trade_vulnerabilities.py`, `config/vulnerabilities.yaml`,
`macroforecast/trade/aggregation/synthesis.py`, `methods.py`, `pyproject.toml`.

Spécification.
1. `config/synthesis.yaml` : reprendre l'exemple S-2.2 intégralement (blocs `SYNTHESIS`
   et `COHERENCE`), commentaires en français expliquant chaque clé.
2. `scripts/compute_synthetic_scores.py` :
   - `load_synthesis_config` (`SYNTHESIS_CONFIG_PATH`, défaut `config/synthesis.yaml`),
     `load_vulnerability_config` (pour `DBNAME`, `CATALOG_ALIAS`, registres amont) ;
   - `synthesis_config_from_params(params) -> SynthesisConfig` (surcharge champ à champ,
     listes → tuples, `methods` via `method_spec_from_mapping`, clés inconnues →
     avertissement) ;
   - `build_source_query(sources, filters, catalog_alias) -> str` : fonction pure
     construisant la requête S-2.3 (première source = grille, jointures gauches avec
     `ON` = conjonction des expressions `JOIN.ON` et de `JOIN.WHERE`, projection des
     `COLUMNS` préfixées par l'alias, `FILTERS.WHERE`, `LAST_N_PERIODS` par sous-requête
     sur les périodes distinctes) ; testable sans base ;
   - `read_source_metrics(conn, query) -> pd.DataFrame` ;
   - fraîcheur (S-2.1) : lire les registres `VULNERABILITIES` (racine
     `VULNERABILITIES`, entrées `last_computed`) et `NETWORK_VULNERABILITIES` ; recalculer
     tous les contextes si `max(last_computed amont) > last_computed synthèse` ou
     `FORCE` ; date capturée avant le calcul ; registre `SYNTHESIS` écrit après succès
     avec `n_cells`, `n_contexts`, `methods` ;
   - exécution : un appel `run_synthesis` par contexte (boucle sur les contextes
     distincts de la source lue), échec d'un contexte capturé, journalisé, poursuite,
     `RuntimeError` global en fin ; écriture par `write_dataframe(conn, df_scores,
     primary_keys=[...contexte..., "reporter", "product", "method"], catalog_alias=...,
     schema=RESULT_SCHEMA)` et des diagnostics `fit` dans le schéma
     `COHERENCE.RESULT_SCHEMA` (même table longue que le script de cohérence, clé
     S-2.6) ;
   - MLflow : un run par exécution, `tracker.log_metrics(report.to_metrics())`, tags
     `result_schema`, `n_contexts`, `created` ;
   - entrée `vulnerabilities-synthesis-script` dans `[project.scripts]`.
3. Tests `tests/test_scripts_synthesis.py` : `build_source_query` sur l'exemple S-2.2
   (chaîne attendue, `LAST_N_PERIODS` null ⇒ pas de sous-requête) ;
   `synthesis_config_from_params` (coercitions, avertissement) ; règle de fraîcheur
   (fonction pure `contexts_to_recompute(last_upstream, last_synthesis, force)`).

Conventions : docstring de module en français expliquant le rôle et la place dans le
pipeline (comme les scripts existants) ; aucune valeur méthodologique dans le script.
Vérifie avec `uv run pytest tests -q`.
```

---

## P10 — Script `scripts/compute_synthesis_coherence.py`

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Sonnet | non | P08, P09 |

```
Contexte. Second script de la synthèse : il lit la table des métriques (même requête que
le script de synthèse) et la table des scores (`SYNTHESIS.RESULT_SCHEMA`), appelle le
runner pur `run_coherence` (`macroforecast/trade/aggregation/coherence.py`) et écrit la
table longue des diagnostics dans `COHERENCE.RESULT_SCHEMA` (schéma distinct du même
catalogue). Il est calqué sur `scripts/compute_synthetic_scores.py` (P09).

Lis : `AGREGATION_ARCHITECTURE.md` D-02, S-2.1, S-2.5, S-2.6, S-2.7 ;
`scripts/compute_synthetic_scores.py`, `config/synthesis.yaml`,
`macroforecast/trade/aggregation/coherence.py`.

Spécification.
1. `scripts/compute_synthesis_coherence.py` : chargement du bloc `COHERENCE`,
   `coherence_config_from_params`, réutilisation de `build_source_query` et de
   `synthesis_config_from_params` importés depuis le script de synthèse (ou déplacés
   dans un module `scripts/_synthesis_common.py` si tu juges la duplication trop
   forte : justifie) ; lecture des scores par
   `SELECT * FROM "<catalog>"."<SYNTHESIS.RESULT_SCHEMA>"."fact_table"` filtrée sur les
   contextes lus ; fraîcheur contre le registre `SYNTHESIS` (racine `COHERENCE`) ;
   un appel `run_coherence` par contexte, échecs capturés ; écriture
   `write_dataframe(..., primary_keys=[contexte..., "level", "reporter", "product",
   "family", "statistic", "item_a", "item_b"], schema=COHERENCE.RESULT_SCHEMA)` ; MLflow
   un run par exécution ; entrée `vulnerabilities-coherence-script` dans `pyproject`.
2. Tests : fonctions pures du script (sélection des contextes, coercitions).

Vérifie avec `uv run pytest tests -q`.
```

---

## P11 — Tests de bout en bout sur catalogue DuckLake temporaire

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Sonnet | non | P09, P10 |

```
Contexte. Les scripts de synthèse et de cohérence ne peuvent pas être exécutés contre
le bucket S3 du projet. Je veux un test de bout en bout sur données fictives, avec un
catalogue DuckLake local (fixture `ducklake_conn` de `tests/conftest.py`, skip propre
sans extension ni `dt_ducklake_manager`).

Lis : `tests/conftest.py`, `tests/test_storage2_tables.py` (usage de `write_dataframe`),
`scripts/compute_synthetic_scores.py`, `scripts/compute_synthesis_coherence.py`,
`AGREGATION_ARCHITECTURE.md` S-2.3 à S-2.6, S-4.

Tâches.
1. Fixture construisant, dans le catalogue temporaire, un schéma `indicators` (grille
   2 périodes x 4 pays x 30 produits CN8, flux 1, indicateur `VALUE_IN_EUROS`, freq `A`,
   colonnes `HHI`, `CDI2`, `CDI3` et `_ALERT`) et un schéma `network_indicators`
   (30 produits HS6 x 2 années, `classification='HS2022'`, `EXPORT_HHI`,
   `CENTRALITY_RISK`, `CLUSTERING_W`) via `write_dataframe`.
2. Test : exécuter la partie « lecture → run_synthesis → écriture » du script (factoriser
   le script en fonctions `run_from_connections(conn, config, ...)` appelables sans
   `DuckLakeConnector.from_postgres` ni variables d'environnement, si ce n'est pas déjà
   le cas) ; relire la table `synthesis` ; vérifier les colonnes S-2.4, l'unicité de la
   clé, que chaque méthode configurée est présente, que `n_by_product = 4`,
   `n_by_reporter = 30`, `n_global = 120` pour les méthodes non sautées, et que la
   ré-exécution est idempotente (mêmes lignes après upsert).
3. Même chose pour la cohérence : table `synthesis_diagnostics`, familles `metrics`,
   `methods`, `fit` présentes, clé unique.
4. Marquer ces tests `slow` (`pytest.mark.slow`, enregistré dans `pyproject`).

Vérifie avec `uv run pytest tests -q -m "not slow"` puis `uv run pytest tests -q -m slow`.
```

---

## P12 — Documentation LaTeX, partie 1 : préambule, bibliographie, chapitres 1 à 3

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | non | aucun (lecture d'ARCH suffit) |

```
Contexte. La note `Documentation agregation metriques.tex` (2 007 lignes, français)
présente des méthodes pour ordonner des vecteurs de R^d sans pondération a priori. Elle
est réécrite dans un nouveau fichier `Methodologie synthese multicritere.tex`, plus
descriptif, didactique et indépendant du projet, avec une bibliographie externe
`bibliography.bib`. Ce prompt produit le squelette complet, le préambule, la
bibliographie et les chapitres 1 à 3 ; les chapitres suivants sont produits par les
prompts P13 à P16, qui s'insèrent dans ce squelette.

Lis : `AGREGATION_ARCHITECTURE.md` sections 3 (tout), 5, 9 (S-3 intégralement),
Annexes A et B ; l'ancien fichier `Documentation agregation metriques.tex` (préambule,
§1, §2 à réutiliser en les corrigeant).

Tâches.
1. Préambule : reprendre celui de l'ancien fichier en corrigeant M-16 (couleurs
   définies AVANT `hyperref`, retirer `hidelinks`), ajouter `natbib` (`[round]`),
   `\bibliographystyle{plainnat}`, `\bibliography{bibliography}`, un environnement
   `hypothese` (theoremstyle definition), une macro `\medskipafter` n'est PAS souhaitée :
   les espacements sont écrits explicitement (S-3.2).
2. Squelette : `\section` pour les 10 chapitres du plan S-3.4 et les annexes A à D, avec
   pour chaque chapitre non rédigé un commentaire `% À rédiger : prompt P1x` et les
   `\label` fixés à l'avance (`sec:intro`, `sec:cadre`, `sec:pretraitement`,
   `sec:ordres`, `sec:poids`, `sec:agregation`, `sec:ot`, `sec:smaa`, `sec:comparer`,
   `sec:application`, `app:notations`, `app:algos`, `app:code`) pour que les renvois des
   chapitres suivants compilent.
3. `bibliography.bib` : toutes les entrées ★ de l'Annexe B d'ARCH, avec champs complets
   (auteurs, titre, revue/éditeur, volume, pages, année, DOI ou arXiv quand connu) ;
   les entrées non ★ également, si tu es sûr des références. Clés exactement celles
   d'ARCH.
4. Chapitre 1 « Introduction » : le problème (ordonner R^d, absence d'ordre total
   compatible avec la structure vectorielle et invariant par permutation), ce qu'implique
   « aucune pondération a priori » (déplacement de l'hypothèse vers une propriété
   statistique), taxonomie des familles (figure TikZ : ordres partiels / pondérations
   endogènes / agrégations / rangs multivariés / exploration des poids / contrôle), plan.
5. Chapitre 2 « Cadre et notations » : table des notations (Annexe A d'ARCH), unités et
   métriques, polarité et orientation (affine), dominance de Pareto (définition, figure
   du cône), monotonie stricte et faible (définitions distinctes), compensation
   (définition et exemples), invariances (affine, monotone, permutation), rangs et
   ex æquo (convention « rang 1 = plus élevé, ex æquo moyennés »), notion de groupe
   (l'estimation se fait à l'intérieur d'un ensemble d'unités comparables).
6. Chapitre 3 « Prétraitement » : orientation ; queues de distribution (winsorisation
   et son coût en ex æquo M-08, transformations monotones) ; schémas de normalisation
   (table : formule, effet, invariances, méthodes compatibles) ; diagnostics de
   corrélation (Spearman, KMO, Bartlett, ρ̄ moyen des |ρ|) ; algorithme de
   prétraitement ; remarque sur l'invariance du front.
7. Style : S-3.3 (descriptif, aucune injonction à l'infinitif ; idée → hypothèses →
   définitions → algorithme → propriétés → paramètres → limites). Espacements S-3.2
   appliqués strictement. Aucune référence au projet ni à des « positions NC » hors
   d'exemples explicitement marqués comme tels.
8. Compiler : `latexmk -pdf -interaction=nonstopmode "Methodologie synthese multicritere.tex"`
   (MiKTeX installé ; laisser MiKTeX installer les paquets manquants si nécessaire) ;
   corriger toutes les erreurs et les avertissements de citations manquantes ; nettoyer
   les fichiers auxiliaires (`latexmk -c`).
```

---

## P13 — Documentation LaTeX, partie 2 : ordres partiels (ch. 4) et pondérations (ch. 5)

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | non | P12 |

```
Contexte. Suite de la rédaction de `Methodologie synthese multicritere.tex` (squelette
et chapitres 1–3 déjà écrits par P12 ; conventions en tête du fichier et dans
`AGREGATION_ARCHITECTURE.md` S-3). Ce prompt rédige le chapitre 4 « Ordres partiels et
dominance » et le chapitre 5 « Pondérations endogènes ».

Lis : `AGREGATION_ARCHITECTURE.md` M-02, M-03, M-04, M-05, M-11, M-12, M-13, M-18,
M-19, M-20, A-01, A-05, A-07, A-09, D-03, D-04, D-05, S-1.3, S-1.5, S-3, Annexe A ;
les chapitres 1–3 du nouveau fichier ; les §3 et §4 de l'ancien fichier (matière à
reprendre, en corrigeant) ; si le code de P02/P03 existe, `pareto.py` et `weights.py`
pour aligner algorithmes et noms.

Chapitre 4, contenu attendu.
- Front de Pareto, couches, comptage de dominance (avec la preuve courte que
  `x_i ≻ x_k ⇒ δ(i) ≥ δ(k) + 2`), profondeur normalisée ; figure du cône ; algorithme
  matriciel (petits n), algorithme de balayage (front exact), calcul par blocs
  (comptage) ; espérance du cardinal du front sous indépendance (`bentley1978`) avec les
  ordres de grandeur `n = 5000`, `d ∈ {3, 4, 6}` et l'effet de la corrélation.
- Front ε : définition de Laumanns et al. (2002) (relation `x_ij + ε_j ≥ x_kj`),
  pourquoi la relation « ε-dominé par personne » est vide (contre-exemple à trois
  points, figure), algorithme glouton, propriétés (sous-ensemble du front, maximum
  conservé, monotone en ε), calibrage de ε par échelle robuste.
- Réduction préalable de d : regroupement des métriques redondantes (CAH sur
  1 − |ρ^S|), représentant = moyenne des rangs normalisés, inclusion des fronts.
- Quantile de cône (Hamel & Kostner 2018) : définition `F_C(x) = inf_w F_{wᵀX}(wᵀx)`,
  lien avec la dominance et le comptage, estimation par tirages Dirichlet, bornes
  inférieure/supérieure, algorithme, propriétés (monotone faible, sans poids,
  orienté, niveau interprétable).
- Mentions « pour aller plus loin » : extensions linéaires d'un ordre partiel
  (`bruggemann2011`, `fattore2016`, `deloof2006`), profondeurs statistiques
  (`tukey1975`, `zuo2000`, `mosler2013`), sans formalisme détaillé.

Chapitre 5, contenu attendu.
- Distinction en trois classes (M-05) : pondérations simpliciales, directions,
  pondérations individualisées ; tableau « normalisation d'entrée admissible ».
- Entropie de Shannon (`zou2006`) : construction, convention 0 ln 0, colonne constante,
  non-invariance par translation, algorithme.
- CRITIC (`diakoulaki1995`) : construction, variantes (Spearman/MAD, |r|), algorithme.
- ACP : direction (axe 1, signe, saturations négatives et monotonie), variante OCDE–JRC
  corrigée (M-04 : rotation varimax, variances après rotation, affectation,
  normalisation intra-facteur ; formule et exemple numérique à cinq indicateurs
  donnant 0,2 partout ; algorithme), diagnostics KMO/Bartlett/λ₁/d.
- Méta-sélection (S-1.5) : présentée comme heuristique de choix fondée sur les
  diagnostics, arbre de décision (figure TikZ), paramètres, limites.
- Benefit of the doubt (`cherchye2007`, `charnes1978`) : programme, interprétation
  inversée pour une grandeur défavorable (à conserver de l'ancien texte), propriétés
  exactes (M-03 : score dans [0, 1], efficacité faible/forte, monotonie faible,
  ex æquo), dégénérescence et restrictions (région d'assurance `w_j/w_k ∈ [1/ρ, ρ]` en
  premier ; parts de contribution en second avec le plancher δ et le contre-exemple de
  la coordonnée nulle), restriction exacte des contraintes au front (preuve courte),
  algorithme, figure (enveloppe convexe supérieure et rayon).
- Récapitulatif : ce que produisent ces pondérations, et pourquoi leur désaccord est
  une information.

Style et espacements : S-3.2 et S-3.3. Compiler avec `latexmk -pdf`, corriger, `latexmk -c`.
```

---

## P14 — Documentation LaTeX, partie 3 : fonctions d'agrégation (ch. 6) et SMAA (ch. 8)

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | non | P12 |

```
Contexte. Suite de `Methodologie synthese multicritere.tex` (P12, P13). Ce prompt rédige
le chapitre 6 « Fonctions d'agrégation » et le chapitre 8 « Explorer l'incertitude sur
les poids : SMAA ».

Lis : `AGREGATION_ARCHITECTURE.md` M-06, M-08, A-02, A-03, A-04, D-13, D-16, S-1.4,
S-3, Annexe A ; chapitres 1–5 du nouveau fichier ; §5 et §7.5 (SMAA) de l'ancien
fichier ; si P04/P06 existent, `functions.py` et `diagnostics.py`.

Chapitre 6, contenu attendu (pour chaque fonction : idée et problème résolu, formule,
propriétés — compensation, monotonie stricte/faible, invariances —, paramètres,
normalisation admissible, algorithme si non trivial).
- Figure des iso-scores (somme, géométrique, pénalité) reprise et étendue à TOPSIS/VIKOR.
- Somme pondérée ; score de rang moyen (`arjona2023`, `borda1781` ; invariance
  monotone) ; moyenne géométrique (décalage ε comme paramètre de sensibilité) ;
  indice de Mazziotta–Pareto (`mazziotta2016` ; MPI⁺ pour une grandeur défavorable,
  non-monotonie intrinsèque avec contre-exemple) ; TOPSIS (`hwang1981`, variante
  robuste) ; VIKOR (`opricovic2004`, `opricovic2007` ; S, R, Q, paramètre v) ;
  distance de Mahalanobis (`mahalanobis1936`, `rousseeuw1999`, `ledoit2004`) présentée
  comme norme dans l'espace blanchi (M-06), distance au centre vs à l'anti-idéal,
  non-monotonie possible ; projection blanchie orientée (A-04) présentée comme le score
  linéaire du cas elliptique, transition vers le chapitre 7.
- Tableau récapitulatif : fonction × normalisation admissible × poids requis ×
  monotonie × compensation.

Chapitre 8, contenu attendu.
- SMAA (`lahdelma1998`, `lahdelma2001`) : loi uniforme sur le simplexe (Dirichlet),
  indices d'acceptabilité de rang, poids centraux, facteur de confiance à profondeur k,
  restrictions partielles (hit-and-run mentionné), algorithme par blocs avec stockage
  (n, k) seulement ; lien avec le quantile de cône du chapitre 4 (min sur les poids du
  rang vs distribution des rangs) ; lecture des sorties.

Style et espacements : S-3.2, S-3.3. Compiler, corriger, `latexmk -c`.
```

---

## P15 — Documentation LaTeX, partie 4 : rangs multivariés par transport optimal (ch. 7)

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | non | P12 |

```
Contexte. Suite de `Methodologie synthese multicritere.tex`. Ce prompt rédige le
chapitre 7 « Rangs multivariés par transport optimal ». L'ancien §6 est structuré comme
une réponse à l'article de Klein et al. (2025) ; le nouveau chapitre part des sources
(rangs et signes center-outward, estimation entropique, calibration conforme), conserve
l'argument « atypique n'est pas vulnérable » et le score orienté, corrige la loi de
référence (sphérique uniforme, M-01), le cas elliptique (M-06) et le seuil d'alerte
(M-07), et présente le diagnostic d'ellipticité comme une information et non une porte
(M-15).

Lis : `AGREGATION_ARCHITECTURE.md` M-01, M-06, M-07, M-14, M-15, M-17, A-04, A-08, D-06,
D-07, D-11, S-3, Annexe A ; chapitres 1–6 du nouveau fichier ; §6 de l'ancien fichier
(figures TikZ à réutiliser : points a/b, direction u*) ; si P05 existe,
`optimal_transport.py`.

Contenu attendu.
1. Idée et problème résolu : rang unidimensionnel = transport monotone vers l'uniforme ;
   généralisation par gradient de fonction convexe (Brenier) ; ce que cela apporte
   (absence de distribution, prise en compte de la géométrie).
2. Cadre : loi sphérique uniforme U_d (définition précise : direction uniforme sur la
   sphère × rayon uniforme sur [0,1] ; `P(‖U‖ ≤ r) = r`), fonction de répartition
   center-outward, rang `R(z) = ‖T(z)‖`, signe `S(z)`, régions et contours quantiles de
   couverture 1 − α (`chernozhukov2017`, `hallin2021`) ; remarque sur la variante de
   Lebesgue (couverture (1−α)^d) pour expliquer pourquoi on ne la retient pas ;
   propriétés statistiques (`ghosal2022`).
3. Estimation : problème de Sinkhorn (`cuturi2013`, `peyre2019`), carte entropique et
   extension hors échantillon (`pooladian2021`), grille de référence (Sobol + Φ⁻¹, rayons
   uniformes stratifiés), hyperparamètres (ε relatif au coût moyen, m), contrôle
   d'uniformité des normes (KS), ajustement sur sous-échantillon et transport par lots
   (la carte est définie hors échantillon), algorithme complet.
4. Orientation : pourquoi le rang mesure l'atypicité (figure a/b), direction de
   vulnérabilité `u*` (image du pôle `x⁺`, sensibilité au quantile), trois scores
   (projection, cône `θ₀`, modulation), propriétés (monotonie non garantie coordonnée par
   coordonnée ; invariances), algorithme.
5. Cas elliptique : forme de la carte `T(z) = h(r) Σ^{-1/2}(z − μ)/r`, conséquences
   (rang = transformation monotone de la distance de Mahalanobis au centre ; score
   orienté = projection blanchie modulée), diagnostic d'ellipticité (τ_b entre les
   paires correspondantes), lecture : « le transport n'a rien ajouté » vs « la géométrie
   non elliptique compte », sans règle d'exclusion (M-15) ; discussion honnête du
   compromis généralité / coût d'estimation à distance finie.
6. Seuil d'alerte : scores de non-conformité (radial, projeté), calibration conforme par
   séparation ajustement/calibration (`vovk2005`, `lei2018`, `angelopoulos2023`),
   garantie de couverture sous échangeabilité, versions descriptives sur l'échantillon
   d'ajustement, ensemble d'alerte radial ∩ cône, usages (suivi temporel, nouvelles
   unités) ; figure des régions quantiles avec cône et seuil ; algorithme.
7. Dimension : dégradation avec d, plage favorable, remèdes (réduction de d, sous-
   ensemble de métriques, diagnostics).
8. Travaux apparentés : `klein2025`, `thurin2025` (prédiction conforme multivariée par
   transport) en une remarque.

Style et espacements : S-3.2, S-3.3. Compiler, corriger, `latexmk -c`.
```

---

## P16 — Documentation LaTeX, partie 5 : comparer et contrôler (ch. 9), application (ch. 10), annexes

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Opus | non | P12 à P15 ; P07/P08 pour l'annexe de correspondance code |

```
Contexte. Fin de `Methodologie synthese multicritere.tex` : chapitre 9 « Comparer les
synthèses et contrôler leur cohérence », chapitre 10 « Application : synthèse de
métriques de vulnérabilité commerciale », annexes A (notations), B (algorithmes
récapitulatifs), C (correspondance document ↔ code), D (bibliographie), puis relecture
globale et compilation finale.

Lis : `AGREGATION_ARCHITECTURE.md` M-09, M-10, M-20, A-06, D-08, D-09, D-10, D-11, D-15,
D-16, S-1.4, S-2.2, S-2.4, S-2.5, S-2.6, S-3, Annexe A ; chapitres 1–8 du nouveau
fichier ; §7 de l'ancien fichier (protocole Q1–Q5, consensus, validation externe) ; le
code de `macroforecast/trade/aggregation` s'il existe (noms exacts pour l'annexe C).

Chapitre 9, contenu attendu.
- Figure du protocole (reprise) ; Q1 concordance globale (τ_b, τ pondéré `vigna2015`, W
  avec mention de la correction d'ex æquo, déplacement moyen de rang, CAH sur 1 − τ_b) ;
  Q2 concordance en tête (recouvrement top-k, RBO `webber2010`) ; Q3 cohérence avec la
  dominance (V_strict et V_tie, contrôle du rang des membres du front, lecture) ;
  Q4 stabilité : bootstrap « ajuster sur le rééchantillon, classer la population »
  (M-09, D-08, `efron1994`, `goldstein1996`, `xie2009`), propagation de l'incertitude
  des métriques (présentée comme extension nécessitant σ̂_ij), stabilité aux choix
  méthodologiques (grille, indices de Sobol `saisana2005`) ; Q5 réductibilité (τ
  score-métrique, leave-one-metric-out, contributions) ; classements consensus (Borda,
  Copeland avec présélection, Kemeny MILP explicité : variables, antisymétrie,
  3-cycles ; `kemeny1959`, `dwork2001`) ; unités litigieuses ; validation externe ;
  algorithme du tableau de bord.

Chapitre 10, contenu attendu (seul chapitre contextuel).
- Données : deux familles de métriques (partenaires HHI/CDI2/CDI3 ; réseau EXPORT_HHI,
  CENTRALITY_RISK, CLUSTERING_W), leurs polarités, la jointure CN8→HS6 et année ;
  contextes (période, flux, indicateur, fréquence) ; les trois niveaux et leur
  interprétation ; tailles typiques (≈ 30, ≈ 5 000, ≈ 1,5·10^5) et conséquences
  (méthodes admissibles par niveau, tableau D-11).
- Configuration : extrait YAML commenté (S-2.2), correspondance méthode ↔ chapitre.
- Lecture des sorties : table des scores (S-2.4, exemple de lignes), table des
  diagnostics (S-2.5, S-2.6), diagnostics à regarder en premier (W, front share, KMO,
  V_strict, disputed_share, ellipticity), cas d = 6 pour le transport (M-17).
- Ce que le dispositif ne fait pas (validation externe absente, incertitude des
  métriques non propagée).

Annexes : A notations (table complète) ; B algorithmes récapitulatifs (renvois) ;
C correspondance : pour chaque section, fonctions/classes de
`macroforecast.trade.aggregation` (noms exacts) ; D `\bibliography{bibliography}`.

Relecture globale : cohérence des notations avec l'annexe A, aucun infinitif injonctif,
espacements S-3.2 partout (vérifier par grep : chaque `\begin{itemize}`,
`\begin{figure}`, `\begin{algorithm}`, `\[` est précédé de `\medskip` et suivi de
`\bigskip` après son `\end`), aucune référence non résolue ni citation manquante
(`latexmk` sans avertissement `undefined`), `latexmk -c` final. Rapporte le nombre de
pages, de figures, d'algorithmes et de références.
```

---

## P17 — Nettoyage, README, CLAUDE.md, instantané d'API

| Modèle | Mode plan | Prérequis |
|---|---|---|
| Sonnet | non | P11, P16 |

```
Contexte. Le module de synthèse, ses deux scripts et la nouvelle documentation sont en
place. Il reste à mettre le dépôt en cohérence.

Lis : `README.md`, `CLAUDE.md`, `pyproject.toml`, `tools/api_snapshot.py`,
`macroforecast/__init__.py`, `macroforecast/trade/__init__.py`,
`macroforecast/trade/aggregation/__init__.py`, `AGREGATION_ARCHITECTURE.md` sections 7
et 8, `kubernetes/workflow.yaml`.

Tâches.
1. `README.md` : décrire `trade/aggregation/` (méthodes, trois niveaux, deux scripts,
   schémas `synthesis` et `synthesis_diagnostics`), l'extra `optimal-transport`, l'ordre
   des étapes du pipeline (download → BACI → vulnérabilités → synthèse → cohérence), les
   marqueurs `slow`.
2. `CLAUDE.md` : mentionner le module `aggregation`, les deux scripts et la note
   `Methodologie synthese multicritere.tex` + `bibliography.bib` comme références
   méthodologiques ; rappeler la convention « rang 1 = plus vulnérable ».
3. `macroforecast/__init__.py` : exposer `run_synthesis`, `run_coherence`,
   `SynthesisConfig`, `CoherenceConfig` avec les mêmes conventions de renommage que les
   configurations existantes.
4. `tools/api_snapshot.py` : régénérer l'instantané si cet outil le prévoit ; sinon
   documenter.
5. `kubernetes/workflow.yaml` / `docker/Dockerfile` : ajouter les deux entrées de script
   à la suite des étapes existantes si le workflow liste les étapes ; sinon ne rien
   inventer et le signaler.
6. `.gitignore` : ajouter les artefacts LaTeX (`*.aux`, `*.bbl`, `*.blg`, `*.fdb_latexmk`,
   `*.fls`, `*.synctex.gz`, `*.toc`, `*.out`, `*.log` est déjà ignoré).
7. Ancien fichier `Documentation agregation metriques.tex` : NE PAS supprimer ; lister
   dans le rapport final les différences de contenu non reprises (s'il y en a) pour
   décision.
8. `uv run pytest tests -q` doit passer.
```
