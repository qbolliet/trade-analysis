# Architecture — synthèse de métriques de vulnérabilité (`macroforecast.trade.aggregation`)

Document de référence pour la révision de la méthodologie d'agrégation, la correction
et l'extension du module `macroforecast/trade/aggregation`, l'ajout de deux scripts de
pipeline (scores synthétiques, cohérence) et la réécriture de la note LaTeX. Il sert de
source unique aux prompts listés dans `AGREGATION_PROMPTS.md` : chaque prompt renvoie aux
identifiants ci-dessous (`M-xx` méthodologie, `I-xx` implémentation, `A-xx` ajouts,
`D-xx` décisions, `S-xx` spécifications).

Date de rédaction : 2026-09-10. État du dépôt : branche `qb-vulnerabilities`, commit
`fcad92c` (« doc: metrics aggregation »). Les 52 doctests du module passent
(`pytest --doctest-modules macroforecast/trade/aggregation`) ; aucun test unitaire dédié
n'existe. `jax` / `ott-jax` ne sont pas installés dans `.venv`. MiKTeX (`pdflatex`,
`latexmk`, `bibtex`, `biber`) est disponible.

---

## Table des matières

1. [Périmètre et vocabulaire](#1-périmètre-et-vocabulaire)
2. [État des lieux](#2-état-des-lieux)
3. [Problèmes méthodologiques du document (M-xx)](#3-problèmes-méthodologiques-du-document-m-xx)
4. [Problèmes d'implémentation (I-xx)](#4-problèmes-dimplémentation-i-xx)
5. [Méthodes à ajouter (A-xx)](#5-méthodes-à-ajouter-a-xx)
6. [Décisions clés (D-xx)](#6-décisions-clés-d-xx)
7. [Architecture cible du module (S-1)](#7-architecture-cible-du-module-s-1)
8. [Spécification des scripts et des tables (S-2)](#8-spécification-des-scripts-et-des-tables-s-2)
9. [Spécification de la documentation LaTeX (S-3)](#9-spécification-de-la-documentation-latex-s-3)
10. [Tests attendus (S-4)](#10-tests-attendus-s-4)
11. [Hypothèses et questions ouvertes](#11-hypothèses-et-questions-ouvertes)
12. [Annexe A — notations partagées](#annexe-a--notations-partagées)
13. [Annexe B — bibliographie à constituer](#annexe-b--bibliographie-à-constituer)

---

## 1. Périmètre et vocabulaire

**Objet.** On dispose d'une table de cellules (une cellule = un couple `reporter ×
product` à une période, un flux, un indicateur et une fréquence donnés) portant `d`
métriques de vulnérabilité. On veut produire, pour chaque cellule, des *scores
synthétiques* et des *rangs* selon plusieurs méthodes, à trois niveaux de comparaison,
puis mesurer la cohérence des métriques entre elles et la cohérence des synthèses entre
elles.

**Termes utilisés dans ce document.**

| Terme | Définition |
|---|---|
| Cellule | Une ligne de la table des métriques, identifiée par ses clés (`freq, reporter, product, flow, indicators, TIME_PERIOD` pour la famille partenaires). |
| Contexte | Ensemble des clés qui définissent un univers de comparaison et ne varient jamais à l'intérieur d'un groupe : `freq, flow, indicators, TIME_PERIOD`. On ne compare jamais deux périodes ni deux flux entre eux. |
| Niveau | Règle de constitution des groupes à l'intérieur d'un contexte : `by_product` (un groupe par produit, on ordonne les pays), `by_reporter` (un groupe par pays, on ordonne les produits), `global` (un seul groupe, on ordonne toutes les cellules). |
| Groupe | Sous-ensemble de cellules sur lequel une méthode est ajustée puis appliquée. Une méthode à pondération endogène estime ses poids **sur le groupe**. |
| Méthode | Un estimateur `fit(X).predict(X)` produisant un score (plus élevé = plus vulnérable) et éventuellement une alerte booléenne. |
| Score / rang | Le score est la sortie brute de la méthode ; le rang vaut 1 pour la cellule la plus vulnérable du groupe, ex æquo moyennés. |
| Métrique | Une colonne d'entrée (`HHI`, `CDI2`, …), orientée en polarité positive avant tout calcul. |

Les notations mathématiques (`X`, `n`, `d`, `x_i`, `≻_P`, `Δ^{d-1}`, `T`, …) sont fixées
en [Annexe A](#annexe-a--notations-partagées) et doivent être reprises telles quelles dans
la documentation et dans les docstrings.

---

## 2. État des lieux

### 2.1 Module `macroforecast/trade/aggregation` (3 478 lignes)

| Fichier | Contenu | Correspondance note |
|---|---|---|
| `base.py` | `AggregationConfig(id_columns, metric_columns, polarities, …)`, `split_frame`, `attach_scores`, `polarity_vector` | §1 |
| `preprocessing.py` | `PolarityOrienter`, `Winsorizer`, `RankScaler`, `MedianMadScaler`, `GaussianQuantileScaler`, `NORMALIZER_REGISTRY`, KMO, Bartlett, Spearman | §2 |
| `pareto.py` | `pareto_dominance_matrix(X, epsilon)`, `pareto_front`, `epsilon_pareto_front`, `non_dominated_sort`, `dominance_count`, `normalized_dominance_depth` | §3 |
| `weights.py` | `entropy_weights`, `critic_weights`, `pca_weights(rotate)`, `benefit_of_doubt_weights(kappa, restrict_to_front)`, `dirichlet_weights` | §4 |
| `functions.py` | `weighted_sum_score`, `geometric_mean_score`, `mpi_score`, `topsis_score`, `mahalanobis_score` | §5 |
| `estimators.py` | `WeightedAggregator(weighting, aggregation)`, `DominanceCountScorer`, registres | §4–5 |
| `optimal_transport.py` | `spherical_uniform_grid`, `OrientedKantorovichScorer(fit/score_samples)`, `ellipticity_screen` | §6 |
| `diagnostics.py` | τ_b, W, RBO, top-k, `dominance_violation_rate`, `bootstrap_rank_stability`, `smaa_rank_acceptability`, `leave_one_metric_out`, Borda/Copeland/Kemeny, `CoherenceReport` | §7 |
| `runner.py` | `default_pipeline`, `run_aggregation`, `recommended_workflow`, `AggregationReport` | §8 |

Le module opère sur une matrice `(n, d)` unique : il n'a **aucune notion de groupe, de
niveau ni de contexte**, et aucune entrée/sortie DuckLake. Tout cela est à construire
(§7 et §8).

### 2.2 Scripts existants et conventions à respecter

Les scripts `scripts/compute_trade_vulnerabilities.py` et
`scripts/compute_network_vulnerabilities.py` fixent le patron que les nouveaux scripts
doivent reproduire :

- configuration YAML chargée par une fonction `load_*_config` (variable d'environnement
  `*_CONFIG_PATH` puis chemin par défaut) ; construction d'une dataclass gelée de
  paramètres par surcharge champ à champ (`*_config_from_params`) avec coercition
  listes → tuples et avertissement sur clé inconnue ;
- registre JSON de fraîcheur (`statflows.storage.json.Loader/Saver`), confronté au
  registre de l'étape amont ; date capturée **avant** le calcul, écrite **après** succès ;
- connecteurs `DuckLakeConnector.from_postgres(...)` construits dans le script, connexions
  ouvertes/fermées par le script, jamais par le runner ;
- écriture par `statflows.storage.ducklake.tables.write_dataframe(conn, df, primary_keys,
  catalog_alias=..., schema=...)` : **une seule `fact_table` par schéma**, création à la
  première rencontre, upsert par clé primaire ensuite ;
- suivi MLflow via `macroforecast.tracking.get_tracker` (objet nul sans URI) ;
  `tracker.log_params(run_params(config, context))` dans le runner, `report.to_metrics()`
  et `set_tags` dans le script ;
- runner dans le package (`macroforecast/trade/<famille>/runner.py`) : méthodologie pure,
  connexions passées en argument.

### 2.3 Table d'entrée

Famille partenaires (`vulnerabilities.indicators.fact_table`) :
clés `freq, reporter, product, flow, indicators, TIME_PERIOD` ; colonnes `HHI, CDI2, CDI3`
et `HHI_ALERT, CDI2_ALERT, CDI3_ALERT`. `CDI2`/`CDI3` sont nulles hors flux import.
Famille réseau (`vulnerabilities.network_indicators.fact_table`) : clés
`classification, product, year` (HS6, millésime BACI) ; colonnes `CENTRALITY_RISK,
CLUSTERING_W, DIAMETER, EXPORT_HHI, SPOF, SPOF_DECILE` et leurs `_ALERT`. Les deux familles
n'ont ni la même clé produit (CN8 contre HS6) ni la même clé temporelle (`TIME_PERIOD`
contre `year`) : une jointure est nécessaire pour les combiner (cf. D-09, S-2.3).

---

## 3. Problèmes méthodologiques du document (M-xx)

Chaque entrée : constat, pourquoi c'est un problème, correction retenue, référence,
répercussion sur le code. Les preuves numériques citées proviennent d'un script de
vérification exécuté sur le module actuel (résumé en §4).

### M-01 — Loi de référence et test d'uniformité incohérents (transport optimal)

**Constat.** La définition 6.1 pose `R_α = {z : ‖T(z)‖ ≤ 1−α}` avec
`P(Z ∈ R_α) = 1−α`, et l'algorithme 4 impose « vérifier que `{‖T(x_i)‖}` est
approximativement uniforme sur `[0,1]` ». Mais la grille de référence tire les rayons par
`ρ^{1/d}`, c'est-à-dire la loi **uniforme de Lebesgue sur la boule**, pour laquelle
`P(‖U‖ ≤ r) = r^d`. Sous cette cible, `P(Z ∈ R_α) = (1−α)^d` et les normes suivent une loi
Beta(d, 1), pas une loi uniforme.

**Preuve.** Sur la grille produite par `spherical_uniform_grid(4096, d)`, le test de
Kolmogorov–Smirnov contre U[0,1] donne p ≈ 10⁻²²⁶ pour d = 2 et p = 0 pour d = 6 ; contre
Beta(d, 1), p = 1,00.

**Pourquoi c'est un problème.** Le contrôle de convergence de la carte (test KS) rejette
systématiquement une carte correcte, et le seuil d'alerte `r_α = 1−α` n'a pas la
couverture annoncée.

**Correction.** Adopter la cible de Hallin et al. (2021) et Chernozhukov et al. (2017) :
la loi **sphérique uniforme** `U_d` = (direction uniforme sur `S^{d−1}`) × (rayon uniforme
sur `[0,1]`), soit `u = ρ·θ` avec `ρ ~ U[0,1]`. Alors `‖T(Z)‖ ~ U[0,1]`, les régions
quantiles ont la couverture `1−α`, et le test KS contre U[0,1] est le bon contrôle. C'est
la définition canonique des rangs *center-outward*.

**Références.** `hallin2021`, `chernozhukov2017`, `ghosal2022`.
**Code.** I-03.

### M-02 — La « ε-dominance » définie n'est pas un ordre et vide le front

**Constat.** §3.3 définit `x_i ≻^ε x_k ⟺ ∀j, x_ij ≥ x_kj − ε_j` et annonce que cela
« réduit le front ». Cette relation est symétrique pour deux points distants de moins de
`ε` : chacun ε-domine l'autre, donc **tous deux** sont exclus du « front ». Le maximum
global lui-même est ε-dominé par tout voisin à moins de `ε`.

**Preuve.** `pareto_front([[3,3],[2,2],[1,1]], epsilon=1.5)` renvoie `[False, False,
False]`. Sur 500 points uniformes en dimension 3 : front exact = 24 points, ε = 0,05 →
2 points, ε = 0,2 → 0 point ; le point de somme maximale est exclu.

**Correction.** Utiliser la construction de Laumanns et al. (2002) : la relation
`x_i ≽_ε x_k ⟺ ∀j, x_ij + ε_j ≥ x_kj` sert à sélectionner un **sous-ensemble
représentatif du front exact** `F_ε ⊆ F_1` tel que tout point de `X` soit ε-dominé par
un élément de `F_ε`. Algorithme glouton : parcourir les points du front exact par ordre
décroissant d'un critère de tri (somme des coordonnées normalisées, puis comptage de
dominance), conserver un point s'il n'est ε-dominé par aucun point déjà conservé. Le
maximum de somme est traité en premier et donc toujours conservé ; `F_ε` est d'autant
plus petit que `ε` est grand ; `ε → 0` redonne `F_1`.

**Calibrage de ε.** Le document propose l'écart-type bootstrap du bruit d'estimation de
chaque métrique. Ces données (déclarations sous-jacentes) ne sont pas disponibles à
l'étape d'agrégation (M-11). Retenir `ε_j = c · s_j` avec `s_j` une échelle robuste de
la colonne (MAD, ou étendue interquartile) et `c` un paramètre de configuration
(défaut 0,1), exprimé sur les données normalisées.

**Références.** `laumanns2002`, `deb2002`.
**Code.** I-02, A-07.

### M-03 — *Benefit of the doubt* : restrictions de parts dégénérées, propriétés surévaluées

**Constat 1.** Les restrictions `κ/d ≤ w_j x_oj / Σ_k w_k x_ok ≤ 1/(κd)` sont appliquées
« sur données min–max ». Pour tout produit dont une coordonnée vaut 0 (par construction,
au moins le minimum de chaque colonne), la borne inférieure impose `x_oᵀw = 0`, donc
`w = 0` et `s^BoD(x_o) = 0`, quelle que soit la valeur de ses autres métriques.

**Preuve.** `X = [[0,1,1],[0.5,0.5,0.5],[1,0.2,0.3],[0.3,0,0.9],[0.6,0.7,0]]`,
`κ = 0,5` : scores `[0, 0.885, 0.72, 0, 0]`, poids nuls pour la ligne 0 qui est pourtant
maximale sur deux métriques sur trois. Le résultat est identique avec `κ = 10⁻⁴`.

**Constat 2.** La propriété « `s^BoD = 1` implique non dominé » est fausse avec `w ≥ 0`
(poids nuls admis) : `x_o = (1, 0)` et `x_k = (1, 1)` obtiennent tous deux 1 sous
`w = (1, 0)`. Elle n'est vraie qu'avec des poids strictement positifs (efficacité forte
au sens DEA). De même la « monotonie » satisfaite n'est que faible (`≥`), et le taux de
violation « attendu à 0 » sera positif dès que deux produits comparables sont ex æquo à 1
(cf. M-10).

**Constat 3.** Le score est de plus sans signification sur des données centrées
(valeurs négatives) : la contrainte `Xw ≤ 1` et la borne `(0,1]` supposent `X ≥ 0`.

**Correction.**
- Restrictions par **région d'assurance** (rapports de poids) plutôt que par parts de
  contribution : `w_j / w_k ∈ [1/ρ, ρ]` pour tout `j, k`, paramètre `ρ ≥ 1` (`ρ = ∞` :
  aucune restriction). Ces contraintes sont linéaires, symétriques entre critères (aucune
  hiérarchie), toujours réalisables (le vecteur uniforme convient) et insensibles aux
  coordonnées nulles. Conserver les restrictions par parts en option, avec un plancher
  `δ > 0` ajouté à `X` et documenté comme paramètre de sensibilité.
- Énoncer les propriétés exactes : score dans `[0, 1]` ; `s = 1` ⟺ efficacité faible
  (appartenance à l'enveloppe convexe supérieure élargie) ; avec `ρ < ∞`, tout poids est
  strictement positif et `s = 1` implique la non-domination ; monotonie faible.
- Exiger `X ≥ 0` (validation) : normalisation min–max ou rangs.
- Conserver la restriction des contraintes au front de Pareto exact : elle est **exacte**
  (le maximum d'une forme linéaire à coefficients positifs sur un ensemble fini est
  atteint sur un point non dominé). Ne jamais utiliser le front ε (M-02) pour cela.

**Références.** `cherchye2007`, `charnes1978`.
**Code.** I-04.

### M-04 — Pondération OCDE–JRC : formule non conforme au *Handbook*

**Constat.** §4.3 écrit `w_j ∝ max_m ℓ_jm² · λ_m / Σ_{m'} λ_{m'}` avec `λ_m` les valeurs
propres **avant** rotation. La procédure du *Handbook* (Nardo et al. 2008, étape
« Factor analysis », d'après Nicoletti et al. 2000) est : (i) rotation varimax des `M`
axes retenus ; (ii) affectation de chaque indicateur au facteur `m*(j)` sur lequel son
carré de saturation est maximal ; (iii) à l'intérieur de chaque facteur, normalisation
des carrés de saturations des indicateurs **affectés à ce facteur** pour qu'ils somment
à 1 ; (iv) pondération de chaque facteur par sa part de variance expliquée **après
rotation** `V_m / Σ V_m` avec `V_m = Σ_j ℓ'_{jm}²`. D'où
`w_j = (V_{m*} / Σ_m V_m) · ℓ'_{j m*}² / Σ_{k : m*(k) = m*} ℓ'_{k m*}²`, qui somme à 1 sans
renormalisation.

**Exemple.** Deux facteurs, quatre indicateurs de saturation² 0,8 sur le facteur 1
(`V_1 = 3,2`), un indicateur de saturation² 0,8 sur le facteur 2 (`V_2 = 0,8`).
*Handbook* : `w = 0,2` pour chacun des cinq. Formule du document, renormalisée :
`0,235` pour les quatre premiers, `0,059` pour le cinquième.

**Correction.** Réécrire la formule et l'algorithme ; utiliser les variances après
rotation ; préciser que la variante « axe 1 seul » produit une **direction** (saturations
de norme 1, éventuellement négatives), pas un vecteur du simplexe, et que le score est
alors une projection sur données **standardisées** (M-05).

**Références.** `nardo2008` (p. 89–90), `nicoletti2000`, `kaiser1958`.
**Code.** I-05.

### M-05 — Pondérations « dans le simplexe » : énoncé contredit par l'ACP et Mahalanobis

**Constat.** L'introduction de §4 annonce un vecteur `w ∈ Δ^{d−1}` pour les quatre
méthodes ; l'ACP (axe 1) fournit un vecteur unitaire à composantes de signe quelconque, et
Mahalanobis des « poids implicites » matriciels. Le document ne précise pas non plus que
l'ACP suppose des données centrées-réduites alors que le pipeline par défaut normalise
en min–max.

**Correction.** Distinguer explicitement dans la section 5 : (a) *pondérations
simpliciales* (entropie, CRITIC, OCDE–JRC, méta-sélection) ; (b) *directions* (ACP axe 1,
projection blanchie) qui définissent un score linéaire sur données standardisées et
n'ont pas vocation à être combinées avec une moyenne géométrique ou TOPSIS ;
(c) *pondérations individualisées* (BoD). Préciser pour chaque méthode la normalisation
d'entrée admissible (tableau S-1.4).

**Code.** I-05 (validation des poids dans `WeightedAggregator`).

### M-06 — Mahalanobis : « somme pondérée déguisée », orientation et cas elliptique

**Constat 1.** `s^Mah(x) = √((x−x⁻)ᵀΣ⁻¹(x−x⁻))` est une **norme** dans l'espace
blanchi, pas une somme pondérée ; l'intitulé de la remarque est inexact.

**Constat 2.** La distance à un coin `x⁻` n'est pas orientée : un produit situé
*en deçà* de l'anti-idéal sur toutes les métriques (queue basse à 1 %) est à distance
positive et se classe au-dessus de produits situés exactement en `x⁻`. Avec des termes
extra-diagonaux négatifs dans `Σ⁻¹`, le gradient `2Σ⁻¹(x−x⁻)` peut avoir des composantes
négatives : la monotonie peut être violée (le document le dit correctement).

**Constat 3.** §6.5 affirme que dans le cas elliptique « le score de transport optimal se
réduit exactement à une distance de Mahalanobis ». Pour une loi elliptique de centre `μ`
et de dispersion `Σ`, la carte de Brenier vers `U_d` est
`T(z) = h(r) · Σ^{−1/2}(z−μ)/r` avec `r = ‖Σ^{−1/2}(z−μ)‖` et `h` croissante. Donc :
le **rang** `‖T(z)‖ = h(r)` est une transformation monotone de la distance de Mahalanobis
**au centre** ; le **score orienté** `⟨T(z), u*⟩ = (h(r)/r) · ⟨Σ^{−1/2}(z−μ), u*⟩` est une
projection blanchie modulée radialement. Aucun des deux ne se réduit à la distance de
Mahalanobis à l'anti-idéal. Le critère d'ellipticité (`τ` de Kendall entre `s^OT_proj`
et `s^Mah`) compare donc deux objets non comparables et peut être faible sur un nuage
parfaitement elliptique.

**Correction.** (a) Renommer la remarque (« une norme dans l'espace blanchi ») ;
(b) introduire la **projection blanchie orientée**
`s^white(x) = ⟨Σ^{−1/2}(x−μ), v⟩`, `v = Σ^{−1/2}(x⁺−μ)/‖Σ^{−1/2}(x⁺−μ)‖`, comme
contrepartie linéaire exacte du score de Kantorovitch orienté dans le cas elliptique
(A-04) ; (c) définir le diagnostic d'ellipticité comme `τ_b(s^OT_proj, s^white)` et
`τ_b(‖T̄(x)‖, d_Mah(x, μ))`, sans en faire une porte (D-07) ; (d) garder `s^Mah` à
l'anti-idéal comme méthode à part entière, avec sa non-monotonie mesurée.

**Références.** `chernozhukov2017` (cas elliptique, prop. 2.x), `mahalanobis1936`,
`rousseeuw1999`, `ledoit2004`.
**Code.** I-10, I-14, A-04.

### M-07 — Seuil d'alerte : quantile de référence ≠ calibration conforme

**Constat.** §6.6 définit `r̂_α = inf{r : Û(B(0,r)) ≥ 1−α}` avec `Û` la mesure de
référence empirique, puis invoque une « garantie valable à distance finie ». La garantie
conforme ne porte pas sur un quantile de la mesure cible : elle s'obtient en prenant le
`⌈(1−α)(n_cal+1)⌉`-ième plus petit score de non-conformité d'un **échantillon de
calibration disjoint** de l'échantillon d'ajustement (split conformal), sous
échangeabilité. Sur l'échantillon d'ajustement lui-même, le seuil est descriptif.

**Correction (D-06).** Deux calibrations, toutes deux conservées :
- *radiale* : score de non-conformité `‖T̄(x)‖`, ensemble d'alerte
  `A_α = {x : ‖T̄(x)‖ > r̂_α et ⟨S(x), u*⟩ ≥ cos θ₀}` ; la couverture `≤ α` de l'alerte
  découle de celle du complémentaire de la région ;
- *projetée* : score de non-conformité `s^OT_proj(x)` lui-même, alerte
  `{s^OT_proj(x) > q̂_{1−α}}` ; c'est une prédiction conforme unidimensionnelle sur le score
  orienté, plus simple et directement orientée.
Paramètres : `α`, `θ₀`, `conformal ∈ {fit, split}`, `calibration_fraction`. Le seuil et
la fraction d'alertes observée sont rapportés.

**Références.** `vovk2005`, `lei2018`, `angelopoulos2023`, `klein2025` (comme travail
apparenté), `thurin2025`.
**Code.** I-03, A-08.

### M-08 — Winsorisation avant min–max : ex æquo au sommet

**Constat.** La chaîne par défaut (`Winsorizer(0.99)` puis `MinMaxScaler`) rabat les
1 % les plus vulnérables de chaque métrique sur la valeur 1. Sur 5 000 produits, 50 sont
ex æquo au maximum sur chaque métrique : le classement des plus vulnérables, qui est
précisément l'objet recherché, y est indécidable métrique par métrique.

**Preuve.** 5 000 tirages log-normaux : 50 valeurs exactement égales à 1,0 après
winsorisation à 0,99 et min–max.

**Correction (D-12).** Winsorisation désactivée par défaut (`quantile = None`) ;
lorsqu'elle est activée, le document doit énoncer le compromis (stabilité des statistiques
d'échelle contre perte de discrimination dans la queue) et recommander, pour les méthodes
sensibles aux extrêmes, la normalisation par rangs ou par quantiles gaussiens, qui n'a pas
besoin de winsorisation.

### M-09 — Intervalle de confiance sur un rang (commentaire `/!\` n° 2)

**Constat.** §7.4 décrit un bootstrap « sur les produits » et laisse ouverte la question
de la construction d'un intervalle de rang. L'implémentation actuelle classe chaque
produit **à l'intérieur du rééchantillon** (avec doublons) : les rangs obtenus ne sont pas
sur la même échelle que le rang d'origine, et un produit absent d'un tirage n'a pas de
rang (`n_draws_observed`).

**Correction (D-08).** Distinguer deux sources d'incertitude et deux procédures :
1. *Incertitude d'estimation de la méthode* (poids, normalisation, carte de transport) :
   pour `b = 1..B`, rééchantillonner les lignes avec remise, **ajuster** la chaîne sur le
   rééchantillon, puis **scorer et classer la population complète** d'origine. Chaque
   produit reçoit exactement `B` rangs comparables ; l'intervalle est l'intervalle de
   percentiles (5 %–95 % pour 90 %). Les méthodes sans état ajusté (Pareto, MPI) n'ont pas
   de bootstrap de ce type.
2. *Incertitude des métriques elles-mêmes* : perturbation `x_ij^(b) ~ N(x̂_ij, σ̂_ij²)`
   ou rééchantillonnage des déclarations ; requiert `σ̂_ij`, non disponible ici (M-11).
   À documenter comme extension, non implémentée.
Le document expliquera qu'un rang est une statistique de l'échantillon entier et que son
intervalle s'interprète comme en *league tables* (Goldstein & Spiegelhalter 1996 ; Xie,
Singh & Zhang 2009).

**Références.** `goldstein1996`, `xie2009`, `saisana2005`, `efron1994`.
**Code.** I-09.

### M-10 — Taux de violation : inversions strictes et ex æquo confondus

**Constat.** `V(s)` compte `s(x_i) ≤ s(x_k)` ; un ex æquo entre deux produits comparables
compte comme une violation. Or plusieurs méthodes produisent des ex æquo légitimes
(BoD à 1, scores discrets). Le seuil de litige (`η = 50` rangs) est absolu alors que les
groupes vont de 27 (pays) à 200 000 (global) lignes.

**Correction.** Rapporter `V_strict` (inversions `s_i < s_k`) et `V_tie` (égalités) ;
exprimer le seuil de litige comme une fraction de la taille du groupe (défaut 1 %,
minimum 1).

**Code.** I-08.

### M-11 — Données de calibration indisponibles à l'étape d'agrégation

Le bootstrap des déclarations (calibrage de `ε`, propagation de l'incertitude des
métriques) suppose l'accès aux flux bruts. L'étape d'agrégation ne lit que la table des
métriques. Le document présentera ces options comme relevant de l'étape de calcul des
métriques (qui pourrait exporter `σ̂_ij`), et retiendra pour `ε` une échelle robuste de
colonne (M-02).

### M-12 — Entropie : colonne constante et dépendance à la normalisation

Après min–max, une colonne constante est nulle : `p_ij = 0/0`. Le document doit fixer la
convention (poids nul, colonne signalée) et rappeler que la méthode dépend de la
normalisation (non invariante par translation), ce qui est dit, mais sans préciser que
seule la min–max (ou les rangs) est admissible. Une métrique de poids nul ne participe
plus à la monotonie stricte ; sans conséquence pratique, mais à énoncer.

### M-13 — CRITIC : variante robuste annoncée mais non implémentée ; variante `|r|`

Le document recommande « MAD et Spearman » ; le code ne change que la corrélation.
Ajouter la variante complète (`MAD_j · Σ_k (1 − ρ^S_jk)`) et mentionner la variante
`Σ_k (1 − |r_jk|)` (une anticorrélation forte est aussi une redondance d'information ;
le choix est une hypothèse à expliciter).

**Code.** I-06.

### M-14 — Section transport optimal centrée sur Klein et al.

La section s'ouvre par « répond à la question posée sur l'article de Klein… » et
structure son propos comme une réponse à cet article (remarque 3.4, propositions 3.2 et
3.6, « ce qui n'est pas transposable »). Réécrire la section à partir des sources : rangs
et signes *center-outward* (Chernozhukov et al. 2017 ; Hallin et al. 2021), estimation
entropique (Cuturi 2013 ; Pooladian & Niles-Weed 2021), propriétés statistiques (Ghosal &
Sen 2022), calibration conforme (Vovk et al. 2005 ; Lei et al. 2018). Conserver le passage
« atypique contre vulnérable » (figure des deux points `a` et `b`) et la construction du
score orienté ; citer Klein et al. et Thurin et al. comme travaux apparentés.

### M-15 — Critère d'ellipticité comme porte de décision (commentaire `/!\` n° 1)

**Position retenue (D-07).** La remarque « si `τ > 0,95`, la complexité n'est pas
justifiable » est retirée comme règle. Le score de Kantorovitch est calculé dès que la
dépendance est installée et que `n` et `d` le permettent ; le diagnostic d'ellipticité
(corrigé, M-06) est **rapporté** pour l'interprétation (« sur ce groupe, le transport n'a
rien ajouté à la version linéaire »). Le document présentera l'argument dans les deux
sens : la méthode est plus générale (aucune hypothèse de forme), mais son estimation à
distance finie coûte des hyperparamètres et se dégrade avec `d` ; le diagnostic permet de
savoir si cette généralité a été utile.

### M-16 — Forme : ton prescriptif, coquilles, structure

- Phrases à l'infinitif injonctif (« Privilégier… », « Vérifier… », « Faire varier… ») à
  reformuler en descriptions (« La méthode X est adaptée lorsque… », « Le contrôle
  consiste à… »).
- Coquilles : `medskip` sans barre oblique (définition « Compensation »), « ne est pas »,
  « auxuqelles », « non trivia ~ », résumé tronqué (« rangs de Kantorovitch fondés. »),
  numérotation `avertissement` incohérente.
- Préambule : `\definecolor{Bleu}` doit précéder `\usepackage[...linkcolor=Bleu]{hyperref}`
  ; `hidelinks` et `colorlinks=true` sont contradictoires.
- Bibliographie en fin de fichier (`thebibliography`) à remplacer par `bibliography.bib`
  + `natbib`.
- Contexte projet (positions NC, « comité », « dossier administratif ») à déplacer dans le
  chapitre d'application.

### M-17 — Malédiction de la dimension : la limite `d ≤ 6` est le cas d'usage

Le document conseille `d ≤ 6` pour le transport optimal ; le cas d'usage a 6 métriques.
Le chapitre d'application doit traiter ce point : réduction préalable de `d`
(regroupement des métriques redondantes, D-03), diagnostic de convergence (KS) et
sensibilité à `ε` et à `m`, et stratégie d'ajustement sur sous-échantillon pour les
grands `n` (D-11).

### M-18 — Coût du front de Pareto au niveau global

`pareto_dominance_matrix` est `O(n²)` en mémoire (deux tableaux `n × n × d`
intermédiaires). Pour le niveau global (`n ≈ 1,4·10⁵ à 2·10⁵` cellules par contexte),
c'est impossible tel quel (`n² ≈ 2·10¹⁰`). Le document doit distinguer : front exact
(algorithme de balayage après tri, `O(n log n + n·|F|·d)`), couches (épluchage itératif
du front), comptage de dominance (inhérent `O(n²d)`, calculé par blocs). Voir A-07.

**Références.** `kung1975`, `bentley1978`, `deb2002`.

### M-19 — Réduction préalable de `d` : représentants non spécifiés

« Le front étant ensuite calculé sur les représentants de groupes » : préciser que le
représentant d'un groupe de métriques est la **moyenne des métriques normalisées du
groupe** (sous-indice), que la dominance sur les moyennes est impliquée par la dominance
sur les métriques (donc `F_1(réduit) ⊆ F_1(complet)`), et que le nombre de groupes est
soit fixé, soit déduit d'un seuil de dissimilarité `1 − |ρ^S| < θ` sur le dendrogramme.

### M-20 — Points mineurs

- `W` de Kendall donné sans correction d'ex æquo ; à mentionner.
- SMAA : préciser que la matrice `b` complète n'est jamais stockée au-delà de `r ≤ k`.
- Espérance du cardinal du front (§3.3) : donner la référence (`bentley1978`) et les
  ordres de grandeur du cas d'usage (`n = 5 000` : `d = 3 → ~36`, `d = 4 → ~103`,
  `d = 6 → ~374` sous indépendance ; simulation : 414 ; avec corrélation 0,5 : 71).
- Kemeny : la formulation MILP (antisymétrie + interdiction des 3-cycles) est à donner
  explicitement, avec la préselection.

---

## 4. Problèmes d'implémentation (I-xx)

Vérifications exécutées avec `.venv` (Python 3.13.4, scikit-learn 1.9.0, scipy 1.18.0,
numpy 2.3.5, pandas 2.3.3).

### I-01 — Front, comptage et violations calculés sur `X` non orienté (`runner.py`)

`run_aggregation` et `recommended_workflow` appellent `pareto_front(X)`,
`dominance_count(X)` et `compute_coherence_report(scores, X)` sur la matrice brute
issue de `split_frame`, avant `PolarityOrienter`. Avec `polarities={"m2": -1}` sur
`m1 = [0.9, 0.2, 0.5, 0.6]`, `m2 = [10, 2, 5, 4]` : front brut `[T, F, F, F]`, front
orienté `[T, T, F, T]` ; `report.pareto_front_size = 1` au lieu de 3. **Correction** :
orienter `X` une fois en tête de runner (`X_or`) et l'utiliser pour tous les calculs de
dominance ; les métriques actuelles sont toutes de polarité `+1`, le bug est donc latent.

### I-02 — `pareto_dominance_matrix(epsilon)` (M-02)

Remplacer par `epsilon_pareto_set` (glouton de Laumanns) ; conserver la matrice exacte.
`epsilon_pareto_front` devient un alias de la nouvelle fonction.

### I-03 — `optimal_transport.py`

- `spherical_uniform_grid` : rayons `ρ^{1/d}` (M-01) → `ρ` (loi sphérique uniforme).
- `fit_report` : KS contre U[0,1], correct seulement après la correction précédente.
- `PointCloud(..., epsilon=self.epsilon)` : `ε` absolu, donc dépendant de l'échelle des
  données. Utiliser `scale_cost="mean"` (ε relatif au coût moyen) et documenter la
  normalisation d'entrée attendue (`quantile_gaussian` ou `standard`).
- Pas de sous-échantillonnage : Sinkhorn sur `n × m` avec `n = 2·10⁵`, `m = 2¹⁴` est
  irréalisable en mémoire. Ajuster sur un sous-échantillon (`fit_sample_size`, défaut
  20 000) et transporter tous les points par lots (`batch_size`) : la carte entropique est
  définie hors échantillon.
- API : `score_samples` au lieu de `predict` ; pas `BaseEstimator` (donc ni `get_params`
  ni `clone`). Adopter `BaseEstimator` + `fit/predict` (+ `transport`, `ranks`, `signs`,
  `alert`).
- Aucune des trois variantes de score au-delà de la projection ; pas de seuil d'alerte
  (M-07).
- `fit` ne journalise pas la non-convergence (`converged=False` silencieux).

### I-04 — `benefit_of_doubt_weights` (M-03)

- Score 0 pour toute ligne comportant un 0 (preuve en M-03).
- Aucun estimateur ; la méthode n'est pas utilisable dans `run_aggregation`
  (`WEIGHTING_REGISTRY` ne la contient pas et son résultat est un score, pas un poids).
- Pas de validation `X ≥ 0`.
**Correction** : restrictions par région d'assurance (`rho`), option `shares` avec
plancher `delta` ; classe `BenefitOfDoubtScorer(BaseEstimator)` dont `fit` mémorise les
lignes du front (contraintes) et `predict` résout un programme par ligne ; validation.

### I-05 — `pca_weights` et `WeightedAggregator`

- `pca_weights` suppose `X` standardisé, mais `default_pipeline` normalise en min–max par
  défaut : l'ACP est alors celle de la covariance des données min–max, pas de la
  corrélation. **Correction** : calculer sur la matrice de corrélation `np.corrcoef` (donc
  indépendamment de l'échelle), et pour la variante « axe 1 » appliquer la projection sur
  `Z` standardisé à l'intérieur de l'estimateur (nouvelle classe `PcaProjectionScorer`) ;
  la variante OCDE renvoie un vecteur du simplexe utilisable avec toute normalisation.
- Formule OCDE (M-04) : variances après rotation, affectation, normalisation
  intra-facteur.
- La variante simple renvoie un vecteur non simplicial pouvant contenir des négatifs ;
  `WeightedAggregator` l'accepte pour `topsis` et `geometric_mean`, ce qui n'a pas de sens.
  **Correction** : `WeightedAggregator.fit` vérifie `w ≥ 0` et `Σw = 1` (tolérance) pour
  les agrégations qui l'exigent et lève `ValueError` sinon.

### I-06 — `critic_weights` : `NaN` sur colonne constante

`np.corrcoef` renvoie `NaN` pour une colonne constante (après min–max : colonne nulle),
et les poids deviennent tous `NaN` (vérifié). **Correction** : `nan_to_num(corr, 0)` sur les
corrélations, `σ_j = 0` donne un poids nul ; ajouter `scale="std"|"mad"` (M-13).

### I-07 — `geometric_mean_score` : `NaN` sur données négatives

Sur `X` centré-réduit, 451 scores `NaN` sur 500 (vérifié). **Correction** : validation
`X ≥ 0` avec message explicite nommant la normalisation attendue.

### I-08 — `dominance_violation_rate` et `compute_coherence_report`

Renvoyer `(V_strict, V_tie)` (M-10) ; `dispute_threshold` → `dispute_fraction`
(défaut 0,01, minimum 1 rang). Ajouter au rapport : rang médian et rang maximal des
membres du front (contrôle associé de §7.3 du document, non implémenté).

### I-09 — `bootstrap_rank_stability` et `smaa_rank_acceptability`

- Bootstrap : re-spécification D-08 (ajustement sur rééchantillon, classement de la
  population complète) ; conserver le nombre de tirages `B` et la largeur `ci`.
- SMAA : `rank_counts` est `(n, n)` en `int64` (`n = 5 000` → 200 Mo ; global impossible).
  Ne conserver que `(n, k)` (`r ≤ k`) et le facteur de confiance ; vectoriser par lots de
  tirages (`X @ Wᵀ` par blocs de `T_b` tirages puis `argsort` par colonne).
- `leave_one_metric_out` : correct ; à généraliser au niveau des groupes (S-2.5).

### I-10 — `ellipticity_screen` et porte dans `recommended_workflow`

Comparer `s^OT_proj` à `s^white` (A-04) et `‖T̄(x)‖` à `d_Mah(x, μ)` ; supprimer la
condition `tau < ellipticity_threshold` d'inclusion du score (D-07) ; garder
`ot_dimension_limit` et `min_group_size` comme seules conditions.

### I-11 — `default_pipeline(winsorize_quantile=0.99)` (M-08)

Défaut `None` (pas de winsorisation) ; `Winsorizer` accepte `quantile=None` comme identité.

### I-12 — `AggregationConfig` : champs hérités inutilisés

`high_score_threshold` et `shares_tolerance` ne sont lus nulle part dans le module.
Les retirer (ou les documenter comme réservés) ; ajouter les champs utiles au niveau
groupe (S-1.2).

### I-13 — Absence de tests

Seuls les doctests existent. Aucun test des estimateurs sur cas dégénérés (colonne
constante, `n < d`, `NaN`), aucun test de bout en bout. Voir S-4.

### I-14 — `mahalanobis_score`

Non orienté (M-06) ; `MinCovDet` sur `n = 2·10⁵` est lent (plusieurs minutes) — proposer
`ledoit_wolf` par défaut au niveau global via `MethodSpec.params`. Ajouter
`whitened_projection_score` (A-04).

### I-15 — `RankScaler.transform` hors échantillon

`np.interp` sur `sorted_train_` avec doublons a un comportement non spécifié. Utiliser
`np.searchsorted(side="right")` normalisé, ou `scipy.stats.percentileofscore` vectorisé.

### I-16 — `split_frame` et valeurs manquantes

`check_array` lève sur `NaN` ; or `CDI2`/`CDI3` sont nulles hors import et une jointure
avec la famille réseau produira des nulles. Le runner doit sélectionner, **par méthode**,
les lignes complètes sur les métriques de la méthode (D-15) et renvoyer `NaN` ailleurs.

### I-17 — `copeland_rank` et `kemeny_rank` au niveau global

`copeland_rank` construit une matrice `(n, n)` de votes : impossible pour `n = 2·10⁵`.
Le calculer sur une présélection Borda (`top_n`), comme Kemeny, avec le reste dans l'ordre
de Borda.

### I-18 — `recommended_workflow`

Jeu de méthodes et normalisation codés en dur ; acceptable comme *workflow de
démonstration* si le runner de synthèse (S-1.6) est piloté par la configuration. À
conserver, à aligner sur les corrections ci-dessus, à ne pas utiliser dans les scripts.

---

## 5. Méthodes à ajouter (A-xx)

Critère de sélection : combler une lacune réelle du dispositif (orientation, absence de
poids, cohérence de rang), rester calculable à `n = 2·10⁵`, et être citée dans la
littérature. Les méthodes marquées *doc seulement* sont présentées dans la note mais non
implémentées.

### A-01 — Quantile de cône (Hamel & Kostner 2018) — implémenter

**Idée.** Le comptage de dominance est l'écart entre la fonction de répartition jointe
et la fonction de survie jointe empiriques. Hamel et Kostner définissent la *fonction de
répartition de cône* `F_C(x) = inf_{w ∈ Δ^{d−1}} F_{wᵀX}(wᵀx)`, c'est-à-dire le **pire
rang unidimensionnel** de `x` parmi toutes les sommes pondérées à poids positifs. Elle
est monotone pour `≻_P`, sans poids, orientée, et donne un niveau `α` interprétable
(« `x` est au-dessus de `1 − F_C(x)` de la population sous toute pondération »).

**Estimation.** Sur une grille `w^(1..T)` (tirages Dirichlet, réutilisés par SMAA) :
`F̂_C(x_i) = min_t rang(wᵀ⁽ᵗ⁾ x_i) / n`. Coût `O(T · n log n)`. Le score est `F̂_C` ; la
version *sup* (`max_t`) donne le meilleur cas, et l'écart entre les deux mesure
l'indétermination par produit (complément naturel de SMAA).

**Références.** `hamel2018`, `kong2012` (quantiles directionnels, *doc seulement*).

### A-02 — Score de rang moyen (Borda sur métriques) — implémenter

Moyenne des rangs normalisés `(rg(x_ij) − 1)/(n − 1)` sur les métriques, à poids égaux
ou endogènes. C'est la méthode de référence de la littérature appliquée (Arjona et al.
2023, « approche par rangs » ; le SPOF du module réseau en est un cas particulier). Elle
existe implicitement (normalisation `rank` + somme pondérée) ; l'ajouter comme méthode
nommée `rank_mean` pour la lisibilité de la configuration et du document.

**Références.** `arjona2023`, `borda1781`.

### A-03 — VIKOR — implémenter

Compromis entre l'utilité de groupe `S_i = Σ_j w_j (A⁺_j − x_ij)/(A⁺_j − A⁻_j)` et le
regret individuel `R_i = max_j w_j (A⁺_j − x_ij)/(A⁺_j − A⁻_j)` :
`Q_i = v (S_i − S⁻)/(S⁺ − S⁻) + (1 − v)(R_i − R⁻)/(R⁺ − R⁻)`, `v ∈ [0,1]`. Sibling naturel
de TOPSIS avec un paramètre de compensation explicite ; monotone pour `w > 0`. Score
retourné : `1 − Q_i`.

**Références.** `opricovic2004`, `opricovic2007`.

### A-04 — Projection blanchie orientée — implémenter

`s^white(x) = ⟨Σ^{−1/2}(x − μ), v⟩` avec `μ`, `Σ` robustes (MCD ou Ledoit–Wolf) et
`v` la direction blanchie de l'idéal `x⁺` (M-06). Linéaire, contrepartie exacte du score
de Kantorovitch orienté dans le cas elliptique ; sert de méthode et de référence du
diagnostic d'ellipticité.

### A-05 — Méta-pondération « auto » — implémenter (D-04)

Sélection, par groupe, du schéma de pondération partagé le plus adapté aux diagnostics
du groupe (règles en S-1.5). Rapport de sélection persisté (S-2.6).

### A-06 — τ de Kendall pondéré (Vigna 2015) — implémenter

`scipy.stats.weightedtau` : corrélation de rangs pondérée hyperboliquement vers les
premiers rangs, complément de RBO pour Q2 ; symétrique et sans paramètre de profondeur.

**Références.** `vigna2015`.

### A-07 — Algorithmes de front pour grands `n` — implémenter

- Front exact par balayage : tri décroissant par somme des coordonnées ; un point n'est
  dominé que par un point de somme supérieure ; on maintient le front courant et on
  compare chaque point à ce front (`O(n log n + n |F| d)`).
- Couches : épluchage itératif par le même balayage.
- Comptage de dominance par blocs (`chunk_size` lignes contre toutes les colonnes,
  `O(n²d)` temps, `O(chunk · n)` mémoire), avec barre de progression optionnelle.
- Front ε glouton (M-02).

**Références.** `kung1975`, `bentley1978`, `laumanns2002`.

### A-08 — Seuil d'alerte conforme (radial et projeté) — implémenter (M-07)

### A-09 — *Doc seulement*

- PROMETHEE II (flux net d'outranking, Brans & Vincke 1985) : partiellement
  compensatoire, `O(n²)`, mentionné comme alternative.
- Classement par extensions linéaires d'un ordre partiel (Bruggemann & Patil 2011 ;
  Fattore 2016) : raffinement théorique du comptage de dominance ; approximation locale de
  De Loof et al. (2006) ; coût prohibitif au-delà de quelques centaines d'éléments.
- MEREC (Keshavarz-Ghorabaee et al. 2021) : pondération par effet de retrait ; cousin de
  *leave-one-metric-out*.
- Profondeurs statistiques (Tukey 1975 ; Zuo & Serfling 2000 ; Mosler 2013) : comme le
  rang center-outward, non orientées.

---

## 6. Décisions clés (D-xx)

### D-01 — Les scores synthétiques ne sont **pas** ajoutés en colonnes à la table des métriques

Réponse à la question « les ajouter comme nouvelles colonnes à la même base de
données ». Même **catalogue** (`vulnerabilities`) : oui. Même **table** : non, pour quatre
raisons.

1. `write_dataframe` écrit une `fact_table` unique par schéma et l'upsert de
   `dt_ducklake_manager` (`DatabaseUpdater._update_metadata_safe`) ne traite que
   l'intersection des colonnes existantes et nouvelles : **il ne sait pas ajouter une
   colonne** à une table existante. Ajouter des colonnes de score reviendrait à
   recréer la table des métriques.
2. Un upsert par clé primaire remplace les lignes fournies : l'étape de synthèse
   réécrirait les lignes de l'étape de métriques (couplage fort, risque d'écraser un
   recalcul concurrent, deux registres de fraîcheur sur une même table).
3. Un score est **relatif à un groupe** (niveau, contexte) et à une **méthode** ; sa
   sémantique diffère de celle d'une métrique absolue. Une ligne de métriques a un sens
   seule ; une ligne de scores n'en a qu'avec ses colonnes `n_*` et sa méthode.
4. L'ensemble des méthodes est piloté par configuration et changera ; une table
   « longue par méthode » (S-2.4) est stable au schéma, là où des colonnes par méthode
   imposeraient une migration à chaque changement.

**Décision.** Schéma `synthesis` du catalogue `vulnerabilities`, une ligne par
`cellule × méthode`, avec **trois paires (score, rang)** (une par niveau), ce qui satisfait
l'exigence « trois colonnes de reranking par méthode ».

### D-02 — La cohérence est stockée dans un schéma distinct du même catalogue

Accord avec la proposition. Schéma `synthesis_diagnostics`, table longue
(`statistique × niveau × groupe × objet a × objet b → valeur`), S-2.6. Justification :
granularité différente (groupe, pas cellule), volumétrie différente, schéma stable.

### D-03 — Pareto : classe unique, cône ε corrigé et réduction préalable de `d`

Accord avec la proposition, sous réserve de la correction M-02. Classe `ParetoScorer`
(S-1.3) exposant le front exact, le front ε glouton, les couches, le comptage et la
réduction de `d` par regroupement. Dans les scripts, pour `d = 6` et `n = 5 000` par
pays : réduction par regroupement à seuil (`1 − |ρ^S| < 0,3` → même groupe), puis front ε
sur les représentants avec `ε = 0,1 · MAD`. Le front exact et le comptage sur les métriques
non réduites restent calculés (invariants gratuits à `n = 5 000`) ; au niveau global,
seuls le front exact (balayage) et le comptage par blocs sont calculés, les couches
étant désactivées par défaut (`layers: false`).

### D-04 — Méta-méthode de pondération : oui, pour les schémas à poids partagés

Accord. BoD attribue une pondération **par produit** et n'est pas un vecteur de poids
partagé ; il reste une méthode distincte et n'entre pas dans la sélection. La sélection
porte sur `{entropy, critic, pca_oecd, equal}` selon les règles S-1.5, et son résultat
est un rapport (`AutoWeightingReport`) persisté par groupe.

### D-05 — BoD : contraintes restreintes au front exact, restrictions par région d'assurance

Voir M-03. La restriction au front est déjà implémentée et exacte ; elle est conservée.
Les restrictions de parts sont remplacées par défaut par des rapports de poids bornés.

### D-06 — Seuil d'alerte du transport optimal : conservé, deux calibrations

Voir M-07 et A-08. Colonnes `alert_{niveau}` de la table des scores pour la méthode
`kantorovich` ; seuils rapportés dans les diagnostics.

### D-07 — Le diagnostic d'ellipticité n'est pas une porte

Voir M-15. Le score de Kantorovitch est produit dès que `jax`/`ott-jax` sont installés,
`d ≤ ot_dimension_limit` et `n ≥ min_group_size` ; les diagnostics `ellipticity_tau_*`
sont persistés.

### D-08 — Intervalle de rang : bootstrap « ajuster sur rééchantillon, classer la population »

Voir M-09. Colonnes `rank_low_{niveau}`, `rank_high_{niveau}` renseignées pour les
méthodes listées dans `bootstrap.methods` ; `B` petit par défaut (50) ; désactivé au
niveau global par défaut.

### D-09 — Trois niveaux, un contexte, une sélection de flux

Niveaux `by_product`, `by_reporter`, `global` à l'intérieur de chaque contexte
`(freq, flow, indicators, TIME_PERIOD)`. Le script filtre les contextes par configuration
(`FILTERS`, S-2.2) ; par défaut flux import, indicateur valeur, fréquence annuelle,
`N` dernières périodes. Interprétation retenue des trois demandes :
« pour une nomenclature indépendamment pour chaque pays » = `by_product` (on ordonne les
pays d'un produit) ; « par pays, ordonne les nomenclatures » = `by_reporter` ; « toutes
les nomenclatures pour l'ensemble des pays » = `global`. À confirmer (§11).

### D-10 — Format long par méthode, trois paires (score, rang), rang 1 = plus vulnérable

Voir S-2.4. Rangs par `rankdata(-score, method="average")`, `NaN` propagé.

### D-11 — Passage à l'échelle par méthode

| Méthode | `by_product` (`n ≈ 30`) | `by_reporter` (`n ≈ 5 000`) | `global` (`n ≈ 2·10⁵`) |
|---|---|---|---|
| Pareto exact / comptage | oui | oui (`O(n²)` matriciel) | front par balayage ; comptage par blocs ; couches désactivées |
| Entropie, CRITIC, rang moyen, somme, géométrique, TOPSIS, VIKOR, MPI | oui (≥ `min_group_size`) | oui | oui |
| ACP / OCDE, Mahalanobis, projection blanchie | `n ≥ 30` requis | oui | `ledoit_wolf` recommandé |
| BoD | oui | oui (`n` PL, ≈ 30 s) | oui (`n` PL, ≈ 10 min ; `joblib` optionnel) |
| Kantorovitch | non (défaut `min_group_size = 500`) | oui | ajustement sur `fit_sample_size = 20 000`, transport par lots |
| SMAA, quantile de cône | oui | oui | tirages par lots ; stockage `(n, k)` |
| Bootstrap | oui pour méthodes ajustées | oui, `B = 50` | désactivé par défaut |
| Consensus Copeland / Kemeny | oui | présélection `top_n = 100` (Kemeny) | présélection pour les deux |

### D-12 — Winsorisation désactivée par défaut

Voir M-08.

### D-13 — Normalisation par méthode, avec défauts

La configuration fixe une normalisation par défaut, surchargeable **par méthode** ; le
runner valide la compatibilité (tableau S-1.4). Défauts : `minmax` pour entropie, CRITIC,
BoD, géométrique, TOPSIS, VIKOR, MPI ; `standard` pour ACP axe 1, Mahalanobis, projection
blanchie ; `quantile_gaussian` pour Kantorovitch ; aucune (`none`) pour Pareto, rang
moyen, quantile de cône (invariants par transformation monotone).

### D-14 — Suivi MLflow : un run par exécution de script

Métriques agrégées par niveau (`synthesis.by_reporter.kendall_w_median`, …), paramètres
= configuration aplatie, artefacts : top-k par méthode et niveau, matrices `τ`, poids,
rapports de sélection. Pas un run par groupe (des milliers de groupes `by_product`).

### D-15 — Valeurs manquantes : par méthode, lignes complètes

Pour chaque méthode et chaque groupe, seules les lignes sans `NaN` sur **les métriques de
la méthode** sont ajustées et scorées ; les autres reçoivent `NaN` (score et rang). `n_*`
compte les lignes effectivement scorées. Le nombre de lignes exclues est rapporté.

### D-16 — Conventions de sortie

Score : plus élevé = plus vulnérable, quelle que soit la méthode (les méthodes
« coût » sont retournées en `−` ou `1 − ·`). Rang : 1 = plus vulnérable, ex æquo
moyennés, `NaN` conservé. Alerte : booléen nullable (`NULL` si la méthode n'en définit
pas ou si la ligne n'est pas scorée).

### D-17 — Compatibilité Kedro

Les runners (`run_synthesis`, `run_coherence`) sont des fonctions pures « DataFrame → 
DataFrames + rapport » (les connexions sont utilisées uniquement pour lire/écrire dans
des fonctions séparées), pour devenir des nœuds Kedro sans réécriture.

### D-18 — Documentation : nouveau fichier, bibliographie externe

Nouveau fichier `Methodologie synthese multicritere.tex` (l'ancien est conservé jusqu'à
validation puis supprimé), `bibliography.bib` à la racine, `natbib`, compilation
`latexmk -pdf`. Spécification en S-3.

---

## 7. Architecture cible du module (S-1)

### S-1.1 Arborescence

```
macroforecast/trade/aggregation/
├── __init__.py            # réexports (inchangé dans l'esprit)
├── base.py                # AggregationConfig (nettoyé), split_frame, attach_scores, complete_rows_mask
├── preprocessing.py       # + Winsorizer(quantile=None), RankScaler corrigé, make_preprocessing(spec)
├── pareto.py              # + ParetoScorer, epsilon_pareto_set, pareto_front_sweep, dominance_count_chunked, MetricReducer
├── weights.py             # entropy(+guards), critic(scale, method, abs_corr), pca_weights(corr, OCDE corrigé), auto_weights, bod (rho/shares)
├── functions.py           # + rank_mean_score, vikor_score, whitened_projection_score, cone_quantile_score ; validations
├── estimators.py          # WeightedAggregator (validations), PcaProjectionScorer, BenefitOfDoubtScorer, ConeQuantileScorer, SmaaScorer, ParetoScorer réexporté
├── optimal_transport.py   # OrientedKantorovichScorer(BaseEstimator) corrigé + alertes + sous-échantillonnage
├── methods.py             # NOUVEAU : MethodSpec, METHOD_REGISTRY, build_method(spec, config) -> Pipeline
├── diagnostics.py         # corrections I-08/I-09/I-17 ; + weighted_tau, front_rank_summary, group-level helpers
├── synthesis.py           # NOUVEAU : SynthesisConfig, LEVELS, iter_groups, run_synthesis (pur), SynthesisReport
├── coherence.py           # NOUVEAU : CoherenceConfig, run_coherence (pur), CoherenceRunReport
└── runner.py              # default_pipeline / run_aggregation / recommended_workflow alignés (démo)
```

### S-1.2 Configurations

```python
@dataclass(frozen=True)
class AggregationConfig:
    """Conventions de colonnes d'un groupe (inchangé : id_columns, metric_columns, polarities)."""
    id_columns: Tuple[str, ...]
    metric_columns: Tuple[str, ...]
    polarities: Mapping[str, int] = field(default_factory=dict)

@dataclass(frozen=True)
class MethodSpec:
    """Une méthode configurée."""
    name: str                              # nom de colonne/valeur `method` dans la table
    kind: str                              # clé de METHOD_REGISTRY (cf. S-1.4)
    metrics: Optional[Tuple[str, ...]] = None   # sous-ensemble ; None = toutes
    normalization: Optional[str] = None    # surcharge D-13
    params: Mapping[str, Any] = field(default_factory=dict)  # transmis à l'estimateur
    min_group_size: Optional[int] = None   # surcharge du défaut de la méthode
    levels: Optional[Tuple[str, ...]] = None    # None = tous les niveaux

@dataclass(frozen=True)
class SynthesisConfig:
    context_columns: Tuple[str, ...] = ("freq", "flow", "indicators", "TIME_PERIOD")
    reporter_col: str = "reporter"
    product_col: str = "product"
    metric_columns: Tuple[str, ...] = ("HHI", "CDI2", "CDI3")
    polarities: Tuple[Tuple[str, int], ...] = ()
    levels: Tuple[str, ...] = ("by_product", "by_reporter", "global")
    methods: Tuple[MethodSpec, ...] = (...)      # défaut documenté en S-2.2
    normalization: str = "minmax"
    winsorize_quantile: Optional[float] = None
    min_group_size: int = 30
    rank_ties: str = "average"
    consensus: Tuple[str, ...] = ("borda", "copeland")
    consensus_top_n: int = 100
    smaa_n_draws: int = 2_000
    smaa_k: int = 50
    bootstrap_methods: Tuple[str, ...] = ()
    bootstrap_n: int = 50
    bootstrap_ci: float = 0.9
    bootstrap_levels: Tuple[str, ...] = ("by_product", "by_reporter")
    random_state: int = 0
    ot_dimension_limit: int = 8
    artifact_top_n: int = 50

@dataclass(frozen=True)
class CoherenceConfig:
    """Paramètres du second script (S-2.5)."""
    topk_depths: Tuple[int, ...] = (10, 50, 100)
    rbo_p: float = 0.98
    dispute_fraction: float = 0.01
    lomo: bool = False              # leave-one-metric-out (ré-ajustement)
    lomo_methods: Tuple[str, ...] = ()
    metric_pairs: bool = True       # τ entre métriques
```

`SynthesisConfig.methods` par défaut : voir l'exemple YAML S-2.2 (le défaut Python est
identique). Les scripts construisent ces dataclasses avec le même mécanisme
`*_config_from_params` que les scripts existants ; `methods` est une liste de mappings
YAML convertie en `MethodSpec`.

### S-1.3 `ParetoScorer` (D-03)

```python
class MetricReducer(BaseEstimator, TransformerMixin):
    """Regroupe les métriques redondantes (CAH sur 1 - |rho_Spearman|) et remplace chaque
    groupe par la moyenne de ses colonnes normalisées (rang normalisé par défaut)."""
    def __init__(self, n_groups=None, threshold=0.3, linkage="average"): ...
    def fit(self, X): ...          # groups_ : List[Tuple[int, ...]]
    def transform(self, X): ...    # (n, d')

class ParetoScorer(BaseEstimator):
    """Front, front epsilon, couches et comptage de dominance ; score = profondeur normalisée."""
    def __init__(self, score="dominance_depth", epsilon=None, epsilon_scale="mad",
                 reduce=None, layers=True, chunk_size=2048, sort_key="sum"):
        # epsilon : None | float (multiplicateur de l'échelle robuste) | array (d,) absolu
        # reduce  : None | MetricReducer (ajusté dans fit)
    def fit(self, X, y=None): ...                     # epsilon_ (d',), reducer_
    def front(self, X) -> np.ndarray[bool]            # exact, balayage
    def epsilon_front(self, X) -> np.ndarray[bool]    # glouton de Laumanns, sous-ensemble de front(X)
    def layers(self, X) -> np.ndarray[int]            # 1 = front ; 0 si désactivé
    def dominance_count(self, X) -> np.ndarray[int]   # par blocs
    def predict(self, X) -> np.ndarray[float]         # dominance_depth = count/(n-1)
    def alert(self, X) -> np.ndarray[bool]            # appartenance au front epsilon (ou exact si epsilon None)
```

Invariants testables : `epsilon_front(X) ⊆ front(X)` ; `argmax(sum) ∈ epsilon_front(X)` ;
`epsilon = 0` ⟹ égalité des deux fronts ; `x_i ≻ x_k` ⟹ `layers[i] < layers[k]` et
`count[i] ≥ count[k] + 2` ; `front_sweep(X) == ~any(pareto_dominance_matrix(X), axis=0)`.

### S-1.4 Registre des méthodes (`methods.py`)

| `kind` | Estimateur | Normalisation admissible (défaut en gras) | Poids | Alerte | `min_group_size` défaut |
|---|---|---|---|---|---|
| `pareto` | `ParetoScorer` | **none** (monotone-invariant) | — | front ε | 2 |
| `rank_mean` | `WeightedAggregator("equal"/…, "weighted_sum")` sur `RankScaler` | **rank** | égaux ou endogènes | — | 2 |
| `weighted` | `WeightedAggregator(weighting, aggregation)` | **minmax**, rank | `entropy`, `critic`, `pca_oecd`, `auto`, `equal` | — | 30 |
| `pca_projection` | `PcaProjectionScorer` | **standard**, quantile_gaussian | direction | — | 30 |
| `mpi` | `WeightedAggregator("none", "mpi")` | **minmax**, standard, robust | — | — | 3 |
| `topsis` / `vikor` | `WeightedAggregator(w, "topsis"/"vikor")` | **minmax**, rank | simpliciaux | — | 30 |
| `bod` | `BenefitOfDoubtScorer(rho, restriction, delta)` | **minmax**, rank | individuels | — | 3 |
| `mahalanobis` | `WeightedAggregator("none","mahalanobis")` | **standard**, quantile_gaussian | implicite | — | 30 |
| `whitened_projection` | `WeightedAggregator("none","whitened_projection")` | **standard**, quantile_gaussian | implicite | — | 30 |
| `kantorovich` | `OrientedKantorovichScorer` | **quantile_gaussian**, standard | direction estimée | radiale / projetée | 500 |
| `smaa` | `SmaaScorer(aggregation, k)` : score = facteur de confiance | **minmax**, rank | Dirichlet | — | 3 |
| `cone_quantile` | `ConeQuantileScorer(n_draws)` | **none** | Dirichlet | — | 3 |

`build_method(spec, config) -> Pipeline` assemble `[orient, (winsorize), scale, estimator]`
avec la normalisation résolue (`spec.normalization` sinon défaut de la méthode) et
vérifie l'admissibilité (levée `ValueError` sinon). Tout estimateur expose `fit`,
`predict` ; optionnellement `alert(X) -> bool[n]` et `fit_report() -> dataclass`.

### S-1.5 Méta-pondération `auto_weights` (A-05)

Entrées : `X` (groupe, orienté, normalisé selon la méthode aval), seuils
`kmo_min=0.6`, `bartlett_alpha=0.05`, `axis1_share_min=0.5`, `redundancy_rho=0.3`,
`max_weight=0.6`. Algorithme :

1. `d == 1` → `equal`.
2. Diagnostics sur `Z` standardisé : `kmo`, `p_bartlett`, `λ₁/d`, poids OCDE `w_pca`
   (tous ≥ 0 par construction), `ρ̄ = moyenne |ρ^S_jk|, j≠k`.
3. Si `kmo ≥ kmo_min` et `p_bartlett ≤ bartlett_alpha` et `λ₁/d ≥ axis1_share_min` →
   candidat `pca_oecd`.
4. Sinon si `ρ̄ ≥ redundancy_rho` → candidat `critic`.
5. Sinon → candidat `entropy`.
6. Garde de dégénérescence : si `max_j w_j ≥ max_weight`, passer au candidat suivant dans
   l'ordre `pca_oecd → critic → entropy → equal`.
7. Retour `(w, AutoWeightingReport(selected, candidates_tried, kmo, p_bartlett,
   axis1_share, mean_abs_rho, max_weight))`.

Les seuils sont des paramètres de `MethodSpec.params` ; la règle est documentée comme
une heuristique et non comme un résultat.

### S-1.6 `run_synthesis` (pur)

```python
def run_synthesis(
    df_metrics: pd.DataFrame,          # cellules x métriques, déjà filtrées sur les contextes
    config: SynthesisConfig,
    *,
    tracker: RunTracker = NULL_TRACKER,
    log_artifacts: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, SynthesisReport]:
    """Renvoie (df_scores [S-2.4], df_group_diagnostics [S-2.6, famille 'fit'], report)."""
```

Boucle : pour chaque contexte → pour chaque niveau → pour chaque groupe →
`X, index = split_frame(...)` → pour chaque méthode applicable (`levels`, `n ≥
min_group_size`, lignes complètes D-15) → `pipeline.fit(X_c).predict(X_c)` → score, rang,
alerte → consensus (Borda, Copeland présélectionné) → SMAA/quantile de cône (tirages
partagés) → bootstrap (D-08) si demandé. Les diagnostics d'ajustement (poids, sélection
auto, convergence OT, KS, seuils d'alerte, `n` exclus) sont émis en format long
(S-2.6, `family = "fit"`). Le rapport agrège par niveau : nombre de groupes, tailles
médianes, méthodes sautées et pourquoi, temps par méthode.

### S-1.7 `run_coherence` (pur)

```python
def run_coherence(
    df_metrics: pd.DataFrame, df_scores: pd.DataFrame,
    synthesis_config: SynthesisConfig, config: CoherenceConfig,
    *, tracker=NULL_TRACKER, log_artifacts=True,
) -> Tuple[pd.DataFrame, CoherenceRunReport]:
    """Renvoie (df_diagnostics [S-2.6, familles 'metrics' et 'methods'], report)."""
```

Par contexte × niveau × groupe : statistiques de la famille `metrics` (S-2.5.a) et de la
famille `methods` (S-2.5.b). `lomo=True` ré-ajuste les méthodes listées via
`build_method` (coût maîtrisé par `lomo_methods`).

---

## 8. Spécification des scripts et des tables (S-2)

### S-2.1 Scripts

| Script | Entrée console (`pyproject`) | Config | Registre | Amont |
|---|---|---|---|---|
| `scripts/compute_synthetic_scores.py` | `vulnerabilities-synthesis-script` | `config/synthesis.yaml`, bloc `SYNTHESIS` | `trade/vulnerabilities/synthesis/last_computation.json`, racine `SYNTHESIS` | `compute_trade_vulnerabilities.py`, `compute_network_vulnerabilities.py` |
| `scripts/compute_synthesis_coherence.py` | `vulnerabilities-coherence-script` | même fichier, bloc `COHERENCE` | `trade/vulnerabilities/synthesis/last_coherence.json`, racine `COHERENCE` | `compute_synthetic_scores.py` |

Ordre Argo : `download → process_baci_hs → compute_trade_vulnerabilities ∥
compute_network_vulnerabilities → compute_synthetic_scores → compute_synthesis_coherence`.

**Fraîcheur.** Le registre amont des vulnérabilités partenaires est indexé par
`(reporter, product)` sans période ; on ne peut pas savoir quelles périodes ont bougé.
Règle v1 : si `max(last_computed)` d'un registre amont (partenaires **ou** réseau) est
postérieur à `last_computed` du registre de synthèse, **tous les contextes sélectionnés
par `FILTERS`** sont recalculés ; sinon rien. Clé `FORCE: true` pour forcer. La date
écrite est capturée avant le calcul. Le script de cohérence applique la même règle contre
le registre de synthèse. Amélioration v2 (non implémentée) : registre de synthèse indexé
par contexte.

**Erreurs.** Comme `compute_network_vulnerabilities.py` : un contexte en échec n'arrête
pas les autres ; échec global en fin de parcours ; seuls les contextes réussis sont écrits
et datés.

### S-2.2 Configuration `config/synthesis.yaml` (exemple complet)

```yaml
# Synthèse des métriques de vulnérabilité (scores synthétiques) et cohérence.
# Même catalogue DuckLake que les métriques (VULNERABILITIES.DBNAME / CATALOG_ALIAS de
# config/vulnerabilities.yaml) ; deux schémas résultat distincts.

SYNTHESIS:
  BUCKET: "qbollietdgddi"
  RESULT_SCHEMA: "synthesis"
  PATHS:
    DATA_PATH: "trade/datasets/vulnerabilities/synthesis/"
    LAST_COMPUTATION_PATH: "trade/vulnerabilities/synthesis/last_computation.json"
  FORCE: false

  # Sources : la première est la grille ; les suivantes sont jointes à gauche par des
  # expressions SQL DuckDB évaluées dans le catalogue (aucune logique de jointure en Python).
  SOURCES:
    - SCHEMA: "indicators"                       # famille partenaires (Eurostat)
      ALIAS: "p"
      COLUMNS: ["HHI", "CDI2", "CDI3"]
    - SCHEMA: "network_indicators"               # famille réseau (BACI)
      ALIAS: "n"
      COLUMNS: ["EXPORT_HHI", "CENTRALITY_RISK", "CLUSTERING_W"]
      JOIN:
        ON:
          - 'substr(p."product", 1, 6) = n."product"'
          - 'CAST(substr(p."TIME_PERIOD", 1, 4) AS INTEGER) = n."year"'
        WHERE: 'n."classification" = ''HS2022'''

  # Contextes retenus (prédicat SQL sur la grille p)
  FILTERS:
    WHERE: 'p."flow" = 1 AND p."indicators" = ''VALUE_IN_EUROS'' AND p."freq" = ''A'''
    LAST_N_PERIODS: 5

  MLFLOW:
    TRACKING_URI: null
    EXPERIMENT: "vulnerabilities-synthesis"
    LOG_ARTIFACTS: true

  PARAMETERS:
    context_columns: ["freq", "flow", "indicators", "TIME_PERIOD"]
    reporter_col: "reporter"
    product_col: "product"
    metric_columns: ["HHI", "CDI2", "CDI3", "EXPORT_HHI", "CENTRALITY_RISK", "CLUSTERING_W"]
    polarities: []                       # toutes en polarité positive
    levels: ["by_product", "by_reporter", "global"]
    normalization: "minmax"
    winsorize_quantile: null
    min_group_size: 30
    consensus: ["borda", "copeland"]
    consensus_top_n: 100
    smaa_n_draws: 2000
    smaa_k: 50
    bootstrap_methods: ["critic_sum", "auto_sum"]
    bootstrap_n: 50
    bootstrap_levels: ["by_product", "by_reporter"]
    random_state: 0
    methods:
      - {name: "pareto", kind: "pareto",
         params: {epsilon: 0.1, epsilon_scale: "mad", reduce: {threshold: 0.3}, layers: true}}
      - {name: "pareto_global", kind: "pareto", levels: ["global"],
         params: {epsilon: 0.1, reduce: {threshold: 0.3}, layers: false}}
      - {name: "rank_mean", kind: "rank_mean"}
      - {name: "entropy_sum", kind: "weighted", params: {weighting: "entropy", aggregation: "weighted_sum"}}
      - {name: "critic_sum", kind: "weighted", params: {weighting: "critic", aggregation: "weighted_sum",
                                                        weighting_params: {method: "spearman", scale: "mad"}}}
      - {name: "auto_sum", kind: "weighted", params: {weighting: "auto", aggregation: "weighted_sum"}}
      - {name: "auto_geo", kind: "weighted", params: {weighting: "auto", aggregation: "geometric_mean",
                                                      aggregation_params: {epsilon: 1.0e-3}}}
      - {name: "mpi", kind: "mpi"}
      - {name: "topsis_critic", kind: "topsis", params: {weighting: "critic", robust: true}}
      - {name: "vikor_critic", kind: "vikor", params: {weighting: "critic", v: 0.5}}
      - {name: "bod", kind: "bod", params: {rho: 4.0, restriction: "assurance_region"}}
      - {name: "whitened", kind: "whitened_projection", params: {covariance_estimator: "mcd"}}
      - {name: "kantorovich", kind: "kantorovich",
         metrics: ["HHI", "CDI2", "CDI3", "EXPORT_HHI"],       # sous-ensemble (M-17)
         params: {epsilon: 0.1, n_target: 4096, fit_sample_size: 20000, alpha: 0.05,
                  theta0_degrees: 60.0, conformal: "split", alert: "projected"}}
      - {name: "smaa", kind: "smaa", params: {aggregation: "weighted_sum"}}
      - {name: "cone_quantile", kind: "cone_quantile"}

COHERENCE:
  RESULT_SCHEMA: "synthesis_diagnostics"
  PATHS:
    DATA_PATH: "trade/datasets/vulnerabilities/synthesis_diagnostics/"
    LAST_COMPUTATION_PATH: "trade/vulnerabilities/synthesis/last_coherence.json"
  FORCE: false
  MLFLOW:
    TRACKING_URI: null
    EXPERIMENT: "vulnerabilities-coherence"
    LOG_ARTIFACTS: true
  PARAMETERS:
    topk_depths: [10, 50, 100]
    rbo_p: 0.98
    dispute_fraction: 0.01
    lomo: true
    lomo_methods: ["critic_sum", "auto_sum"]
    metric_pairs: true
```

Remarques : les valeurs de `FILTERS.WHERE` (`indicators`, `freq`) sont à vérifier contre
les modalités réelles de la table (§11). Une méthode absente d'un niveau (`levels`) ou
sous `min_group_size` produit des `NaN` et une ligne de diagnostic `skipped`.

### S-2.3 Lecture des sources (script de synthèse)

Le script construit **une** requête DuckDB à partir de `SOURCES` et `FILTERS` :

```sql
SELECT p."freq", p."flow", p."indicators", p."TIME_PERIOD", p."reporter", p."product",
       p."HHI", p."CDI2", p."CDI3",
       n."EXPORT_HHI", n."CENTRALITY_RISK", n."CLUSTERING_W"
FROM "vulnerabilities"."indicators"."fact_table" AS p
LEFT JOIN "vulnerabilities"."network_indicators"."fact_table" AS n
       ON substr(p."product", 1, 6) = n."product"
      AND CAST(substr(p."TIME_PERIOD", 1, 4) AS INTEGER) = n."year"
      AND n."classification" = 'HS2022'
WHERE p."flow" = 1 AND p."indicators" = 'VALUE_IN_EUROS' AND p."freq" = 'A'
  AND p."TIME_PERIOD" IN (SELECT DISTINCT "TIME_PERIOD" FROM ... ORDER BY 1 DESC LIMIT 5)
```

Le `WHERE` de jointure est placé dans le `ON` (jointure gauche). Les lignes sans
correspondance réseau ont des `NULL` traités par D-15. La construction de la requête est
une fonction pure testable (`build_source_query(sources, filters, catalog_alias)`).

### S-2.4 Table `synthesis.fact_table` (scores)

| Colonne | Type | Rôle |
|---|---|---|
| `freq, flow, indicators, TIME_PERIOD` | selon source | contexte (**PK**) |
| `reporter`, `product` | VARCHAR | cellule (**PK**) |
| `method` | VARCHAR | nom de `MethodSpec` ou pseudo-méthode (**PK**) |
| `score_by_product`, `score_by_reporter`, `score_global` | DOUBLE | score (plus élevé = plus vulnérable), `NULL` si non scoré |
| `rank_by_product`, `rank_by_reporter`, `rank_global` | DOUBLE | rang 1 = plus vulnérable, ex æquo moyennés |
| `n_by_product`, `n_by_reporter`, `n_global` | INTEGER | taille du groupe effectivement scoré |
| `alert_by_product`, `alert_by_reporter`, `alert_global` | BOOLEAN | alerte (Pareto : front ε ; Kantorovitch : seuil ; sinon `NULL`) |
| `rank_low_by_product`, `rank_high_by_product`, … (6 colonnes) | DOUBLE | bornes bootstrap (D-08), `NULL` sinon |

Pseudo-méthodes : `consensus_borda`, `consensus_copeland` (rang consensus dans
`rank_*`, score = `−rang`), `consensus_kemeny` si activé. Pour `smaa`, `score = p_i`
(facteur de confiance à profondeur `k`) et le rang est celui de `p_i`. Pour
`cone_quantile`, `score = F̂_C(x_i)`.

Exemple de lignes (contexte `A / 1 / VALUE_IN_EUROS / 2024`) :

| reporter | product | method | score_by_product | rank_by_product | n_by_product | score_by_reporter | rank_by_reporter | n_by_reporter | score_global | rank_global | n_global | alert_by_reporter |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| FR | 85411000 | critic_sum | 0.71 | 3.0 | 27 | 0.64 | 112.0 | 4 980 | 0.66 | 2 431.0 | 134 460 | NULL |
| FR | 85411000 | pareto | 0.38 | 4.0 | 27 | 0.91 | 15.0 | 4 980 | 0.87 | 640.0 | 134 460 | true |
| FR | 85411000 | kantorovich | NULL | NULL | 27 | 0.58 | 97.0 | 4 980 | 0.55 | 2 210.0 | 134 460 | true |

(`kantorovich` non calculé à `by_product` : `n = 27 < 500`.)

Clé primaire : `(freq, flow, indicators, TIME_PERIOD, reporter, product, method)`.

### S-2.5 Statistiques calculées par le script de cohérence

Toutes par contexte × niveau × groupe (le groupe est `product` pour `by_product`,
`reporter` pour `by_reporter`, `ALL` pour `global`).

**(a) Famille `metrics` — cohérence des métriques entre elles**

| `statistic` | `item_a` | `item_b` | Définition |
|---|---|---|---|
| `spearman` | métrique | métrique | `ρ^S_jk` |
| `kendall_tau_b` | métrique | métrique | `τ_b(x_·j, x_·k)` |
| `kendall_w` | `` | `` | `W` des `d` classements induits par les métriques |
| `kmo` | `` / métrique | `` | KMO global / par variable |
| `bartlett_p` | `` | `` | p-valeur |
| `axis1_share` | `` | `` | `λ₁/d` |
| `mean_abs_rho` | `` | `` | moyenne des `|ρ^S_jk|`, `j ≠ k` |
| `pareto_front_share` | `` | `` | `|F₁|/n` |
| `pareto_front_share_expected` | `` | `` | `(ln n)^{d−1}/((d−1)! n)` |
| `n_rows`, `n_complete` | `` | `` | volumétrie |

**(b) Famille `methods` — cohérence des synthèses**

| `statistic` | `item_a` | `item_b` | Définition |
|---|---|---|---|
| `kendall_tau_b` | méthode | méthode | Q1 |
| `weighted_tau` | méthode | méthode | A-06 |
| `rbo` | méthode | méthode | Q2, `p = rbo_p` |
| `topk_overlap_{k}` | méthode | méthode | Q2, `k ∈ topk_depths` |
| `kendall_w` | `` | `` | Q1, toutes méthodes |
| `violation_strict`, `violation_tie` | méthode | `` | Q3 (M-10) |
| `front_median_rank`, `front_max_rank` | méthode | `` | Q3, contrôle associé |
| `disputed_share` | `` | `` | part des cellules dont l'étendue de rang > `dispute_fraction · n` |
| `rank_interval_width_median` | méthode | `` | Q4, largeur médiane de l'intervalle bootstrap |
| `smaa_confidence_top{k}_share` | `` | `` | part des cellules avec `p_i ≥ 0,5` |
| `lomo_tau`, `lomo_topk_overlap` | méthode | métrique | Q5 |
| `score_metric_tau` | méthode | métrique | Q5, `τ_b(s, x_·j)` |
| `ellipticity_tau_proj`, `ellipticity_tau_rank` | `kantorovich` | `` | M-06 |
| `cluster_id` | méthode | `` | groupe de la CAH sur `1 − τ_b` (nombre de groupes par seuil 0,2) |

**(c) Famille `fit` — émise par le script de synthèse**

`weight` (`item_a` = méthode, `item_b` = métrique), `auto_selected__{scheme}` (valeur 1),
`kmo`, `axis1_share`, `ot_converged`, `ot_ks_p`, `ot_alert_threshold`, `ot_alert_share`,
`ot_direction_cos_q90_q99`, `n_skipped_incomplete`, `skipped_min_group_size` (valeur 1).

### S-2.6 Table `synthesis_diagnostics.fact_table`

| Colonne | Type | Rôle |
|---|---|---|
| `freq, flow, indicators, TIME_PERIOD` | | contexte (**PK**) |
| `level` | VARCHAR | `by_product` / `by_reporter` / `global` (**PK**) |
| `reporter`, `product` | VARCHAR | groupe ; `'ALL'` quand non applicable (**PK**, jamais `NULL`) |
| `family` | VARCHAR | `metrics` / `methods` / `fit` (**PK**) |
| `statistic` | VARCHAR | (**PK**) |
| `item_a`, `item_b` | VARCHAR | méthode / métrique ; `''` si absent (**PK**) |
| `value` | DOUBLE | |
| `n` | INTEGER | taille du groupe |

Volumétrie indicative par contexte : `by_product` 5 000 groupes × ~200 lignes = 10⁶ ;
`by_reporter` 27 × ~200 ; `global` ~200. Acceptable.

### S-2.7 Rapports et MLflow

`SynthesisReport.to_metrics()` : par niveau, `n_groups`, `n_cells_scored`,
`n_methods_skipped`, `median_group_size`, `seconds_{method}` ; global :
`n_contexts`, `created`. Artefacts : `top_{level}_{method}.csv` (top `artifact_top_n`),
`weights_{level}.csv`, `auto_selection_{level}.csv`, `ot_report_{level}.csv`.
`CoherenceRunReport.to_metrics()` : par niveau, médianes de `kendall_w`,
`disputed_share`, `violation_strict` par méthode, `mean_abs_rho`. Artefacts :
`tau_matrix_{level}_{contexte}.csv`, `lomo_{level}.csv`.

---

## 9. Spécification de la documentation LaTeX (S-3)

### S-3.1 Fichiers

- `Methodologie synthese multicritere.tex` (nouveau) ; `bibliography.bib` (nouveau) ;
  compilation `latexmk -pdf -interaction=nonstopmode "Methodologie synthese multicritere.tex"`
  (MiKTeX installé) ; `natbib` avec `\bibliographystyle{plainnat}` et `\citep`/`\citet`.
- Préambule repris de l'existant (couleurs définies **avant** `hyperref`, `hidelinks`
  retiré), environnements `definition`, `propriete`, `remarque`, `hypothese` (nouveau),
  `algorithm`/`algpseudocode` en français, `tikz`.
- L'ancien fichier `Documentation agregation metriques.tex` est conservé jusqu'à
  validation du nouveau, puis supprimé (décision utilisateur).

### S-3.2 Conventions typographiques (obligatoires)

- `\medskip` après chaque titre de section, sous-section et sous-sous-section ;
- `\medskip` avant chaque `itemize`/`enumerate`, `figure`, `table`, équation hors texte
  (`\[`, `align`), `algorithm`, environnement de théorème ;
- `\bigskip` en fin de section et de sous-section, entre paragraphes (`\paragraph`),
  après chaque `itemize`/`enumerate`, `figure`, `table`, équation hors texte,
  `algorithm`, environnement de théorème ;
- pas de `\bigskip` immédiatement suivi d'un titre de section (le titre porte déjà
  son espacement) ; pas de double `\bigskip`.

### S-3.3 Ton et structure

- Descriptif et didactique : présenter l'idée et le problème résolu, puis les hypothèses,
  les définitions, les formules, l'algorithme, les propriétés (monotonie stricte/faible,
  invariances, compensation, complexité), les paramètres et leurs effets, les limites.
  Aucune phrase à l'infinitif injonctif ; les recommandations sont formulées comme des
  conditions d'adéquation (« La méthode est adaptée lorsque… »).
- Indépendant du projet : le corps traite de « vecteurs de `R^d` » et d'« unités » ;
  la synthèse de métriques de vulnérabilité commerciale est le **chapitre
  d'application** (10), qui décrit les données, les niveaux, la configuration, la
  lecture des sorties et les diagnostics attendus.
- Chaque symbole est défini avant son premier usage et figure dans la table des notations
  (Annexe A du présent document, reprise en annexe du `.tex`).
- Chaque méthode a au moins un algorithme (`algorithmic`) dont l'implémentation se déduit
  sans ambiguïté (entrées, sorties, conventions d'ex æquo, valeurs de repli), et une figure
  lorsque l'intuition est géométrique (cône de dominance, front ε, iso-scores, régions
  quantiles, direction `u*`, seuil d'alerte, région d'assurance BoD).

### S-3.4 Plan

1. Introduction : le problème d'ordonner `R^d` ; absence d'ordre canonique ; taxonomie
   des familles (figure) ; plan.
2. Cadre et notations : `X`, polarité, dominance, monotonie stricte et faible,
   compensation, invariances (affine, monotone), ex æquo et rangs, groupes.
3. Prétraitement : orientation, queues (winsorisation et son coût M-08, transformations),
   normalisations (table), diagnostics de corrélation (KMO, Bartlett, `ρ̄`), algorithme.
4. Ordres partiels : front, couches, comptage, profondeur ; espérance du cardinal du
   front ; front ε (Laumanns) ; réduction de `d` ; quantile de cône (A-01) ; algorithmes
   (balayage, blocs, glouton) ; figures.
5. Pondérations endogènes : simpliciales (entropie, CRITIC et variantes, OCDE–JRC
   corrigée), directions (ACP axe 1), méta-sélection (S-1.5), individualisées (BoD
   corrigé : région d'assurance, front, propriétés exactes) ; figure BoD.
6. Fonctions d'agrégation : somme, géométrique, MPI, TOPSIS, VIKOR, rang moyen,
   Mahalanobis (norme blanchie), projection blanchie orientée ; figure iso-scores ;
   tableau normalisation admissible × méthode.
7. Rangs multivariés par transport optimal : cadre center-outward (loi sphérique
   uniforme), rang et signe, carte entropique et extension hors échantillon, score
   orienté (trois variantes), cas elliptique (M-06), seuil d'alerte et calibration
   conforme (M-07), hyperparamètres, dimension, sous-échantillonnage ; figures (a/b,
   régions quantiles, cône `θ₀`).
8. Incertitude sur les poids : SMAA, lien avec le quantile de cône.
9. Comparer et contrôler : Q1–Q5 (avec `weightedtau`), bootstrap (D-08), consensus
   (Borda, Copeland, Kemeny MILP), validation externe.
10. Application : synthèse de métriques de vulnérabilité commerciale (données, six
    métriques, trois niveaux, configuration, lecture des tables, diagnostics, cas
    `d = 6`).
Annexes : A. Notations ; B. Algorithmes récapitulatifs ; C. Correspondance
document ↔ code (`macroforecast.trade.aggregation`) ; D. Bibliographie.

---

## 10. Tests attendus (S-4)

Répertoire `tests/aggregation/`, `pytest`, données synthétiques (`numpy.random.default_rng`),
sans S3 ni DuckLake sauf mention. Aucun test ne doit dépendre de `jax` (skip propre).

| Fichier | Couverture |
|---|---|
| `test_preprocessing.py` | polarité ; winsorisation `None`/`q` ; `RankScaler` (ex æquo, hors échantillon) ; MAD ; KMO/Bartlett sur structure connue |
| `test_pareto.py` | invariants S-1.3 ; égalité balayage/matrice sur aléatoire ; comptage par blocs = comptage matriciel ; front ε ⊆ front, maximum conservé, `ε = 0` ; `MetricReducer` |
| `test_weights.py` | entropie (colonne constante → 0) ; CRITIC (constante → 0, MAD/Spearman) ; OCDE sur exemple M-04 (`w = 0,2 × 5`) ; `auto_weights` règles ; BoD (ligne à zéro non pénalisée en `assurance_region`, `s ≤ 1`, front ⇒ `s = 1` avec `ρ < ∞`) |
| `test_functions.py` | monotonie stricte sur paires dominantes pour somme/géo/TOPSIS/VIKOR/rang moyen ; validation `X ≥ 0` ; MPI cas M-08 ; projection blanchie = OT orienté sur gaussienne (τ > 0,95, si `jax`) |
| `test_optimal_transport.py` (skip sans `jax`) | normes uniformes (KS p > 0,01) sur gaussienne ; `predict` = projection ; alerte : part ≈ `α` sur calibration ; sous-échantillonnage reproductible |
| `test_diagnostics.py` | `V_strict`/`V_tie` ; RBO/τ/W sur cas connus ; bootstrap D-08 (`B` rangs par produit) ; SMAA `(n, k)` ; Copeland présélectionné ; Kemeny petit cas |
| `test_synthesis.py` | `run_synthesis` sur table jouet (2 contextes, 3 pays, 20 produits) : colonnes S-2.4, `NaN` D-15, `min_group_size`, consensus, rangs cohérents avec scores |
| `test_coherence.py` | `run_coherence` : familles/statistiques S-2.5, sentinelles `'ALL'`/`''`, PK unique |
| `test_scripts_synthesis.py` (skip sans `dt_ducklake_manager`) | `build_source_query` ; bout en bout sur catalogue `.ducklake` temporaire (fixture `ducklake_conn`) : écriture puis relecture, upsert idempotent |

---

## 11. Hypothèses et questions ouvertes

Hypothèses retenues en l'absence de réponse (le travail est spécifié avec elles ; les
changer ne modifie que la configuration ou une règle locale) :

1. **Six métriques** : `HHI, CDI2, CDI3` (partenaires) + `EXPORT_HHI, CENTRALITY_RISK,
   CLUSTERING_W` (réseau), jointes par `substr(product, 1, 6) = product` et année,
   millésime `HS2022`. `DIAMETER`, `SPOF`, `SPOF_DECILE` exclues (SPOF est déjà une
   agrégation ; `DIAMETER` est entier et faiblement discriminant). Toutes en polarité
   `+1`. **Question** : est-ce le bon jeu ? faut-il inclure `DIAMETER` ?
2. **Niveaux** : interprétation D-09. **Question** : confirmer que « pour une nomenclature
   indépendamment pour chaque pays » signifie « ordonner les pays d'un même produit ».
3. **Contextes** : flux import, indicateur valeur, fréquence annuelle, cinq dernières
   périodes. **Question** : modalités exactes de `indicators` et `freq` dans
   `DS-045409` ; faut-il aussi les flux export (`CDI2`/`CDI3` y sont nulles) ?
4. **Fraîcheur v1** : recalcul de tous les contextes sélectionnés dès qu'un registre amont
   est plus récent. **Question** : acceptable en attendant un registre par contexte ?
5. **Méthodes par défaut** : la liste S-2.2. **Question** : conserver `mahalanobis`
   (distance) en plus de `whitened_projection` ? Kemeny activé (coût) ?
6. **Ancien document** : conservé jusqu'à validation du nouveau. **Question** : le
   supprimer ensuite, ou le garder comme archive dans `docs/archive/` ?
7. **Nom de fichier** de la nouvelle note : `Methodologie synthese multicritere.tex`.

---

## Annexe A — notations partagées

| Symbole | Définition |
|---|---|
| `n`, `d` | nombre d'unités (lignes) et de métriques (colonnes) d'un groupe |
| `[n] = {1, …, n}` | indices des unités |
| `X = (x_ij) ∈ R^{n×d}` | matrice brute ; `x_i ∈ R^d` la ligne `i` ; `x_{·j}` la colonne `j` |
| `p ∈ {−1, +1}^d` | polarités ; `X̊ = X diag(p)` la matrice orientée (haut = vulnérable) |
| `X̃` | matrice orientée puis normalisée (schéma `ν`) |
| `Z` | matrice orientée puis centrée-réduite |
| `x_i ≻_P x_k` | dominance de Pareto stricte sur `X̊` |
| `F_1`, `F_ℓ`, `F_ε` | front, couche `ℓ`, front ε (sous-ensemble de `F_1`) |
| `δ(i)`, `δ̄(i)` | comptage de dominance, profondeur normalisée `δ/(n−1)` |
| `Δ^{d−1}` | simplexe des poids |
| `w`, `w^(o)` | poids partagés ; poids individuels de l'unité `o` (BoD) |
| `s : R^d → R` | fonction de score (plus élevé = plus vulnérable) |
| `r_i` | rang de `i`, `1` = plus vulnérable, ex æquo moyennés |
| `ρ^S_jk`, `r_jk` | corrélation de Spearman ; de Pearson |
| `R`, `Σ`, `μ` | matrice de corrélation ; covariance (robuste) ; centre (robuste) |
| `λ_m`, `a_m`, `ℓ_jm`, `ℓ'_jm`, `V_m` | valeurs/vecteurs propres ; saturations ; saturations après rotation ; variance d'un facteur après rotation |
| `x⁺`, `x⁻` | pôles idéal et anti-idéal (quantiles marginaux `q⁺`, `q⁻`) |
| `U_d`, `B_d`, `S^{d−1}` | loi sphérique uniforme ; boule unité ; sphère unité |
| `T`, `T̄_ε` | carte center-outward ; son estimateur entropique (hors échantillon) |
| `R(z) = ‖T(z)‖`, `S(z) = T(z)/‖T(z)‖` | rang et signe center-outward |
| `u*`, `θ₀`, `α`, `r̂_α`, `q̂_{1−α}` | direction de vulnérabilité ; demi-angle du cône ; niveau ; seuil radial ; seuil projeté |
| `ε`, `m` | régularisation entropique ; taille de la grille de référence |
| `τ_b`, `τ_w`, `W`, `RBO_p`, `J_k` | Kendall ; Kendall pondéré ; concordance ; rank-biased overlap ; recouvrement top-`k` |
| `V_strict(s)`, `V_tie(s)` | taux d'inversions strictes ; taux d'ex æquo sur paires dominantes |
| `B`, `T` | tirages bootstrap ; tirages de poids (SMAA, cône) |
| `ρ` (BoD) | borne des rapports de poids de la région d'assurance |
| `κ`, `δ` (BoD) | paramètre de parts ; plancher additif (option) |

## Annexe B — bibliographie à constituer

Clés `bibkey` à créer dans `bibliography.bib` (auteur, titre, support, année). Les
entrées marquées ★ sont requises ; les autres sont citées dans les mentions
*doc seulement*.

Transport optimal et rangs multivariés : ★`chernozhukov2017` (Chernozhukov, Galichon,
Hallin, Henry, *Monge–Kantorovich depth, quantiles, ranks and signs*, Ann. Statist. 2017),
★`hallin2021` (Hallin, del Barrio, Cuesta-Albertos, Matrán, Ann. Statist. 2021),
★`ghosal2022` (Ghosal, Sen, *Multivariate ranks and quantiles using optimal transport*,
Ann. Statist. 2022), ★`cuturi2013`, ★`pooladian2021`, ★`peyre2019`, ★`cuturi2022`
(OTT), `klein2025`, `thurin2025`.
Prédiction conforme : ★`vovk2005` (*Algorithmic Learning in a Random World*),
★`lei2018` (JASA), ★`angelopoulos2023` (FnT ML).
Indicateurs composites : ★`nardo2008`, ★`nicoletti2000` (OECD ECO/WKP 226),
★`saisana2005`, ★`arjona2023`, `korniyenko2017`, `bonneau2020`, `jaravel2021`.
Pondérations : ★`diakoulaki1995`, ★`zou2006` (entropie), `shannon1948`,
★`cherchye2007`, ★`charnes1978` (DEA), ★`kaiser1958` (varimax), ★`kaiser1974` (KMO),
★`bartlett1951`, `keshavarz2021` (MEREC).
Agrégations : ★`hwang1981` (TOPSIS), ★`opricovic2004`, `opricovic2007`, ★`mazziotta2016`,
★`mahalanobis1936`, ★`rousseeuw1999`, ★`ledoit2004`, `brans1985` (PROMETHEE).
Ordres partiels : ★`deb2002`, ★`laumanns2002`, ★`kung1975`, ★`bentley1978`,
★`hamel2018`, `kong2012`, `fattore2016`, `bruggemann2011`, `deloof2006`, `tukey1975`,
`zuo2000`, `mosler2013`, `goldberg1989`, `fonseca1993`.
Comparaison et consensus : ★`kendall1948`, ★`webber2010`, ★`vigna2015`, ★`lahdelma1998`,
★`lahdelma2001`, ★`borda1781`, ★`copeland1951`, ★`kemeny1959`, `dwork2001`,
★`goldstein1996`, ★`xie2009`, ★`efron1994`.
