# Extension de la parcimonie aux autres étapes : gains, mise en œuvre, décision

> **Date** : 2026-10-14 · **Statut** : pour décision, non planifié.
>
> **Contexte** : la première itération (`AUDIT_WATERMARKS_AVAL.md`, étapes 0 à 3) couvre le socle
> (curseurs, raison `pending`, verrou méthodologique, expiration bornée), la synthèse, les
> partenaires et BACI. Ce document décrit, pour chaque étape ou mécanisme **hors** de ce périmètre :
> - ce qu'on y gagnerait ;
> - ce qu'il en coûterait ;
> - comment le mettre en œuvre avec les briques de la première itération ;
> - comment mesurer le gain **avant** de décider.
>
> Le tableau ci-dessous résume les recommandations. Le détail vient ensuite.

| # | Extension | Nature du gain | Gain attendu | Coût | Recommandation |
|---|---|---|---|---|---|
| E1 | Propagation des suppressions (partenaires, réseau, BACI) | **Justesse** (FN-3) | Lignes orphelines supprimées | moyen | **À faire** (prochaine itération) |
| E2 | Publication `serving` incrémentale par année | Coût quotidien | Élevé si `cell_scores` / `flows` sont gros | moyen | **À faire après mesure** |
| E3 | Écriture BACI par différence, `fit_id` hors des lignes | Reprise idempotente, flux de changements exploitable | Faible à moyen | moyen | Avec E1 (même helper) |
| E4 | Référentiels via `write_dataframe_with_report` | Déclencheur de E2 | Faible seul | faible | Avec E2 |
| E5 | Suppression des contextes sortis de la grille de synthèse | Justesse | Faible | faible | Avec E1 |
| E6 | Cohérence pilotée par `changes_since` sur `synthesis` | Coût hebdomadaire | Faible (après l'étape 1) | moyen | **Ne pas faire** pour l'instant |
| E7 | Réseau piloté par `changes_since` sur `baci_hs<V>` par année | Coût hebdomadaire | Très faible | moyen | **Ne pas faire** |
| E8 | Sortie anticipée des nœuds aval quand `n_modified == 0` | Démarrage de pods | Faible | faible | Optionnel |
| E9 | Suppressions côté fournisseur (statflows) | Justesse en amont | Dépend des sources | statflows | À instruire dans statflows |

---

## E1 — Propagation des suppressions (partenaires, réseau, BACI)

**Problème** (FN-3 de l'audit). Les écritures des partenaires et du réseau sont des upserts purs
(`macroforecast/trade/vulnerabilities/runner.py:131`). Une cellule qui disparaît d'un recalcul reste
en table indéfiniment. Exemples :
- partenaire retiré d'une nomenclature ;
- produit absent d'un millésime après réestimation BACI ;
- filtre resserré ;
- code devenu non convertible.

Ces lignes orphelines remontent ensuite dans la synthèse et dans `serving`.

**Gain.**
- Justesse des résultats : c'est un faux négatif, pas une question de coût.
- Sans E1, une suppression vue par `changes_since` en amont ne peut pas se répercuter : l'aval
  recalcule mais n'efface rien.

**Mise en œuvre.**
- Nouveau helper `DuckLakeTable.replace_slice(df, slice_where, primary_keys)` dans
  `kedro_pipeline/io/ducklake.py`. Il fait tout dans une seule transaction :
  1. enregistrement de `df` comme vue temporaire ;
  2. `DELETE FROM t WHERE <slice_where> AND (pk) NOT IN (SELECT pk FROM df)` : seules les clés
     absentes du nouveau résultat sont supprimées ;
  3. upsert de `df` (`DatabaseUpdater`, inchangé : les lignes identiques ne sont pas réécrites) ;
  4. message de commit, comme aujourd'hui.
- La tranche (`slice_where`) correspond exactement au périmètre recalculé :
  - **partenaires** : `classification`, couples `(reporter, product)` planifiés, sens recalculés
    (`flow`) ;
  - **réseau** : `classification = V`, sens recalculés ;
  - **BACI** : `year = y` (remplace le `delete_where` actuel, voir E3).
- Le helper ne supprime **jamais** hors de la tranche. Une tranche mal définie est le principal
  risque : une suppression trop large effacerait des résultats valides. Chaque appel est donc
  couvert par un test « une tranche voisine reste intacte ».

**Coût.** Environ 1 jour : helper, trois appels, tests. Pas de migration de données. Le premier run
après déploiement nettoie les orphelins des seules tranches recalculées. Pour un nettoyage complet,
un forçage par étape (`FORCE_STEPS=partners,network`) suffit.

**Mesure avant décision.** Une requête d'audit, en lecture seule, compte les lignes de `indicators`
et de `network_indicators` absentes d'un recalcul complet sur un catalogue de test. Si le compte est
nul, le risque est théorique mais reste ouvert.

**Recommandation.** À faire à la prochaine itération : c'est le seul faux négatif connu qui reste
ouvert après les étapes 0 à 3.

---

## E2 — Publication `serving` incrémentale par année

**Problème** (FP-7). `publish_serving` recrée toutes les tables (`MODE: full`, `CREATE OR REPLACE`,
`kedro_pipeline/io/serving.py:293`) deux fois par cycle (quotidien et hebdomadaire), même sans aucun
changement. Le mode `by_year` existe déjà (`DELETE` + `INSERT` des années demandées,
`io/serving.py:283-290`), mais il n'est piloté que par un paramètre manuel.

**Gain.**
- Les jours où rien n'a changé, la publication devient un no-op : aucune transaction et aucun
  nouveau snapshot `serving`, donc moins de maintenance aussi.
- Les autres jours, seules les années touchées sont republiées. Comme l'incrémental Comext ne
  ramène que 10 périodes, une révision touche rarement plus de 2 ou 3 années.
- Ordre de grandeur : le gain est proportionnel à `durée_full × (1 − part des années touchées)`, à
  mesurer (voir plus bas).

**Mise en œuvre.**
1. Curseurs `serving` (module `cursors.py` de l'étape 0) sur chaque table source :
   `eurostat.<comext>` (pour `flows`), `vulnerabilities.indicators`,
   `vulnerabilities.network_indicators`, `vulnerabilities.synthesis`,
   `vulnerabilities.synthesis_diagnostics`, plus les schémas de référence (E4).
2. Clés demandées à `changes_since` : la colonne d'année de chaque source (`TIME_PERIOD`, `year`).
   La conversion en année réutilise `period_year` (`io/registry_views.py`).
3. Décision :
   - aucune clé touchée : **pas de publication**, et les curseurs avancent ;
   - années touchées : `by_year` sur ces années ;
   - repli `full` si l'un des cas suivants se présente :
     - un référentiel a changé (les libellés traversent toutes les années) ;
     - l'empreinte des gabarits SQL et des paramètres `serving` a changé ;
     - une table n'est pas déclarée locale à l'année ;
     - un curseur est en repli (snapshots expirés, premier run).
4. Déclaration explicite `YEAR_LOCAL: true` par table dans `parameters_serving.yml`. Elle n'est
   posée qu'après vérification que la requête de la table ne lit, pour une année, que des lignes de
   cette année. Exemples :
   - `NORM_PARTITION` contient bien `year` ;
   - aucun `LAG` ni fenêtre multi-années.

   Une table sans cette déclaration est toujours republiée en entier.
5. La reprise en deux phases s'applique telle quelle. Les années touchées non publiées (échec) sont
   persistées dans `_cursors.json` (champ `pending_years`) avant l'avancement des curseurs.
6. Le mutex Argo `trade-serving` existant suffit à sérialiser les publications.

**Coût.** 1,5 à 2 jours : curseurs multi-sources, décision, `YEAR_LOCAL`, tests d'égalité
`by_year` contre `full`.

**Risques.**
- **Faux négatif si `YEAR_LOCAL` est déclaré à tort.** Parade : un test d'égalité, en CI, entre une
  publication `full` et une publication `by_year` après la révision d'une seule année, sur données
  fictives, **pour chaque table déclarée**.
- **Tables sources dans plusieurs catalogues.** C'est un curseur par `(catalogue, schéma)`, ce que
  le format prévoit déjà.

**Mesure avant décision.**
- Relever dans MLflow les métriques `serving/<table>/rows` et la durée du run `publish_serving` sur
  les derniers runs.
- Relever, sur une semaine, la décision qu'aurait prise la nouvelle règle (aucune publication / années /
  `full`), par une journalisation ponctuelle ajoutée pour la mesure : les modes d'observation `legacy` /
  `shadow` des étapes 0 à 3 n'existent plus, la production repartant de bases vierges.
- Le gain est significatif si la majorité des runs quotidiens auraient été des no-op ou ne
  touchent que 1 à 3 années.

**Recommandation.** À faire si la mesure confirme qu'une publication `full` dépasse quelques
minutes. C'est le principal gain de coût restant après les étapes 0 à 3.

---

## E3 — Écriture BACI par différence, `fit_id` hors des lignes

**Problème** (FP-3).
- Chaque année est réécrite par suppression puis upsert (`kedro_pipeline/steps/baci.py:1112-1119`),
  avec une colonne constante `fit_id` (`2167`).
- Le flux de changements DuckLake voit donc **toutes** les lignes comme supprimées et réinsérées,
  même quand les valeurs sont identiques : reprise d'une passe interrompue, ou réestimation dont le
  résultat ne bouge pas.

**Gain.**
- Une reprise idempotente ne crée plus de changement : moins de fichiers de suppression, moins de
  maintenance.
- `changes_since` devient exploitable sur `baci_hs<V>`, ce qui est un prérequis de E7, peu utile en
  l'état.
- En pratique, une réestimation sur données nouvelles modifie presque toutes les valeurs, car les
  paramètres de gravité sont globaux au millésime. **Le gain de calcul est donc faible.** C'est
  surtout un gain d'hygiène du stockage.

**Mise en œuvre.**
- `DuckLakeYearWriter` appelle `replace_slice(df_année, "year = y", primary_keys)` (helper E1) au
  lieu de `upsert_many(..., delete_where=...)`.
- `fit_id` sort des lignes : il est déjà dans le registre (`extra.fit_id`) et peut aller dans
  `commit_info` du snapshot.
- Le contrôle de « table mixte entre deux ajustements » repose alors sur `years_written` du
  registre, qui existe déjà.
- Point d'attention : le non-déterminisme flottant (sommes parallèles) peut produire des écarts au
  dernier bit, donc des « changements » parasites. Un arrondi documenté, en paramètre (par exemple
  12 chiffres significatifs), est à évaluer.

**Coût.** 1 jour. Aucune migration : la production repart de bases vierges, `fit_id` est simplement absente de
la table dès sa création.

**Recommandation.** À faire avec E1, puisque c'est le même helper, uniquement si E1 est retenu.

---

## E4 — Référentiels via `write_dataframe_with_report`

**Problème.** `publish_reference` réécrit les référentiels à chaque téléchargement (upsert
idempotent, donc sans changement de ligne s'ils sont identiques). Aucune étape ne sait si un libellé
a changé.

**Gain.** Pris isolément, nul. C'est le **déclencheur** du repli `full` de E2 en cas de changement
de libellé.

**Mise en œuvre.**
- `kedro_pipeline/steps/reference.py:440-460` passe à `write_dataframe_with_report`.
- `rows_changed` est journalisé en métrique `reference/<table>/rows_changed`.
- Le curseur `serving` sur chaque schéma de référence (E2) suffit à détecter le changement : aucun
  registre supplémentaire n'est nécessaire.

**Coût.** Moins d'une demi-journée.

**Recommandation.** Avec E2.

---

## E5 — Suppression des contextes sortis de la grille de synthèse

**Problème.**
- Un contexte qui disparaît de la grille garde ses lignes dans `synthesis` et
  `synthesis_diagnostics`, ainsi que son entrée de registre. Causes possibles :
  - filtre modifié ;
  - période retirée ;
  - millésime désactivé.
- Après l'étape 1, un contexte dont toutes les lignes sources ont été supprimées apparaît dans les
  clés touchées, mais ne figure plus dans la grille.

**Gain.** Justesse de `serving` et du tableau de bord.

**Mise en œuvre.** En fin de run de synthèse et de cohérence :
- contextes du registre absents de la grille → suppression de leurs lignes (`delete_where` par
  contexte, helper existant `replacement_predicate`) ;
- suppression de leur entrée de registre ;
- métrique `synthesis/contexts_removed`.

Garde-fou : on ne supprime rien si la grille est vide (lecture en échec), ni si plus de
`MAX_REMOVED_SHARE` (par exemple 20 %) des contextes disparaissent d'un coup, auquel cas on lève une
alerte.

**Coût.** Une demi-journée.

**Recommandation.** Avec E1.

---

## E6 — Cohérence pilotée par `changes_since` sur `synthesis`

**Idée.** Remplacer la cascade par registre (contexte recalculé en synthèse → cohérence recalculée)
par une lecture des lignes réellement modifiées de `synthesis`. Un contexte resynthétisé à
l'identique ne relancerait plus la cohérence.

**Gain.**
- Faible après l'étape 1 : la synthèse ne recalcule plus que les contextes dont les entrées ont
  réellement changé, et un changement d'entrée modifie presque toujours les scores.
- Le seul cas gagnant est un recalcul de méthode (`--bump`) qui ne change pas les scores : rare par
  construction.

**Coût.**
- Il faut d'abord rendre l'écriture de la synthèse « par différence » : FP-6, aujourd'hui
  suppression puis upsert par méthode, ce qui réécrit les lignes identiques.
- Il faut aussi un curseur et la gestion de `pending` pour la cohérence.
- Environ 1,5 jour.

**Recommandation.** **Ne pas faire** pour l'instant. La cascade actuelle est exacte (copies de
`last_computed` par contexte, sans comparaison d'horloges). À réévaluer seulement si les métriques
`freshness/units_new_data` de la cohérence montrent de nombreux recalculs sans effet.

---

## E7 — Réseau piloté par `changes_since` sur `baci_hs<V>`, par année

**Idée.** Ne recalculer que les années modifiées d'un millésime.

**Gain.** Très faible. Une passe BACI réestime le millésime entier avec des paramètres globaux :
presque toutes les années changent à chaque passe. La cascade actuelle (millésime entier, sur
passe terminée) est la bonne granularité.

**Prérequis.** E3, faute de quoi toutes les lignes apparaissent modifiées.

**Recommandation.** **Ne pas faire.**

---

## E8 — Sortie anticipée des nœuds aval quand `n_modified == 0`

**Idée.**
- Exposer `DownloadReport.n_modified` en sortie des nœuds `fetch_*`, comme jeu de données Kedro.
- Les nœuds aval quotidiens (partenaires, `publish_serving`) sortent immédiatement quand toutes les
  sources lues indiquent 0.

**Gain.**
- Faible. Après les étapes 2 et E2, ces nœuds décident déjà en quelques secondes qu'il n'y a rien à
  faire, à partir du registre ou des curseurs.
- Le gain réel se limite au démarrage des pods Argo, à condition de conditionner la tâche dans le
  manifeste (`when:`) plutôt que dans le nœud.

**Risque.** Le rapport ne couvre que le run courant. Un run aval précédent en échec doit tout de
même être rejoué. La condition doit donc être « `n_modified == 0` **et** aucune unité `pending` »,
ce qui oblige à lire le registre de toute façon. L'intérêt devient marginal.

**Recommandation.** Optionnel, à envisager seulement si le coût de démarrage des pods devient
sensible sur Onyxia.

---

## E9 — Suppressions côté fournisseur (statflows)

**Problème.** Une observation retirée par Eurostat ou Comtrade (confidentialité, correction) n'est
pas supprimée par l'upsert de statflows. Elle reste dans la table de faits, et donc dans tout l'aval.

**Piste.** Dans statflows, pour une requête qui renvoie la **totalité** de son périmètre
(téléchargement complet, pas l'incrémental `N_LAST_OBSERVATIONS`), supprimer les clés du périmètre
de la requête absentes de la réponse, avec le même mécanisme que `replace_slice`. Les suppressions
seraient alors visibles par `changes_since` (déjà comptées) et propagées par E1.

**Recommandation.** À instruire dans statflows : vérifier d'abord, sur un échantillon, si les
fournisseurs retirent effectivement des observations. Hors de ce dépôt.

---

## Ordre suggéré si tout est retenu

1. **E1 + E3 + E5**, justesse : un même helper `replace_slice` et une même itération.
2. **E4 + E2**, coût de `serving`, après une semaine de mesure.
3. E6, E7 et E8 seulement sur constat chiffré.
