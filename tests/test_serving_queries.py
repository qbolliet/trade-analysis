"""Tests des requêtes de service des paramètres ``serving`` (tables et dérivation des millésimes).

Les requêtes RÉELLES de la configuration sont exécutées par ``publish_serving`` sur des
catalogues DuckLake fichiers alimentés de données fictives (``build_serving_world``).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from kedro_pipeline.steps.serving import (
    build_context,
    flow_label_sql,
    individual_partner_sql,
    product_level_label_sql,
    publish_serving,
    rank_uncertainty_sql,
    render_sql,
    source_tables,
    synthesis_pivot_sql,
    total_partner_sql,
)

from conftest import (
    SERVING_NETWORK_EXPORT_SHIFT,
    SERVING_NETWORK_HHI,
    SERVING_PRODUCTS,
    SERVING_QUANTITY_FACTOR,
    SERVING_REPORTERS,
    SERVING_YEARS,
    SERVING_ZERO_QUANTITY_CELL,
    build_serving_world,
    serving_configs,
)


class RecordingTracker:
    """Tracker minimal qui mémorise les métriques reçues."""

    def __init__(self) -> None:
        self.metrics: dict = {}

    def log_metrics(self, metrics, step=None) -> None:
        self.metrics.update(metrics)


def _read(world, table: str) -> pd.DataFrame:
    """Relit une table de service publiée."""
    with world.catalog.connect() as conn:
        return conn.execute(f"SELECT * FROM {world.catalog.qualified_name(table)}").df()


@pytest.fixture
def published(serving_world):
    """Catalogue de service publié une fois (mode full), avec son résultat."""
    tracker = RecordingTracker()
    result = publish_serving(
        serving_world.tables,
        serving_world.catalog,
        params=serving_world.params,
        runtime=serving_world.runtime,
        tracker=tracker,
    )
    assert result["failures"] == {}, result["failures"]
    return serving_world, result, tracker


def test_all_tables_published_with_metrics(published) -> None:
    world, result, tracker = published
    assert result["tables"] == list(world.params["TABLES"])
    assert result["missing_sources"] == []
    for table in result["tables"]:
        assert tracker.metrics[f"serving/{table}/rows"] == result["rows"][table]
        assert f"serving/{table}/seconds" in tracker.metrics
        assert f"serving/{table}/files" in tracker.metrics
    assert tracker.metrics["serving/mode_full"] == 1.0
    assert tracker.metrics["serving/n_failures"] == 0.0
    # Métriques de qualité (serving.METRICS) : deux méthodes d'incertitude par cellule
    # classée (auto_sum, critic_sum), échanges connus pour toutes les lignes en vigueur
    for name in world.params["METRICS"]:
        assert f"serving/{name}" in tracker.metrics
    assert tracker.metrics["serving/cell_scores/uncertainty_coverage_by_reporter"] == 1.0
    assert tracker.metrics["serving/cell_scores/trade_value_coverage"] == 1.0


def test_cell_scores_columns_and_one_row_per_cell(published) -> None:
    world, _, _ = published
    df = _read(world, "cell_scores")
    params = world.params
    expected = {
        "classification", "hs_vintage", "in_force", "year", "flow", "flow_label", "reporter",
        "reporter_label", "reporter_display", "product", "product_label", "product_display",
        "product_level", "product_level_label", "trade_value_eur", "trade_quantity_100kg",
        "HHI", "CDI2", "CDI3", "HHI_ALERT", "WORLD_HHI", "CENTRALITY_RISK",
        "CLUSTERING_W", "SPOF", "SPOF_ALERT",
    }
    for method in params["METHODS"]:
        for level in params["LEVELS"]:
            expected |= {f"{method}_score_{level}", f"{method}_rank_{level}"}
    for level in params["LEVELS"]:
        expected |= {f"primary_score_{level}", f"primary_rank_{level}", f"primary_n_{level}"}
        expected |= {f"rank_{kind}_{level}" for kind in ("min", "max", "q25", "q75", "spread")}
        expected.add(f"n_methods_{level}")
    expected |= {f"{metric}_norm" for metric in params["NORM_METRICS"]}
    assert expected <= set(df.columns)
    # Méthode non retenue : absente des colonnes
    assert not any(column.startswith("pareto_") for column in df.columns)

    keys = ["classification", "reporter", "product", "flow", "year"]
    assert not df.duplicated(keys).any()
    # Grille filtrée sur VALUE_IN_EUROS / A : 2 années × 3 pays × 4 produits × 2 flux
    assert len(df) == len(SERVING_YEARS) * len(SERVING_REPORTERS) * len(SERVING_PRODUCTS) * 2
    assert set(df["indicators"]) == {"VALUE_IN_EUROS"}


def test_cell_scores_nomenclature_columns(published) -> None:
    world, _, _ = published
    df = _read(world, "cell_scores").set_index(["year", "reporter", "product", "flow"])
    assert df["in_force"].all()
    assert set(df.loc[2019, "hs_vintage"]) == {"HS2017"}
    assert set(df.loc[2023, "hs_vintage"]) == {"HS2022"}
    # Code stocké en entier : zéro initial restitué, NC8 → CN<année>, SH6 → millésime
    assert df.loc[(2019, "FR", "01012100", "1"), "classification"] == "CN2019"
    assert df.loc[(2019, "FR", "854110", "1"), "classification"] == "HS2017"
    assert df.loc[(2019, "FR", "01012100", "1"), "product_label"] == "Horses"
    assert df.loc[(2019, "FR", "01012100", "1"), "product_level"] == 8
    assert df.loc[(2019, "FR", "854110", "1"), "reporter_label"] == "France"


def test_cell_scores_network_join_on_vintage_in_force(published) -> None:
    world, _, _ = published
    df = _read(world, "cell_scores")
    # Chaque année est jointe au réseau de SON millésime, jamais à l'autre (C-11)
    # Chaque flux est joint au réseau de SON sens (export : graphe transposé)
    for year, vintage in ((2019, "HS2017"), (2023, "HS2022")):
        for flow, shift in (("1", 0.0), ("2", SERVING_NETWORK_EXPORT_SHIFT)):
            rows = df[(df["year"] == year) & (df["flow"] == flow)]
            assert set(rows["WORLD_HHI"].dropna().round(6)) == {round(SERVING_NETWORK_HHI[vintage] + shift, 6)}
    # Tous les codes SH6 / NC8 de la grille ont leur SH6 dans le réseau (010121 compris)
    assert df["WORLD_HHI"].notna().all()
    # Alerte du flux export (concentration de la demande au-dessus du seuil), pas de l'import
    assert df.loc[df["flow"] == "2", "WORLD_HHI_ALERT"].all()
    assert not df.loc[df["flow"] == "1", "WORLD_HHI_ALERT"].any()


def test_cell_scores_ranks_consistent(published) -> None:
    world, _, _ = published
    df = _read(world, "cell_scores")
    params = world.params
    # Groupes de la synthèse : pays × niveau de nomenclature (produits classés par finesse)
    context = ["flow", "year", "product_level"]
    for method in params["METHODS"]:
        # Rang 1 = score maximal au sein du groupe (pays) : plus vulnérable
        column_score = f"{method}_score_by_reporter"
        column_rank = f"{method}_rank_by_reporter"
        for _, group in df.groupby(context + ["reporter"]):
            top = group.loc[group[column_rank] == group[column_rank].min()]
            assert group[column_rank].min() == 1.0
            assert top[column_score].iloc[0] == group[column_score].max()
    primary = params["PRIMARY_METHOD"]
    pd.testing.assert_series_equal(
        df["primary_rank_by_product"], df[f"{primary}_rank_by_product"], check_names=False
    )


def test_cell_scores_norm_in_unit_interval(published) -> None:
    world, _, _ = published
    df = _read(world, "cell_scores")
    for metric in world.params["NORM_METRICS"]:
        values = df[f"{metric}_norm"].dropna()
        assert ((values >= 0) & (values <= 1)).all()
    grouped = df.groupby(["flow", "year"])["HHI_norm"]
    assert (grouped.min() == 0).all() and (grouped.max() == 1).all()


def test_flows_bounded_to_top_partners(published) -> None:
    world, _, _ = published
    df = _read(world, "flows")
    top = world.params["TOP_PARTNERS"]
    cell = ["year", "reporter", "product", "flow"]
    individual = df[~df["is_aggregate"]]
    assert individual.groupby(cell).size().max() == top
    assert individual["rank"].between(1, top).all()
    aggregates = df[df["is_aggregate"]]
    assert set(aggregates["partner"]) == {"WORLD", "EXT_EU", "EXT_EU27_2020"}
    assert aggregates["rank"].isna().all()
    # Union sans agrégat WORLD : parts rapportées à EXT_EU27_2020 (PARTNERS.TOTAL)
    union = df[df["reporter"] == "EU27_2020"]
    assert "WORLD" not in set(union["partner"])
    assert (union.loc[union["partner"] == "EXT_EU27_2020", "share"].round(9) == 1.0).all()
    # INT_EU27_2020 (agrégat à « _ ») jamais classé comme partenaire individuel
    assert "INT_EU27_2020" not in set(df["partner"])
    world_share = df.loc[df["partner"] == "WORLD", "share"]
    assert (world_share.round(9) == 1.0).all()
    # Rang 1 = premier partenaire (valeur maximale de la cellule)
    first = individual[individual["rank"] == 1].set_index(cell)["value_eur"].sort_index()
    assert (first == individual.groupby(cell)["value_eur"].max().sort_index()).all()
    # Quantité exposée (facteur fictif), NULL pour la cellule où Comext publie 0
    reporter, product, flow, year = SERVING_ZERO_QUANTITY_CELL
    zero = (
        (df["reporter"] == reporter) & (df["product"] == str(product)) & (df["flow"] == str(flow))
        & (df["year"] == int(year))
    )
    assert zero.any() and df.loc[zero, "quantity_100kg"].isna().all()
    ratio = df.loc[~zero, "quantity_100kg"] / df.loc[~zero, "value_eur"]
    assert ratio.round(9).eq(SERVING_QUANTITY_FACTOR).all()
    assert set(df["partner_label"].dropna()) >= {"World", "Country US"}


def test_coherence_tables(published) -> None:
    world, _, _ = published
    metrics = _read(world, "coherence_metrics")
    methods = _read(world, "coherence_methods")
    assert {"metric_a", "metric_b", "statistic", "level", "value", "n"} <= set(metrics.columns)
    assert {"method_a", "method_b"} <= set(methods.columns)
    # Groupe « ALL » → NULL ; item vide → NULL ; famille fit exclue
    row = metrics[metrics["statistic"] == "kendall_w"].iloc[0]
    assert pd.isna(row["reporter"]) and row["product"] == "85411000"
    assert pd.isna(row["metric_a"])
    assert "weight" not in set(methods["statistic"])
    assert set(methods["statistic"]) == {"kendall_tau_b", "lomo_tau"}
    assert set(metrics["hs_vintage"]) == {"HS2017", "HS2022"}
    # Un diagnostic par niveau de nomenclature, distingués par product_level
    spearman = metrics[(metrics["statistic"] == "spearman") & (metrics["year"] == 2019)]
    assert sorted(spearman["product_level"]) == [6, 8]


def test_reference_copies(published) -> None:
    world, _, _ = published
    products = _read(world, "products")
    assert set(products["source"]) == {"eurostat", "comtrade"}
    countries = _read(world, "countries").set_index(["source", "code"])
    assert bool(countries.loc[("eurostat", "FR"), "is_reporter"])
    assert bool(countries.loc[("eurostat", "US"), "is_partner"])
    assert bool(countries.loc[("eurostat", "EXT_EU"), "is_aggregate"])
    assert countries.loc[("comtrade", "251"), "iso3"] == "FRA"
    vintages = _read(world, "hs_vintages")
    assert len(vintages) == len(world.runtime["NOMENCLATURES"]["HS"])
    assert set(_read(world, "hs_concordance")["relationship"]) == {"1:1", "n:1"}


def test_missing_optional_sources_are_stubbed(tmp_path: Path) -> None:
    world = build_serving_world(tmp_path, with_reference=False, omit=("network",))
    result = publish_serving(world.tables, world.catalog, params=world.params, runtime=world.runtime)
    assert result["failures"] == {}
    assert "ref_eurostat_products" in result["missing_sources"]
    assert "network" in result["missing_sources"]
    df = _read(world, "cell_scores")
    assert df["product_label"].isna().all()
    assert df["WORLD_HHI"].isna().all()
    assert result["rows"]["products"] == 0
    assert result["metrics"]["serving/missing_sources"] == len(result["missing_sources"])


def test_missing_required_source_fails_without_writing(tmp_path: Path) -> None:
    world = build_serving_world(tmp_path, omit=("comext",))
    result = publish_serving(world.tables, world.catalog, params=world.params, runtime=world.runtime)
    assert result["tables"] == []
    # cell_scores, première table publiée, lit Comext (échanges totaux) : elle échoue la première
    assert "cell_scores" in result["failures"]
    with world.catalog.connect() as conn:
        # ROLLBACK complet : aucune table créée, pas même celles écrites avant flows
        assert not any(
            key.startswith(f"serving.{world.params['SCHEMA']}.")
            for key in world.catalog.existing_tables(conn)
        )


@pytest.mark.parametrize("profile", ["base", "demo"])
def test_source_tables_follow_profile_schemas(profile: str) -> None:
    configs = serving_configs(profile)
    locations, tables = source_tables(
        eurostat=configs["eurostat"],
        comtrade=configs["comtrade"],
        vulnerabilities=configs["vulnerabilities"],
        synthesis=configs["synthesis"],
    )
    assert set(locations) == {"eurostat", "comtrade", "vulnerabilities"}
    prefix = "demo_" if profile == "demo" else ""
    assert tables["indicators"].schema == f"{prefix}indicators"
    assert tables["synthesis"].schema == f"{prefix}synthesis"
    assert tables["diagnostics"].schema == f"{prefix}synthesis_diagnostics"
    assert tables["comext"].qualified_name == 'eurostat."DS_045409"."fact_table"'
    assert tables["hs_vintages"].catalog_alias == "comtrade"
    # Tous les gabarits se rendent avec les variables disponibles
    context, _ = build_context(tables, {t.key for t in tables.values()}, configs["serving"])
    for spec in configs["serving"]["TABLES"].values():
        render_sql(spec["SQL"], context)
    assert configs["serving"]["SCHEMA"] == f"{prefix}dashboard"


def test_render_unknown_variable_raises() -> None:
    with pytest.raises(KeyError):
        render_sql("SELECT * FROM $unknown", {})


def test_fragments_reject_unsafe_identifiers() -> None:
    with pytest.raises(ValueError):
        synthesis_pivot_sql(["auto_sum; DROP TABLE x"], ["by_product"], "auto_sum")
    with pytest.raises(ValueError):
        synthesis_pivot_sql(["auto_sum"], ["by_country"], "auto_sum")
    assert "strpos" in individual_partner_sql({"EXCLUDE_UNDERSCORE": True})


def test_cell_scores_read_real_nomenclature_columns(tmp_path: Path) -> None:
    """Table partenaires avec lignes historiques : colonnes lues, jointures au millésime."""
    from statflows.storage.ducklake.tables import write_dataframe

    world = build_serving_world(tmp_path)
    indicators = world.tables["indicators"]
    connector = world.factory(world.locations[indicators.catalog_alias], None, None)
    conn = connector.connect()
    try:
        # Lignes historiques : flux 2023 du code stable 854110 convertis vers HS2017
        qualified = f'"{indicators.catalog_alias}"."{indicators.schema}"."fact_table"'
        df_past = conn.execute(
            f"SELECT * FROM {qualified} WHERE product = 854110 AND \"TIME_PERIOD\" = '2023'"
        ).df().assign(classification="HS2017", hs_vintage="HS2017", in_force=False)
        write_dataframe(
            conn, df_past,
            ["classification", "freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD"],
            catalog_alias=indicators.catalog_alias, schema=indicators.schema,
        )
    finally:
        conn.close()

    result = publish_serving(world.tables, world.catalog, params=world.params, runtime=world.runtime)
    assert result["failures"] == {}

    df = _read(world, "cell_scores")
    assert not df.duplicated(["classification", "reporter", "product", "flow", "year"]).any()
    past = df[~df["in_force"]]
    assert len(past) == len(SERVING_REPORTERS) * 2
    assert set(past["classification"]) == {"HS2017"} and set(past["year"]) == {2023}
    # Ligne historique 2023 jointe au réseau HS2017, ligne en vigueur au réseau HS2022
    imports = df[(df["flow"] == "1") & (df["product"] == "854110") & (df["year"] == 2023)]
    assert set(imports.loc[~imports["in_force"], "WORLD_HHI"].round(6)) == {SERVING_NETWORK_HHI["HS2017"]}
    assert set(imports.loc[imports["in_force"], "WORLD_HHI"].round(6)) == {SERVING_NETWORK_HHI["HS2022"]}
    # Scores de synthèse (en vigueur seulement) : jamais reportés sur une ligne historique
    assert past["primary_score_by_reporter"].isna().all()
    assert df.loc[df["in_force"] & (df["reporter"] != "EU27_2020"), "primary_score_by_reporter"].notna().any()


def test_cell_scores_labels_and_trade_totals(published) -> None:
    world, _, _ = published
    cells = _read(world, "cell_scores")
    flows = _read(world, "flows")
    # Libellé des flux et libellés « code — libellé » des filtres
    assert dict(zip(cells["flow"], cells["flow_label"])) == {"1": "Import", "2": "Export"}
    fr = cells[(cells["reporter"] == "FR") & (cells["product"] == "854110")].iloc[0]
    assert fr["reporter_display"] == "FR — France"
    assert fr["product_display"] == "854110 — Diodes (HS6)"
    # Échanges totaux de la cellule = partenaire WORLD de flows (valeur et quantité),
    # EXT_EU27_2020 pour l'Union qui n'a pas d'agrégat WORLD
    keys = ["year", "reporter", "product", "flow"]
    total_partner = flows["reporter"].map(lambda r: "EXT_EU27_2020" if r == "EU27_2020" else "WORLD")
    world_flows = flows[flows["partner"] == total_partner].set_index(keys)
    merged = cells.set_index(keys).join(world_flows[["value_eur", "quantity_100kg"]])
    pd.testing.assert_series_equal(
        merged["trade_value_eur"], merged["value_eur"], check_names=False
    )
    pd.testing.assert_series_equal(
        merged["trade_quantity_100kg"], merged["quantity_100kg"], check_names=False
    )
    assert merged["trade_value_eur"].notna().all()
    reporter, product, flow, year = SERVING_ZERO_QUANTITY_CELL
    zero = merged.loc[(int(year), reporter, str(product), str(flow))]
    assert pd.isna(zero["trade_quantity_100kg"]) and zero["trade_value_eur"] > 0


def test_cell_scores_rank_uncertainty_matches_synthesis(published) -> None:
    """Dispersion des rangs entre méthodes recalculée depuis la table de synthèse fictive."""
    world, _, _ = published
    cells = _read(world, "cell_scores")
    methods = world.params["UNCERTAINTY_METHODS"]
    synthesis_table = world.tables["synthesis"]
    connector = world.factory(world.locations[synthesis_table.catalog_alias], None, None)
    conn = connector.connect()
    try:
        synthesis = conn.execute(f"SELECT * FROM {synthesis_table.qualified_name}").df()
    finally:
        conn.close()
    synthesis = synthesis[synthesis["method"].isin(methods)]
    # Méthodes fictives présentes : auto_sum et critic_sum (pareto et le consensus exclus)
    assert set(synthesis["method"]) == {"auto_sum", "critic_sum"}
    synthesis = synthesis.assign(
        product=synthesis["product"].str.zfill(8).where(synthesis["product"].str.len() > 6,
                                                        synthesis["product"].str.zfill(6)),
        year=synthesis["TIME_PERIOD"].astype(str).str[:4].astype(int),
        flow=synthesis["flow"].astype(str),
    )
    # Niveau dérivé : rangs de chaque méthode renumérotés au sein du niveau de nomenclature
    within = ["hs_vintage", "freq", "flow", "indicators", "TIME_PERIOD", "reporter", "method"]
    synthesis["rank_by_reporter_level"] = synthesis.groupby(
        within + [synthesis["product"].str.len()]
    )["rank_by_reporter"].rank(method="average")
    keys = ["year", "reporter", "product", "flow"]
    for level in world.params["LEVELS"]:
        expected = synthesis.groupby(keys)[f"rank_{level}"].agg(
            n="count", low="min", high="max",
            q25=lambda s: s.quantile(0.25), q75=lambda s: s.quantile(0.75),
        )
        actual = cells.set_index(keys).loc[expected.index]
        assert (actual[f"n_methods_{level}"] == expected["n"]).all()
        assert np.allclose(actual[f"rank_min_{level}"], expected["low"])
        assert np.allclose(actual[f"rank_max_{level}"], expected["high"])
        assert np.allclose(actual[f"rank_q25_{level}"], expected["q25"])
        assert np.allclose(actual[f"rank_spread_{level}"], expected["q75"] - expected["q25"])


def _uncertainty_frame(methods, levels=("by_reporter",)) -> pd.DataFrame:
    """Applique le fragment d'incertitude à une petite table de synthèse calculée à la main."""
    import duckdb

    rows = [
        # Cellule A : quatre méthodes retenues, ex aequo (rang moyen 2) compris
        ("A", "m1", 1.0), ("A", "m2", 2.0), ("A", "m3", 2.0), ("A", "m4", 7.0),
        # Consensus et méthode hors liste : ignorés
        ("A", "consensus_borda", 1.0), ("A", "other", 40.0),
        # Cellule B : accord parfait de trois méthodes
        ("B", "m1", 3.0), ("B", "m2", 3.0), ("B", "m3", 3.0),
        # Cellule C : une seule méthode a classé la cellule (m2 absente, m3 non classée)
        ("C", "m1", 5.0), ("C", "m3", None),
    ]
    conn = duckdb.connect()
    conn.execute("CREATE TABLE s (cell VARCHAR, method VARCHAR, rank_by_reporter DOUBLE)")
    conn.executemany("INSERT INTO s VALUES (?, ?, ?)", rows)
    fragment = rank_uncertainty_sql(methods, list(levels))
    return conn.execute(f"SELECT cell, {fragment} FROM s GROUP BY cell ORDER BY cell").df().set_index("cell")


def test_rank_uncertainty_hand_computed() -> None:
    df = _uncertainty_frame(["m1", "m2", "m3", "m4"])
    a = df.loc["A"]
    # Rangs [1, 2, 2, 7] : quantiles à interpolation linéaire (positions 0,75 et 2,25)
    assert a["n_methods_by_reporter"] == 4
    assert (a["rank_min_by_reporter"], a["rank_max_by_reporter"]) == (1.0, 7.0)
    assert a["rank_q25_by_reporter"] == pytest.approx(1.75)
    assert a["rank_q75_by_reporter"] == pytest.approx(3.25)
    assert a["rank_spread_by_reporter"] == pytest.approx(1.5)
    # Accord parfait : dispersion nulle
    b = df.loc["B"]
    assert b["n_methods_by_reporter"] == 3 and b["rank_spread_by_reporter"] == 0.0
    # Un seul rang : aucune dispersion mesurable, colonnes NULL
    c = df.loc["C"]
    assert c["n_methods_by_reporter"] == 1
    assert c[[f"rank_{k}_by_reporter" for k in ("min", "max", "q25", "q75", "spread")]].isna().all()


def test_rank_uncertainty_all_non_consensus_methods() -> None:
    df = _uncertainty_frame(None)
    # Sans liste : toutes les méthodes hors consensus, « other » comprise
    a = df.loc["A"]
    assert a["n_methods_by_reporter"] == 5
    assert a["rank_max_by_reporter"] == 40.0 and a["rank_min_by_reporter"] == 1.0


def test_rank_uncertainty_rejects_bad_input() -> None:
    with pytest.raises(ValueError):
        rank_uncertainty_sql(["auto_sum'; DROP TABLE x"], ["by_reporter"])
    with pytest.raises(ValueError):
        rank_uncertainty_sql([], ["by_reporter"])
    with pytest.raises(ValueError):
        rank_uncertainty_sql(None, ["by_country"])


def test_total_partner_sql() -> None:
    in_list, priority = total_partner_sql(["WORLD", "EXT_EU27_2020"])
    assert in_list == "'WORLD', 'EXT_EU27_2020'"
    assert priority == "CASE partner WHEN 'WORLD' THEN 0 WHEN 'EXT_EU27_2020' THEN 1 END"
    with pytest.raises(ValueError):
        total_partner_sql([])


def test_cell_scores_ranks_within_nomenclature_level(published) -> None:
    """Rangs par pays renumérotés au sein du niveau de nomenclature (SH6 et NC8 fictifs)."""
    world, _, _ = published
    df = _read(world, "cell_scores")
    labels = df.groupby("product_level")["product_level_label"].unique()
    assert {level: list(values) for level, values in labels.items()} == {6: ["SH6"], 8: ["NC8"]}
    ranked = df[df["primary_rank_by_reporter"].notna()]
    group = ["hs_vintage", "flow", "year", "reporter", "product_level"]
    # Restriction du classement commun : même ordre, rangs 1..n dans le niveau
    expected = ranked.groupby(group)["primary_rank_by_reporter"].rank(method="average")
    pd.testing.assert_series_equal(
        ranked["primary_rank_by_reporter_level"], expected, check_names=False
    )
    sizes = ranked.groupby(group)["product"].transform("size")
    assert (ranked["primary_n_by_reporter_level"] == sizes).all()
    # Un seul code SH6 par pays dans le monde fictif : rang 1 de son niveau
    assert (ranked.loc[ranked["product_level"] == 6, "primary_rank_by_reporter_level"] == 1).all()
    pd.testing.assert_series_equal(
        ranked["primary_score_by_reporter_level"], ranked["primary_score_by_reporter"],
        check_names=False,
    )


def test_product_level_label_sql() -> None:
    assert product_level_label_sql({}) == "CAST(length(g.product) AS VARCHAR)"
    assert product_level_label_sql({8: "NC8"}, "product") == (
        "CASE length(product) WHEN 8 THEN 'NC8' ELSE CAST(length(product) AS VARCHAR) END"
    )
    with pytest.raises(ValueError):
        product_level_label_sql({8: "NC8"}, "g.product; DROP")
    with pytest.raises(ValueError):
        product_level_label_sql({"huit": "NC8"})


def test_flow_label_sql() -> None:
    assert flow_label_sql({}) == "g.flow"
    assert flow_label_sql({"1": "Import d'O"}, "flow") == "CASE flow WHEN '1' THEN 'Import d''O' ELSE flow END"
    with pytest.raises(ValueError):
        flow_label_sql({"1": "Import"}, "g.flow; DROP")
