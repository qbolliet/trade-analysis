"""Tests des requêtes de service de ``config/serving.yaml`` (PS-29.2, PS-29.3).

Les requêtes RÉELLES de la configuration sont exécutées par ``publish_serving`` sur des
catalogues DuckLake fichiers alimentés de données fictives (``build_serving_world``).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from kedro_pipeline.steps.serving import (
    build_context,
    individual_partner_sql,
    publish_serving,
    render_sql,
    source_tables,
    synthesis_pivot_sql,
)

from conftest import (
    SERVING_NETWORK_HHI,
    SERVING_PRODUCTS,
    SERVING_REPORTERS,
    SERVING_YEARS,
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


def test_cell_scores_columns_and_one_row_per_cell(published) -> None:
    world, _, _ = published
    df = _read(world, "cell_scores")
    params = world.params
    expected = {
        "classification", "hs_vintage", "in_force", "year", "flow", "reporter",
        "reporter_label", "product", "product_label", "product_level",
        "HHI", "CDI2", "CDI3", "HHI_ALERT", "EXPORT_HHI", "CENTRALITY_RISK",
        "CLUSTERING_W", "SPOF", "SPOF_ALERT",
    }
    for method in params["METHODS"]:
        for level in params["LEVELS"]:
            expected |= {f"{method}_score_{level}", f"{method}_rank_{level}"}
    for level in params["LEVELS"]:
        expected |= {f"primary_score_{level}", f"primary_rank_{level}", f"primary_n_{level}"}
    expected |= {f"{metric}_norm" for metric in params["NORM_METRICS"]}
    assert expected <= set(df.columns)
    # Méthode non retenue : absente des colonnes
    assert not any(column.startswith("pareto_") for column in df.columns)

    keys = ["classification", "reporter", "product", "flow", "year"]
    assert not df.duplicated(keys).any()
    # Grille filtrée sur VALUE_IN_EUROS / A : 2 années × 3 pays × 4 produits × 2 flux
    assert len(df) == len(SERVING_YEARS) * len(SERVING_REPORTERS) * len(SERVING_PRODUCTS) * 2
    assert set(df["indicators"]) == {"VALUE_IN_EUROS"}


def test_cell_scores_derived_nomenclature_columns(published) -> None:
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
    for year, vintage in ((2019, "HS2017"), (2023, "HS2022")):
        values = set(df.loc[df["year"] == year, "EXPORT_HHI"].dropna().round(6))
        assert values == {SERVING_NETWORK_HHI[vintage]}
    # Tous les codes SH6 / NC8 de la grille ont leur SH6 dans le réseau (010121 compris)
    assert df["EXPORT_HHI"].notna().all()


def test_cell_scores_ranks_consistent(published) -> None:
    world, _, _ = published
    df = _read(world, "cell_scores")
    params = world.params
    context = ["flow", "year"]
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
    assert set(aggregates["partner"]) == {"WORLD", "EXT_EU"}
    assert aggregates["rank"].isna().all()
    # INT_EU27_2020 (agrégat à « _ ») jamais classé comme partenaire individuel
    assert "INT_EU27_2020" not in set(df["partner"])
    world_share = df.loc[df["partner"] == "WORLD", "share"]
    assert (world_share.round(9) == 1.0).all()
    # Rang 1 = premier partenaire (valeur maximale de la cellule)
    first = individual[individual["rank"] == 1].set_index(cell)["value"].sort_index()
    assert (first == individual.groupby(cell)["value"].max().sort_index()).all()
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
    assert df["EXPORT_HHI"].isna().all()
    assert result["rows"]["products"] == 0
    assert result["metrics"]["serving/missing_sources"] == len(result["missing_sources"])


def test_missing_required_source_fails_without_writing(tmp_path: Path) -> None:
    world = build_serving_world(tmp_path, omit=("comext",))
    result = publish_serving(world.tables, world.catalog, params=world.params, runtime=world.runtime)
    assert result["tables"] == []
    assert "flows" in result["failures"]
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
