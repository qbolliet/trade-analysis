"""Chaque contrôle configuré vise une métrique effectivement émise par son script (PS-31.2).

Un contrôle sur une métrique que l'étape n'émet jamais serait silencieusement ``skipped`` : le
verdict du run ne dirait plus rien. Pour chaque script, les métriques sont relevées sur un run
FICTIF (catalogues DuckLake temporaires, ``CapturingTracker`` en guise de tracker) ; tout contrôle
de ``config/tracking.yaml`` dont la métrique manque fait échouer le test, comme le nom d'un nœud de
la configuration qui ne correspond à aucun script.
"""

from __future__ import annotations

import itertools
from dataclasses import replace
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any, Dict, Iterable

import numpy as np
import pandas as pd
import pytest
import yaml

from macroforecast.tracking import CapturingTracker, rekey_metrics
from macroforecast.tracking.report import Check, checks_for_node

ROOT = Path(__file__).resolve().parents[2]
TRACKING = yaml.safe_load((ROOT / "config" / "tracking.yaml").read_text(encoding="utf-8"))["tracking"]
DEMO_TRACKING = yaml.safe_load(
    (ROOT / "config" / "profiles" / "demo" / "tracking.yaml").read_text(encoding="utf-8")
)["tracking"]


def _nodes() -> Dict[str, str]:
    """Nœuds de rapport de chaque script (l'exemple de millésime donne les nœuds par millésime)."""
    import scripts.compute_network_vulnerabilities as network
    import scripts.compute_synthesis_coherence as coherence
    import scripts.compute_synthetic_scores as synthesis
    import scripts.compute_trade_vulnerabilities as partners
    import scripts.download_comtrade as comtrade
    import scripts.download_eurostat_comext as eurostat
    import scripts.publish_serving as serving

    return {
        "download_eurostat": eurostat.NODE,
        "download_comtrade": comtrade.NODE,
        "process_baci": "process_baci_HS2017",
        "partners": partners.NODE,
        "network": f"{network.NODE}_HS2017",
        "synthesis": synthesis.NODE,
        "coherence": coherence.NODE,
        "serving": serving.NODE,
    }


def _checks(node: str) -> list[Check]:
    checks = checks_for_node(node, TRACKING)
    assert checks, f"aucun contrôle configuré pour le nœud {node!r}"
    return checks


def assert_checks_target_emitted(node: str, emitted: Iterable[str]) -> None:
    """Échoue si un contrôle configuré pour ``node`` vise une métrique non émise."""
    emitted = set(emitted)
    missing = [check.metric for check in _checks(node) if check.metric not in emitted]
    assert not missing, f"{node}: contrôle(s) sur des métriques jamais émises : {missing} ; émises : {sorted(emitted)}"


# ──────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────


def test_demo_profile_carries_the_same_tracking_configuration() -> None:
    assert DEMO_TRACKING == TRACKING


def test_every_configured_key_matches_a_script_node_and_every_node_has_checks() -> None:
    nodes = list(_nodes().values())
    for pattern, entries in TRACKING["CHECKS"].items():
        assert any(fnmatchcase(node, pattern) for node in nodes), f"clé {pattern!r} : aucun script"
        assert entries, f"clé {pattern!r} sans contrôle"
    for node in nodes:
        assert _checks(node)


def test_every_configured_check_is_well_formed() -> None:
    for pattern in TRACKING["CHECKS"]:
        for check in checks_for_node(pattern.replace("*", "x"), TRACKING):
            assert check.label != check.metric, f"{check.metric}: libellé lisible attendu"
    report = TRACKING["REPORT"]
    assert report["PLOTLY_JS"] in ("inline", "cdn")
    # Sous la limite de longueur d'un tag MLflow (PQ-19)
    assert 0 < report["MAX_DESCRIPTION_CHARS"] <= 8000


# ──────────────────────────────────────────────────────────────────────
# Téléchargements
# ──────────────────────────────────────────────────────────────────────


def _download_report():
    from statflows.core.reports import DownloadReport, HttpStats, QueryReport, RateLimitStats

    queries = [
        QueryReport(identity_key="q1", rows_written=10, duration_seconds=1.0,
                    http=HttpStats(n_requests=1, total_seconds=0.5), rate_limit=RateLimitStats(total_wait_seconds=0.2)),
        QueryReport(identity_key="q2", duration_seconds=1.0, error_type="HTTPError", error_message="500"),
    ]
    return DownloadReport(
        processed=2, rows_written=10, errors=1, n_queries_planned=3, n_queries_remaining=1,
        duration_seconds=2.0, queries=queries,
    )


@pytest.mark.parametrize("script", ["download_eurostat", "download_comtrade"])
def test_download_checks_target_emitted_metrics(script: str) -> None:
    from kedro_pipeline.io.download_report import download_run_metrics

    assert_checks_target_emitted(_nodes()[script], download_run_metrics(_download_report()))


def test_download_run_metrics_shares() -> None:
    from kedro_pipeline.io.download_report import download_run_metrics

    metrics = download_run_metrics(_download_report())
    assert metrics["download/error_share"] == 0.5 and metrics["download/wait_share"] == pytest.approx(0.1)


# ──────────────────────────────────────────────────────────────────────
# Étapes calculées sur catalogue DuckLake (fictif) — marquées slow
# ──────────────────────────────────────────────────────────────────────

slow = pytest.mark.slow


class _NoCloseConnection:
    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def close(self) -> None:
        return None


class _FakeConnector:
    def __init__(self, conn: Any, alias: str) -> None:
        self._conn, self.catalog_alias = conn, alias

    def connect(self) -> Any:
        return _NoCloseConnection(self._conn)


@slow
def test_baci_and_network_checks_target_emitted_metrics(
    ducklake_conn, synthetic_world, synthetic_reference, synthetic_section, tmp_path
) -> None:
    """Redressement BACI puis vulnérabilités de réseau sur le monde fictif."""
    import datetime as dt

    from kedro_pipeline.synthetic.comtrade import ReportingConfig, SyntheticComtradeClient
    from macroforecast.trade.processing import BaciConfig, HsHarmonizer, required_columns, run_baci
    from scripts.download_comtrade import build_split_queries
    from scripts.process_baci_hs import _read_comtrade_fact_table, coverage_metrics
    from statflows.core.download import download_updates
    from statflows.storage.ducklake.tables import write_dataframe

    conn, alias = ducklake_conn
    products = ["280519", "810510", "854110", "300490"]
    client = SyntheticComtradeClient(
        synthetic_world, synthetic_reference, ReportingConfig.from_mapping(synthetic_section["REPORTING"]),
        product_universe=products,
    )
    queries = build_split_queries(
        "C_A_HS",
        {"reporters": pd.DataFrame({"code": ["251"]}), "products": pd.DataFrame({"code": products})},
        {"frequency": "annual", "flows": ["M", "X"], "type_code": "C", "classification": "HS"},
        {"reporters": {"include": None}, "products": {"include": products}},
        periods=["2019", "2020", "2021"], products_step=2,
    )
    download_updates(
        client=client, queries=queries, connector=_FakeConnector(conn, alias),
        structures_path=tmp_path / "structures.json", last_download_path=tmp_path / "last.json",
        bucket=None, max_runtime=None,
    )
    config = replace(BaciConfig(), min_mirror_flows=5, fas_countries=("CAN",))
    columns = list(dict.fromkeys(required_columns(config) + [config.schema.classification_col]))
    declarations = _read_comtrade_fact_table(conn, "C_A_HS", columns, "period", [2019, 2020, 2021])
    isos = list(synthetic_world.iso3)
    dist = pd.DataFrame(list(itertools.permutations(isos, 2)), columns=["iso_o", "iso_d"])
    dist["distw"] = np.random.default_rng(0).uniform(500, 15000, len(dist))
    dist["contig"] = 0
    geo = pd.DataFrame({"iso3": isos, "landlocked": 0})

    tracker = CapturingTracker()
    result, report = run_baci(declarations, dist, geo, config=config, tracker=tracker, log_artifacts=True)
    emitted = {
        **coverage_metrics([2019, 2020, 2021], {2019: 1.0, 2020: 1.0, 2021: 1.0}, 2019, None),
        **rekey_metrics(report.to_metrics()),
    }
    assert_checks_target_emitted(_nodes()["process_baci"], emitted)
    # Les tables d'artefacts dont se servent les sections BACI sont bien journalisées
    assert {"tonnage/conversion_rates.csv", "fobisation/valuation_regimes.csv", "quality/sigma_by_country.csv"} <= set(
        tracker.tables
    )
    assert "gravity/coefficients.json" in tracker.dicts

    # Vulnérabilités de réseau sur la table BACI écrite dans le catalogue
    from macroforecast.trade.vulnerabilities.base import NetworkVulnerabilityConfig
    from macroforecast.trade.vulnerabilities.runner import run_network_vulnerabilities

    write_dataframe(conn, result, config.primary_keys, catalog_alias=alias, schema="baci_hs2017")
    net_tracker = CapturingTracker()
    net_report = run_network_vulnerabilities(
        conn, source_catalog_alias=alias, source_schema="baci_hs2017", classification="HS2017",
        result_schema="network_indicators", config=NetworkVulnerabilityConfig(), tracker=net_tracker,
        log_artifacts=False,
    )
    assert_checks_target_emitted(_nodes()["network"], rekey_metrics(net_report.to_metrics()))


@slow
def test_partner_vulnerabilities_checks_target_emitted_metrics(ducklake_conn, synthetic_world) -> None:
    from kedro_pipeline.synthetic.comext import ComextConfig, ComextTemplate, build_comext
    from macroforecast.trade.vulnerabilities.runner import run_vulnerabilities
    from statflows.storage.ducklake.tables import write_dataframe

    conn, alias = ducklake_conn
    iso2 = {c.iso3: c.iso2 for c in synthetic_world.config.countries}
    dims = {"freq": "A", "partner": "*", "flow": ["1", "2"], "indicators": ["QUANTITY_IN_100KG", "VALUE_IN_EUROS"]}
    frames = [
        build_comext(synthetic_world, iso2, ComextConfig.from_mapping({}), ComextTemplate(),
                     reporter=reporter, product=product, dimensions=dims)
        for reporter in ("FR", "DE", "IT", "EU27_2020")
        for product in ("28051910", "81051000", "85411000")
    ]
    data = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    keys = ["freq", "reporter", "partner", "product", "flow", "indicators", "TIME_PERIOD"]
    write_dataframe(conn, data, keys, catalog_alias=alias, schema="DS_045409")
    tracker = CapturingTracker()
    report = run_vulnerabilities(
        conn, source_catalog_alias=alias, source_schema="DS_045409", result_schema="indicators",
        tracker=tracker, log_artifacts=True,
    )
    assert_checks_target_emitted(_nodes()["partners"], rekey_metrics(report.to_metrics()))
    assert any(path.startswith("vulnerabilities/") for path in tracker.tables)


def _synthesis_config():
    from macroforecast.trade.aggregation import MethodSpec, SynthesisConfig

    return SynthesisConfig(
        metric_columns=("HHI", "CDI2", "CDI3", "EXPORT_HHI", "CENTRALITY_RISK", "CLUSTERING_W"),
        levels=("by_product", "by_reporter", "global"),
        min_group_size=3,
        consensus=("borda",),
        random_state=0,
        methods=(
            MethodSpec(name="rank_mean", kind="rank_mean"),
            MethodSpec(name="critic_sum", kind="weighted",
                       params={"weighting": "critic", "aggregation": "weighted_sum",
                               "weighting_params": {"method": "spearman", "scale": "mad"}}),
        ),
    )


def _query(catalog_alias: str) -> str:
    from scripts.compute_synthetic_scores import build_source_query

    sources = [
        {"SCHEMA": "indicators", "ALIAS": "p", "COLUMNS": ["HHI", "CDI2", "CDI3"]},
        {
            "SCHEMA": "network_indicators", "ALIAS": "n",
            "COLUMNS": ["EXPORT_HHI", "CENTRALITY_RISK", "CLUSTERING_W"],
            "JOIN": {
                "ON": ['substr(p."product", 1, 6) = n."product"',
                       'CAST(substr(p."TIME_PERIOD", 1, 4) AS INTEGER) = n."year"'],
                "WHERE": "n.\"classification\" = 'HS2022'",
            },
        },
    ]
    filters = {"WHERE": 'p."flow" = 1 AND p."indicators" = \'VALUE_IN_EUROS\' AND p."freq" = \'A\'',
               "LAST_N_PERIODS": None}
    return build_source_query(sources, filters, catalog_alias)


@slow
def test_synthesis_and_coherence_checks_target_emitted_metrics(synthesis_source_tables) -> None:
    from macroforecast.trade.aggregation import CoherenceConfig
    from scripts.compute_synthesis_coherence import run_from_connections as run_coherence
    from scripts.compute_synthetic_scores import run_from_connections as run_synthesis
    from scripts._run_report import RunScope

    conn, alias = synthesis_source_tables.conn, synthesis_source_tables.catalog_alias
    config = _synthesis_config()

    synthesis_tracker = CapturingTracker()
    scope = RunScope(_nodes()["synthesis"], TRACKING)
    reports, failures, _, n_contexts = run_synthesis(
        conn, conn, _query(alias), config, catalog_alias=alias, result_schema="synthesis",
        diagnostics_schema="synthesis_diagnostics", tracker=synthesis_tracker, log_artifacts=False, scope=scope,
    )
    assert reports and not failures and n_contexts == len(reports)
    assert_checks_target_emitted(_nodes()["synthesis"], synthesis_tracker.metrics)
    # Le rapport de run a été publié, sain, avec ses sections
    assert synthesis_tracker.tags["health"] == "ok" and "report/report.html" in synthesis_tracker.texts

    coherence_tracker = CapturingTracker()
    coherence_scope = RunScope(_nodes()["coherence"], TRACKING)
    reports, failures, _, _ = run_coherence(
        conn, conn, _query(alias), "synthesis", config,
        CoherenceConfig(topk_depths=(5, 10), rbo_p=0.9, dispute_fraction=0.01, lomo=False, metric_pairs=True),
        catalog_alias=alias, diagnostics_schema="synthesis_diagnostics",
        tracker=coherence_tracker, log_artifacts=False, scope=coherence_scope,
    )
    assert reports and not failures
    assert_checks_target_emitted(_nodes()["coherence"], coherence_tracker.metrics)
    assert coherence_tracker.tags["health"] == "ok"


@slow
def test_serving_checks_target_emitted_metrics(serving_world) -> None:
    from kedro_pipeline.steps.serving import publish_serving

    tracker = CapturingTracker()
    result = publish_serving(
        serving_world.tables, serving_world.catalog, params=serving_world.params,
        runtime=serving_world.runtime, tracker=tracker,
    )
    assert not result["failures"]
    assert_checks_target_emitted(_nodes()["serving"], tracker.metrics)
