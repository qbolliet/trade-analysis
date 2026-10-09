"""Fonctions d'étape de la synthèse et de la cohérence sur poignées vers un catalogue local.

Le catalogue DuckLake a ses métadonnées en SQLite (``tests/sqlite_catalog.py``) : les
poignées paresseuses donnent à chaque processus de travail sa propre connexion, ce qui
permet de comparer un et deux processus. Les registres amont sont écrits sur disque,
comme ceux des étapes partenaires et réseau.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("dt_ducklake_manager")

from kedro_pipeline.io.ducklake import DuckLakeLocation, DuckLakeTable  # noqa: E402
from kedro_pipeline.io.freshness import FreshnessRegistry, RegistryEntry, Unit  # noqa: E402
from kedro_pipeline.steps import synthesis as synthesis_step  # noqa: E402
from kedro_pipeline.steps.coherence import run_coherence_step  # noqa: E402
from kedro_pipeline.steps.synthesis import context_registry, run_synthesis_step  # noqa: E402
from macroforecast.tracking import CapturingTracker  # noqa: E402
from sqlite_catalog import ALIAS, SqliteCatalogConnector, catalog_paths, open_catalog  # noqa: E402

pytestmark = pytest.mark.slow

PERIODS = ("2022", "2023")
REPORTERS = ("FR", "DE", "IT", "ES")
PRODUCTS = [f"{100000 + i:06d}00" for i in range(12)]
NOMENCLATURES = {"HS2017": 2017, "HS2022": 2022}
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
CONTEXT_COLUMNS = ["freq", "flow", "indicators", "TIME_PERIOD"]

SYNTHESIS = {
    "RESULT_SCHEMA": "synthesis",
    "BUCKET": None,
    "FLOWS": ["import", "export"],
    "PATHS": {"DATA_PATH": "unused"},
    "TRACKING": {"LOG_ARTIFACTS": False},
    "WRITE_BATCH_CONTEXTS": 2,
    "SOURCES": [
        {"SCHEMA": "indicators", "ALIAS": "p", "COLUMNS": ["HHI", "CDI2", "CDI3"]},
        {
            "SCHEMA": "network_indicators", "ALIAS": "n",
            "COLUMNS": ["WORLD_HHI", "CENTRALITY_RISK"],
            "JOIN": {
                "ON": [
                    'substr(p."product", 1, 6) = n."product"',
                    'CAST(substr(p."TIME_PERIOD", 1, 4) AS INTEGER) = n."year"',
                    'n."flow" = p."flow"',
                ],
                "WHERE": "n.\"classification\" = 'HS2022'",
            },
        },
    ],
    "FILTERS": {"WHERE": "p.\"indicators\" = 'VALUE_IN_EUROS' AND p.\"freq\" = 'A'", "LAST_N_PERIODS": None},
    "PARAMETERS": {
        "metric_columns": ["HHI", "CDI2", "CDI3", "WORLD_HHI", "CENTRALITY_RISK"],
        "context_columns": CONTEXT_COLUMNS,
        "levels": ["by_product", "by_reporter", "global"],
        "min_group_size": 3,
        "consensus": ["borda"],
        "random_state": 0,
        "methods": [
            {"name": "rank_mean", "kind": "rank_mean"},
            {"name": "critic_sum", "kind": "weighted", "params": {"weighting": "critic"}},
        ],
    },
}
COHERENCE = {
    "RESULT_SCHEMA": "synthesis_diagnostics",
    "PATHS": {"DATA_PATH": "unused"},
    "TRACKING": {"LOG_ARTIFACTS": False},
    "PARAMETERS": {"topk_depths": [5], "lomo": False},
}
RUNTIME = {"NOMENCLATURES": {"HS": NOMENCLATURES}}


class _SqliteFactory:
    """Fabrique picklable de connecteurs du catalogue SQLite (signature de ``build_connector``)."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def __call__(self, location, pg, s3, **kwargs):
        return SqliteCatalogConnector(*catalog_paths(self.root))


def _write_sources(root: Path) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    rng = np.random.default_rng(0)
    indicators = pd.DataFrame([
        {"freq": "A", "flow": flow, "indicators": "VALUE_IN_EUROS", "TIME_PERIOD": period,
         "reporter": reporter, "product": product, "in_force": True, "hs_vintage": "HS2022",
         "HHI": rng.uniform(), "CDI2": rng.uniform(), "CDI3": rng.uniform()}
        for flow in (1, 2) for period in PERIODS for reporter in REPORTERS for product in PRODUCTS
    ])
    network = pd.DataFrame([
        {"product": product[:6], "year": int(period), "classification": "HS2022", "flow": flow,
         "WORLD_HHI": rng.uniform(), "CENTRALITY_RISK": rng.uniform()}
        for flow in (1, 2) for period in PERIODS for product in PRODUCTS
    ])
    conn = open_catalog(*catalog_paths(root))
    try:
        write_dataframe(conn, indicators, ["freq", "flow", "indicators", "TIME_PERIOD", "reporter", "product"],
                        catalog_alias=ALIAS, schema="indicators")
        write_dataframe(conn, network, ["product", "year", "classification", "flow"],
                        catalog_alias=ALIAS, schema="network_indicators")
    finally:
        conn.close()


def _handle(root: Path, schema: str) -> DuckLakeTable:
    location = DuckLakeLocation(dbname="v", catalog_alias=ALIAS, schema=schema, bucket=None, data_path="unused")
    return DuckLakeTable.lazy(location, None, None, connector_factory=_SqliteFactory(root))


def _partners(root: Path) -> FreshnessRegistry:
    registry = FreshnessRegistry(
        f"{root.as_posix()}/partners/{{reporter}}.json", None, "partners",
        shard_of=lambda unit: unit.get("reporter"),
    )
    for reporter in REPORTERS:
        for product in PRODUCTS:
            registry.upsert(RegistryEntry(
                Unit.of(classification="HS2022", reporter=reporter, product=product), T0,
                upstream_watermark=T0, reason="first",
            ))
    registry.save()
    return registry


def _synthesise(root: Path, name: str, *, n_jobs: int, cadence_check: bool = False, tracker=None):
    block = {**SYNTHESIS, "RESULT_SCHEMA": name,
             "STATE": {"PATH_TEMPLATE": f"{root.as_posix()}/{name}/state/{{TIME_PERIOD}}.json"}}
    state = context_registry(block, None, "synthesis", CONTEXT_COLUMNS)
    return run_synthesis_step(
        _handle(root, name), _handle(root, f"{name}_diag"), state, [_partners(root)], [],
        params={"SYNTHESIS": block, "COHERENCE": COHERENCE}, vulnerability_params={},
        runtime=RUNTIME, tracker=tracker, n_jobs=n_jobs, cadence_check=cadence_check, run_id="wf-1",
    ), block


def _table(root: Path, schema: str) -> pd.DataFrame:
    conn = open_catalog(*catalog_paths(root))
    try:
        frame = conn.execute(f'SELECT * FROM "{ALIAS}"."{schema}"."fact_table"').df()
    finally:
        conn.close()
    keys = [column for column in frame.columns if frame[column].dtype == object or column == "flow"]
    return frame.sort_values(keys, kind="mergesort").reset_index(drop=True)


@pytest.fixture
def catalog(tmp_path: Path) -> Path:
    (tmp_path / "data").mkdir()
    open_catalog(*catalog_paths(tmp_path)).close()
    _write_sources(tmp_path)
    return tmp_path


def test_synthesis_step_on_handles_matches_one_and_two_processes(catalog: Path) -> None:
    tracker = CapturingTracker()
    one, _ = _synthesise(catalog, "j1", n_jobs=1, tracker=tracker)
    two, _ = _synthesise(catalog, "j2", n_jobs=2)
    n_contexts = len(PERIODS) * 2

    assert one.step == "synthesis" and one.reportable and one.failures == {}
    assert one.n_units_planned == one.n_units_succeeded == n_contexts
    assert one.units_label == f"{n_contexts} contextes"
    assert one.metrics["synthesis/contexts_run"] == n_contexts and one.metrics["parallel/n_jobs"] == 1.0
    assert tracker.metrics["synthesis/contexts_run"] == n_contexts
    assert one.tags["result_schema"] == "j1"
    assert len(one.outputs["outcome"].computed) == n_contexts
    one.raise_if_failed()
    pd.testing.assert_frame_equal(_table(catalog, "j1"), _table(catalog, "j2"), check_exact=True)

    # Seconde exécution : rien de périmé, pas de rapport ; contrôle de cadence : sortie immédiate
    again, _ = _synthesise(catalog, "j1", n_jobs=1)
    assert again.n_units_planned == 0 and not again.reportable
    skipped, _ = _synthesise(catalog, "j1", n_jobs=1, cadence_check=True)
    assert skipped.outputs == {"skipped_by_cadence": True} and not skipped.reportable
    assert skipped.metrics == {"freshness/skipped_by_cadence": 1.0}


def test_coherence_step_on_handles_after_the_synthesis(catalog: Path) -> None:
    synthesis, block = _synthesise(catalog, "synthesis", n_jobs=1)
    assert synthesis.failures == {}
    coherence_block = {**COHERENCE, "STATE": {"PATH_TEMPLATE": f"{catalog.as_posix()}/coh/{{TIME_PERIOD}}.json"}}
    result = run_coherence_step(
        _handle(catalog, "synthesis"), _handle(catalog, "synthesis_diagnostics"),
        context_registry(coherence_block, None, "coherence", CONTEXT_COLUMNS),
        context_registry(block, None, "synthesis", CONTEXT_COLUMNS),
        params={"SYNTHESIS": block, "COHERENCE": coherence_block}, vulnerability_params={},
        runtime=RUNTIME, n_jobs=1,
    )
    assert result.step == "coherence" and result.failures == {} and result.reportable
    assert result.n_units_succeeded == len(PERIODS) * 2
    assert {"metrics", "methods"} <= set(_table(catalog, "synthesis_diagnostics")["family"])


def test_failed_context_is_listed_and_raised_after_the_others(catalog: Path, monkeypatch) -> None:
    real = synthesis_step.compute_synthesis_context

    def flaky(df_context, config, **kwargs):
        if str(df_context["TIME_PERIOD"].iloc[0]) == "2022" and int(df_context["flow"].iloc[0]) == 1:
            raise RuntimeError("contexte en échec")
        return real(df_context, config, **kwargs)

    monkeypatch.setattr(synthesis_step, "compute_synthesis_context", flaky)
    result, _ = _synthesise(catalog, "flaky", n_jobs=1)
    assert len(result.failures) == 1 and result.n_units_succeeded == len(PERIODS) * 2 - 1
    assert next(iter(result.failures.values())).startswith("RuntimeError: contexte en échec")
    with pytest.raises(RuntimeError, match=r"1 contexte\(s\) en échec sur 4"):
        result.raise_if_failed()
