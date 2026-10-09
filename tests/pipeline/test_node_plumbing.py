"""Mécanique commune des nœuds : sorties de suivi, ordre des échecs, hooks, datasets."""

from __future__ import annotations

import pickle
from types import SimpleNamespace
from typing import Any, Dict, List

import pandas as pd
import pytest
from kedro.io import DatasetError

from kedro_pipeline.hooks import TradeRunHooks, with_resolved_n_jobs
from kedro_pipeline.io.clients import ClientFactory
from kedro_pipeline.io.datasets import (
    ClientFactoryDataset,
    ConcordanceCache,
    ConcordanceCacheDataset,
    DuckLakeCatalogsDataset,
)
from kedro_pipeline.io.ducklake import (
    CheckedConnectorFactory,
    DuckLakeLocation,
    FileConnectorFactory,
    connector_factory_for,
)
from kedro_pipeline.pipelines._common import (
    artifacts_of,
    finish_step,
    metrics_of,
    n_jobs_of,
    reporting_outputs,
    reporting_values,
)
from kedro_pipeline.steps.result import StepResult

LOCATION = DuckLakeLocation(
    dbname="vulnerabilities", catalog_alias="vulnerabilities", schema="indicators",
    bucket="b", data_path="trade/datasets/vulnerabilities/",
)


class _Registry:
    """Registre factice : consigne ses sauvegardes dans un journal partagé."""

    def __init__(self, journal: List[str]) -> None:
        self.journal = journal

    def save(self) -> None:
        self.journal.append("save")


# ── Sorties de suivi ──────────────────────────────────────────────────────


def test_metrics_carry_unit_counts_and_prefixed_unit_runs() -> None:
    child = StepResult("partners", 3, 3, metrics={"freshness/units_new_data": 3})
    result = StepResult("partners", 2, 1, failures={"HS2017": "boom"}, children={"HS2022": child})
    metrics = metrics_of(result)
    assert metrics["units/planned"] == 2 and metrics["units/failed"] == 1
    assert metrics["HS2022/freshness/units_new_data"] == 3.0


def test_artifacts_are_partitioned_without_extension() -> None:
    table = pd.DataFrame({"a": [1]})
    child = StepResult("baci", artifacts={"output/rows_by_year.csv": table})
    result = StepResult("baci", artifacts={"download/queries.csv": table, "x": "not a table"},
                        children={"HS2017": child})
    assert set(artifacts_of(result)) == {"download/queries", "HS2017/output/rows_by_year"}


def test_reporting_values_target_the_declared_outputs_only() -> None:
    outputs = reporting_outputs("compute_synthetic_scores")
    values = reporting_values([outputs["metrics"], "synthesis.scores"], StepResult("synthesis", 1, 1))
    assert list(values) == ["mlflow.metrics.compute_synthetic_scores"]


# ── Ordre des échecs ──────────────────────────────────────────────────────


def test_registries_are_saved_before_the_failure_is_raised() -> None:
    journal: List[str] = []
    failure = RuntimeError("1 contexte en échec")
    result = StepResult("synthesis", 2, 1, failures={"c": "boom"}, failure_exception=failure)
    with pytest.raises(RuntimeError) as info:
        finish_step(result, _Registry(journal), outputs={"table": "handle"})
    assert journal == ["save"]
    # Le résultat voyage avec l'exception jusqu'au hook d'erreur
    assert info.value.step_result is result


def test_success_returns_handles_and_reporting_outputs() -> None:
    journal: List[str] = []
    values = finish_step(StepResult("x", 1, 1), _Registry(journal), outputs={"table": "handle"})
    assert journal == ["save"] and set(values) == {"table", "metrics", "artifacts"}


def test_a_never_blocking_step_does_not_raise_and_another_failure_can_be_raised() -> None:
    failed = StepResult("reference", 2, 1, failures={"products": "boom"})
    assert finish_step(failed, artifacts=False, failure=None)["metrics"]["units/failed"] == 1
    coverage = StepResult("coverage", 1, 1)
    with pytest.raises(RuntimeError) as info:
        finish_step(coverage, failure=failed)
    assert info.value.step_result is coverage


# ── Paramètres d'exécution ─────────────────────────────────────────────────


def test_n_jobs_is_resolved_by_the_hook_then_the_block_takes_precedence(monkeypatch) -> None:
    monkeypatch.setenv("NUM_CPU", "6")
    runtime = with_resolved_n_jobs({"N_JOBS": None, "FORCE_STEPS": ""})
    assert runtime == {"N_JOBS": 6, "FORCE_STEPS": ""}
    assert with_resolved_n_jobs({"N_JOBS": 2})["N_JOBS"] == 2
    assert n_jobs_of({"N_JOBS": 3}, runtime) == 3
    assert n_jobs_of({"N_JOBS": None}, runtime) == 6


def test_before_node_run_overrides_the_runtime_input_only() -> None:
    hooks = TradeRunHooks()
    assert hooks.before_node_run(node=None, inputs={"params:baci": {}}) is None
    override = hooks.before_node_run(node=None, inputs={"params:runtime": {"N_JOBS": 4}})
    assert override == {"params:runtime": {"N_JOBS": 4}}


def test_argo_runner_settings_are_given_only_when_missing() -> None:
    hooks = TradeRunHooks()
    context = SimpleNamespace()
    hooks.after_context_created(context)
    assert context.argo.runner.use_memory_datasets is False
    configured = SimpleNamespace(argo="from argo.yml")
    hooks.after_context_created(configured)
    assert configured.argo == "from argo.yml"


def test_on_node_error_saves_the_reporting_outputs_of_the_failed_step() -> None:
    saved: Dict[str, Any] = {}

    class _Catalog:
        def save(self, name: str, value: Any) -> None:
            saved[name] = value

    error = RuntimeError("boom")
    error.step_result = StepResult("network", 1, 0, failures={"HS2017": "boom"})
    outputs = reporting_outputs("compute_network_vulnerabilities")
    node = SimpleNamespace(outputs=["vulnerabilities.network", *outputs.values()])
    TradeRunHooks().on_node_error(error=error, node=node, catalog=_Catalog())
    assert set(saved) == set(outputs.values())
    assert saved[outputs["metrics"]]["units/failed"] == 1
    # Exception sans résultat d'étape : rien à sauvegarder
    TradeRunHooks().on_node_error(error=ValueError("x"), node=node, catalog=_Catalog())


# ── Datasets et fabriques ──────────────────────────────────────────────────


def test_client_factory_dataset_drops_empty_secrets_and_never_describes_them() -> None:
    dataset = ClientFactoryDataset(
        factory="collections.OrderedDict", options={"a": 1},
        credentials={"subscription_key": "secret", "token": ""},
    )
    factory = dataset.load()
    assert isinstance(factory, ClientFactory)
    assert dict(factory()) == {"a": 1, "subscription_key": "secret"}
    assert "secret" not in repr(factory) and "secret" not in str(dataset._describe())
    assert pickle.loads(pickle.dumps(factory))() == factory()
    with pytest.raises(DatasetError):
        dataset.save(factory)


def test_connector_factory_follows_the_credentials(tmp_path) -> None:
    assert isinstance(connector_factory_for({"postgres": {}, "s3": {}}), CheckedConnectorFactory)
    factory = connector_factory_for({"file_root": str(tmp_path)})
    assert factory == pickle.loads(pickle.dumps(factory)) == FileConnectorFactory(str(tmp_path))
    catalogs = DuckLakeCatalogsDataset(credentials={"file_root": str(tmp_path)}).load()
    handle = catalogs.table(LOCATION)
    assert handle.location == LOCATION and handle.connector_factory == factory
    with pytest.raises(DatasetError):
        DuckLakeCatalogsDataset().save(catalogs)


def test_file_catalogs_are_written_and_read_back(tmp_path) -> None:
    pytest.importorskip("dt_ducklake_manager")
    catalogs = DuckLakeCatalogsDataset(credentials={"file_root": str(tmp_path)}).load()
    handle = catalogs.table(LOCATION)
    handle.upsert(pd.DataFrame({"reporter": ["FR", "DE"], "HHI": [0.1, 0.2]}), ["reporter"])
    assert handle.exists()
    assert len(handle.query(f"SELECT * FROM {handle.qualified_name}")) == 2
    assert (tmp_path / "vulnerabilities.sqlite").exists()


def test_concordance_cache_dataset_checks_identity() -> None:
    dataset = ConcordanceCacheDataset(cache_path="trade/unsd", bucket="b")
    assert dataset.load() == ConcordanceCache("trade/unsd", "b")
    dataset.save(ConcordanceCache("trade/unsd", "b"))
    with pytest.raises(DatasetError):
        dataset.save(ConcordanceCache("elsewhere", "b"))


def test_maintenance_node_reports_an_empty_result() -> None:
    from kedro_pipeline.pipelines.maintenance.nodes import maintain_ducklake

    outputs = maintain_ducklake({"CATALOGS": [{"dbname": "eurostat"}]}, {})
    assert outputs == {"metrics": {"units/planned": 0.0, "units/succeeded": 0.0, "units/failed": 0.0}}
