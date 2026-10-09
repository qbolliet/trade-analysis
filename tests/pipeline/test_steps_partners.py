"""Fonction d'étape des métriques partenaires sur poignées vers un catalogue DuckLake local.

Les tables source et résultat sont des poignées paresseuses (``DuckLakeTable.lazy``) dont
la fabrique de connecteurs ouvre un catalogue DuckLake local (métadonnées SQLite) : l'étape ouvre elle-même
ses connexions, une seule fois pour toutes ses passes. Les runs sont enregistrés par une
fabrique de runs factice : un run par passe, avec ses tags et son rapport.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path

import pytest

pytest.importorskip("dt_ducklake_manager")

from kedro_pipeline.io.ducklake import DuckLakeLocation, DuckLakeTable  # noqa: E402
from kedro_pipeline.steps.partners import partner_registry, run_partner_vulnerabilities  # noqa: E402
from kedro_pipeline.steps.result import StepResult, UnitRun  # noqa: E402
from macroforecast.tracking import CapturingTracker  # noqa: E402
from sqlite_catalog import SqliteCatalogConnector, catalog_paths  # noqa: E402
from test_scripts_partners_e2e import (  # noqa: E402
    _CONCORDANCES,
    _VINTAGE_CODES,
    NOMENCLATURES,
    REPORTERS,
    T0,
    _vintage_fact_table,
)

pytestmark = pytest.mark.slow

KEYS = ["freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD", "partner"]


class _CatalogFactory:
    """Fabrique de connecteurs du catalogue local (même signature que ``build_connector``).

    Métadonnées SQLite : l'étape ouvre deux connexions simultanées (source et résultat),
    ce qu'un catalogue à métadonnées DuckDB refuse dans un même processus.
    """

    def __init__(self, root: Path) -> None:
        self.root = root

    def __call__(self, location, pg, s3, **kwargs):
        return SqliteCatalogConnector(*catalog_paths(self.root), alias=location.catalog_alias)


class _Downloads:
    """Vue factice du registre de téléchargement : couples reporter x produit."""

    def __init__(self, pairs) -> None:
        self.pairs = pairs

    def pairs_last_download(self):
        return dict(self.pairs)


class _Runs:
    """Fabrique de runs enregistrant chaque run (tags, rapport publié)."""

    def __init__(self) -> None:
        self.opened: list = []
        self.published: dict = {}

    @contextmanager
    def __call__(self, label, tags):
        tracker = CapturingTracker()
        self.opened.append((label, dict(tags)))
        yield UnitRun(tracker=tracker, publish=lambda result: self.published.__setitem__(label, result))


def _table(root: Path, schema: str) -> DuckLakeTable:
    location = DuckLakeLocation(
        dbname="lake", catalog_alias="lake", schema=schema, bucket=None, data_path="unused",
    )
    return DuckLakeTable.lazy(location, None, None, connector_factory=_CatalogFactory(root))


def _setup(tmp_path: Path):
    from statflows.storage.ducklake.tables import write_dataframe

    (tmp_path / "data").mkdir()
    source, result = _table(tmp_path, "comext"), _table(tmp_path, "indicators")
    with source.connect() as conn:
        write_dataframe(conn, _vintage_fact_table(), KEYS, catalog_alias="lake", schema="comext")
    downloads = _Downloads({
        (reporter, str(product).zfill(6) if len(str(product)) <= 6 else str(product)): T0
        for reporter in REPORTERS for codes in _VINTAGE_CODES.values() for product in codes
    })
    block = {
        "BUCKET": None, "PATHS": {}, "RESULT_SCHEMA": "indicators", "IS_PROVISIONAL": True,
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/{{classification}}/{{reporter}}.json"},
    }
    params = {
        "FLOWS": ["import", "export"],
        "VINTAGES": ["HS2017"],
        "VULNERABILITIES": {"DS": block},
        "TRACKING": {"LOG_ARTIFACTS": False, "DRIFT": True},
    }
    runtime = {"NOMENCLATURES": {"HS": NOMENCLATURES}}
    return source, result, downloads, block, params, runtime


def _run(source, result, downloads, block, params, runtime, runs, *, loader=None):
    return run_partner_vulnerabilities(
        source, result, partner_registry(block, "HS2022"), downloads,
        params=params, runtime=runtime, dataflow="DS", runs=runs,
        concordances_loader=loader or (lambda vintages, nomenclatures: _CONCORDANCES),
        run_id="wf-1",
    )


def test_partner_step_runs_one_tracked_pass_per_nomenclature(tmp_path: Path) -> None:
    source, result, downloads, block, params, runtime = _setup(tmp_path)
    runs = _Runs()
    first = _run(source, result, downloads, block, params, runtime, runs)

    # Une passe en vigueur puis une passe historique, chacune dans son run et son rapport
    assert isinstance(first, StepResult) and first.failures == {}
    assert first.n_units_planned == first.n_units_succeeded == 2
    assert [label for label, _ in runs.opened] == ["HS2022", "HS2017"]
    assert runs.opened[0][1] == {"classification": "HS2022", "in_force": "true"}
    assert runs.opened[1][1] == {"classification": "HS2017", "in_force": "false"}
    assert set(runs.published) == {"HS2022", "HS2017"}
    in_force = first.children["HS2022"]
    assert in_force.step == "partners" and in_force.n_units_planned == len(in_force.outputs["plans"])
    assert any(name.startswith("partners/import/") for name in in_force.metrics)
    assert any(name.startswith("partners/export/") for name in in_force.metrics)
    assert in_force.units_label.endswith("HS2022 × reporter × produit")
    assert in_force.tags["result_schema"] == "indicators"

    # Table résultat : lignes en vigueur et historiques, clé portant la classification
    with result.connect() as conn:
        df = conn.execute('SELECT * FROM "lake"."indicators"."fact_table"').df()
    assert set(df["in_force"]) == {True, False} and df["is_provisional"].all()
    keys = ["classification", "freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD"]
    assert not df.duplicated(keys).any()
    fragment = json.loads(Path(in_force.outputs["written"][0]).read_text(encoding="utf-8"))
    assert fragment["schema_version"] == 2

    # Seconde exécution : rien de périmé, aucun run ouvert, pas de rapport
    again = _Runs()
    second = _run(source, result, downloads, block, params, runtime, again)
    assert second.n_units_planned == 0 and not second.reportable and again.opened == []


def test_failed_pass_is_isolated_and_raised_at_the_end(tmp_path: Path) -> None:
    from statflows.storage.ducklake.tables import write_dataframe
    import pandas as pd

    source, result, downloads, block, params, runtime = _setup(tmp_path)
    # Table résultat créée avant la clé de nomenclature : chaque passe est refusée
    with result.connect() as conn:
        write_dataframe(
            conn, pd.DataFrame({"reporter": ["FR"], "product": ["854140"], "HHI": [0.1]}),
            ["reporter", "product"], catalog_alias="lake", schema="indicators",
        )
    runs = _Runs()
    outcome = _run(source, result, downloads, block, params, runtime, runs)
    assert set(outcome.failures) == {"HS2022", "HS2017"} and outcome.n_units_succeeded == 0
    assert "classification" in outcome.failures["HS2022"] and runs.published == {}
    with pytest.raises(RuntimeError, match=r"Passe\(s\) partenaires en échec : \['HS2017', 'HS2022'\]"):
        outcome.raise_if_failed()


def test_historical_vintages_require_a_correspondence_loader(tmp_path: Path) -> None:
    from kedro_pipeline.steps.partners import plan_partner_passes

    source, result, downloads, block, params, runtime = _setup(tmp_path)
    with pytest.raises(ValueError, match="correspondence-table loader"):
        plan_partner_passes(
            partner_registry(block, "HS2022"), downloads, params=params, runtime=runtime,
            dataflow="DS",
        )
    # Sans millésime historique, une seule passe et aucune table de passage lue
    plan = plan_partner_passes(
        partner_registry(block, "HS2022"), downloads, params={**params, "VINTAGES": []},
        runtime=runtime, dataflow="DS",
    )
    assert [p.label for p in plan.passes] == ["HS2022"] and plan.concordances == {}
