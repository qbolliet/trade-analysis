"""Test de bout en bout de l'étape partenaires avec le registre de fraîcheur v2.

Sur le modèle de ``tests/test_scripts_synthesis_e2e.py`` : un catalogue DuckLake
temporaire (fixture ``ducklake_conn``) reçoit une table de faits Comext fictive,
puis ``run_partner_step`` est appelé deux fois de suite sur la même connexion,
avec un registre de fraîcheur local. La première exécution calcule toutes les
unités et écrit les fragments ; la seconde ne recalcule rien et n'écrit rien.
L'invalidation de l'empreinte d'une métrique (correction de formule) recalcule
ensuite toutes les unités, puis plus rien ; un forçage aussi. Marqué ``slow`` (calcul et écritures DuckLake réels).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("dt_ducklake_manager")

from kedro_pipeline.io.freshness import ForceSpec  # noqa: E402
from macroforecast.trade.vulnerabilities import VulnerabilityConfig  # noqa: E402
from scripts.compute_trade_vulnerabilities import (  # noqa: E402
    partner_registry,
    partner_requested,
    partner_units,
    run_partner_step,
)

pytestmark = pytest.mark.slow

# Grille fictive : 2 périodes x 2 reporters x 3 produits x 2 flux
PERIODS = ("2022", "2023")
REPORTERS = ("FR", "DE")
PRODUCTS = ("28053010", "85414000", "854140")
PARTNERS = ("CN", "US", "JP")
T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
# Référentiel des millésimes de la fixture
NOMENCLATURES = {"HS2017": 2017, "HS2022": 2022}


# Fonction de construction de la table de faits Comext fictive
def _comext_fact_table() -> pd.DataFrame:
    """Partner-level Comext-like rows, aggregates (WORLD, EXT_EU) included."""
    rng = np.random.default_rng(0)
    rows = []
    for period in PERIODS:
        for reporter in REPORTERS:
            for product in PRODUCTS:
                for flow in (1, 2):
                    values = rng.uniform(1.0, 100.0, len(PARTNERS))
                    # Partenaires individuels, total mondial et agrégat extra-UE
                    cell = [
                        *zip(PARTNERS, values),
                        ("WORLD", values.sum()),
                        ("EXT_EU", values.sum() * 0.8),
                    ]
                    rows.extend(
                        {
                            "freq": "A", "reporter": reporter, "product": product, "flow": flow,
                            "indicators": "VALUE_IN_EUROS", "TIME_PERIOD": period,
                            "partner": partner, "OBS_VALUE": float(value),
                        }
                        for partner, value in cell
                    )
    return pd.DataFrame(rows)


def test_partner_step_second_run_recomputes_nothing(ducklake_conn, tmp_path: Path) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    conn, alias = ducklake_conn
    write_dataframe(
        conn, _comext_fact_table(),
        ["freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD", "partner"],
        catalog_alias=alias, schema="comext",
    )

    config = VulnerabilityConfig()
    block = {
        "BUCKET": None,
        "PATHS": {},
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/{{classification}}/{{reporter}}.json"},
    }
    units = partner_units({(r, p): T0 for r in REPORTERS for p in PRODUCTS}, "HS2022")
    requested = partner_requested(config)
    kwargs = dict(
        source_catalog_alias=alias, source_schema="comext",
        result_catalog_alias=alias, result_schema="indicators",
        config=config, nomenclatures=NOMENCLATURES, log_artifacts=False,
    )

    # Première exécution : toutes les unités, un fragment par reporter
    first = run_partner_step(
        conn, conn, registry=partner_registry(block, "HS2022"), units=units,
        requested=requested, force=ForceSpec(), now=T0 + timedelta(hours=1), **kwargs,
    )
    assert set(first.plans) == set(units)
    assert {plan.reason for plan in first.plans.values()} == {"first"}
    assert first.report is not None and first.report.cells > 0
    assert len(first.written) == len(REPORTERS)
    n_rows = conn.execute(f'SELECT count(*) FROM "{alias}"."indicators"."fact_table"').fetchone()[0]
    assert n_rows > 0
    fragment = json.loads(Path(first.written[0]).read_text(encoding="utf-8"))
    assert fragment["schema_version"] == 2
    assert all(
        set(e["fingerprints"]) == {"HHI/import", "CDI2/import", "CDI3/import"}
        for e in fragment["entries"].values()
    )

    # Seconde exécution, registre relu depuis le disque : rien à recalculer
    second = run_partner_step(
        conn, conn, registry=partner_registry(block, "HS2022"), units=units,
        requested=requested, force=ForceSpec(), now=T0 + timedelta(hours=2), **kwargs,
    )
    assert second.plans == {} and second.report is None and second.written == []
    assert conn.execute(
        f'SELECT count(*) FROM "{alias}"."indicators"."fact_table"'
    ).fetchone()[0] == n_rows

    # Invalidation de HHI (formule corrigée) : toutes les unités, raison « fingerprint »
    registry = partner_registry(block, "HS2022")
    registry.invalidate({"HHI"})
    registry.save()
    invalidated = run_partner_step(
        conn, conn, registry=partner_registry(block, "HS2022"), units=units,
        requested=requested, force=ForceSpec(), now=T0 + timedelta(hours=3), **kwargs,
    )
    assert set(invalidated.plans) == set(units)
    assert {plan.reason for plan in invalidated.plans.values()} == {"fingerprint"}
    assert run_partner_step(
        conn, conn, registry=partner_registry(block, "HS2022"), units=units,
        requested=requested, force=ForceSpec(), now=T0 + timedelta(hours=4), **kwargs,
    ).plans == {}

    # Forçage d'une métrique : toutes les unités
    forced = run_partner_step(
        conn, conn, registry=partner_registry(block, "HS2022"), units=units,
        requested=requested, force=ForceSpec(metrics=("HHI",)), now=T0 + timedelta(hours=5),
        **kwargs,
    )
    assert set(forced.plans) == set(units)
    assert {plan.reason for plan in forced.plans.values()} == {"forced"}


def test_adding_export_computes_only_export_rows(ducklake_conn, tmp_path: Path) -> None:
    """Passer de FLOWS=[import] à [import, export] : seules les lignes export sont écrites."""
    from statflows.storage.ducklake.tables import write_dataframe

    conn, alias = ducklake_conn
    write_dataframe(
        conn, _comext_fact_table(),
        ["freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD", "partner"],
        catalog_alias=alias, schema="comext",
    )
    config = VulnerabilityConfig()
    block = {
        "BUCKET": None,
        "PATHS": {},
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/{{classification}}/{{reporter}}.json"},
    }
    units = partner_units({(r, p): T0 for r in REPORTERS for p in PRODUCTS}, "HS2022")
    kwargs = dict(
        source_catalog_alias=alias, source_schema="comext",
        result_catalog_alias=alias, result_schema="indicators",
        config=config, nomenclatures=NOMENCLATURES, log_artifacts=False,
    )
    read = lambda: conn.execute(  # noqa: E731
        f'SELECT * FROM "{alias}"."indicators"."fact_table" ORDER BY ALL'
    ).df()

    # Import seul : aucune ligne export
    run_partner_step(
        conn, conn, registry=partner_registry(block, "HS2022"), units=units,
        requested=partner_requested(config, ("import",)), force=ForceSpec(),
        now=T0 + timedelta(hours=1), flows=("import",), **kwargs,
    )
    imports_only = read()
    assert set(imports_only["flow"]) == {config.import_flow}

    # Ajout de l'export : plans limités aux empreintes export, lignes import intactes
    both = ("import", "export")
    added = run_partner_step(
        conn, conn, registry=partner_registry(block, "HS2022"), units=units,
        requested=partner_requested(config, both), force=ForceSpec(),
        now=T0 + timedelta(hours=2), flows=both, **kwargs,
    )
    assert {plan.reason for plan in added.plans.values()} == {"fingerprint"}
    assert list(added.report.flows) == ["export"]
    after = read()
    assert set(after["flow"]) == {config.import_flow, config.export_flow}
    pd.testing.assert_frame_equal(
        after[after["flow"] == config.import_flow].reset_index(drop=True),
        imports_only.reset_index(drop=True),
    )
    assert after.loc[after["flow"] == config.export_flow, ["HHI", "CDI2", "CDI3"]].notna().all().all()


# ──────────────────────────────────────────────────────────────────────
# Lignes en vigueur et lignes historiques dans une même table
# ──────────────────────────────────────────────────────────────────────

# Codes déclarés (stockés en entiers comme dans Comext) : 854140 scindé en HS2022
_VINTAGE_CODES = {"2019": (854110, 854140, 85411000), "2023": (854110, 854141, 854142, 85411000)}
_CONCORDANCES = {
    ("HS2022", "HS2017"): pd.DataFrame(
        {"source_code": ["854110", "854141", "854142"], "target_code": ["854110", "854140", "854140"]}
    )
}


# Fonction de construction des flux Comext multi-millésimes
def _vintage_fact_table() -> pd.DataFrame:
    rng = np.random.default_rng(5)
    rows = []
    for period, codes in _VINTAGE_CODES.items():
        for reporter in REPORTERS:
            for product in codes:
                for flow in (1, 2):
                    values = rng.uniform(1.0, 100.0, len(PARTNERS))
                    cell = [*zip(PARTNERS, values), ("WORLD", values.sum()), ("EXT_EU", values.sum() * 0.8)]
                    rows += [
                        {"freq": "A", "reporter": reporter, "product": product, "flow": flow,
                         "indicators": "VALUE_IN_EUROS", "TIME_PERIOD": period,
                         "partner": partner, "OBS_VALUE": float(value)}
                        for partner, value in cell
                    ]
    return pd.DataFrame(rows)


def test_in_force_and_historical_passes_share_one_table(ducklake_conn, tmp_path: Path) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    from scripts.compute_trade_vulnerabilities import historical_conversions, historical_requested, historical_units

    conn, alias = ducklake_conn
    write_dataframe(
        conn, _vintage_fact_table(),
        ["freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD", "partner"],
        catalog_alias=alias, schema="comext",
    )
    config = VulnerabilityConfig()
    flows = ("import", "export")
    block = {
        "BUCKET": None, "PATHS": {},
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/{{classification}}/{{reporter}}.json"},
    }
    downloads = {
        (reporter, str(product).zfill(6) if len(str(product)) <= 6 else str(product)): T0
        for reporter in REPORTERS for codes in _VINTAGE_CODES.values() for product in codes
    }
    requested = partner_requested(config, flows)
    kwargs = dict(
        source_catalog_alias=alias, source_schema="comext", result_catalog_alias=alias,
        result_schema="indicators", config=config, nomenclatures=NOMENCLATURES, flows=flows,
        log_artifacts=False, is_provisional=True,
    )
    in_force_units = partner_units(downloads, "HS2022")
    historical = historical_units(
        downloads, historical_conversions(_CONCORDANCES, ["HS2017"], NOMENCLATURES)["HS2017"], "HS2017"
    )
    historical_req = historical_requested(requested, _CONCORDANCES, "HS2017", "drop")

    def run(now):
        in_force = run_partner_step(
            conn, conn, registry=partner_registry(block, "HS2022"), units=in_force_units,
            requested=requested, force=ForceSpec(), now=now, **kwargs,
        )
        past = run_partner_step(
            conn, conn, registry=partner_registry(block, "HS2022"), units=historical.watermarks,
            requested=historical_req, force=ForceSpec(), now=now, target_vintage="HS2017",
            sources=historical.sources, concordances=_CONCORDANCES, **kwargs,
        )
        return in_force, past

    first_in_force, first_past = run(T0 + timedelta(hours=1))
    assert {unit.key for unit in first_past.plans} == {
        f"HS2017|{r}|{p}" for r in REPORTERS for p in ("854110", "854140")
    }
    df = conn.execute(f'SELECT * FROM "{alias}"."indicators"."fact_table"').df()
    keys = ["classification", "freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD"]
    assert not df.duplicated(keys).any()
    assert df["is_provisional"].all()

    # Lignes en vigueur : millésime de la période, CN<année> pour les NC8
    current = df[df["in_force"]]
    by_period = current.groupby("TIME_PERIOD")["classification"].agg(set).to_dict()
    assert by_period == {"2019": {"HS2017", "CN2019"}, "2023": {"HS2022", "CN2023"}}
    assert set(current.loc[current["TIME_PERIOD"] == "2019", "hs_vintage"]) == {"HS2017"}
    # Lignes historiques : flux 2023 SH6 convertis vers HS2017, jamais avant 2022
    past = df[~df["in_force"]]
    assert set(past["TIME_PERIOD"]) == {"2023"} and set(past["classification"]) == {"HS2017"}
    assert set(past["hs_vintage"]) == {"HS2017"}
    assert set(past["product"]) == {854110, 854140}
    # Série HS2017 de 854140 : 2019 en vigueur + 2023 historique
    series = df[(df["classification"] == "HS2017") & (df["product"] == 854140)]
    assert set(series["TIME_PERIOD"]) == {"2019", "2023"}
    # Code stable : la ligne historique égale la ligne en vigueur de la même période
    metrics = ["HHI", "CDI2", "CDI3"]
    cell = ["reporter", "flow"]
    stable_now = current[(current["product"] == 854110) & (current["TIME_PERIOD"] == "2023")]
    stable_past = past[past["product"] == 854110]
    pd.testing.assert_frame_equal(
        stable_now.sort_values(cell)[metrics].reset_index(drop=True),
        stable_past.sort_values(cell)[metrics].reset_index(drop=True),
    )

    # Seconde exécution : rien à recalculer, dans aucune passe
    second_in_force, second_past = run(T0 + timedelta(hours=2))
    assert second_in_force.plans == {} and second_past.plans == {}


def test_partner_step_refuses_a_table_without_classification(ducklake_conn, tmp_path: Path) -> None:
    from statflows.storage.ducklake.tables import write_dataframe

    conn, alias = ducklake_conn
    write_dataframe(
        conn, _comext_fact_table(),
        ["freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD", "partner"],
        catalog_alias=alias, schema="comext",
    )
    # Table résultat à l'ancienne clé (sans classification)
    write_dataframe(
        conn,
        pd.DataFrame({"freq": ["A"], "reporter": ["FR"], "product": ["854140"], "flow": [1],
                      "indicators": ["VALUE_IN_EUROS"], "TIME_PERIOD": ["2023"], "HHI": [0.5]}),
        ["freq", "reporter", "product", "flow", "indicators", "TIME_PERIOD"],
        catalog_alias=alias, schema="indicators",
    )
    block = {
        "BUCKET": None, "PATHS": {},
        "STATE": {"PATH_TEMPLATE": f"{tmp_path.as_posix()}/state/{{classification}}/{{reporter}}.json"},
    }
    with pytest.raises(RuntimeError, match="migrate_indicators_key"):
        run_partner_step(
            conn, conn, registry=partner_registry(block, "HS2022"),
            units=partner_units({("FR", "854140"): T0}, "HS2022"),
            requested=partner_requested(VulnerabilityConfig()), force=ForceSpec(),
            source_catalog_alias=alias, source_schema="comext", result_catalog_alias=alias,
            result_schema="indicators", config=VulnerabilityConfig(), nomenclatures=NOMENCLATURES,
            log_artifacts=False,
        )
