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
        config=config, log_artifacts=False,
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
    assert all(set(e["fingerprints"]) == {"HHI", "CDI2", "CDI3"} for e in fragment["entries"].values())

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
