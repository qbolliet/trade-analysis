"""Tests du monde fictif : flux vrais, déterminisme, concentration configurée."""

from __future__ import annotations

import numpy as np
import pytest

from kedro_pipeline.synthetic.world import SyntheticWorld, WorldConfig, _by_prefix


def _hhi(flows) -> float:
    """HHI des parts des exportateurs dans le commerce mondial d'un produit."""
    shares = flows.groupby("exporter")["value"].sum()
    shares = shares / shares.sum()
    return float((shares**2).sum())


def test_flows_are_deterministic(synthetic_section: dict) -> None:
    """Même configuration → mêmes flux, dans deux instances indépendantes."""
    first = SyntheticWorld(WorldConfig.from_mapping(synthetic_section)).flows("280519", 2020)
    second = SyntheticWorld(WorldConfig.from_mapping(synthetic_section)).flows("280519", 2020)
    assert first.equals(second)


def test_flows_have_expected_shape(synthetic_world: SyntheticWorld) -> None:
    """Pas de flux d'un pays vers lui-même, valeurs et poids positifs, colonnes attendues."""
    flows = synthetic_world.flows("281000", 2019)
    assert not flows.empty
    assert sorted(flows.columns) == ["exporter", "importer", "product", "value", "weight", "year"]
    assert (flows["exporter"] != flows["importer"]).all()
    assert (flows["value"] > 0).all() and (flows["weight"] > 0).all()


def test_year_outside_range_raises(synthetic_world: SyntheticWorld) -> None:
    """Une année hors bornes est une erreur explicite."""
    with pytest.raises(ValueError, match="outside the simulated range"):
        synthetic_world.flows("281000", 2010)


def test_supplier_bias_anchors_concentration(synthetic_world: SyntheticWorld) -> None:
    """Cobalt (préfixe 8105, biais COD) : le premier exportateur est COD et l'offre est concentrée."""
    flows = synthetic_world.flows("810510", 2020)
    top = flows.groupby("exporter")["value"].sum().idxmax()
    assert top == "COD"
    diversified = np.mean([_hhi(synthetic_world.flows(f"30049{d}", 2020)) for d in range(6)])
    assert _hhi(flows) > diversified


def test_trade_grows_with_trend(synthetic_section: dict) -> None:
    """Le commerce mondial suit la tendance configurée (croissance ≈ TREND_GROWTH par an)."""
    synthetic_section["MODEL"].update({"TREND_GROWTH": 0.5, "YEAR_SHOCKS": {}})
    world = SyntheticWorld(WorldConfig.from_mapping(synthetic_section))
    totals = [world.flows("281000", year)["value"].sum() for year in (2019, 2020, 2021)]
    assert totals[0] < totals[1] < totals[2]


def test_config_validation(synthetic_section: dict) -> None:
    """Univers trop petit ou bornes inversées → ValueError."""
    with pytest.raises(ValueError, match="at least two"):
        WorldConfig.from_mapping({**synthetic_section, "COUNTRIES": synthetic_section["COUNTRIES"][:1]})
    with pytest.raises(ValueError, match="must not precede"):
        WorldConfig.from_mapping({**synthetic_section, "YEARS": {"START": 2021, "END": 2019}})


def test_by_prefix_longest_wins() -> None:
    """Le préfixe le plus long l'emporte, à défaut la valeur par défaut."""
    assert _by_prefix("810510", 1.0, {"81": 2.0, "8105": 3.0}) == 3.0
    assert _by_prefix("999999", 1.0, {"81": 2.0}) == 1.0
