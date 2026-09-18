"""Tests de construction et d'ordonnancement des requêtes de téléchargement (PS-12).

Comtrade : liste année-majeure (une année complète avant la suivante), lots de
produits formés une fois dans l'ordre naturel. Eurostat : liste produit-majeure,
reporters dans l'ordre de la liste d'inclusion. Aucun appel réseau : codelists
fictives et faux client pour les périodes.
"""

from __future__ import annotations

from typing import Any, List, Optional

import pandas as pd
import pytest

from scripts.download_comtrade import (
    build_split_queries as build_comtrade_queries,
    cap_queries,
    plan_queries,
    resolve_periods,
)
from scripts.download_eurostat_comext import build_split_queries as build_eurostat_queries


# ──────────────────────────────────────────────────────────────────────
# Comtrade
# ──────────────────────────────────────────────────────────────────────

_NULL_FILTERS = {"include": None, "include_regex": None, "exclude": None, "exclude_regex": None}


def _codes(codes: List[str]) -> pd.DataFrame:
    """Codelist fictive (colonne ``code``)."""
    return pd.DataFrame({"code": codes})


def _comtrade_dims() -> dict:
    """Codelists fictives, volontairement non triées."""
    return {
        "reporters": _codes(["251", "276"]),
        "products": _codes(["854150", "010121", "85", "854140", "010129"]),
    }


def _comtrade_filters(**products: Any) -> dict:
    """Filtres de découpage : reporters non restreints, produits HS6."""
    return {
        "periods": {"start": None, "end": None},
        "reporters": dict(_NULL_FILTERS),
        "products": {**_NULL_FILTERS, "include_regex": r"^\d{6}$", **products},
    }


def test_comtrade_period_major_order_ps_12_1() -> None:
    """Exemple de PS-12.1 : une année complète (tous ses lots) avant la suivante."""
    queries = build_comtrade_queries(
        "C_A_HS",
        _comtrade_dims(),
        {"frequency": "annual", "flows": ["M", "X"]},
        _comtrade_filters(),
        periods=["2023", "2024"],
        products_step=2,
        periods_order="desc",
    )
    assert [(q.periods, q.products) for q in queries] == [
        ("2024", ["010121", "010129"]),
        ("2024", ["854140", "854150"]),
        ("2023", ["010121", "010129"]),
        ("2023", ["854140", "854150"]),
    ]
    # Reporters non restreints : une seule valeur None (tous), pas un éclatement
    assert all(q.reporters is None for q in queries)
    assert all(q.flows == ["M", "X"] for q in queries)


def test_comtrade_ascending_order_and_same_batches() -> None:
    """``asc`` inverse la boucle des années ; les lots restent identiques."""
    queries = build_comtrade_queries(
        "C_A_HS", _comtrade_dims(), {}, _comtrade_filters(),
        periods=["2024", "2022", "2023"], products_step=3, periods_order="asc",
    )
    assert [q.periods for q in queries] == ["2022", "2022", "2023", "2023", "2024", "2024"]
    assert [q.products for q in queries[:2]] == [
        ["010121", "010129", "854140"], ["854150"],
    ]


def test_comtrade_restricted_reporters_sent_as_single_list() -> None:
    """Reporters filtrés : la liste entière est envoyée dans chaque requête."""
    filters = _comtrade_filters()
    filters["reporters"] = {**_NULL_FILTERS, "include": ["276", "251"]}
    queries = build_comtrade_queries(
        "C_A_HS", _comtrade_dims(), {}, filters, periods=["2024"], products_step=10,
    )
    assert len(queries) == 1
    assert queries[0].reporters == ["251", "276"]


def test_comtrade_without_product_batches() -> None:
    """Sans découpage produit (branche autrefois en erreur), une requête par année."""
    queries = build_comtrade_queries(
        "C_A_HS", _comtrade_dims(), {}, _comtrade_filters(),
        periods=["2023", "2024"], products_step=None,
    )
    assert [(q.periods, q.products) for q in queries] == [
        ("2024", ["010121", "010129", "854140", "854150"]),
        ("2023", ["010121", "010129", "854140", "854150"]),
    ]


def test_comtrade_rejects_invalid_arguments() -> None:
    """Ordre de périodes inconnu ou clés incohérentes → ValueError."""
    with pytest.raises(ValueError, match="periods_order"):
        build_comtrade_queries(
            "C_A_HS", _comtrade_dims(), {}, _comtrade_filters(),
            periods=["2024"], periods_order="random",
        )
    with pytest.raises(ValueError, match="similar keys"):
        build_comtrade_queries(
            "C_A_HS", {"products": _codes(["010121"])}, {}, _comtrade_filters(),
            periods=["2024"],
        )


class _FakePeriodsClient:
    """Faux client : périodes valides de ``period_start`` à 2025 (l'année en cours exclue)."""

    def __init__(self) -> None:
        self.calls: list = []

    def get_valid_periods(
        self, period_start: Optional[str] = None, frequency: str = "annual", **kwargs: Any
    ) -> List[str]:
        self.calls.append((period_start, frequency, kwargs))
        return [str(y) for y in range(int(period_start), 2026)]


def test_resolve_periods_defaults_to_analysis_start_and_includes_end() -> None:
    """Début nul → première année de l'analyse ; borne de fin incluse."""
    client = _FakePeriodsClient()
    assert resolve_periods(client, {"start": None, "end": 1996}, 1994) == ["1994", "1995", "1996"]
    # La borne de fin n'est jamais transmise au client (qui l'exclurait)
    assert client.calls == [("1994", "annual", {})]
    assert resolve_periods(client, {"start": 2024, "end": None}, 1994) == ["2024", "2025"]


def test_plan_queries_uses_runtime_start_and_parameters() -> None:
    """La planification combine config de dataset, runtime et codelists."""
    config = {
        "DATAFLOW": "C_A_HS",
        "parameters": {"C_A_HS": {"products_step": 2, "max_queries": None, "periods_order": "desc"}},
        "fixed_dims": {"C_A_HS": {"frequency": "annual"}},
        "split_filters": {"C_A_HS": _comtrade_filters()},
    }
    runtime = {"ANALYSIS_START_YEAR": {"comtrade": 2024}}
    queries = plan_queries(config, runtime, _FakePeriodsClient(), _comtrade_dims())
    assert [(q.periods, q.products[0]) for q in queries] == [
        ("2025", "010121"), ("2025", "854140"), ("2024", "010121"), ("2024", "854140"),
    ]


def test_cap_queries() -> None:
    """``None`` = pas de plafond ; sinon les premières requêtes, dans l'ordre."""
    assert cap_queries([3, 1, 2], None) == [3, 1, 2]
    assert cap_queries([3, 1, 2], 2) == [3, 1]
    assert cap_queries([3, 1, 2], 0) == []


# ──────────────────────────────────────────────────────────────────────
# Eurostat
# ──────────────────────────────────────────────────────────────────────

def _eurostat_dims() -> dict:
    return {
        "reporter": _codes(["DE", "FR", "IT", "EU27_2020"]),
        "product": _codes(["280530", "01", "854140"]),
    }


def _eurostat_filters() -> dict:
    return {
        "reporter": {**_NULL_FILTERS, "include": ["FR", "EU27_2020", "DE"]},
        "product": dict(_NULL_FILTERS),
    }


def test_eurostat_product_major_order_with_include_order() -> None:
    """Boucle externe produits (ordre naturel), interne reporters (ordre d'include)."""
    queries = build_eurostat_queries(
        "DS-045409", _eurostat_dims(), {"freq": "A"}, _eurostat_filters(),
        start_period="1988",
    )
    assert [(q.dimensions["product"], q.dimensions["reporter"]) for q in queries] == [
        ("01", "FR"), ("01", "EU27_2020"), ("01", "DE"),
        ("280530", "FR"), ("280530", "EU27_2020"), ("280530", "DE"),
        ("854140", "FR"), ("854140", "EU27_2020"), ("854140", "DE"),
    ]
    # Toutes les années dans une requête, à partir de la première année de l'analyse
    assert all(q.start_period == "1988" for q in queries)
    # Ordre d'insertion des dimensions inchangé (fixes, puis ordre de la config)
    assert list(queries[0].dimensions) == ["freq", "reporter", "product"]


def test_eurostat_identity_key_ignores_start_period() -> None:
    """``start_period`` n'entre pas dans la clé du registre (pas de re-téléchargement)."""
    with_start = build_eurostat_queries(
        "DS-045409", _eurostat_dims(), {"freq": "A"}, _eurostat_filters(), start_period="1988",
    )
    without = build_eurostat_queries(
        "DS-045409", _eurostat_dims(), {"freq": "A"}, _eurostat_filters(),
    )
    assert [q.identity_key() for q in with_start] == [q.identity_key() for q in without]


@pytest.mark.parametrize(
    "kwargs", [{"products_step": 2}, {"period_windows": [[2015, None], [1988, 2014]]}]
)
def test_eurostat_unsupported_parameters(kwargs: dict) -> None:
    """Lots de produits (PR-03) et fenêtres de périodes (PD-07) : non implémentés."""
    with pytest.raises(NotImplementedError):
        build_eurostat_queries(
            "DS-045409", _eurostat_dims(), {}, _eurostat_filters(), **kwargs
        )
