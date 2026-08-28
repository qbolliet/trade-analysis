"""Tests de caractérisation — helpers purs de ``macroforecast.datasets.core.download``.

Comportement figé : ``_schema_name``, ``_parse_iso``, ``_json_safe``, ``_primary_keys``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from types import SimpleNamespace

import pytest

from macroforecast.datasets.core.download import (
    _json_safe,
    _parse_iso,
    _primary_keys,
    _schema_name,
)

UTC = timezone.utc


# ──────────────────────────────────────────────────────────────────────
# _schema_name
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("dataflow", "expected"),
    [
        ("DSD_KEI@DF_KEI", "DSD_KEI_DF_KEI"),
        ("namq_10_gdp", "namq_10_gdp"),
        ("10_gdp", "df_10_gdp"),
        ("a-b.c", "a_b_c"),
        ("", ""),
        ("already_ok", "already_ok"),
    ],
)
def test_schema_name(dataflow: str, expected: str) -> None:
    assert _schema_name(dataflow) == expected


# ──────────────────────────────────────────────────────────────────────
# _parse_iso
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("value", [None, "", "not-a-date"])
def test_parse_iso_falsy_or_unparseable_returns_none(value) -> None:
    assert _parse_iso(value) is None


def test_parse_iso_naive_date_assumed_utc() -> None:
    assert _parse_iso("2024-01-02") == datetime(2024, 1, 2, tzinfo=UTC)


def test_parse_iso_z_suffix_is_utc() -> None:
    assert _parse_iso("2024-01-02T03:04:05Z") == datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)


def test_parse_iso_offset_converted_to_utc() -> None:
    result = _parse_iso("2024-01-02T03:04:05+02:00")
    assert result == datetime(2024, 1, 2, 1, 4, 5, tzinfo=UTC)
    assert result.tzinfo == UTC


# ──────────────────────────────────────────────────────────────────────
# _json_safe
# ──────────────────────────────────────────────────────────────────────


class _Color(Enum):
    RED = "red"


def test_json_safe_enum_to_value() -> None:
    assert _json_safe(_Color.RED) == "red"


def test_json_safe_datetime_to_isoformat() -> None:
    dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert _json_safe(dt) == dt.isoformat()


def test_json_safe_dict_keys_coerced_to_str_and_recursed() -> None:
    assert _json_safe({1: _Color.RED, "n": [2, 3]}) == {"1": "red", "n": [2, 3]}


def test_json_safe_tuple_becomes_list() -> None:
    assert _json_safe((1, _Color.RED, (4, 5))) == [1, "red", [4, 5]]


@pytest.mark.parametrize("value", ["s", 1, 1.5, True, None])
def test_json_safe_primitives_unchanged(value) -> None:
    assert _json_safe(value) == value


# ──────────────────────────────────────────────────────────────────────
# _primary_keys
# ──────────────────────────────────────────────────────────────────────


def _structure(*names: str) -> SimpleNamespace:
    return SimpleNamespace(dimensions=[SimpleNamespace(name=n) for n in names])


def test_primary_keys_case_insensitive_match_keeps_df_casing() -> None:
    structure = _structure("REF_AREA", "Partner")
    columns = ["ref_area", "partner", "TIME_PERIOD", "OBS_VALUE"]

    assert _primary_keys(structure, columns) == ["ref_area", "partner", "TIME_PERIOD"]


def test_primary_keys_time_period_appended_last() -> None:
    structure = _structure("TIME_PERIOD", "REF_AREA")
    columns = ["REF_AREA", "TIME_PERIOD"]

    # TIME_PERIOD est traité par la dimension puis dédupliqué ; l'ordre suit la structure
    assert _primary_keys(structure, columns) == ["TIME_PERIOD", "REF_AREA"]


def test_primary_keys_structure_none_returns_only_time_period() -> None:
    assert _primary_keys(None, ["a", "b", "time_period"]) == ["time_period"]


def test_primary_keys_no_match_returns_empty() -> None:
    assert _primary_keys(_structure("REF_AREA"), ["x", "y"]) == []
