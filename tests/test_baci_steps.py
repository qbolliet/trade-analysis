"""Étape BACI : porte de complétude, blocs de chapitres, E/S DuckDB et reprise.

Les fonctions pures de ``kedro_pipeline.steps.baci`` sont testées sur des cas
limites ; ``DuckDBPassIO`` et ``DuckLakeYearWriter`` sont exercés de bout en bout
sur un catalogue DuckLake temporaire (fichiers de travail Parquet en local) et
comparés au redressement monobloc.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from baci_synthetic import cepii_tables, comtrade_declarations
from kedro_pipeline.steps.baci import (
    DuckDBPassIO,
    DuckLakeYearWriter,
    chapter_blocks,
    chapter_links,
    concordance_pairs,
    eligible_years,
    work_root,
)
from macroforecast.trade.processing import (
    BaciConfig,
    HsHarmonizer,
    run_baci,
    run_baci_passes,
)
from scripts.process_baci_hs import (
    baci_requested,
    baci_unit,
    compute_fit_id,
    requested_targets,
    select_targets,
    with_year_written,
)
from kedro_pipeline.io.freshness import RegistryEntry, UpstreamMark

KEYS = ["exporter", "importer", "product", "year"]


@pytest.fixture(autouse=True)
def _quiet() -> None:
    logging.disable(logging.INFO)
    yield
    logging.disable(logging.NOTSET)


def _planned(*years: int):
    """Liste planifiée fictive : deux lots de produits par année."""
    return [Query(periods=str(year), products=batch) for year in years for batch in (["01"], ["02"])]


Query = SimpleNamespace


# ──────────────────────────────────────────────────────────────────────
# Porte de complétude
# ──────────────────────────────────────────────────────────────────────


def test_eligible_years_without_any_download() -> None:
    assert eligible_years({}, _planned(2021, 2022), 1.0) == {}
    assert eligible_years({}, [], 0.0) == {}


def test_eligible_years_share_exactly_at_the_threshold_is_eligible() -> None:
    batches = {2021: {("01",): "d"}, 2022: {("01",): "d", ("02",): "d"}}
    assert eligible_years(batches, _planned(2021, 2022), 0.5) == {2021: 0.5, 2022: 1.0}
    assert eligible_years(batches, _planned(2021, 2022), 0.51) == {2022: 1.0}


def test_eligible_years_outside_the_range_are_excluded() -> None:
    batches = {year: {("01",): "d", ("02",): "d"} for year in (1993, 2021, 2030)}
    planned = _planned(1993, 2021, 2030)
    assert eligible_years(batches, planned, 1.0, 1994, period_end=2025) == {2021: 1.0}
    # Lots téléchargés mais non planifiés : ignorés
    assert eligible_years({2040: {("01",): "d"}}, planned, 0.0) == {1993: 0.0, 2021: 0.0, 2030: 0.0}


def test_eligible_years_reads_a_registry_view() -> None:
    view = SimpleNamespace(batches_by_year=lambda: {2022: {("01",): "d", ("02",): "d"}})
    assert eligible_years(view, _planned(2022), 1.0) == {2022: 1.0}


# ──────────────────────────────────────────────────────────────────────
# Nomenclatures et blocs de chapitres
# ──────────────────────────────────────────────────────────────────────


def test_concordance_pairs_and_chapter_links() -> None:
    assert concordance_pairs({2017: ["H5"], 2023: ["H5", "H6"]}, {"HS2017": [2017, 2023], "HS2022": [2023]}) == [
        ("HS2017", "HS2022"),
        ("HS2022", "HS2017"),
    ]
    table = pd.DataFrame({"source_code": ["850110", "840110", "010121"], "target_code": ["840110", "840110", "010121"]})
    assert chapter_links({("HS2022", "HS2017"): table}, ["H5", "H6"], "HS2017") == [("85", "84")]


def test_chapter_blocks_keep_linked_chapters_together() -> None:
    counts = {"01": 4, "02": 4, "28": 3, "84": 5, "85": 5}
    assert chapter_blocks(counts, [], None) == [sorted(counts)]
    assert chapter_blocks(counts, [], 100) == [sorted(counts)]
    blocks = chapter_blocks(counts, [("85", "84")], 11)
    assert blocks == [["01", "02", "28"], ["84", "85"]]
    # Chaque chapitre dans exactement un bloc
    assert sorted(c for block in blocks for c in block) == sorted(counts)


def test_chapter_blocks_fail_explicitly_when_a_component_is_too_large() -> None:
    with pytest.raises(ValueError, match="cannot be split further"):
        chapter_blocks({"84": 6, "85": 6, "01": 1}, [("85", "84")], 10)
    with pytest.raises(ValueError, match="MAX_ROWS_PER_CHUNK=5"):
        chapter_blocks({"84": 6, "01": 1}, [], 5)


# ──────────────────────────────────────────────────────────────────────
# Restriction des millésimes, registre et fit_id
# ──────────────────────────────────────────────────────────────────────


def test_baci_targets_restrict_the_vintages() -> None:
    targets = {"HS2022": {"RESULT_SCHEMA": "a"}, "HS2017": {"RESULT_SCHEMA": "b"}, "HS1992": {"RESULT_SCHEMA": "c"}}
    assert list(select_targets(targets, requested_targets([], {"BACI_TARGETS": "HS1992"}))) == ["HS1992"]
    # La ligne de commande prime sur l'environnement ; ordre de la configuration conservé
    chosen = requested_targets(["--targets", "HS1992,HS2022"], {"BACI_TARGETS": "HS2017"})
    assert list(select_targets(targets, chosen)) == ["HS2022", "HS1992"]
    assert list(select_targets(targets, requested_targets([], {}))) == list(targets)
    with pytest.raises(ValueError, match="Unknown BACI targets"):
        select_targets(targets, ["HS2030"])


def test_years_written_and_fit_id() -> None:
    entry = RegistryEntry(baci_unit("HS2017"), extra={"fit_id": "f", "years_written": []})
    entry = with_year_written(with_year_written(entry, 2019), 2018)
    assert entry.extra["years_written"] == [2018, 2019] and entry.extra["fit_id"] == "f"
    requested = baci_requested(BaciConfig())
    watermark = UpstreamMark.of_entry(datetime(2026, 9, 1, tzinfo=timezone.utc), 5)
    same = compute_fit_id("HS2017", [2018, 2019], watermark, requested)
    assert same == compute_fit_id("HS2017", [2019, 2018], watermark, requested)
    # Changement de périmètre : nouvel identifiant, donc nouveaux fichiers de travail
    other = compute_fit_id("HS2017", [2018, 2019, 2020], watermark, requested)
    assert other != same
    assert work_root("w", "HS2017", other, None) != work_root("w", "HS2017", same, None)


# ──────────────────────────────────────────────────────────────────────
# Entrées / sorties DuckDB et écriture DuckLake de bout en bout
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def comtrade_catalog(ducklake_conn):
    """Table de faits Comtrade fictive dans un catalogue DuckLake temporaire."""
    conn, alias = ducklake_conn
    df = comtrade_declarations(seed=3, density=0.2, years=(2018, 2019, 2020, 2021))
    conn.register("_src", df)
    conn.execute(f'CREATE SCHEMA {alias}."C_A_HS"')
    conn.execute(f'CREATE TABLE {alias}."C_A_HS".fact_table AS SELECT * FROM _src')
    conn.unregister("_src")
    return conn, alias, df


def _harmonizer() -> HsHarmonizer:
    return HsHarmonizer({}, target_vintage="HS2017")


def _monobloc(df: pd.DataFrame) -> pd.DataFrame:
    df_dist, df_geo = cepii_tables(seed=0)
    result, _ = run_baci(_harmonizer().fit_transform(df), df_dist, df_geo, config=BaciConfig())
    return result


def _io(conn, alias, root: Path, *, on_year_written=None, max_rows=None):
    writer = DuckLakeYearWriter(
        conn, catalog_alias=alias, schema="baci_hs2017", primary_keys=KEYS,
        columns={"fit_id": "fit-1", "is_provisional": True}, run_id="wf-test",
        commit_message="test", on_year_written=on_year_written,
    )
    io = DuckDBPassIO(
        conn, source_schema="C_A_HS", years=[2018, 2019, 2020, 2021], root=root.as_posix(),
        writer=writer, harmonizer_factory=_harmonizer, max_rows_per_chunk=max_rows,
    )
    return io, writer


def _written(conn, alias) -> pd.DataFrame:
    return conn.execute(f'SELECT * FROM {alias}."baci_hs2017".fact_table').df()


@pytest.mark.slow
@pytest.mark.parametrize("max_rows", [None, 6000], ids=["years", "chapter-blocks"])
def test_duckdb_passes_write_the_monobloc_result(comtrade_catalog, tmp_path, max_rows) -> None:
    conn, alias, df = comtrade_catalog
    df_dist, df_geo = cepii_tables(seed=0)
    written_years = []
    io, writer = _io(conn, alias, tmp_path / "work", max_rows=max_rows,
                     on_year_written=lambda year, rows: written_years.append((year, rows)))
    report, rows_by_year = run_baci_passes(io, df_dist, df_geo, config=BaciConfig())

    # Découpage effectif en blocs quand la limite est basse
    blocks = {(year, block) for year, block, _ in io.chunks_read}
    assert len(blocks) == (4 if max_rows is None else 8)
    # Table écrite = redressement monobloc, avec fit_id et étiquette provisoire
    out = _written(conn, alias)
    expected = _monobloc(df)
    pd.testing.assert_frame_equal(
        out[expected.columns].sort_values(KEYS).reset_index(drop=True),
        expected.sort_values(KEYS).reset_index(drop=True),
        check_dtype=False, rtol=1e-8,
    )
    assert set(out["fit_id"]) == {"fit-1"} and out["is_provisional"].all()
    assert writer.created and report.flows == len(out)
    assert written_years == list(rows_by_year.itertuples(index=False, name=None))
    # Rapport d'harmonisation fusionné : toutes les déclarations lues
    assert io.harmonization_report().n_input_rows == len(df)
    # Fichiers de travail présents, puis supprimés en fin de passe réussie
    assert (tmp_path / "work" / "mirror").exists() and (tmp_path / "work" / "p0" / "done.parquet").exists()
    io.cleanup()
    assert not (tmp_path / "work").exists()


@pytest.mark.slow
def test_interrupted_pass_is_resumed_with_the_same_fit_id(comtrade_catalog, tmp_path) -> None:
    conn, alias, df = comtrade_catalog
    df_dist, df_geo = cepii_tables(seed=0)
    registry = {"years_written": []}

    def interrupt_after_two_years(year: int, rows: int) -> None:
        registry["years_written"].append(year)
        if len(registry["years_written"]) == 2:
            raise RuntimeError("pod interrompu")

    io, _ = _io(conn, alias, tmp_path / "work", on_year_written=interrupt_after_two_years)
    with pytest.raises(RuntimeError, match="pod interrompu"):
        run_baci_passes(io, df_dist, df_geo, config=BaciConfig())
    # Table mixte : deux années écrites sous ce fit_id, fichiers de travail conservés
    assert sorted(_written(conn, alias)["year"].unique()) == [2018, 2019]
    assert registry["years_written"] == [2018, 2019]

    # Reprise : même fit_id → passe de préparation réutilisée, tout est réécrit
    registry["years_written"] = []
    resumed, _ = _io(conn, alias, tmp_path / "work",
                     on_year_written=lambda year, rows: registry["years_written"].append(year))
    run_baci_passes(resumed, df_dist, df_geo, config=BaciConfig())
    assert resumed.chunks_read == []
    assert registry["years_written"] == [2018, 2019, 2020, 2021]
    out = _written(conn, alias)
    expected = _monobloc(df)
    pd.testing.assert_frame_equal(
        out[expected.columns].sort_values(KEYS).reset_index(drop=True),
        expected.sort_values(KEYS).reset_index(drop=True),
        check_dtype=False, rtol=1e-8,
    )


# ──────────────────────────────────────────────────────────────────────
# Mémoire : le pic des passes ne croît pas avec le nombre d'années
# ──────────────────────────────────────────────────────────────────────


def _peak_mib(fn) -> float:
    """Pic d'allocation Python (tracemalloc, numpy compris) d'un appel, en Mio."""
    import gc
    import tracemalloc

    gc.collect()
    tracemalloc.start()
    try:
        fn()
        return tracemalloc.get_traced_memory()[1] / 2 ** 20
    finally:
        tracemalloc.stop()


@pytest.mark.slow
def test_memory_of_the_passes_is_bounded_by_one_chunk(tmp_path) -> None:
    import duckdb

    df_dist, df_geo = cepii_tables(seed=0)
    peaks = {}
    for n_years in (4, 12):
        years = tuple(range(2010, 2010 + n_years))
        df = comtrade_declarations(seed=1, density=0.3, years=years)
        monobloc = _peak_mib(lambda: run_baci(df, df_dist, df_geo, config=BaciConfig()))
        conn = duckdb.connect()
        conn.execute('CREATE SCHEMA "C_A_HS"')
        conn.register("_src", df)
        conn.execute('CREATE TABLE "C_A_HS".fact_table AS SELECT * FROM _src')
        conn.unregister("_src")
        del df
        io = DuckDBPassIO(
            conn, source_schema="C_A_HS", years=list(years), root=(tmp_path / f"w{n_years}").as_posix(),
            writer=lambda year, blocks: [len(block) for block in blocks],
        )
        passes = _peak_mib(lambda: run_baci_passes(io, df_dist, df_geo, config=BaciConfig()))
        peaks[n_years] = (monobloc, passes)
    # Monobloc : pic proportionnel au nombre d'années ; passes : quasi constant
    assert peaks[12][0] > 2.5 * peaks[4][0]
    assert peaks[12][1] < 1.5 * peaks[4][1]
    assert peaks[12][1] < 0.4 * peaks[12][0]


def test_exists_distinguishes_a_present_file_from_an_absent_one(tmp_path) -> None:
    """Un chemin sans caractère générique n'est signalé présent que s'il existe réellement."""
    import duckdb

    (tmp_path / "done.parquet").touch()
    io = DuckDBPassIO.__new__(DuckDBPassIO)
    io.conn = duckdb.connect()
    assert io._exists(f"{tmp_path}/done.parquet")
    assert not io._exists(f"{tmp_path}/absent.parquet")
    assert io._exists(f"{tmp_path}/*.parquet")


# ──────────────────────────────────────────────────────────────────────
# Décision sur les données modifiées et lectures épinglées
# ──────────────────────────────────────────────────────────────────────


def test_vintage_mark_keeps_the_fit_id_stable_while_data_does_not_change() -> None:
    from kedro_pipeline.io.freshness import UpstreamMark, parse_instant
    from kedro_pipeline.steps.baci import vintage_mark

    t1 = parse_instant("2026-10-01")
    marks = {2018: {("a",): UpstreamMark(t1, 4)}, 2019: {("a",): UpstreamMark(t1, 9), ("b",): UpstreamMark()}}
    mark = vintage_mark(marks, [2018, 2019])
    assert mark.snapshot == 9 and vintage_mark(marks, [2030]) is None
    requested = baci_requested(BaciConfig())
    # Re-téléchargement sans modification : même marque, même fit_id (reprise idempotente)
    assert compute_fit_id("HS2017", [2018, 2019], mark, requested) == compute_fit_id(
        "HS2017", [2018, 2019], vintage_mark(marks, [2019, 2018]), requested
    )
    revised = {**marks, 2019: {("a",): UpstreamMark(t1, 10)}}
    assert compute_fit_id("HS2017", [2018, 2019], vintage_mark(revised, [2018, 2019]), requested) != (
        compute_fit_id("HS2017", [2018, 2019], mark, requested)
    )


def test_baci_inputs_cover_cepii_and_the_concordances_actually_used(tmp_path) -> None:
    from kedro_pipeline.io.freshness import FreshnessRegistry, RegistryEntry
    from kedro_pipeline.steps.baci import baci_inputs, concordance_checksums, stale_concordance_vintages

    dist, geo = cepii_tables()
    # Nouveau fichier de distances : une paire de moins, nouvelle empreinte
    assert baci_requested(BaciConfig(), baci_inputs(dist, geo)) != baci_requested(
        BaciConfig(), baci_inputs(dist.iloc[1:], geo)
    )

    # Tables de passage : seules celles utilisées par la passe du millésime comptent
    registry = {"HS2022-HS2017": {"checksum": "x"}}
    state = FreshnessRegistry(f"{tmp_path.as_posix()}/{{vintage}}.json", None, "baci", lambda u: u.key)
    state.upsert(RegistryEntry(baci_unit("HS2017"), extra={
        "concordance_checksums": concordance_checksums(registry, [("HS2022", "HS2017")]),
    }))
    state.upsert(RegistryEntry(baci_unit("HS2012"), extra={"concordance_checksums": {}}))
    # Table téléchargée pour une autre étape : aucun millésime n'est réestimé
    assert stale_concordance_vintages(state, ["HS2017", "HS2012"], {**registry, "HS2022-HS2012": {"checksum": "z"}}) == []
    # Table utilisée rafraîchie : réestimation du seul millésime concerné
    assert stale_concordance_vintages(state, ["HS2017", "HS2012"], {"HS2022-HS2017": {"checksum": "y"}}) == ["HS2017"]


def test_baci_rule_on_marks_respects_the_refresh_interval() -> None:
    from datetime import timedelta

    from kedro_pipeline.io.freshness import UpstreamMark, parse_instant
    from kedro_pipeline.steps.baci import baci_is_new_data, completed_entry

    unit, now = baci_unit("HS2017"), parse_instant("2026-10-15")
    mark = UpstreamMark(parse_instant("2026-10-01"), 12)
    plan = SimpleNamespace(reason="first")
    entry = completed_entry(unit, plan, "f", [2018], now - timedelta(days=8), mark, {"baci": "x"})
    assert entry.extra["upstream_snapshot"] == 12
    rule = baci_is_new_data({unit: [2018]}, {"MIN_INTERVAL_DAYS": 7}, now)
    assert not rule(unit, entry, mark)
    assert rule(unit, entry, UpstreamMark(mark.modified, 13))
    # Intervalle minimal non écoulé : la révision attend la passe suivante
    recent = completed_entry(unit, plan, "f", [2018], now - timedelta(days=2), mark, {"baci": "x"})
    assert not rule(unit, recent, UpstreamMark(mark.modified, 13))


def test_comtrade_reads_can_be_pinned_at_a_snapshot(comtrade_catalog) -> None:
    from kedro_pipeline.io.cursors import current_snapshot
    from kedro_pipeline.steps.baci import classifications_by_year, read_comtrade_year, rows_by_chapter

    conn, alias, df = comtrade_catalog
    conn.execute(f"USE {alias}")
    pinned = current_snapshot(conn, alias)
    conn.execute(f'DELETE FROM {alias}."C_A_HS".fact_table WHERE CAST(substr(CAST("period" AS VARCHAR), 1, 4) AS INTEGER) = 2019')

    assert 2019 in classifications_by_year(conn, "C_A_HS", years=[2019], at_snapshot=pinned)
    assert classifications_by_year(conn, "C_A_HS", years=[2019]) == {}
    assert sum(rows_by_chapter(conn, "C_A_HS", year=2019, at_snapshot=pinned).values()) > 0
    assert len(read_comtrade_year(conn, "C_A_HS", ["period"], year=2019, at_snapshot=pinned)) > 0
