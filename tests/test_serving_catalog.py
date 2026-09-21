"""Tests de ``ServingCatalog`` (PS-29.1) et de la lecture « comme Superset » (PS-30.1).

Catalogues DuckLake fichiers, aucune connexion réseau. Le lecteur concurrent est un
``cursor()`` de la session d'écriture : même instance DuckDB, transaction distincte
(DuckDB refuse un second ATTACH du même fichier ``.ducklake`` dans un processus ; en
production, catalogue PostgreSQL, Superset est un autre processus).
"""

from __future__ import annotations

import pandas as pd
import pytest

from kedro_pipeline.io.serving import ServingPublicationError, ServingTableSpec

# Requête source : grille des indicateurs (deux années, 2019 et 2023)
_SOURCE = """
    SELECT CAST(substr(CAST(p."TIME_PERIOD" AS VARCHAR), 1, 4) AS INTEGER) AS year,
           CAST(p."reporter" AS VARCHAR) AS reporter,
           CAST(p."product" AS VARCHAR) AS product,
           CAST(p."flow" AS VARCHAR) AS flow,
           CAST(p."HHI" AS DOUBLE) * {factor} AS hhi
    FROM {indicators} AS p
    WHERE CAST(p."indicators" AS VARCHAR) = 'VALUE_IN_EUROS'
"""


def _specs(world, factor: float = 1.0, extra: str = "", broken: bool = False):
    """Deux tables : une partitionnée (cells), une petite (reporters)."""
    sql = _SOURCE.format(factor=factor, indicators=world.tables["indicators"].qualified_name)
    if extra:
        sql = sql.replace("AS hhi", f"AS hhi, {extra}")
    cells = ServingTableSpec(
        "cells", sql, partitioned=True, sort_by=("year", "reporter", "product")
    )
    reporters = ServingTableSpec(
        "reporters",
        "SELECT * FROM missing_table" if broken else f"SELECT DISTINCT reporter FROM ({sql})",
    )
    return [cells, reporters]


def _read(conn, world, table: str) -> pd.DataFrame:
    """Relit une table de service, triée."""
    df = conn.execute(f"SELECT * FROM {world.catalog.qualified_name(table)}").df()
    return df.sort_values(list(df.columns)).reset_index(drop=True)


def test_publish_is_a_single_transaction(serving_world) -> None:
    world = serving_world
    with world.catalog.connect() as conn:
        world.catalog.publish(_specs(world, 1.0), "full", conn=conn)
        before_cells = _read(conn, world, "cells")
        seen = {}

        def reader(writer) -> None:
            # Lecteur concurrent pendant la transaction : ancien état des DEUX tables
            cursor = writer.cursor()
            seen["cells"] = _read(cursor, world, "cells")
            seen["n_reporters"] = cursor.execute(
                f"SELECT count(*) FROM {world.catalog.qualified_name('reporters')}"
            ).fetchone()[0]
            seen["cursor"] = cursor

        world.catalog.publish(_specs(world, 2.0), "full", before_commit=reader, conn=conn)
        pd.testing.assert_frame_equal(seen["cells"], before_cells)
        # Après COMMIT, le même lecteur voit le nouvel état
        after = _read(seen["cursor"], world, "cells")
        assert (after["hhi"].round(9) == (before_cells["hhi"] * 2).round(9)).all()


def test_rollback_on_failure_keeps_previous_state(serving_world) -> None:
    world = serving_world
    with world.catalog.connect() as conn:
        world.catalog.publish(_specs(world, 1.0), "full", conn=conn)
        before = _read(conn, world, "cells")
        with pytest.raises(ServingPublicationError) as info:
            # cells est réécrite (×3) avant l'échec de reporters : tout est annulé
            world.catalog.publish(_specs(world, 3.0, broken=True), "full", conn=conn)
        assert info.value.table == "reporters"
        pd.testing.assert_frame_equal(_read(conn, world, "cells"), before)
        assert len(_read(conn, world, "reporters")) == 3


def test_partitioning_is_effective(serving_world) -> None:
    world = serving_world
    with world.catalog.connect() as conn:
        stats = world.catalog.publish(_specs(world), "full", conn=conn)
        files = [
            row[0]
            for row in conn.execute(
                "SELECT data_file FROM ducklake_list_files(?, 'cells', schema => ?)",
                [world.params["CATALOG_ALIAS"], world.params["SCHEMA"]],
            ).fetchall()
        ]
        assert stats["cells"].files == len(files) == 2
        assert {f.split("year=")[1][:4] for f in files} == {"2019", "2023"}
        # Élagage : une lecture filtrée sur year ne lit qu'un fichier
        plan = conn.execute(
            f"EXPLAIN ANALYZE SELECT sum(hhi) FROM {world.catalog.qualified_name('cells')} "
            "WHERE year = 2023"
        ).fetchall()[0][1]
        assert "Total Files Read: 1" in plan
        assert stats["reporters"].files == 1


def test_by_year_only_changes_requested_years(serving_world) -> None:
    world = serving_world
    with world.catalog.connect() as conn:
        world.catalog.publish(_specs(world, 1.0), "full", conn=conn)
        before = _read(conn, world, "cells")
        stats = world.catalog.publish(_specs(world, 5.0), "by_year", years=[2023], conn=conn)
        assert stats["cells"].mode == "by_year"
        # Table non partitionnée : toujours recréée
        assert stats["reporters"].mode == "full"
        after = _read(conn, world, "cells")
        assert len(after) == len(before)
        old, new = before[before["year"] == 2019], after[after["year"] == 2019]
        pd.testing.assert_frame_equal(old.reset_index(drop=True), new.reset_index(drop=True))
        ratio = (
            after[after["year"] == 2023]["hhi"].sum() / before[before["year"] == 2023]["hhi"].sum()
        )
        assert ratio == pytest.approx(5.0)


def test_by_year_falls_back_to_full_on_new_column(serving_world) -> None:
    world = serving_world
    with world.catalog.connect() as conn:
        world.catalog.publish(_specs(world, 1.0), "full", conn=conn)
        stats = world.catalog.publish(
            _specs(world, 5.0, extra="1 AS new_column"), "by_year", years=[2023], conn=conn
        )
        assert stats["cells"].mode == "full"
        after = _read(conn, world, "cells")
        assert "new_column" in after.columns
        # Retour en full : toutes les années reflètent la nouvelle requête
        assert after["new_column"].notna().all()


def test_by_year_requires_years(serving_world) -> None:
    with pytest.raises(ValueError):
        serving_world.catalog.publish(_specs(serving_world), "by_year", years=[])
    with pytest.raises(ValueError):
        serving_world.catalog.publish(_specs(serving_world), "weekly")


def test_republishing_is_idempotent(serving_world) -> None:
    world = serving_world
    with world.catalog.connect() as conn:
        first = world.catalog.publish(_specs(world), "full", conn=conn)
        content = {name: _read(conn, world, name) for name in first}
        second = world.catalog.publish(_specs(world), "full", conn=conn)
        for name in first:
            pd.testing.assert_frame_equal(_read(conn, world, name), content[name])
            assert second[name].rows == first[name].rows


def test_read_like_superset(serving_world) -> None:
    """SQLAlchemy + duckdb-engine, ATTACH READ_ONLY du seul catalogue serving (PS-30.1)."""
    sqlalchemy = pytest.importorskip("sqlalchemy")
    pytest.importorskip("duckdb_engine")
    world = serving_world
    world.catalog.publish(_specs(world))  # session d'écriture fermée ensuite

    catalog_file = (world.root / f"{world.params['DBNAME']}.ducklake").as_posix()
    engine = sqlalchemy.create_engine("duckdb:///:memory:")

    @sqlalchemy.event.listens_for(engine, "connect")
    def _attach(dbapi_connection, _record) -> None:
        # Mécanisme (b) de PS-30.1 : écouteur « connect » qui attache serving en lecture seule
        dbapi_connection.execute("LOAD ducklake")
        dbapi_connection.execute(f"ATTACH 'ducklake:{catalog_file}' AS serving (READ_ONLY)")
        dbapi_connection.execute("USE serving")

    schema = world.params["SCHEMA"]
    with engine.connect() as connection:
        assert f"serving.{schema}" in sqlalchemy.inspect(connection).get_schema_names()
        rows = connection.execute(
            sqlalchemy.text(
                f"SELECT reporter, product, hhi FROM {schema}.cells "
                "WHERE year = :year AND reporter = :reporter"
            ),
            {"year": 2023, "reporter": "FR"},
        ).fetchall()
        assert len(rows) == 4 * 2  # 4 produits × 2 flux
        with pytest.raises(sqlalchemy.exc.DBAPIError):
            connection.execute(sqlalchemy.text(f"DELETE FROM {schema}.cells"))
    engine.dispose()
