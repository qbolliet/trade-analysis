"""Tests de la page de référence des paramètres (``kedro trade docs-config``).

La page est générée depuis les commentaires YAML de ``config/base`` : les tests couvrent
l'attribution des commentaires aux clés, la mise en forme des valeurs et la commande, sur
des fichiers temporaires puis sur la configuration réelle du dépôt.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

# La lecture des commentaires exige l'extra `docs`
pytest.importorskip("ruamel.yaml")

from kedro_pipeline.docs_config import build_configuration_page, format_value, read_sections

ROOT = Path(__file__).resolve().parents[1]


def _write(folder: Path, name: str, text: str) -> Path:
    """Écrit un fichier de paramètres temporaire."""
    path = folder / name
    path.write_text(text, encoding="utf-8")
    return path


def test_comment_above_inline_and_blank_line(tmp_path: Path) -> None:
    """Commentaire au-dessus de la clé et en fin de ligne ; un commentaire isolé par une
    ligne vide n'est pas attribué à la clé suivante."""
    path = _write(
        tmp_path, "parameters_x.yml",
        "# Bloc racine\n"
        "x:\n"
        "  # Seuil des alertes\n"
        "  # sur deux lignes\n"
        "  limit: 3\n"
        "  other: true  # Drapeau\n"
        "\n"
        "  # Commentaire orphelin\n"
        "\n"
        "  bare: null\n",
    )
    (section,) = read_sections(path)
    assert section.key == "x"
    assert list(section.comment_lines) == ["Bloc racine"]
    comments = {row.path: row.comment for row in section.rows}
    assert comments == {"x.limit": "Seuil des alertes sur deux lignes", "x.other": "Drapeau", "x.bare": ""}
    values = {row.path: row.value for row in section.rows}
    assert values == {"x.limit": "3", "x.other": "true", "x.bare": "null"}


def test_section_rows_only_when_commented(tmp_path: Path) -> None:
    """Un sous-mapping sans commentaire n'ajoute pas de ligne de section ; avec commentaire, oui."""
    path = _write(
        tmp_path, "parameters_x.yml",
        "x:\n"
        "  plain:\n"
        "    a: 1\n"
        "  # Sous-bloc documenté\n"
        "  documented:\n"
        "    b: [1, 2]\n",
    )
    (section,) = read_sections(path)
    assert [(row.path, row.is_section) for row in section.rows] == [
        ("x.plain.a", False), ("x.documented", True), ("x.documented.b", False),
    ]


def test_merged_keys_do_not_fail(tmp_path: Path) -> None:
    """Une clé héritée d'une ancre YAML (``<<: *base``) n'a pas de position : lue sans commentaire."""
    path = _write(
        tmp_path, "parameters_x.yml",
        "x:\n"
        "  base: &base\n"
        "    # Commentaire de la clé d'ancre\n"
        "    shared: 1\n"
        "  derived:\n"
        "    <<: *base\n"
        "    own: 2\n",
    )
    (section,) = read_sections(path)
    paths = [row.path for row in section.rows]
    assert "x.derived.shared" in paths and "x.derived.own" in paths


def test_format_value_truncates_and_keeps_unicode() -> None:
    """Valeurs sur une ligne, accents conservés, coupure par une ellipse."""
    assert format_value("é", 20) == '"é"'
    assert format_value(list(range(100)), 20).endswith("…")
    assert len(format_value(list(range(100)), 20)) == 20


def test_page_escapes_table_separators(tmp_path: Path) -> None:
    """Une barre verticale dans une valeur reste dans la cellule de code ; dans un commentaire,
    elle est échappée pour ne pas ajouter de colonne."""
    _write(tmp_path, "parameters_x.yml", 'x:\n  # a | b\n  regex: "(a|b)"\n')
    page = build_configuration_page(tmp_path)
    assert '| `x.regex` | `"(a|b)"` | a \\| b |' in page


def test_empty_folder_raises(tmp_path: Path) -> None:
    """Un dossier sans fichier de paramètres est une erreur explicite."""
    with pytest.raises(FileNotFoundError):
        build_configuration_page(tmp_path)


def test_docs_config_command_on_repository_config(tmp_path: Path) -> None:
    """La commande documente chaque fichier ``parameters_*.yml`` de config/base, sans secret."""
    from kedro_pipeline.cli import docs_config

    output = tmp_path / "configuration.md"
    result = CliRunner().invoke(docs_config, ["--output", str(output)])
    assert result.exit_code == 0, result.output
    page = output.read_text(encoding="utf-8")
    for path in sorted((ROOT / "config" / "base").glob("parameters_*.yml")):
        assert f"## `{path.name}`" in page
    assert "`tracking.REPORT.PLOTLY_JS`" in page
    # Fins de ligne LF, quel que soit le système
    assert b"\r" not in output.read_bytes()
