"""Reference page of the pipeline parameters, generated for the documentation site.

:func:`build_configuration_page` reads the ``parameters_*.yml`` files of a
configuration folder and renders one Markdown table per root key: dotted key,
value and the YAML comment written above the key (or, failing that, at the end
of its line). The page is used by ``kedro trade docs-config`` and is never
edited by hand: the comments of the YAML files are its only source, so that the
reference cannot drift from the configuration.

The comments are read with ``ruamel.yaml`` (``docs`` extra of the project),
imported lazily so that the rest of the project does not depend on it.
"""
# Importation des modules
# Modules de base
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Sequence

# Motif des fichiers de paramètres lus
PARAMETER_FILES_GLOB = "parameters_*.yml"

# Texte d'introduction de la page générée
_INTRODUCTION = (
    "Référence des paramètres de `{source}`, **générée** par `kedro trade docs-config` "
    "à partir des fichiers `{pattern}` : la colonne « Commentaire » reprend le commentaire "
    "YAML placé au-dessus de la clé. Les valeurs sont celles de l'environnement `base` ; "
    "les environnements `demo` et `cloud` en surchargent certaines. Cette page ne doit pas "
    "être éditée à la main : modifier les commentaires dans les fichiers YAML."
)


# Ligne du tableau de référence
@dataclass(frozen=True)
class ParameterRow:
    """One line of the reference table.

    Attributes:
        path: Dotted path of the key (``tracking.REPORT.HTML``).
        value: Formatted value of the key, empty for a section (mapping).
        comment: YAML comment of the key, lines joined by a space.
        is_section: Whether the key holds a mapping that is described by its own rows.
        depth: Nesting level of the key (0 for a root key).
    """

    path: str
    value: str
    comment: str
    is_section: bool
    depth: int


# Section d'un fichier : une clé racine et ses lignes
@dataclass(frozen=True)
class ParameterSection:
    """Root key of a parameter file, with its comment and the rows below it.

    Attributes:
        file_name: Name of the YAML file.
        key: Root key.
        comment_lines: Comment lines written above the root key (line breaks kept).
        rows: Rows of the nested keys, in file order.
    """

    file_name: str
    key: str
    comment_lines: Sequence[str]
    rows: Sequence[ParameterRow]


# Chargeur YAML conservant les positions des clés
def _load_yaml(text: str) -> Any:
    """Parse a YAML text with ``ruamel.yaml``, keeping comments and key positions.

    Args:
        text: YAML text.

    Returns:
        The parsed document (``CommentedMap`` for a mapping).

    Raises:
        ImportError: If ``ruamel.yaml`` is not installed.
    """
    try:
        from ruamel.yaml import YAML
    except ImportError as error:
        raise ImportError(
            "ruamel.yaml is required to read the YAML comments: "
            "install the documentation dependencies (`uv sync --extra docs`)."
        ) from error
    return YAML(typ="rt").load(text)


# Nettoyage d'une ligne de commentaire
def _strip_comment_line(line: str) -> str:
    """Remove the comment marker and its single following space.

    Args:
        line: Raw line of a YAML file starting (after indentation) with ``#``.

    Returns:
        The comment text, empty for a bare ``#`` line.

    Examples:
        >>> _strip_comment_line("  # Seuil des alertes")
        'Seuil des alertes'
        >>> _strip_comment_line("  #")
        ''
    """
    text = line.strip().lstrip("#")
    return text[1:] if text.startswith(" ") else text


# Commentaire situé au-dessus d'une clé
def _comment_above(lines: Sequence[str], key_line: int) -> List[str]:
    """Collect the block of comment lines directly above a key.

    The block stops at the first blank line or non-comment line, so a comment
    separated from the key by a blank line is not attributed to it.

    Args:
        lines: Lines of the YAML file.
        key_line: Zero-based line number of the key.

    Returns:
        The comment lines, in file order, without their ``#`` marker.

    Examples:
        >>> _comment_above(["# a", "# b", "key: 1"], 2)
        ['a', 'b']
        >>> _comment_above(["# a", "", "key: 1"], 2)
        []
    """
    block: List[str] = []
    index = key_line - 1
    while index >= 0 and lines[index].lstrip().startswith("#"):
        block.append(_strip_comment_line(lines[index]))
        index -= 1
    return block[::-1]


# Commentaire en fin de ligne d'une clé
def _inline_comment(mapping: Any, key: Any, key_line: int) -> Optional[str]:
    """Return the comment written at the end of the line of a key, if any.

    ``ruamel.yaml`` also attaches to a key the comment lines that precede the
    next key: the token is kept only when it starts on the line of the key.

    Args:
        mapping: ``CommentedMap`` that holds the key.
        key: Key of the mapping.
        key_line: Zero-based line number of the key.

    Returns:
        The comment text, ``None`` when the line has no trailing comment.
    """
    tokens = mapping.ca.items.get(key)
    token = tokens[2] if tokens and len(tokens) > 2 else None
    if token is None or not token.value.startswith("#"):
        return None
    if token.start_mark.line != key_line:
        return None
    return _strip_comment_line(token.value.splitlines()[0])


# Position d'une clé dans le fichier
def _key_line(mapping: Any, key: Any) -> Optional[int]:
    """Return the zero-based line of a key, ``None`` when the file does not hold it.

    A key brought by a YAML merge (``<<: *anchor``) is written elsewhere than in
    the mapping that exposes it, so it has no position there.

    Args:
        mapping: ``CommentedMap`` that exposes the key.
        key: Key of the mapping.

    Returns:
        The line number, or ``None`` for a merged key.
    """
    try:
        return mapping.lc.key(key)[0]
    except KeyError:
        return None


# Conversion en structure Python simple
def _to_plain(value: Any) -> Any:
    """Convert ``ruamel.yaml`` containers to plain ``dict`` / ``list`` objects.

    Args:
        value: Parsed YAML value.

    Returns:
        The same value with mappings and sequences as built-in containers.

    Examples:
        >>> _to_plain([1, 2])
        [1, 2]
    """
    if isinstance(value, dict):
        return {str(key): _to_plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_plain(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    # Types spécifiques de ruamel (scalaires typés, dates)
    return str(value)


# Mise en forme d'une valeur
def format_value(value: Any, max_length: int) -> str:
    """Format a parameter value on a single line, truncated to ``max_length`` characters.

    Args:
        value: Parsed YAML value (scalar, list or mapping).
        max_length: Maximum number of characters kept; the value is cut with an ellipsis
            beyond it.

    Returns:
        A compact JSON-like representation (``null``, ``true``, quoted strings…).

    Examples:
        >>> format_value(None, 20)
        'null'
        >>> format_value(["a", "b"], 20)
        '["a", "b"]'
        >>> format_value("x" * 30, 10)
        '"xxxxxxxx…'
    """
    text = json.dumps(_to_plain(value), ensure_ascii=False)
    return text if len(text) <= max_length else text[: max_length - 1] + "…"


# Extraction des lignes d'un mapping
def _collect_rows(
    mapping: Any, lines: Sequence[str], prefix: str, depth: int, max_value_length: int
) -> List[ParameterRow]:
    """Flatten a mapping into rows, depth first, in file order.

    A key holding a non-empty mapping gives a section row only when it carries a
    comment (otherwise its children are enough); every other key gives a row
    with its value. Sequences are leaves.

    Args:
        mapping: ``CommentedMap`` to flatten.
        lines: Lines of the YAML file, to read the comments above the keys.
        prefix: Dotted path of the mapping (empty for the document root).
        depth: Nesting level of the mapping's keys.
        max_value_length: Maximum length of a formatted value.

    Returns:
        The rows of the mapping and of its sub-mappings.
    """
    rows: List[ParameterRow] = []
    for key, value in mapping.items():
        key_line = _key_line(mapping, key)
        path = f"{prefix}.{key}" if prefix else str(key)
        comment_lines: List[str] = []
        if key_line is not None:
            comment_lines = _comment_above(lines, key_line)
            inline = _inline_comment(mapping, key, key_line)
            if inline:
                comment_lines = [*comment_lines, inline]
        comment = _join_comment(comment_lines)
        if isinstance(value, dict) and value:
            if comment:
                rows.append(ParameterRow(path, "", comment, True, depth))
            rows.extend(_collect_rows(value, lines, path, depth + 1, max_value_length))
        else:
            rows.append(
                ParameterRow(path, format_value(value, max_value_length), comment, False, depth)
            )
    return rows


# Assemblage des lignes d'un commentaire
def _join_comment(comment_lines: Sequence[str]) -> str:
    """Join comment lines into a single cell text.

    Consecutive lines are joined by a space; an empty comment line (paragraph
    break) becomes a line break.

    Args:
        comment_lines: Comment lines without their ``#`` marker.

    Returns:
        The joined text, empty when there is no comment.

    Examples:
        >>> _join_comment(["Seuil", "des alertes"])
        'Seuil des alertes'
        >>> _join_comment(["Premier", "", "Second"])
        'Premier<br>Second'
    """
    paragraphs: List[List[str]] = [[]]
    for line in comment_lines:
        if line.strip():
            paragraphs[-1].append(line.strip())
        elif paragraphs[-1]:
            paragraphs.append([])
    return "<br>".join(" ".join(words) for words in paragraphs if words)


# Lecture d'un fichier de paramètres
def read_sections(path: Path, max_value_length: int = 160) -> List[ParameterSection]:
    """Read a parameter file into one section per root key.

    Args:
        path: ``parameters_*.yml`` file.
        max_value_length: Maximum length of a formatted value.

    Returns:
        The sections of the file, in file order; empty if the file is not a mapping.

    Raises:
        ImportError: If ``ruamel.yaml`` is not installed.

    Examples:
        >>> import tempfile
        >>> folder = Path(tempfile.mkdtemp())
        >>> _ = (folder / "parameters_x.yml").write_text(
        ...     "# Bloc x\\nx:\\n  # Seuil\\n  limit: 3\\n", encoding="utf-8")
        >>> section = read_sections(folder / "parameters_x.yml")[0]
        >>> section.key, list(section.comment_lines), section.rows[0].path
        ('x', ['Bloc x'], 'x.limit')
    """
    text = path.read_text(encoding="utf-8")
    document = _load_yaml(text)
    if not isinstance(document, dict):
        return []
    lines = text.splitlines()
    sections: List[ParameterSection] = []
    for key, value in document.items():
        key_line = _key_line(document, key)
        comment_lines = _comment_above(lines, key_line) if key_line is not None else []
        if isinstance(value, dict) and value:
            rows = _collect_rows(value, lines, str(key), 1, max_value_length)
        else:
            rows = [ParameterRow(str(key), format_value(value, max_value_length), "", False, 0)]
        sections.append(ParameterSection(path.name, str(key), comment_lines, rows))
    return sections


# Cellule de tableau en code
def _code_cell(text: str) -> str:
    """Wrap a text in a Markdown code span for a table cell.

    Pipes need no escaping inside a code span: the Markdown table parser keeps
    them in the cell.

    Args:
        text: Text to display verbatim.

    Returns:
        The code span, delimited by double backticks when the text contains a backtick.

    Examples:
        >>> _code_cell("a|b")
        '`a|b`'
        >>> _code_cell("a`b")
        '`` a`b ``'
    """
    return f"`` {text} ``" if "`" in text else f"`{text}`"


# Rendu Markdown d'une section
def _render_section(section: ParameterSection) -> List[str]:
    """Render a section as a heading, its comment and a table.

    Args:
        section: Section to render.

    Returns:
        The Markdown lines of the section.
    """
    output = [f"### `{section.key}`", ""]
    # Commentaire de la clé racine, retours à la ligne conservés
    if section.comment_lines:
        for line in section.comment_lines:
            output.append(f"> {line.strip()}  " if line.strip() else ">")
        output.append("")
    output.extend(["| Clé | Valeur | Commentaire |", "|---|---|---|"])
    for row in section.rows:
        value = "*(section)*" if row.is_section else _code_cell(row.value)
        # Barre verticale hors code : séparateur de colonnes, à échapper
        comment = row.comment.replace("|", "\\|")
        output.append(f"| {_code_cell(row.path)} | {value} | {comment} |")
    output.append("")
    return output


# Construction de la page
def build_configuration_page(
    source_dir: Path, max_value_length: int = 160, source_label: Optional[str] = None
) -> str:
    """Build the Markdown reference page of the parameters of a configuration folder.

    Args:
        source_dir: Folder holding the ``parameters_*.yml`` files (``config/base``).
        max_value_length: Maximum length of a displayed value (longer values are cut).
        source_label: Folder name shown in the introduction (``source_dir`` when omitted).

    Returns:
        The Markdown page: one heading per file, one sub-heading and table per root key.

    Raises:
        FileNotFoundError: If ``source_dir`` holds no parameter file.
        ImportError: If ``ruamel.yaml`` is not installed.

    Examples:
        >>> import tempfile
        >>> folder = Path(tempfile.mkdtemp())
        >>> _ = (folder / "parameters_x.yml").write_text("x:\\n  # Seuil\\n  limit: 3\\n", encoding="utf-8")
        >>> page = build_configuration_page(folder)
        >>> "| `x.limit` | `3` | Seuil |" in page
        True
    """
    files = sorted(source_dir.glob(PARAMETER_FILES_GLOB))
    if not files:
        raise FileNotFoundError(f"No {PARAMETER_FILES_GLOB} file in {source_dir}")
    label = source_label if source_label is not None else str(source_dir)
    page = [
        "# Configuration", "",
        _INTRODUCTION.format(source=label, pattern=PARAMETER_FILES_GLOB), "",
    ]
    for path in files:
        page.extend([f"## `{path.name}`", ""])
        for section in read_sections(path, max_value_length):
            page.extend(_render_section(section))
    return "\n".join(page).rstrip() + "\n"
