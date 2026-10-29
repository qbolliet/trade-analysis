"""Construction de l'archive importable du tableau de bord Superset « Vulnérabilités ».

Le dossier versionné (``superset/vulnerabilites/`` : ``metadata.yaml``,
``databases/``, ``datasets/``, ``charts/``, ``dashboards/``) est le contenu d'un
export Superset décompressé. Ce script le réempaquette en une archive zip à
racine unique, le format attendu par ``POST /api/v1/dashboard/import/`` et par
``superset import-dashboards -p <archive>``, en appliquant au passage les
réglages propres à l'environnement cible :

- ``--assets`` : archive de type « assets », pour ``POST /api/v1/assets/import/``.
  L'import d'un tableau de bord ne remplace que le tableau de bord : les
  datasets et graphiques déjà présents (même uuid) sont conservés tels quels,
  si bien qu'une modification de leur SQL ou de leurs colonnes n'est pas
  appliquée. L'import « assets » remplace tout (connexion, datasets,
  graphiques, tableau de bord) : c'est la forme à utiliser pour mettre à jour
  un Superset où le tableau de bord existe déjà ;

- ``--schema`` : schéma du catalogue ``serving`` visé par les datasets. Les
  exports versionnés visent le schéma de production ``dashboard`` ; la recette
  utilise ``demo_dashboard``. La substitution porte sur la clé ``schema`` des
  datasets et sur le SQL des datasets virtuels (``serving.dashboard`` devient
  ``serving.<schéma>``) ;
- ``--default-year`` : valeur par défaut des filtres natifs portant sur la
  colonne ``year``. Un filtre natif n'a pas de valeur par défaut dynamique, et
  sans année fixée les classements de toutes les années se mélangent : la
  valeur doit être explicite, et correspondre à la dernière année classée de
  l'environnement ;
- ``--top-n`` : nombre d'unités les plus vulnérables tracées par défaut dans
  les courbes, porté par les paramètres de gabarit (``template_params``) des
  datasets virtuels qui le déclarent.

L'archive est validée avant écriture : fichiers YAML lisibles, références
croisées résolues (base de chaque dataset, dataset de chaque graphique et de
chaque filtre natif, graphiques de la mise en page), absence de mot de passe
dans les connexions, et absence dans le SQL des datasets virtuels des
constructions DuckDB ``PIVOT`` / ``UNPIVOT``, que l'analyseur SQL de Superset
rejette (le graphique échoue alors à l'affichage, pas à l'import).

Examples:
    Archive de recette (schéma de démonstration, dernière année classée 2021)::

        $ python tools/superset_bundle.py superset/vulnerabilites \\
            -o /tmp/vulnerabilites.zip --schema demo_dashboard --default-year 2021

    Import par l'API (jeton et jeton CSRF obtenus par ``/api/v1/security/``)::

        $ curl -X POST "$SUPERSET/api/v1/dashboard/import/" \\
            -H "Authorization: Bearer $TOKEN" -H "X-CSRFToken: $CSRF" \\
            -F formData=@/tmp/vulnerabilites.zip -F overwrite=true

    Mise à jour d'un Superset où le tableau de bord existe déjà::

        $ python tools/superset_bundle.py superset/vulnerabilites \\
            -o /tmp/vulnerabilites.zip --assets --default-year 2023
        $ curl -X POST "$SUPERSET/api/v1/assets/import/" \\
            -H "Authorization: Bearer $TOKEN" -H "X-CSRFToken: $CSRF" \\
            -F bundle=@/tmp/vulnerabilites.zip

    Import dans le conteneur Superset::

        $ superset import-dashboards -p /tmp/vulnerabilites.zip -u admin
"""

from __future__ import annotations

import argparse
import copy
import io
import json
import re
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional

import yaml

# Schéma de production visé par les exports versionnés
SOURCE_SCHEMA = "dashboard"
# Alias du catalogue DuckLake attaché par la connexion Superset
CATALOG_ALIAS = "serving"
# Sous-dossiers d'un export Superset
ASSET_DIRECTORIES = ("databases", "datasets", "charts", "dashboards")
# Colonne des filtres natifs d'année
YEAR_COLUMN = "year"
# Paramètre de gabarit du nombre d'unités tracées par défaut
TOP_N_PARAMETER = "top_n"
# Clés interdites dans une connexion exportée
SECRET_KEYS = ("password", "masked_encrypted_extra", "encrypted_extra")
# Type d'export versionné, et type d'une archive pour l'import « assets »
DASHBOARD_TYPE = "Dashboard"
ASSETS_TYPE = "assets"

# Identifiant de schéma admis (substitué dans du SQL)
_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Constructions DuckDB rejetées par l'analyseur SQL de Superset (sqlglot)
_UNSUPPORTED_SQL = re.compile(r"\b(UNPIVOT|PIVOT)\b", re.IGNORECASE)


# Exception levée lorsqu'un export est incohérent
class BundleError(ValueError):
    """Raised when the export folder is inconsistent or cannot be bundled."""


# Fonction de substitution du schéma dans un texte (clé schema ou SQL)
def substitute_schema(text: Optional[str], schema: str) -> Optional[str]:
    """Replace the production serving schema by ``schema`` in a text.

    Only the qualified form ``serving.dashboard`` is replaced (as a whole
    word), so a column or table merely containing ``dashboard`` is untouched.

    Args:
        text: Value of a ``schema`` key or SQL of a virtual dataset; ``None``
            is returned unchanged.
        schema: Target schema of the ``serving`` catalog.

    Returns:
        The text with ``serving.dashboard`` replaced by ``serving.<schema>``.

    Raises:
        BundleError: If ``schema`` is not a plain identifier.

    Examples:
        >>> substitute_schema("SELECT * FROM serving.dashboard.cell_scores", "demo_dashboard")
        'SELECT * FROM serving.demo_dashboard.cell_scores'
        >>> substitute_schema("serving.dashboards_old", "demo_dashboard")
        'serving.dashboards_old'
    """
    if not _SCHEMA_NAME.match(schema):
        raise BundleError(f"Invalid schema '{schema}': expected a plain identifier")
    if text is None:
        return None
    pattern = re.compile(rf"\b{CATALOG_ALIAS}\.{SOURCE_SCHEMA}\b")
    return pattern.sub(f"{CATALOG_ALIAS}.{schema}", text)


# Fonction de lecture des fichiers YAML d'un export
def load_assets(source: Path) -> Dict[str, Dict[str, Any]]:
    """Read every YAML file of an export folder.

    Args:
        source: Export folder (``metadata.yaml`` and the asset directories).

    Returns:
        Relative POSIX path -> parsed content, ``metadata.yaml`` included.

    Raises:
        BundleError: If ``metadata.yaml`` is missing or a file is not a YAML
            mapping.
    """
    if not (source / "metadata.yaml").is_file():
        raise BundleError(f"{source} has no metadata.yaml: not a Superset export folder")
    assets: Dict[str, Dict[str, Any]] = {}
    for path in sorted(source.rglob("*.yaml")):
        relative = path.relative_to(source).as_posix()
        if relative != "metadata.yaml" and relative.split("/")[0] not in ASSET_DIRECTORIES:
            continue
        content = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(content, dict):
            raise BundleError(f"{relative}: expected a YAML mapping")
        assets[relative] = content
    return assets


# Fonction de parcours des fichiers d'une catégorie
def _of_kind(assets: Mapping[str, Dict[str, Any]], kind: str) -> Iterator[Dict[str, Any]]:
    """Yield the assets stored under ``<kind>/``."""
    for path, content in assets.items():
        if path.startswith(f"{kind}/"):
            yield content


# Fonction de lecture d'un champ JSON exporté en texte ou en objet
def _as_mapping(value: Any) -> Dict[str, Any]:
    """Return ``value`` as a dict (``None``, JSON text or mapping)."""
    if value in (None, ""):
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


# Fonction d'application des réglages d'environnement
def configure_assets(
    assets: Mapping[str, Dict[str, Any]],
    *,
    schema: Optional[str] = None,
    default_year: Optional[int] = None,
    top_n: Optional[int] = None,
) -> Dict[str, Dict[str, Any]]:
    """Apply the target-environment settings to a copy of the assets.

    Args:
        assets: Parsed export (see :func:`load_assets`).
        schema: Target schema of the ``serving`` catalog; unchanged when
            ``None``.
        default_year: Default value of the native filters on ``year``;
            unchanged when ``None``.
        top_n: Number of most vulnerable units drawn by default, written in
            the ``template_params`` of the datasets that declare it; unchanged
            when ``None``.

    Returns:
        A configured deep copy of ``assets``.

    Raises:
        BundleError: If ``schema`` is invalid or ``top_n`` is not positive.

    Examples:
        >>> assets = {"datasets/serving/t.yaml": {"schema": "serving.dashboard", "sql": None}}
        >>> configure_assets(assets, schema="demo_dashboard")["datasets/serving/t.yaml"]["schema"]
        'serving.demo_dashboard'
    """
    if top_n is not None and int(top_n) < 1:
        raise BundleError(f"top_n must be a positive integer, got {top_n}")
    configured = copy.deepcopy(dict(assets))

    for dataset in _of_kind(configured, "datasets"):
        # Schéma de la table physique et SQL des datasets virtuels
        if schema is not None:
            dataset["schema"] = substitute_schema(dataset.get("schema"), schema)
            dataset["sql"] = substitute_schema(dataset.get("sql"), schema)
        # Nombre d'unités par défaut, seulement là où il est déclaré
        if top_n is not None:
            params = _as_mapping(dataset.get("template_params"))
            if TOP_N_PARAMETER in params:
                params[TOP_N_PARAMETER] = int(top_n)
                # Objet YAML, seule forme admise par la validation de l'import
                dataset["template_params"] = params

    if default_year is not None:
        for dashboard in _of_kind(configured, "dashboards"):
            metadata = dashboard.get("metadata") or {}
            for native_filter in metadata.get("native_filter_configuration") or []:
                columns = {
                    (target.get("column") or {}).get("name")
                    for target in native_filter.get("targets") or []
                }
                if YEAR_COLUMN not in columns:
                    continue
                # État par défaut ET filtre appliqué au premier affichage
                native_filter["defaultDataMask"] = {
                    **(native_filter.get("defaultDataMask") or {}),
                    "filterState": {"value": [int(default_year)]},
                    "extraFormData": {
                        "filters": [{"col": YEAR_COLUMN, "op": "IN", "val": [int(default_year)]}]
                    },
                }
    return configured


# Fonction de recherche récursive d'une clé
def _find_keys(value: Any, keys: tuple) -> List[str]:
    """Return the keys of ``keys`` present (non-empty) anywhere in ``value``."""
    found: List[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in keys and item not in (None, "", {}):
                found.append(key)
            found += _find_keys(item, keys)
    elif isinstance(value, list):
        for item in value:
            found += _find_keys(item, keys)
    return found


# Fonction de validation des références croisées
def validate_assets(assets: Mapping[str, Dict[str, Any]]) -> None:
    """Check that an export is self-contained and carries no secret.

    Args:
        assets: Parsed (possibly configured) export.

    Raises:
        BundleError: Listing every problem found: metadata type, unresolved
            database, dataset or chart reference, a secret in a database, or
            SQL that the Superset parser rejects.
    """
    problems: List[str] = []
    if assets.get("metadata.yaml", {}).get("type") != DASHBOARD_TYPE:
        problems.append(f"metadata.yaml: type must be '{DASHBOARD_TYPE}'")

    databases = {db.get("uuid") for db in _of_kind(assets, "databases")}
    datasets = {ds.get("uuid") for ds in _of_kind(assets, "datasets")}
    charts = {chart.get("uuid") for chart in _of_kind(assets, "charts")}

    for path, content in assets.items():
        if path.startswith("databases/"):
            for key in _find_keys(content, SECRET_KEYS):
                problems.append(f"{path}: secret key '{key}' must not be exported")
        elif path.startswith("datasets/"):
            if content.get("database_uuid") not in databases:
                problems.append(f"{path}: unknown database {content.get('database_uuid')}")
            unsupported = _UNSUPPORTED_SQL.search(content.get("sql") or "")
            if unsupported:
                problems.append(
                    f"{path}: {unsupported.group(1).upper()} is rejected by the Superset SQL parser"
                )
        elif path.startswith("charts/") and content.get("dataset_uuid") not in datasets:
            problems.append(f"{path}: unknown dataset {content.get('dataset_uuid')}")
        elif path.startswith("dashboards/"):
            for component in (content.get("position") or {}).values():
                if isinstance(component, dict) and component.get("type") == "CHART":
                    uuid = (component.get("meta") or {}).get("uuid")
                    if uuid not in charts:
                        problems.append(f"{path}: layout chart {uuid} not exported")
            metadata = content.get("metadata") or {}
            for native_filter in metadata.get("native_filter_configuration") or []:
                for target in native_filter.get("targets") or []:
                    uuid = target.get("datasetUuid")
                    if uuid is not None and uuid not in datasets:
                        problems.append(
                            f"{path}: filter {native_filter.get('id')} targets unknown dataset {uuid}"
                        )
    if not any(path.startswith("dashboards/") for path in assets):
        problems.append("no dashboard in the export")
    if problems:
        raise BundleError("Invalid Superset export:\n- " + "\n- ".join(problems))


# Fonction de construction de l'archive en mémoire
def build_bundle(
    source: Path,
    *,
    schema: Optional[str] = None,
    default_year: Optional[int] = None,
    top_n: Optional[int] = None,
    root: Optional[str] = None,
    assets_import: bool = False,
) -> bytes:
    """Build the importable zip archive of an export folder.

    Args:
        source: Export folder.
        schema: Target schema of the ``serving`` catalog (see
            :func:`configure_assets`).
        default_year: Default year of the native filters.
        top_n: Default number of units drawn by the curves.
        root: Name of the single top-level folder of the archive; the name of
            ``source`` by default.
        assets_import: Declare the archive as an ``assets`` bundle, for
            ``POST /api/v1/assets/import/``, which overwrites the existing
            datasets and charts too (a dashboard import keeps them).

    Returns:
        The zip archive, as bytes.

    Raises:
        BundleError: If the export is invalid (see :func:`validate_assets`).
    """
    assets = configure_assets(
        load_assets(source), schema=schema, default_year=default_year, top_n=top_n
    )
    validate_assets(assets)
    if assets_import:
        assets["metadata.yaml"] = {**assets["metadata.yaml"], "type": ASSETS_TYPE}
    prefix = root or source.resolve().name
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative, content in assets.items():
            archive.writestr(
                f"{prefix}/{relative}",
                yaml.safe_dump(content, sort_keys=False, allow_unicode=True),
            )
    return buffer.getvalue()


# Fonction d'analyse de la ligne de commande
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse the command line.

    Args:
        argv: Arguments; ``sys.argv[1:]`` when ``None``.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Build the importable zip archive of a Superset export folder."
    )
    parser.add_argument("source", type=Path, help="Export folder (e.g. superset/vulnerabilites)")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Zip archive to write")
    parser.add_argument(
        "--schema",
        help=f"Schema of the '{CATALOG_ALIAS}' catalog (default: {SOURCE_SCHEMA}, as exported)",
    )
    parser.add_argument("--default-year", type=int, help="Default value of the year filters")
    parser.add_argument("--top-n", type=int, help="Default number of units drawn by the curves")
    parser.add_argument(
        "--assets",
        action="store_true",
        help="Build an 'assets' bundle (POST /api/v1/assets/import/), which also overwrites "
        "existing datasets and charts",
    )
    return parser.parse_args(argv)


# Point d'entrée
def main(argv: Optional[List[str]] = None) -> int:
    """Build the archive; return the process exit code.

    Args:
        argv: Command-line arguments.

    Returns:
        ``0`` on success, ``1`` if the export is invalid.
    """
    args = parse_args(argv)
    try:
        payload = build_bundle(
            args.source,
            schema=args.schema,
            default_year=args.default_year,
            top_n=args.top_n,
            assets_import=args.assets,
        )
    except BundleError as exc:
        print(exc, file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(payload)
    print(f"{args.output} ({len(payload)} octets)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
