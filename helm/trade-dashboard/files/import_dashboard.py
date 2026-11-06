"""Import of a versioned Superset dashboard export through the REST API (Helm hook Job).

Steps: rebuild the export folder from the flattened ConfigMap keys, wait for
Superset to answer ``/health``, log in, build an ``assets`` archive with
``superset_bundle.py`` (target schema, default year and top N from the
environment) and post it to ``/api/v1/assets/import/``. That import is
idempotent and overwrites the database connection, the datasets, the charts and
the dashboard, so it both creates and updates them.

Then the default values of the prefilled menus (« Produit à détailler »,
« Produits des courbes », « Pays des courbes ») are computed on the data of the
environment: the versioned export carries the values of the demonstration data,
which would not designate what the charts draw elsewhere. One ranking query per
menu runs through the data API of Superset on the imported ``cell_scores``
dataset (the Superset connection already reads the catalog: no credential is
needed here). When the values differ from the export, the archive is rebuilt
with them and imported a second time.

Nothing sensitive is logged: neither the password, nor the access or CSRF
tokens, nor the response bodies (which may echo connection parameters); only
HTTP statuses, file counts and the archive size.

Environment:
    SUPERSET_URL: Base URL of Superset (``http://<service>:8088``).
    SUPERSET_ADMIN_USERNAME, SUPERSET_ADMIN_PASSWORD: Account used for the import.
    DASHBOARD_SOURCE: Folder of the flattened export (ConfigMap mount).
    DASHBOARD_SCHEMA: Target schema of the catalog (empty: schema of the export).
    DASHBOARD_DEFAULT_YEAR, DASHBOARD_TOP_N: Optional default filter values.
    DASHBOARD_MENU_DEFAULTS: ``data`` (default) to compute the menu defaults on
        the data, ``export`` to keep the values of the export.
    BUNDLE_TOOL: Path of ``superset_bundle.py``.
    WAIT_SECONDS: Maximum wait for Superset to be healthy.

Examples:
    $ SUPERSET_URL=http://trade-superset:8088 python import_dashboard.py
    Superset disponible
    Import : HTTP 200 (31415 octets envoyés)
    Menus : NATIVE_FILTER-PAYS-DETAIL 1, NATIVE_FILTER-PAYS-EXTRA 5, NATIVE_FILTER-PRODUIT-EXTRA 5
    Menus identiques à l'export : pas de second import
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import importlib.util
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Requêtes HTTP (présent dans l'image Superset)
import requests

# Séparateur des chemins aplatis dans les clés du ConfigMap (« / » y est interdit)
PATH_SEPARATOR = "__"
# Mode de calcul des menus préremplis : sur les données, ou valeurs de l'export
MENU_DEFAULTS_FROM_DATA = "data"
MENU_DEFAULTS_FROM_EXPORT = "export"


# Reconstruction de l'arborescence de l'export
def rebuild_export(flat_dir: Path, target: Path) -> Dict[str, int]:
    """Rebuild the export tree from files whose names flatten their relative path.

    Args:
        flat_dir: Folder of the ConfigMap mount (``charts__a.yaml`` …).
        target: Destination folder.

    Returns:
        The number of files by top-level folder (``metadata.yaml`` under ``""``).

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as flat, tempfile.TemporaryDirectory() as out:
        ...     _ = (Path(flat) / "charts__a.yaml").write_text("x: 1")
        ...     rebuild_export(Path(flat), Path(out))
        {'charts': 1}
    """
    counts: Dict[str, int] = {}
    for source in sorted(flat_dir.iterdir()):
        # Les montages de ConfigMap contiennent des liens (..data) : fichiers seuls
        if source.name.startswith("..") or not source.is_file():
            continue
        relative = Path(*source.name.split(PATH_SEPARATOR))
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
        top = relative.parts[0] if len(relative.parts) > 1 else ""
        counts[top] = counts.get(top, 0) + 1
    return counts


# Attente de la disponibilité de Superset
def wait_until_healthy(session: requests.Session, url: str, wait_seconds: int) -> None:
    """Poll ``<url>/health`` until it answers 200.

    Args:
        session: HTTP session.
        url: Base URL of Superset.
        wait_seconds: Maximum wait.

    Raises:
        TimeoutError: If Superset is not healthy in time.
    """
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            if session.get(f"{url}/health", timeout=10).status_code == 200:
                print("Superset disponible", flush=True)
                return
        except requests.RequestException:
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"Superset indisponible après {wait_seconds} s")
        time.sleep(10)


# Authentification et jeton CSRF
def login(session: requests.Session, url: str, username: str, password: str) -> None:
    """Log in with the database provider and set the bearer and CSRF headers.

    Args:
        session: HTTP session (keeps the session cookie the CSRF token is bound to).
        url: Base URL of Superset.
        username: Account name.
        password: Account password.

    Raises:
        RuntimeError: If the login or the CSRF request fails (status only).
    """
    response = session.post(
        f"{url}/api/v1/security/login",
        json={"username": username, "password": password, "provider": "db", "refresh": False},
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Connexion refusée : HTTP {response.status_code}")
    session.headers["Authorization"] = f"Bearer {response.json()['access_token']}"
    response = session.get(f"{url}/api/v1/security/csrf_token/", timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"Jeton CSRF refusé : HTTP {response.status_code}")
    session.headers["X-CSRFToken"] = response.json()["result"]
    session.headers["Referer"] = url


# Chargement de l'outil d'empaquetage
def load_bundle_tool(path: Path):
    """Import ``superset_bundle.py`` from its path.

    Args:
        path: Path of the module file.

    Returns:
        The module.
    """
    spec = importlib.util.spec_from_file_location("superset_bundle", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Envoi d'une archive à l'import « assets »
def post_bundle(session: requests.Session, url: str, archive: bytes) -> None:
    """Post an ``assets`` archive to ``/api/v1/assets/import/``.

    Args:
        session: Logged-in HTTP session.
        url: Base URL of Superset.
        archive: Zip archive built by ``superset_bundle.py``.

    Raises:
        RuntimeError: If the import is refused (HTTP status only).
    """
    response = session.post(
        f"{url}/api/v1/assets/import/",
        files={"bundle": ("dashboard.zip", archive, "application/zip")},
        timeout=300,
    )
    print(f"Import : HTTP {response.status_code} ({len(archive)} octets envoyés)", flush=True)
    if response.status_code != 200:
        raise RuntimeError(f"Import refusé : HTTP {response.status_code}")


# Identifiant d'un dataset du tableau de bord importé
def dataset_id(session: requests.Session, url: str, slug: str, table_name: str) -> int:
    """Return the identifier of a dataset of the imported dashboard.

    Args:
        session: Logged-in HTTP session.
        url: Base URL of Superset.
        slug: Dashboard slug.
        table_name: Dataset table name.

    Returns:
        The dataset identifier.

    Raises:
        RuntimeError: If the request fails or the dataset is not found.
    """
    response = session.get(f"{url}/api/v1/dashboard/{slug}/datasets", timeout=60)
    if response.status_code != 200:
        raise RuntimeError(f"Datasets du tableau de bord : HTTP {response.status_code}")
    for dataset in response.json().get("result") or []:
        if dataset.get("table_name") == table_name:
            return int(dataset["id"])
    raise RuntimeError(f"Dataset {table_name} absent du tableau de bord {slug}")


# Calcul des valeurs par défaut des menus préremplis
def compute_menu_defaults(
    session: requests.Session, url: str, tool: Any, assets: Dict[str, Dict[str, Any]]
) -> Dict[str, List[Any]]:
    """Compute the default values of the prefilled menus on the data of Superset.

    Args:
        session: Logged-in HTTP session.
        url: Base URL of Superset.
        tool: The ``superset_bundle`` module.
        assets: Configured export (schema, default year, top N).

    Returns:
        Filter identifier -> values, best rank first.

    Raises:
        RuntimeError: If the dashboard has no slug or a data request fails
            (HTTP status only: the body may echo the query).
    """
    slugs = [content.get("slug") for path, content in assets.items() if path.startswith("dashboards/")]
    if not slugs or not slugs[0]:
        raise RuntimeError("Tableau de bord sans slug : menus non calculables")
    datasource = dataset_id(session, url, slugs[0], tool.MENU_TABLE)
    defaults: Dict[str, List[Any]] = {}
    for query in tool.menu_queries(assets):
        response = session.post(
            f"{url}/api/v1/chart/data", json=tool.chart_data_payload(query, datasource), timeout=300
        )
        if response.status_code != 200:
            raise RuntimeError(f"Menu {query.filter_id} : HTTP {response.status_code}")
        result = (response.json().get("result") or [{}])[0]
        values = tool.menu_values(result.get("data") or [], query)
        if not values:
            # Pays ou produit par défaut introuvable, ou année sans rang : menu vide,
            # cohérent avec un graphique vide
            print(f"Attention : menu {query.filter_id} sans valeur (défauts à vérifier)", flush=True)
        defaults[query.filter_id] = values
    print("Menus : " + ", ".join(f"{name} {len(values)}" for name, values in defaults.items()), flush=True)
    return defaults


# Lecture d'un entier optionnel de l'environnement
def _optional_int(value: Optional[str]) -> Optional[int]:
    """Return ``int(value)``, or ``None`` for an unset or empty variable."""
    return int(value) if value else None


# Exécution de l'import
def main() -> int:
    """Run the import.

    Returns:
        0 on success.

    Raises:
        RuntimeError: If the import is refused (HTTP status only).
    """
    env = os.environ
    url = env["SUPERSET_URL"].rstrip("/")
    mode = env.get("DASHBOARD_MENU_DEFAULTS") or MENU_DEFAULTS_FROM_DATA
    if mode not in (MENU_DEFAULTS_FROM_DATA, MENU_DEFAULTS_FROM_EXPORT):
        raise RuntimeError(f"DASHBOARD_MENU_DEFAULTS invalide : {mode}")
    settings = {
        "schema": env.get("DASHBOARD_SCHEMA") or None,
        "default_year": _optional_int(env.get("DASHBOARD_DEFAULT_YEAR")),
        "top_n": _optional_int(env.get("DASHBOARD_TOP_N")),
    }
    session = requests.Session()
    wait_until_healthy(session, url, int(env.get("WAIT_SECONDS", "900")))
    with tempfile.TemporaryDirectory() as folder:
        export = Path(folder) / "dashboard"
        counts = rebuild_export(Path(env.get("DASHBOARD_SOURCE", "/dashboard")), export)
        print("Export : " + ", ".join(f"{name or 'racine'} {count}" for name, count in sorted(counts.items())), flush=True)
        tool = load_bundle_tool(Path(env.get("BUNDLE_TOOL", "/tools/superset_bundle.py")))
        login(session, url, env["SUPERSET_ADMIN_USERNAME"], env["SUPERSET_ADMIN_PASSWORD"])
        # Premier import : crée ou met à jour les datasets interrogés ensuite
        post_bundle(session, url, tool.build_bundle(export, assets_import=True, **settings))
        if mode == MENU_DEFAULTS_FROM_EXPORT:
            return 0
        # Menus préremplis recalculés sur les données de l'environnement
        assets = tool.configure_assets(tool.load_assets(export), **settings)
        defaults = compute_menu_defaults(session, url, tool, assets)
        if defaults == tool.current_menu_defaults(assets):
            print("Menus identiques à l'export : pas de second import", flush=True)
            return 0
        post_bundle(
            session,
            url,
            tool.build_bundle(export, assets_import=True, menu_defaults=defaults, **settings),
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
