"""Import of a versioned Superset dashboard export through the REST API (Helm hook Job).

Steps: rebuild the export folder from the flattened ConfigMap keys, wait for
Superset to answer ``/health``, log in, build an ``assets`` archive with
``superset_bundle.py`` (target schema, default year and top N from the
environment) and post it to ``/api/v1/assets/import/``. That import is
idempotent and overwrites the database connection, the datasets, the charts and
the dashboard, so it both creates and updates them.

Nothing sensitive is logged: neither the password, nor the access or CSRF
tokens, nor the response bodies (which may echo connection parameters); only
HTTP statuses, file counts and the archive size.

Environment:
    SUPERSET_URL: Base URL of Superset (``http://<service>:8088``).
    SUPERSET_ADMIN_USERNAME, SUPERSET_ADMIN_PASSWORD: Account used for the import.
    DASHBOARD_SOURCE: Folder of the flattened export (ConfigMap mount).
    DASHBOARD_SCHEMA: Target schema of the catalog (empty: schema of the export).
    DASHBOARD_DEFAULT_YEAR, DASHBOARD_TOP_N: Optional default filter values.
    BUNDLE_TOOL: Path of ``superset_bundle.py``.
    WAIT_SECONDS: Maximum wait for Superset to be healthy.

Examples:
    $ SUPERSET_URL=http://trade-superset:8088 python import_dashboard.py
    Superset disponible
    Import : HTTP 200 (31415 octets envoyés)
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
from typing import Dict

# Requêtes HTTP (présent dans l'image Superset)
import requests

# Séparateur des chemins aplatis dans les clés du ConfigMap (« / » y est interdit)
PATH_SEPARATOR = "__"


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
    session = requests.Session()
    wait_until_healthy(session, url, int(env.get("WAIT_SECONDS", "900")))
    with tempfile.TemporaryDirectory() as folder:
        export = Path(folder) / "dashboard"
        counts = rebuild_export(Path(env.get("DASHBOARD_SOURCE", "/dashboard")), export)
        print("Export : " + ", ".join(f"{name or 'racine'} {count}" for name, count in sorted(counts.items())), flush=True)
        tool = load_bundle_tool(Path(env.get("BUNDLE_TOOL", "/tools/superset_bundle.py")))
        archive = tool.build_bundle(
            export,
            schema=env.get("DASHBOARD_SCHEMA") or None,
            default_year=int(env["DASHBOARD_DEFAULT_YEAR"]) if env.get("DASHBOARD_DEFAULT_YEAR") else None,
            top_n=int(env["DASHBOARD_TOP_N"]) if env.get("DASHBOARD_TOP_N") else None,
            assets_import=True,
        )
    login(session, url, env["SUPERSET_ADMIN_USERNAME"], env["SUPERSET_ADMIN_PASSWORD"])
    response = session.post(
        f"{url}/api/v1/assets/import/",
        files={"bundle": ("dashboard.zip", archive, "application/zip")},
        timeout=300,
    )
    print(f"Import : HTTP {response.status_code} ({len(archive)} octets envoyés)", flush=True)
    if response.status_code != 200:
        raise RuntimeError(f"Import refusé : HTTP {response.status_code}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
