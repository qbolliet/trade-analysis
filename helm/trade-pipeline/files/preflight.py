"""Pre-flight check of a pipeline deployment, run by ``helm test``.

Checks, without ever printing a value: that every expected environment
variable (filled from the secrets) is set, that the MLflow server answers its
``/health`` endpoint, that PostgreSQL accepts the credentials (``psql``) and
that the S3 prefix can be listed with the S3 keys (DuckDB ``httpfs``).
Each check prints one ``OK`` / ``KO`` / ``SKIP`` line; the exit code is the
number of failed checks (0: all passed).

Environment:
    PREFLIGHT_REQUIRED: Names of the required variables, separated by spaces.
    PREFLIGHT_S3_PATH: S3 prefix to list (empty: check skipped).
    PREFLIGHT_TIMEOUT: Timeout of each network check, in seconds.

Examples:
    $ PREFLIGHT_REQUIRED="PGHOST PGUSER" python preflight.py
    OK   variable PGHOST
    ...
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import base64
import os
import subprocess
import sys
import urllib.request
from typing import Callable, List, Tuple


# Contrôle de présence des variables
def check_variables(names: List[str]) -> List[Tuple[str, bool, str]]:
    """Check that each variable is set and non-empty.

    Args:
        names: Variable names.

    Returns:
        One ``(label, passed, detail)`` result per variable; the detail never
        contains the value.

    Examples:
        >>> os.environ["PREFLIGHT_DEMO"] = "x"
        >>> check_variables(["PREFLIGHT_DEMO"])
        [('variable PREFLIGHT_DEMO', True, '')]
    """
    return [(f"variable {name}", bool(os.environ.get(name)), "" if os.environ.get(name) else "absente ou vide") for name in names]


# Contrôle du serveur MLflow
def check_mlflow(timeout: float) -> Tuple[bool, str]:
    """Call ``<MLFLOW_TRACKING_URI>/health``, with basic authentication when configured.

    Args:
        timeout: Timeout in seconds.

    Returns:
        ``(passed, detail)``; the detail holds the HTTP status or the error type only.
    """
    uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    if not uri.startswith(("http://", "https://")):
        return False, "MLFLOW_TRACKING_URI absente ou non HTTP"
    request = urllib.request.Request(uri.rstrip("/") + "/health")
    user, password = os.environ.get("MLFLOW_TRACKING_USERNAME"), os.environ.get("MLFLOW_TRACKING_PASSWORD")
    if user and password:
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        request.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status == 200, f"HTTP {response.status}"
    except Exception as error:  # noqa: BLE001 - type d'erreur seul, sans message (URL, hôte)
        return False, type(error).__name__


# Contrôle de PostgreSQL
def check_postgres(timeout: float) -> Tuple[bool, str]:
    """Run ``SELECT 1`` with ``psql``, which reads the ``PG*`` variables.

    Args:
        timeout: Connection timeout in seconds.

    Returns:
        ``(passed, detail)``; the detail holds the exit code only (psql messages may
        name the host or the user).
    """
    environment = {**os.environ, "PGCONNECT_TIMEOUT": str(int(timeout))}
    try:
        completed = subprocess.run(
            ["psql", "-X", "-q", "-t", "-c", "SELECT 1"], env=environment, capture_output=True, timeout=timeout + 5
        )
    except Exception as error:  # noqa: BLE001
        return False, type(error).__name__
    return completed.returncode == 0, f"code {completed.returncode}"


# Contrôle de S3
def check_s3(path: str, timeout: float) -> Tuple[bool, str]:
    """List an S3 prefix with DuckDB ``httpfs`` and the ``AWS_*`` variables.

    Args:
        path: S3 prefix (``s3://bucket/prefix``).
        timeout: Timeout in seconds.

    Returns:
        ``(passed, detail)``; the detail holds the error type only.
    """
    import duckdb

    connection = duckdb.connect()
    try:
        connection.execute("LOAD httpfs")
        connection.execute(f"SET http_timeout = {int(timeout * 1000)}")
        # Secret en mémoire, jamais écrit dans un journal (seul le type d'erreur est affiché)
        quoted = {
            name: os.environ.get(name, "").replace("'", "''")
            for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_S3_ENDPOINT", "AWS_DEFAULT_REGION")
        }
        connection.execute(
            f"CREATE SECRET preflight (TYPE s3, KEY_ID '{quoted['AWS_ACCESS_KEY_ID']}', "
            f"SECRET '{quoted['AWS_SECRET_ACCESS_KEY']}', ENDPOINT '{quoted['AWS_S3_ENDPOINT']}', "
            f"REGION '{quoted['AWS_DEFAULT_REGION']}', URL_STYLE 'path')"
        )
        connection.execute("SELECT count(*) FROM glob(?)", [path.rstrip("/") + "/*"]).fetchone()
        return True, ""
    except Exception as error:  # noqa: BLE001
        return False, type(error).__name__
    finally:
        connection.close()


# Exécution des contrôles
def main() -> int:
    """Run every check and print one line per check.

    Returns:
        The number of failed checks.
    """
    timeout = float(os.environ.get("PREFLIGHT_TIMEOUT", "15"))
    required = os.environ.get("PREFLIGHT_REQUIRED", "").split()
    results = check_variables(required)
    network: List[Tuple[str, Callable[[], Tuple[bool, str]]]] = [
        ("MLflow /health", lambda: check_mlflow(timeout)),
        ("PostgreSQL", lambda: check_postgres(timeout)),
    ]
    s3_path = os.environ.get("PREFLIGHT_S3_PATH", "")
    if s3_path:
        network.append((f"S3 {s3_path}", lambda: check_s3(s3_path, timeout)))
    else:
        print("SKIP S3 (preflight.s3Path vide)")
    for label, check in network:
        passed, detail = check()
        results.append((label, passed, detail))
    failures = 0
    for label, passed, detail in results:
        failures += not passed
        print(f"{'OK ' if passed else 'KO '}  {label}{f' ({detail})' if detail else ''}")
    print(f"{len(results) - failures}/{len(results)} contrôles réussis")
    return failures


if __name__ == "__main__":
    sys.exit(main())
