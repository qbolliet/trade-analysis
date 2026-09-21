# syntax=docker/dockerfile:1.7

# Version d'uv épinglée (reproductibilité) : `latest` est proscrit
FROM ghcr.io/astral-sh/uv:0.12.17 AS uv

# ---------------------------------------------------------------------------
# Étage de build : résolution figée (uv.lock) et extensions DuckDB
# ---------------------------------------------------------------------------
FROM python:3.13-slim AS builder

# `git` est requis : statflows et dt-ducklake-manager sont installés depuis des
# dépôts git ([tool.uv.sources]). `build-essential` + `libpq-dev` : psycopg2 (dépendance
# de dt-ducklake-manager) n'est disponible qu'en sdist ; la libpq d'exécution est fournie
# à l'étage final par postgresql-client
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /bin/uv

# Extras installés en plus des dépendances de base.
#   --build-arg UV_EXTRAS="--extra tracking --extra reports --extra optimal-transport"
ARG UV_EXTRAS="--extra tracking --extra reports --extra optimal-transport"

# Interpréteur système uniquement : un Python téléchargé par uv vivrait hors de /app
# et ne serait pas copié dans l'étage final
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /app

# Couche des dépendances (invalidée uniquement si pyproject.toml / uv.lock changent)
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project ${UV_EXTRAS}

# Code source et configuration (config/local/ est exclu par .dockerignore)
COPY macroforecast/ macroforecast/
COPY scripts/ scripts/
COPY kedro_pipeline/ kedro_pipeline/
COPY config/ config/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev ${UV_EXTRAS}

# Extensions DuckDB préinstallées (pas de téléchargement au démarrage des pods) ;
# HOME=/app place le cache ~/.duckdb/extensions dans l'image, lisible par l'utilisateur
# non root de l'étage final
ENV HOME=/app
RUN /app/.venv/bin/python -c "import duckdb; c = duckdb.connect(); [c.install_extension(e) for e in ('ducklake', 'postgres', 'httpfs')]"

# ---------------------------------------------------------------------------
# Étage final : runtime minimal
# ---------------------------------------------------------------------------
FROM python:3.13-slim

# postgresql-client : sauvegarde pg_dump du catalogue DuckLake (PD-16)
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home --home-dir /app app

COPY --from=builder --chown=app:app /app /app

ARG GIT_SHA=unknown
ENV PATH="/app/.venv/bin:$PATH" \
    HOME=/app \
    PYTHONUNBUFFERED=1 \
    GIT_SHA=${GIT_SHA}

WORKDIR /app
USER 1000

# Pas d'entrypoint : les pods Argo appellent explicitement la commande
# (comtrade-script, baci-hs-script, ...)
ENTRYPOINT []
CMD ["python", "-c", "import macroforecast; print('ok')"]
