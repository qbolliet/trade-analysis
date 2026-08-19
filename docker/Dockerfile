# Image initiale à utiliser
FROM python:3.12-slim

# Création du répertoire de travail
WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Installation d'uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Configuration d'uv
ENV UV_VENV_IN_PROJECT=1
ENV UV_CACHE_DIR=/tmp/uv-cache

# Copie de tous les fichiers utiles
COPY pyproject.toml uv.lock README.md ./
COPY macroforecast/ ./macroforecast/
COPY scripts/ ./scripts/
COPY parameters/ ./parameters/

# Installation du projet avec uv
RUN uv sync

# Nettoyage du cache pour réduire la taille de l'image
RUN rm -rf /tmp/uv-cache

# Activation automatique de l'environnement virtuel
ENV PATH="/app/.venv/bin:$PATH"

# Définition du point d'entrée avec uv run
ENTRYPOINT ["uv", "run"]

# Commande par défaut (peut être surchargée)
CMD ["eurostat-script"]