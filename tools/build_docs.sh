#!/usr/bin/env bash
# Construction du site de documentation statique : pages mkdocs-material + rendu kedro-viz.
#
# Usage, depuis la racine du dépôt (le script s'y place lui-même) :
#   uv run --extra docs bash tools/build_docs.sh
# Résultat : site/index.html (mkdocs) et site/pipeline/index.html (kedro-viz). Aucun secret
# n'est nécessaire : les jeux de données du catalogue ne se connectent pas à l'instanciation.
#
# Variables facultatives :
#   KEDRO_ENV  environnement Kedro décrit par kedro-viz (défaut du projet : local, qui se
#              réduit à config/base tant que config/local est vide)
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Dossier de sortie de `kedro viz build` : fixe (« build » dans le dossier courant), sans
# option pour le changer en version 12.4 ; doit rester cohérent avec site_dir de mkdocs.yml
VIZ_BUILD_DIR="build"
SITE_DIR="site"

export KEDRO_DISABLE_TELEMETRY=true
export MLFLOW_DISABLE_AGENT_HINT=1

echo "==> Page de référence des paramètres (docs/configuration.md)"
kedro trade docs-config

# Nettoyage préalable : les ressources de kedro-viz portent un hash dans leur nom, une
# sortie précédente laisserait des fichiers obsolètes qui seraient copiés dans le site
echo "==> Rendu statique de kedro-viz (${VIZ_BUILD_DIR}/)"
rm -rf "${VIZ_BUILD_DIR}"
kedro viz build

# Mode strict : un lien interne rompu ou un fichier inclus manquant fait échouer la
# construction. `mkdocs build` vide le dossier de sortie, d'où la copie de kedro-viz après
echo "==> Pages mkdocs (${SITE_DIR}/)"
mkdocs build --strict

echo "==> Copie de kedro-viz dans ${SITE_DIR}/pipeline/"
mkdir -p "${SITE_DIR}/pipeline"
cp -R "${VIZ_BUILD_DIR}/." "${SITE_DIR}/pipeline/"

# Contrôle de la sortie
for page in "${SITE_DIR}/index.html" "${SITE_DIR}/pipeline/index.html"; do
  if [ ! -f "${page}" ]; then
    echo "Fichier attendu absent : ${page}" >&2
    exit 1
  fi
done
echo "Site construit dans ${SITE_DIR}/ ($(du -sh "${SITE_DIR}" | cut -f1))"
