"""Fixtures des tests de suivi d'exécution."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def mlflow_uri(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """URI d'un magasin MLflow ``file:`` temporaire.

    MLflow 3.15 met le magasin fichier en mode maintenance et le refuse sans
    ``MLFLOW_ALLOW_FILE_STORE=true`` : la variable est posée pour la durée du test.
    """
    pytest.importorskip("mlflow")
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    return (tmp_path / "mlruns").as_uri()


@pytest.fixture(autouse=True)
def _no_ambient_mlflow_server(monkeypatch: pytest.MonkeyPatch) -> None:
    """Empêche tout test de ce dossier d'écrire sur un serveur MLflow ambiant.

    ``get_tracker`` donne priorité à ``MLFLOW_TRACKING_URI`` (convention MLflow) : sur Onyxia la
    variable pointe vers le serveur du namespace. Les tests qui ouvrent des runs posent
    explicitement l'URI d'un magasin temporaire.
    """
    for name in ("MLFLOW_TRACKING_URI", "MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
