"""Isolation des données fictives (PD-24) : garde d'écriture et retirabilité du code.

Deux propriétés protègent la production :

* les scripts fictifs refusent d'écrire hors d'un catalogue préfixé ``demo_``,
  AVANT toute connexion ou tout appel réseau ;
* aucun code de production ne dépend du code fictif : le retrait (K-17b, K-18) se
  réduit à supprimer des fichiers.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from kedro_pipeline.synthetic.io import ensure_isolated_catalog

ROOT = Path(__file__).resolve().parents[1]
SAFETY = {"REQUIRED_CATALOG_PREFIX": "demo_"}

# Fichiers légitimement liés au code fictif (à supprimer ensemble au retrait)
SYNTHETIC_FILES = {
    "kedro_pipeline/synthetic",
    "scripts/seed_synthetic_comtrade.py",
    "scripts/complete_synthetic_comext.py",
    "config/profiles/demo/synthetic.yaml",
}


def test_guard_accepts_demo_catalogs_only() -> None:
    """Un catalogue préfixé passe ; un catalogue de production est refusé."""
    ensure_isolated_catalog("demo_comtrade", SAFETY)
    with pytest.raises(RuntimeError, match="Refus d'écrire des données FICTIVES"):
        ensure_isolated_catalog("comtrade", SAFETY)


def test_guard_requires_a_configured_prefix() -> None:
    """Sans préfixe configuré, aucune écriture (pas de garde implicite désactivée)."""
    for safety in (None, {}, {"REQUIRED_CATALOG_PREFIX": ""}):
        with pytest.raises(RuntimeError, match="REQUIRED_CATALOG_PREFIX"):
            ensure_isolated_catalog("demo_comtrade", safety)


def test_demo_profile_is_isolated_and_production_is_not() -> None:
    """Le profil demo passe la garde configurée ; les fichiers de production la refusent."""
    import yaml

    safety = yaml.safe_load((ROOT / "config/profiles/demo/synthetic.yaml").read_text())["synthetic"]["SAFETY"]
    for name in ("comtrade", "eurostat"):
        demo = yaml.safe_load((ROOT / f"config/profiles/demo/{name}.yaml").read_text())
        prod = yaml.safe_load((ROOT / f"config/datasets/{name}.yaml").read_text())
        ensure_isolated_catalog(demo["DOWNLOADS"]["DBNAME"], safety)
        with pytest.raises(RuntimeError):
            ensure_isolated_catalog(prod["DOWNLOADS"]["DBNAME"], safety)


def test_comtrade_script_refuses_production_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configuration de production sélectionnée : refus immédiat, avant tout réseau."""
    from scripts.seed_synthetic_comtrade import main

    monkeypatch.setenv("COMTRADE_CONFIG_PATH", str(ROOT / "config/datasets/comtrade.yaml"))
    monkeypatch.setenv("SYNTHETIC_CONFIG_PATH", str(ROOT / "config/profiles/demo/synthetic.yaml"))
    with pytest.raises(RuntimeError, match="Refus d'écrire des données FICTIVES"):
        main([])


def test_comext_script_refuses_production_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    """Idem pour le complément Comext."""
    from scripts.complete_synthetic_comext import main

    monkeypatch.setenv("EUROSTAT_CONFIG_PATH", str(ROOT / "config/datasets/eurostat.yaml"))
    monkeypatch.setenv("SYNTHETIC_CONFIG_PATH", str(ROOT / "config/profiles/demo/synthetic.yaml"))
    with pytest.raises(RuntimeError, match="Refus d'écrire des données FICTIVES"):
        main()


def test_no_production_code_depends_on_synthetic_code() -> None:
    """Seuls les fichiers fictifs (et leurs tests) référencent le code fictif."""
    pattern = re.compile(r"kedro_pipeline\.synthetic|synthetic_comtrade|synthetic_comext|SYNTHETIC_CONFIG_PATH")
    offenders = []
    for base in ("macroforecast", "kedro_pipeline", "scripts", "config", "docker", ".github"):
        for path in (ROOT / base).rglob("*"):
            if not path.is_file() or path.suffix in {".pyc"} or "__pycache__" in path.parts:
                continue
            relative = path.relative_to(ROOT).as_posix()
            if any(relative == f or relative.startswith(f + "/") for f in SYNTHETIC_FILES):
                continue
            if pattern.search(path.read_text(encoding="utf-8", errors="ignore")):
                offenders.append(relative)
    assert offenders == [], f"code de production dépendant du code fictif : {offenders}"
