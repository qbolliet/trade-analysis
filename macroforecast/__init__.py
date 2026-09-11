# API publique du package : points d'entrée des pipelines et leur configuration.
# Importations explicites (pas d'étoile) pour que `import macroforecast` ne
# charge que les symboles réellement consommés et garde une surface lisible.
# Les diagnostics, métriques et primitives internes restent accessibles via
# leurs sous-modules (`macroforecast.trade.vulnerabilities`, etc.).
from .trade import (
    # Redressement BACI
    run_baci,
    BaciConfig,
    BACI_DEFAULT_CONFIG,
    # Vulnérabilités partenaires
    run_vulnerabilities,
    VulnerabilityConfig,
    VULNERABILITY_DEFAULT_CONFIG,
    # Vulnérabilités de réseau
    run_network_vulnerabilities,
    NetworkVulnerabilityConfig,
    NETWORK_VULNERABILITY_DEFAULT_CONFIG,
    # Synthèse multicritère des vulnérabilités
    run_synthesis,
    SynthesisConfig,
    # Cohérence des métriques et des synthèses
    run_coherence,
    CoherenceConfig,
)

__all__ = [
    "run_baci",
    "BaciConfig",
    "BACI_DEFAULT_CONFIG",
    "run_vulnerabilities",
    "VulnerabilityConfig",
    "VULNERABILITY_DEFAULT_CONFIG",
    "run_network_vulnerabilities",
    "NetworkVulnerabilityConfig",
    "NETWORK_VULNERABILITY_DEFAULT_CONFIG",
    "run_synthesis",
    "SynthesisConfig",
    "run_coherence",
    "CoherenceConfig",
]
