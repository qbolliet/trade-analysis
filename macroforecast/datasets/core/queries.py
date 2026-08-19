"""Shared SDMX query request base.

Provider-agnostic parent of the provider query DTOs
(:class:`~macroforecast.datasets.sources.oecd.queries.OECDQueryRequest`,
:class:`~macroforecast.datasets.sources.eurostat.queries.EurostatQueryRequest`).
It mutualises the logic that was previously duplicated across providers:
``to_dict`` / ``from_dict`` (round-trip serialisation), ``identity_key`` (the
download-registry primary key) and ``get_dataflow_key``.

The base carries **no dataclass field** of its own: each provider keeps its own
fields (with provider-specific names, defaults and the ``agency`` field/property
discrepancy), and only inherits the shared methods. The generic ``to_dict`` /
``from_dict`` therefore operate on the *concrete* subclass fields via
:func:`dataclasses.fields`.
"""
# Importation des modules
# Modules de base
from dataclasses import fields
from typing import Any, ClassVar, Dict, Optional, Type
# Modules du package
from .sdmx import SDMXResponseFormat, build_identity_key


# Classe de base commune aux DTOs de requête des providers SDMX
class SDMXQueryRequest:
    """Common base for provider SDMX query DTOs (methods only, no fields).

    Subclasses must be dataclasses exposing at least the ``agency``,
    ``dataflow``, ``version`` and ``dimensions`` attributes (``agency`` may be a
    field or a property). They set :attr:`_FORMAT_ENUM` to their provider
    response-format enum so that :meth:`from_dict` can coerce a string
    ``"format"`` value into the corresponding enum member.

    Attributes:
        _FORMAT_ENUM: Provider response-format enum, used by :meth:`from_dict`
            to convert a textual ``format`` value into an enum member. Left at
            ``None`` on the base (no coercion).
    """

    # Enum de format du provider (surchargé par chaque sous-classe concrète)
    _FORMAT_ENUM: ClassVar[Optional[Type[SDMXResponseFormat]]] = None

    # Méthode de conversion des champs en dictionnaire de kwargs get_data()
    def to_dict(self) -> Dict[str, Any]:
        """Convert to a dictionary suitable for ``get_data()`` kwargs.

        Generic over the concrete dataclass: every field (and only the fields)
        of the subclass is serialised. Properties such as ``agency`` (Eurostat)
        are intentionally excluded, matching the historical behaviour.

        Returns:
            Dictionary mapping each dataclass field name to its value.
        """
        # Sérialisation générique de tous les champs de la dataclass concrète
        return {f.name: getattr(self, f.name) for f in fields(self)}

    # Méthode de création d'une instance à partir d'un dictionnaire
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SDMXQueryRequest":
        """Create a query request from a dictionary specification.

        Symmetric counterpart of :meth:`to_dict`. Keys that do not match a
        dataclass field (e.g. a human-readable ``"description"`` carried by a
        JSON config file) are ignored, and a textual ``"format"`` value is
        coerced into the provider enum when :attr:`_FORMAT_ENUM` is set.

        Args:
            data: Mapping of field names to values (extra keys allowed).

        Returns:
            A new instance of the concrete subclass.

        Examples:
            >>> OECDQueryRequest.from_dict({
            ...     "agency": "OECD.SDD.STES",
            ...     "dataflow": "DSD_KEI@DF_KEI",
            ...     "dimensions": {"REF_AREA": ["FRA"]},
            ...     "format": "csv_labels",
            ...     "description": "ignored",
            ... })  # doctest: +SKIP
        """
        # Filtre des clés inconnues (ex. "description") sur les champs valides
        valid = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in data.items() if k in valid}
        # Coercition du format texte en enum du provider si applicable
        if cls._FORMAT_ENUM is not None and isinstance(kwargs.get("format"), str):
            kwargs["format"] = cls._FORMAT_ENUM(kwargs["format"])
        return cls(**kwargs)

    # Méthode d'extraction de la clé associée au dataflow
    def get_dataflow_key(self) -> str:
        """Get a unique key for this dataflow.

        Returns:
            Key in format ``'agency::dataflow::version'``. Providers that key
            their dataflows differently override this method.
        """
        return f"{self.agency}::{self.dataflow}::{self.version}"

    # Méthode d'extraction d'une clé d'identité stable de la requête
    def identity_key(self) -> str:
        """Return a deterministic identity key for this query.

        Used as the primary key of the download registry (the JSON file mapping
        each query to its last-download date). Encodes only the *selection*
        fields that define which series is fetched (agency, dataflow, version,
        dimensions) — not presentation options such as ``format`` — so that
        re-running the same logical query reuses its registry entry. See
        :func:`macroforecast.datasets.core.sdmx.build_identity_key`.

        Returns:
            Stable identity string.
        """
        # Sérialisation déterministe des dimensions (helper mutualisé)
        return build_identity_key(
            self.agency, self.dataflow, self.version, self.dimensions
        )
