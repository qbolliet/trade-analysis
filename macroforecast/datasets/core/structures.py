"""Dataflow structure management for SDMX queries.

This module provides utilities to manage dataflow structures, including
dimension mappings between names and positions for various OECD dataflows.
"""
# Importation des modules
# Modules de base
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

# Initialisation du logger
logger = logging.getLogger(__name__)


# Structure de données pour une dimension
class DimensionInfo:
    """Information about a single dimension in a dataflow.

    Args:
        name: Dimension identifier (e.g., 'REF_AREA', 'FREQ').
        position: Zero-based position in the dimension filter.
        description: Optional human-readable description.
        codelist: Optional identifier of the codelist enumerating the
            dimension's allowed values (e.g., ``'CXT_FREE_ISO'``). Resolved
            from the dataflow structure (DSD), it tells which codelist to query
            to obtain the dimension's value codes.

    Example:
        >>> dim = DimensionInfo(
        ...     name="REF_AREA", position=0, description="Reference area",
        ...     codelist="CL_AREA",
        ... )
    """

    # Initialisation
    def __init__(
        self,
        name: str,
        position: int,
        description: Optional[str] = None,
        codelist: Optional[str] = None,
    ):
        # Initialisation des attributs
        self.name = name
        self.position = position
        self.description = description
        self.codelist = codelist

    # Méthode de conversion en dictionnaire des attributs
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary with dimension information.
        """
        # Initialisation du dictionnaire avec les attributs
        result = {
            "name": self.name,
            "position": self.position,
        }
        # Ajout de la description si spécifiée
        if self.description:
            result["description"] = self.description
        # Ajout de la codelist associée si spécifiée
        if self.codelist:
            result["codelist"] = self.codelist
        return result

    # Méthode de création d'une instance de la classe à partir d'un dictionnaire
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DimensionInfo":
        """Create instance from dictionary.

        Args:
            data: Dictionary containing dimension information.

        Returns:
            DimensionInfo instance.
        """
        return cls(
            name=data["name"],
            position=data["position"],
            description=data.get("description"),
            codelist=data.get("codelist"),
        )


# Structure de données pour un dataflow
class DataflowStructure:
    """Structure metadata for an OECD dataflow.
    
    Args:
        agency: Agency identifier (e.g., 'OECD.SDD.STES').
        dataflow: Dataflow identifier (e.g., 'DSD_KEI@DF_KEI').
        num_dimensions: Total number of dimensions.
        dimensions: List of DimensionInfo objects.
        description: Optional human-readable description.
    
    Example:
        >>> structure = DataflowStructure(
        ...     agency="OECD.SDD.STES",
        ...     dataflow="DSD_KEI@DF_KEI",
        ...     num_dimensions=7,
        ...     dimensions=[
        ...         DimensionInfo("REF_AREA", 0),
        ...         DimensionInfo("FREQ", 1),
        ...     ],
        ... )
    """
    
    # Initialisation
    def __init__(
        self,
        agency: str,
        dataflow: str,
        num_dimensions: int,
        dimensions: List[DimensionInfo],
        description: Optional[str] = None,
    ):
        # Initialisation des attributs
        self.agency = agency
        self.dataflow = dataflow
        self.num_dimensions = num_dimensions
        self.dimensions = dimensions
        self.description = description
        
        # Construction des index pour accès rapide
        self._name_to_position: Dict[str, int] = {}
        self._position_to_name: Dict[int, str] = {}
        self._build_indexes()
    
    # Méthode auxiliaire de construction des indices
    def _build_indexes(self) -> None:
        """Construction des index de correspondance nom <-> position."""
        # Parcours des dimensions
        for dim in self.dimensions:
            # Complétion des dictionnaires
            self._name_to_position[dim.name] = dim.position
            self._position_to_name[dim.position] = dim.name
    
    # Méthode d'extraction d'une position à partir d'un nom
    def get_position(self, name: str) -> Optional[int]:
        """Get position for a dimension name.
        
        Args:
            name: Dimension name (e.g., 'REF_AREA').
            
        Returns:
            Position index or None if not found.
        """
        # Test explicite de None : la position 0 est falsy, un `or` masquerait
        # donc la première dimension (ex. REF_AREA en position 0).
        position = self._name_to_position.get(name)
        if position is None:
            position = self._name_to_position.get(name.lower())
        return position
    
    # Méthode d'extraction d'un nom à partir d'une position
    def get_name(self, position: int) -> Optional[str]:
        """Get name for a dimension position.
        
        Args:
            position: Zero-based position index.
            
        Returns:
            Dimension name or None if not found.
        """
        return self._position_to_name.get(position)
    
    # Méthode d'extraction de la clé unique associée à une structure
    def get_key(self) -> str:
        """Get unique key for this structure.
        
        Returns:
            Key in format 'agency::dataflow'.
        """
        return f"{self.agency}::{self.dataflow}"
    
    # Méthode de création d'un dictionnaire à partir des éléments caractéristiques de la structure
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation.
        
        Returns:
            Dictionary with structure information.
        """
        return {
            "agency": self.agency,
            "dataflow": self.dataflow,
            "num_dimensions": self.num_dimensions,
            "dimensions": [dim.to_dict() for dim in self.dimensions],
            "description": self.description,
        }
    
    # Méthode de création d'une instance de la classe à partir d'un dictionnaire
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataflowStructure":
        """Create instance from dictionary.
        
        Args:
            data: Dictionary containing structure information.
            
        Returns:
            DataflowStructure instance.
        """
        dimensions = [
            DimensionInfo.from_dict(dim_data)
            for dim_data in data.get("dimensions", [])
        ]
        return cls(
            agency=data["agency"],
            dataflow=data["dataflow"],
            num_dimensions=data["num_dimensions"],
            dimensions=dimensions,
            description=data.get("description"),
        )


# Classe de gestion des structures de dataflows
class DataflowStructureRegistry:
    """Registry for managing dataflow structures.
    
    This class loads, stores, and provides access to dataflow structure
    metadata used for dimension name-to-position resolution.
    
    Args:
        config_path: Optional path to JSON configuration file.
    
    Example:
        >>> registry = DataflowStructureRegistry("structures.json")
        >>> structure = registry.get("OECD.SDD.STES", "DSD_KEI@DF_KEI")
        >>> position = structure.get_position("REF_AREA")
    """
    
    # Initialisation
    def __init__(
        self,
        structures_dict: Optional[Dict[str, Any]] = None,
        config_path: Optional[Union[str, Path]] = None,
    ):
        # Index des structures par clé (agency::dataflow)
        self._structures: Dict[str, DataflowStructure] = {}

        # Chargement depuis dictionnaire d'abord
        if structures_dict:
            self.load_from_dict(structures_dict)

        # Puis depuis fichier (peut écraser)
        if config_path:
            self.load_from_file(config_path)
    
    # Méthode de chargement des structures depuis un dictionnaire
    def load_from_dict(self, data: Dict[str, Any]) -> None:
        """Load structures from dictionary with STRUCTURES key.

        Args:
            data: Dictionary containing structures under 'STRUCTURES' key.

        Raises:
            ValueError: If the data format is invalid.
        """
        # Extraction des structures depuis la clé STRUCTURES
        structures_data = data.get("STRUCTURES", [])

        # Logging
        logger.info(f"Loading {len(structures_data)} structures from dictionary")

        # Parcours des structures
        for structure_data in structures_data:
            # Création de la structure
            structure = DataflowStructure.from_dict(structure_data)
            # Enregistrement de la structure
            self.register(structure)

        # Logging
        logger.info(f"Loaded {len(structures_data)} structures from dictionary")

    # Méthode de construction d'une structure à partir d'un partir d'un fichier json
    def load_from_file(self, path: Union[str, Path]) -> None:
        """Load structures from JSON file.
        
        Args:
            path: Path to JSON configuration file.
            
        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the file is not valid JSON.
        """
        # Conversion en chemin
        path = Path(path)
        # Logging
        logger.info(f"Loading structures from {path}")
        
        # Chargement du fichier json
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Parcours des structures (support des deux clés pour compatibilité)
        structures_data = data.get("STRUCTURES", data.get("structures", []))
        for structure_data in structures_data:
            # Création de la structure
            structure = DataflowStructure.from_dict(structure_data)
            # Enregistrement de la structure
            self.register(structure)

        # Logging
        logger.info(f"Loaded {len(structures_data)} structures")
    
    # Méthode de sérialisation du registre en dictionnaire
    def to_dict(self) -> Dict[str, Any]:
        """Serialise the registry to a ``STRUCTURES``-keyed dictionary.

        Symmetric counterpart of :meth:`load_from_dict`: the returned mapping
        can be re-loaded as-is, which lets callers persist the registry through
        any storage backend (local filesystem or S3) instead of the built-in
        :meth:`save_to_file`.

        Returns:
            Dictionary with all registered structures under the ``STRUCTURES``
            key.

        Examples:
            >>> registry = DataflowStructureRegistry()
            >>> payload = registry.to_dict()
            >>> sorted(payload)
            ['STRUCTURES']
        """
        # Sérialisation de chaque structure enregistrée
        return {
            "STRUCTURES": [
                structure.to_dict()
                for structure in self._structures.values()
            ]
        }

    # Méthode de sauvegarde de structures sous la forme d'un fichier json
    def save_to_file(self, path: Union[str, Path]) -> None:
        """Save structures to JSON file.
        
        Args:
            path: Path to output JSON file.
        """
        # Conversion en chemin
        path = Path(path)
        
        # Création des données à sauvegarder
        data = {
            "STRUCTURES": [
                structure.to_dict()
                for structure in self._structures.values()
            ]
        }
        
        # Sauvegarde du json
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Logging
        logger.info(f"Saved {len(self._structures)} structures to {path}")
    
    # Méthode d'enregistrement des structures
    def register(self, structure: DataflowStructure) -> None:
        """Register a dataflow structure.
        
        Args:
            structure: DataflowStructure to register.
        """
        # Extraction de la clé de la structure
        key = structure.get_key()
        # Enregistrement de la structure
        self._structures[key] = structure
        # Logging
        logger.debug(f"Structure registered: {key}")
    
    # Méthode d'extraction de la structure assciée à un dataflow spécifique
    def get(
        self,
        agency: str,
        dataflow: str,
    ) -> Optional[DataflowStructure]:
        """Get structure for a specific agency and dataflow.
        
        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            
        Returns:
            DataflowStructure or None if not found.
        """
        # Construction de la clé
        key = f"{agency}::{dataflow}"
        # Extraction de la structure
        return self._structures.get(key)
    
    # Méthode vérifiant si la structure asscoiée à un dataflow est enregistrée 
    def has(self, agency: str, dataflow: str) -> bool:
        """Check if structure exists for agency and dataflow.
        
        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            
        Returns:
            True if structure is registered, False otherwise.
        """
        # Construction de la clé
        key = f"{agency}::{dataflow}"
        # Vérification de l'existence de la structure
        return key in self._structures
    
    # Méthode énumérant les structures
    def list_structures(self) -> List[str]:
        """List all registered structure keys.
        
        Returns:
            List of structure keys in format 'agency::dataflow'.
        """
        return list(self._structures.keys())
    
    # Méthode d'association des noms des dimensions à leurs positions
    def resolve_dimensions(
        self,
        agency: str,
        dataflow: str,
        dimensions: Dict[Union[str, int], Union[str, List[str]]],
    ) -> Dict[int, List[str]]:
        """Resolve dimension names to positions.
        
        Converts a dictionary that may contain dimension names as keys
        to a dictionary with integer position keys.
        
        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            dimensions: Dictionary with dimension names or positions as keys.
            
        Returns:
            Dictionary with integer position keys and list values.
            
        Raises:
            ValueError: If a dimension name cannot be resolved.
        """
        # Récupération de la structure
        structure = self.get(agency, dataflow)
        
        # Initialisation du résultat
        result: Dict[int, List[str]] = {}
        
        # Parcours des dimensions
        for key, value in dimensions.items():
            # Normalisation de la valeur en liste
            if isinstance(value, str):
                value_list = [value]
            else:
                value_list = list(value)
            
            # Conversion de la clé
            if isinstance(key, int):
                # La clé est déjà une position
                result[key] = value_list
            else:
                # La clé est un nom de dimension
                # Cas où il n'y a pas de structure
                if structure is None:
                    raise ValueError(
                        f"Structure not found for {agency}::{dataflow}. "
                        f"Unable to resolve dimension name '{key}'. "
                        f"Use numeric positions or register the structure."
                    )
                # Extraction de la position associée à la clé
                position = structure.get_position(key)
                # Cas où la position n'est pas trouvée
                if position is None:
                    # Liste des dimensions disponibles pour le message d'erreur
                    available = [dim.name for dim in structure.dimensions]
                    raise ValueError(
                        f"Dimension '{key}' not found in {agency}::{dataflow}. "
                        f"Available dimensions: {available}"
                    )
                
                # Ajout au résultat
                result[position] = value_list
        
        return result
    
    # Méthode extrayant le nombre de dimensions associés à un dataflow
    def get_num_dimensions(self, agency: str, dataflow: str) -> Optional[int]:
        """Get the number of dimensions for a dataflow.
        
        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            
        Returns:
            Number of dimensions or None if structure not found.
        """
        # Extraction de la structure
        structure = self.get(agency, dataflow)
        # Extraction du nombre de dimensions
        if structure:
            return structure.num_dimensions
        return None

