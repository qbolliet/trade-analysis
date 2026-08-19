"""OECD response parsing helpers.

Pure functions parsing the various OECD API responses into the project's
data structures: SDMX-JSON observations, structure metadata, and
ContentConstraint update dates. They carry no client state and are therefore
exposed as module-level functions rather than methods.
"""
# Importation des modules
import json
import logging
from typing import Any, Dict

import pandas as pd

from ...core.structures import DataflowStructure, DimensionInfo

# Initialisation du logger
logger = logging.getLogger(__name__)


# Fonction de parsing d'une réponse au format json
def parse_json_response(data: Dict[str, Any]) -> pd.DataFrame:
    """Parse SDMX-JSON response to DataFrame.

    Args:
        data: JSON response from OECD API.

    Returns:
        DataFrame with parsed data.
    """
    try:
        # Structure SDMX-JSON
        structure = data.get("structure", data.get("data", {}).get("structure", {}))
        dataSets = data.get("dataSets", data.get("data", {}).get("dataSets", []))

        # Cas où les données sont vides
        if not dataSets:
            logger.warning("No datasets found in response")
            return pd.DataFrame()

        # Extraction des dimensions et de leurs valeurs
        dimensions = structure.get("dimensions", {}).get("observation", [])
        dim_names = [dim["id"] for dim in dimensions]
        dim_values = {
            dim["id"]: [v["id"] for v in dim["values"]]
            for dim in dimensions
        }

        # Extraction des observations
        observations = dataSets[0].get("observations", {})

        # Initialisation de la liste des enregistrements
        records = []

        # Parcours des observations
        for obs_key, obs_data in observations.items():
            # Parsing de la clé (format: "0:1:2:3")
            indices = list(map(int, obs_key.split(":")))

            # Construction de l'enregistrement
            record = {}
            for i, dim_name in enumerate(dim_names):
                if i < len(indices):
                    dim_index = indices[i]
                    record[dim_name] = dim_values[dim_name][dim_index]

            # Ajout de la valeur de l'observation
            if isinstance(obs_data, list) and len(obs_data) > 0:
                record["value"] = obs_data[0]
            else:
                record["value"] = obs_data

            records.append(record)

        # Conversion en DataFrame
        df = pd.DataFrame(records)

        # Logging
        logger.info(f"Parsed {len(df)} observations")
        return df

    except Exception as e:
        # Logging
        logger.error(f"Failed to parse JSON response: {e}")
        logger.debug(f"Response structure: {json.dumps(data, indent=2)[:1000]}")
        raise


# Fonction utilitaire pour créer une structure à partir des métadonnées API
def create_structure_from_api_response(
    agency: str,
    dataflow: str,
    api_response: Dict[str, Any],
) -> DataflowStructure:
    """Create a DataflowStructure from OECD API structure response.

    Parses the structure metadata returned by the OECD API and creates
    a DataflowStructure object. Supports both SDMX v1 (dimensions with
    inline names) and SDMX v2 (dimensions referencing concept schemes).

    Args:
        agency: Agency identifier.
        dataflow: Dataflow identifier.
        api_response: JSON response from structure API endpoint.

    Returns:
        DataflowStructure instance.

    Raises:
        ValueError: If the response cannot be parsed.
    """
    try:
        data = api_response.get("data", api_response)

        # Construction de l'index concept_id → nom lisible
        concept_names: Dict[str, str] = {}
        for scheme in data.get("conceptSchemes", []):
            for concept in scheme.get("concepts", []):
                concept_id = concept.get("id")
                name = (
                    concept.get("names", {}).get("en")
                    or concept.get("name")
                )
                if concept_id and name:
                    concept_names[concept_id] = name

        structures = data.get("structures", data.get("structure", {}))
        dimensions_data = []

        # Format v1 : structure.dimensions.observation
        if "dimensions" in structures:
            dims = structures["dimensions"]
            if "observation" in dims:
                dimensions_data = dims["observation"]
            elif isinstance(dims, list):
                dimensions_data = dims

        # Format v2 : data.dataStructures
        elif "dataStructures" in data:
            ds_list = data["dataStructures"]
            if ds_list:
                ds = ds_list[0]
                components = ds.get("dataStructureComponents", {})
                dim_list = components.get("dimensionList", {})
                dimensions_data = dim_list.get("dimensions", [])

        dimensions = []
        for i, dim_data in enumerate(dimensions_data):
            dim_id = dim_data.get("id", dim_data.get("name", f"DIM_{i}"))
            position = dim_data.get("position", dim_data.get("keyPosition", i))

            # Résolution du nom : inline (v1) puis concept scheme (v2)
            dim_name = dim_data.get("name") or dim_data.get("names", {}).get("en")
            if not dim_name:
                # Extraction de l'identifiant de concept depuis l'URN
                # Ex. "...CS_STES(4.0).REF_AREA" → "REF_AREA"
                concept_identity = dim_data.get("conceptIdentity", "")
                concept_id = concept_identity.rsplit(".", 1)[-1] if concept_identity else dim_id
                dim_name = concept_names.get(concept_id)

            dimensions.append(DimensionInfo(
                name=dim_id,
                position=position,
                description=dim_name if dim_name != dim_id else None,
            ))

        # Tri par position
        dimensions.sort(key=lambda d: d.position)

        return DataflowStructure(
            agency=agency,
            dataflow=dataflow,
            num_dimensions=len(dimensions),
            dimensions=dimensions,
        )

    except Exception as e:
        logger.error(f"Error parsing structure: {e}")
        raise ValueError(f"Unable to parse structure: {e}")
