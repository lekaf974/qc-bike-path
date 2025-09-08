"""Test fixtures for QC Bike Path ETL service."""

import json
from datetime import datetime
from typing import Any


def get_sample_api_response() -> dict[str, Any]:
    """Get sample API response data for testing.

    Returns:
        Sample API response with bike path records
    """
    return {
        "result": {
            "resource_id": "test-resource-id",
            "records": [
                {
                    "id": "1",
                    "name": "Piste Cyclable du Vieux-Port",
                    "type": "Piste cyclable",
                    "surface": "Asphalte",
                    "length_km": 2.5,
                    "latitude": 46.8139,
                    "longitude": -71.2080,
                    "description": "Belle piste le long du fleuve",
                    "status": "Active",
                },
                {
                    "id": "2",
                    "name": "Corridor du Littoral",
                    "type": "Voie cyclable",
                    "surface": "Béton",
                    "length_km": 12.8,
                    "latitude": 46.8229,
                    "longitude": -71.2167,
                    "description": "Piste reliant plusieurs quartiers",
                    "status": "Active",
                },
                {
                    "id": "3",
                    "name": "Piste du Parc",
                    "type": "Sentier récréatif",
                    "surface": "Gravier",
                    "length_km": 4.2,
                    "latitude": 46.8056,
                    "longitude": -71.2442,
                    "description": "Sentier dans un environnement naturel",
                    "status": "En construction",
                },
            ],
            "total": 3,
        },
        "success": True,
    }


def get_sample_geojson_response() -> dict[str, Any]:
    """Get sample GeoJSON response for testing.

    Returns:
        Sample GeoJSON FeatureCollection
    """
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [-71.2080, 46.8139],
                        [-71.2075, 46.8145],
                        [-71.2070, 46.8151],
                    ],
                },
                "properties": {
                    "id": "1",
                    "name": "Piste Cyclable du Vieux-Port",
                    "type": "Piste cyclable",
                    "surface": "Asphalte",
                    "length_km": 2.5,
                    "status": "Active",
                },
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [-71.2167, 46.8229],
                        [-71.2150, 46.8240],
                        [-71.2130, 46.8255],
                        [-71.2110, 46.8270],
                    ],
                },
                "properties": {
                    "id": "2",
                    "name": "Corridor du Littoral",
                    "type": "Voie cyclable",
                    "surface": "Béton",
                    "length_km": 12.8,
                    "status": "Active",
                },
            },
        ],
        "metadata": {
            "total_features": 2,
            "extraction_timestamp": datetime.utcnow().isoformat(),
            "source": "Quebec Open Data Portal",
        },
    }


def get_invalid_records() -> list[dict[str, Any]]:
    """Get invalid records for testing error handling.

    Returns:
        List of invalid record dictionaries
    """
    return [
        # Missing required fields
        {
            "id": "invalid1",
        },
        # Invalid coordinates
        {
            "id": "invalid2",
            "name": "Invalid Coordinates",
            "latitude": 200,  # Invalid latitude
            "longitude": -71.2080,
        },
        # Invalid length
        {
            "id": "invalid3",
            "name": "Invalid Length",
            "latitude": 46.8139,
            "longitude": -71.2080,
            "length_km": "not a number",
        },
        # Empty/null values
        {
            "id": "invalid4",
            "name": "",
            "type": None,
            "surface": "n/a",
        },
    ]


def get_transformed_record_sample() -> dict[str, Any]:
    """Get sample transformed record for testing.

    Returns:
        Sample transformed BikePathRecord as dict
    """
    return {
        "id": "1",
        "name": "Piste Cyclable du Vieux-Port",
        "type": "Piste cyclable",
        "surface": "Asphalte",
        "length_km": 2.5,
        "geometry": {
            "type": "Point",
            "coordinates": [-71.2080, 46.8139],
        },
        "properties": {
            "description": "Belle piste le long du fleuve",
            "status": "Active",
        },
        "source_url": "https://www.donneesquebec.ca/recherche/api/3/action/datastore_search",
        "extraction_timestamp": datetime.utcnow(),
    }


def get_mongodb_test_config() -> dict[str, Any]:
    """Get test MongoDB configuration.

    Returns:
        Test MongoDB configuration
    """
    return {
        "mongodb_url": "mongodb://localhost:27017",
        "mongodb_database": "qc_bike_path_test",
        "mongodb_collection": "bike_paths_test",
        "mongodb_timeout": 5000,
    }


def create_mock_aiohttp_response(data: dict[str, Any], status: int = 200) -> Any:
    """Create mock aiohttp response for testing.

    Args:
        data: Response data
        status: HTTP status code

    Returns:
        Mock response object
    """
    from unittest.mock import AsyncMock
    from unittest.mock import MagicMock

    mock_response = MagicMock()
    mock_response.status = status
    mock_response.json = AsyncMock(return_value=data)
    mock_response.raise_for_status = MagicMock()

    return mock_response


# Test data files (would be loaded from JSON files in a real implementation)
SAMPLE_LARGE_DATASET = {
    "result": {
        "records": [
            {
                "id": f"path_{i}",
                "name": f"Test Path {i}",
                "type": "Piste cyclable" if i % 2 == 0 else "Voie cyclable",
                "surface": "Asphalte" if i % 3 == 0 else "Béton",
                "length_km": round(1.5 + (i * 0.3), 1),
                "latitude": 46.8139 + (i * 0.001),
                "longitude": -71.2080 - (i * 0.001),
                "status": "Active",
            }
            for i in range(100)  # 100 test records
        ],
        "total": 100,
    },
    "success": True,
}


# Error scenarios for testing
ERROR_SCENARIOS = {
    "api_timeout": {
        "error_type": "TimeoutError",
        "message": "API request timed out",
    },
    "api_404": {
        "error_type": "ClientResponseError",
        "status": 404,
        "message": "Resource not found",
    },
    "api_500": {
        "error_type": "ClientResponseError",
        "status": 500,
        "message": "Internal server error",
    },
    "invalid_json": {
        "error_type": "JSONDecodeError",
        "message": "Invalid JSON response",
    },
    "mongodb_connection_error": {
        "error_type": "ServerSelectionTimeoutError",
        "message": "MongoDB connection failed",
    },
    "mongodb_authentication_error": {
        "error_type": "OperationFailure",
        "message": "Authentication failed",
    },
}
