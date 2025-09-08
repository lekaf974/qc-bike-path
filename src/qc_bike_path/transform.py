"""Data transformation module for Quebec bike path data."""

import json
from datetime import datetime
from typing import Any

import geojson
import structlog
from pydantic import BaseModel
from pydantic import Field
from pydantic import ValidationError

from qc_bike_path.config import settings

logger = structlog.get_logger(__name__)


class DataValidationError(Exception):
    """Exception raised during data validation."""

    pass


class DataTransformationError(Exception):
    """Exception raised during data transformation."""

    pass


class BikePathRecord(BaseModel):
    """Pydantic model for a bike path record."""

    id: str | None = Field(None, description="Unique identifier")
    name: str | None = Field(None, description="Path name")
    type: str | None = Field(None, description="Path type")
    surface: str | None = Field(None, description="Surface type")
    length_km: float | None = Field(None, description="Length in kilometers")
    geometry: dict[str, Any] | None = Field(None, description="GeoJSON geometry")
    properties: dict[str, Any] = Field(
        default_factory=dict, description="Additional properties"
    )

    # Metadata
    source_url: str | None = Field(None, description="Data source URL")
    extraction_timestamp: datetime | None = Field(
        None, description="When data was extracted"
    )
    last_updated: datetime | None = Field(None, description="Last update timestamp")

    class Config:
        """Pydantic configuration."""

        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }


class BikePathTransformer:
    """Transformer for Quebec bike path data."""

    def __init__(self) -> None:
        """Initialize the transformer."""
        self.extraction_timestamp = datetime.utcnow()

    def clean_text_field(self, value: Any) -> str | None:
        """Clean and normalize text fields.

        Args:
            value: Raw text value

        Returns:
            Cleaned text or None if empty
        """
        if not value:
            return None

        if not isinstance(value, str):
            value = str(value)

        # Remove extra whitespace and normalize
        cleaned = value.strip()
        if not cleaned or cleaned.lower() in ("n/a", "null", "none", ""):
            return None

        return cleaned

    def clean_numeric_field(self, value: Any) -> float | None:
        """Clean and convert numeric fields.

        Args:
            value: Raw numeric value

        Returns:
            Float value or None if invalid
        """
        if value is None:
            return None

        try:
            # Handle string numbers
            if isinstance(value, str):
                value = value.strip()
                if not value or value.lower() in ("n/a", "null", "none", ""):
                    return None

            return float(value)
        except (ValueError, TypeError):
            logger.warning("Invalid numeric value", value=value)
            return None

    def validate_geometry(self, geometry: dict[str, Any]) -> bool:
        """Validate GeoJSON geometry.

        Args:
            geometry: GeoJSON geometry object

        Returns:
            True if valid, False otherwise
        """
        try:
            # Use geojson library for validation
            if geometry.get("type") in (
                "Point",
                "LineString",
                "Polygon",
                "MultiLineString",
            ):
                geojson_obj = geojson.loads(json.dumps(geometry))
                return geojson_obj.is_valid
        except Exception as e:
            logger.warning(
                "Geometry validation failed", error=str(e), geometry=geometry
            )

        return False

    def extract_coordinates(self, record: dict[str, Any]) -> dict[str, Any] | None:
        """Extract and validate coordinates from record.

        Args:
            record: Raw data record

        Returns:
            GeoJSON geometry or None if invalid
        """
        # Look for geometry in various possible fields
        geometry_fields = ["geometry", "geom", "shape", "coordinates"]

        for field in geometry_fields:
            if field in record and record[field]:
                geometry_data = record[field]

                # If it's a string, try to parse as JSON
                if isinstance(geometry_data, str):
                    try:
                        geometry_data = json.loads(geometry_data)
                    except json.JSONDecodeError:
                        continue

                # Validate geometry structure
                if isinstance(geometry_data, dict) and self.validate_geometry(
                    geometry_data
                ):
                    return geometry_data

        # Look for latitude/longitude fields
        lat_fields = ["latitude", "lat", "y", "coord_y"]
        lon_fields = ["longitude", "lon", "lng", "x", "coord_x"]

        lat = None
        lon = None

        for lat_field in lat_fields:
            if lat_field in record:
                lat = self.clean_numeric_field(record[lat_field])
                if lat is not None:
                    break

        for lon_field in lon_fields:
            if lon_field in record:
                lon = self.clean_numeric_field(record[lon_field])
                if lon is not None:
                    break

        # Create Point geometry if we have coordinates
        if lat is not None and lon is not None:
            return {
                "type": "Point",
                "coordinates": [lon, lat],  # GeoJSON uses [longitude, latitude]
            }

        return None

    def transform_record(self, record: dict[str, Any]) -> BikePathRecord | None:
        """Transform a single bike path record.

        Args:
            record: Raw data record

        Returns:
            Transformed BikePathRecord or None if transformation fails
        """
        try:
            # Extract and clean basic fields
            transformed_data = {
                "id": self.clean_text_field(record.get("id") or record.get("_id")),
                "name": self.clean_text_field(
                    record.get("name") or record.get("nom") or record.get("title")
                ),
                "type": self.clean_text_field(
                    record.get("type")
                    or record.get("type_piste")
                    or record.get("category")
                ),
                "surface": self.clean_text_field(
                    record.get("surface")
                    or record.get("revetement")
                    or record.get("material")
                ),
                "length_km": self.clean_numeric_field(
                    record.get("length_km")
                    or record.get("longueur_km")
                    or record.get("length")
                ),
            }

            # Extract geometry
            geometry = self.extract_coordinates(record)
            if geometry:
                transformed_data["geometry"] = geometry

            # Add metadata
            transformed_data["source_url"] = settings.api_base_url
            transformed_data["extraction_timestamp"] = self.extraction_timestamp

            # Preserve other properties
            properties = {}
            for key, value in record.items():
                if key not in (
                    "id",
                    "_id",
                    "name",
                    "nom",
                    "title",
                    "type",
                    "type_piste",
                    "category",
                    "surface",
                    "revetement",
                    "material",
                    "length_km",
                    "longueur_km",
                    "length",
                    "geometry",
                    "geom",
                    "shape",
                    "coordinates",
                    "latitude",
                    "lat",
                    "y",
                    "coord_y",
                    "longitude",
                    "lon",
                    "lng",
                    "x",
                    "coord_x",
                ):
                    properties[key] = value

            transformed_data["properties"] = properties

            # Validate using Pydantic model
            bike_path = BikePathRecord(**transformed_data)

            logger.debug(
                "Successfully transformed record", id=bike_path.id, name=bike_path.name
            )
            return bike_path

        except ValidationError as e:
            logger.error(
                "Record validation failed", error=str(e), record_id=record.get("id")
            )
            return None
        except Exception as e:
            logger.error(
                "Record transformation failed", error=str(e), record_id=record.get("id")
            )
            return None

    def transform_batch(self, records: list[dict[str, Any]]) -> list[BikePathRecord]:
        """Transform a batch of bike path records.

        Args:
            records: List of raw data records

        Returns:
            List of transformed BikePathRecord objects
        """
        transformed_records = []
        failed_count = 0

        for i, record in enumerate(records):
            try:
                transformed = self.transform_record(record)
                if transformed:
                    transformed_records.append(transformed)
                else:
                    failed_count += 1
            except Exception as e:
                logger.error("Failed to transform record", index=i, error=str(e))
                failed_count += 1

        logger.info(
            "Batch transformation completed",
            total_records=len(records),
            successful=len(transformed_records),
            failed=failed_count,
        )

        return transformed_records

    def create_geojson_feature_collection(
        self, records: list[BikePathRecord]
    ) -> dict[str, Any]:
        """Create a GeoJSON FeatureCollection from transformed records.

        Args:
            records: List of transformed bike path records

        Returns:
            GeoJSON FeatureCollection
        """
        features = []

        for record in records:
            if record.geometry:
                feature = {
                    "type": "Feature",
                    "geometry": record.geometry,
                    "properties": {
                        "id": record.id,
                        "name": record.name,
                        "type": record.type,
                        "surface": record.surface,
                        "length_km": record.length_km,
                        "source_url": record.source_url,
                        "extraction_timestamp": (
                            record.extraction_timestamp.isoformat()
                            if record.extraction_timestamp
                            else None
                        ),
                        **record.properties,
                    },
                }
                features.append(feature)

        feature_collection = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "total_features": len(features),
                "extraction_timestamp": self.extraction_timestamp.isoformat(),
                "source": "Quebec Open Data Portal",
            },
        }

        logger.info("Created GeoJSON FeatureCollection", feature_count=len(features))
        return feature_collection

    def add_metadata(
        self, data: dict[str, Any], additional_metadata: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Add metadata to processed data.

        Args:
            data: Processed data
            additional_metadata: Optional additional metadata

        Returns:
            Data with metadata added
        """
        metadata = {
            "processing_timestamp": datetime.utcnow().isoformat(),
            "extraction_timestamp": self.extraction_timestamp.isoformat(),
            "source": "Quebec Open Data Portal",
            "source_url": settings.api_base_url,
            "transformer_version": "1.0.0",
        }

        if additional_metadata:
            metadata.update(additional_metadata)

        # Add metadata to the data
        if isinstance(data, dict):
            data["metadata"] = metadata

        return data


def transform_bike_path_data(raw_data: dict[str, Any]) -> list[BikePathRecord]:
    """Transform raw bike path data.

    Args:
        raw_data: Raw data from the extraction process

    Returns:
        List of transformed BikePathRecord objects

    Raises:
        DataTransformationError: If transformation fails
    """
    try:
        transformer = BikePathTransformer()

        # Extract records from the API response
        if "result" in raw_data and "records" in raw_data["result"]:
            records = raw_data["result"]["records"]
        elif isinstance(raw_data, list):
            records = raw_data
        else:
            raise DataTransformationError("Invalid data structure: cannot find records")

        if not isinstance(records, list):
            raise DataTransformationError("Records must be a list")

        transformed_records = transformer.transform_batch(records)

        if not transformed_records:
            raise DataTransformationError("No records were successfully transformed")

        logger.info(
            "Data transformation completed",
            input_records=len(records),
            output_records=len(transformed_records),
        )

        return transformed_records

    except Exception as e:
        logger.error("Data transformation failed", error=str(e))
        raise DataTransformationError(f"Failed to transform bike path data: {e}") from e


def create_geojson_from_records(records: list[BikePathRecord]) -> dict[str, Any]:
    """Create GeoJSON FeatureCollection from transformed records.

    Args:
        records: List of transformed bike path records

    Returns:
        GeoJSON FeatureCollection
    """
    transformer = BikePathTransformer()
    return transformer.create_geojson_feature_collection(records)
