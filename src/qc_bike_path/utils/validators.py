"""Data validation utilities for QC Bike Path ETL service."""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import geojson
import structlog
from pydantic import BaseModel
from pydantic import ValidationError


logger = structlog.get_logger(__name__)


class DataValidationError(ValueError):
    """Custom validation error."""

    pass


def validate_geojson_geometry(geometry: Dict[str, Any]) -> bool:
    """Validate GeoJSON geometry structure.
    
    Args:
        geometry: GeoJSON geometry object
        
    Returns:
        True if valid geometry, False otherwise
    """
    if not isinstance(geometry, dict):
        return False
        
    # Check for required fields
    if "type" not in geometry or "coordinates" not in geometry:
        return False
        
    geometry_type = geometry["type"]
    coordinates = geometry["coordinates"]
    
    # Validate based on geometry type
    valid_types = ["Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon"]
    if geometry_type not in valid_types:
        return False
        
    try:
        # Use geojson library for detailed validation
        geojson_obj = geojson.loads(geojson.dumps(geometry))
        return geojson_obj.is_valid
    except Exception as e:
        logger.debug("GeoJSON validation failed", error=str(e))
        return False


def validate_coordinates(coordinates: Union[List, float]) -> bool:
    """Validate coordinate values.
    
    Args:
        coordinates: Coordinate values (can be nested lists)
        
    Returns:
        True if valid coordinates, False otherwise
    """
    if isinstance(coordinates, (int, float)):
        return -180 <= coordinates <= 180  # Basic range check
        
    if isinstance(coordinates, list):
        if not coordinates:
            return False
            
        # Recursively validate nested coordinates
        return all(validate_coordinates(coord) for coord in coordinates)
        
    return False


def validate_url(url: str) -> bool:
    """Validate URL format.
    
    Args:
        url: URL string to validate
        
    Returns:
        True if valid URL, False otherwise
    """
    if not isinstance(url, str) or not url.strip():
        return False
        
    import re
    
    # Basic URL pattern validation
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
    return bool(url_pattern.match(url))


def validate_mongodb_connection_string(connection_string: str) -> bool:
    """Validate MongoDB connection string format.
    
    Args:
        connection_string: MongoDB connection string
        
    Returns:
        True if valid format, False otherwise
    """
    if not isinstance(connection_string, str) or not connection_string.strip():
        return False
        
    # Basic MongoDB connection string validation
    # Should start with mongodb:// or mongodb+srv://
    return (
        connection_string.startswith("mongodb://") or
        connection_string.startswith("mongodb+srv://")
    )


def validate_record_structure(record: Dict[str, Any], required_fields: List[str]) -> bool:
    """Validate that a record has the required structure.
    
    Args:
        record: Record dictionary to validate
        required_fields: List of required field names
        
    Returns:
        True if record has required structure, False otherwise
    """
    if not isinstance(record, dict):
        return False
        
    return all(field in record for field in required_fields)


def sanitize_string_field(value: Any, max_length: Optional[int] = None) -> Optional[str]:
    """Sanitize and validate string fields.
    
    Args:
        value: Value to sanitize
        max_length: Maximum allowed length
        
    Returns:
        Sanitized string or None if invalid
    """
    if value is None:
        return None
        
    # Convert to string
    if not isinstance(value, str):
        value = str(value)
        
    # Strip whitespace
    value = value.strip()
    
    # Check for empty or null-like values
    if not value or value.lower() in ("null", "none", "n/a", ""):
        return None
        
    # Apply length limit
    if max_length and len(value) > max_length:
        logger.warning("String field truncated", original_length=len(value), max_length=max_length)
        value = value[:max_length]
        
    return value


def validate_numeric_range(value: Union[int, float], min_val: Optional[float] = None, 
                          max_val: Optional[float] = None) -> bool:
    """Validate that a numeric value is within acceptable range.
    
    Args:
        value: Numeric value to validate
        min_val: Minimum acceptable value
        max_val: Maximum acceptable value
        
    Returns:
        True if value is within range, False otherwise
    """
    if not isinstance(value, (int, float)):
        return False
        
    if min_val is not None and value < min_val:
        return False
        
    if max_val is not None and value > max_val:
        return False
        
    return True


def validate_bike_path_record(record: Dict[str, Any]) -> List[str]:
    """Validate a bike path record and return list of validation errors.
    
    Args:
        record: Bike path record to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Check basic structure
    if not isinstance(record, dict):
        errors.append("Record must be a dictionary")
        return errors
    
    # Validate coordinates if present
    if "geometry" in record and record["geometry"]:
        if not validate_geojson_geometry(record["geometry"]):
            errors.append("Invalid GeoJSON geometry")
    
    # Validate length if present
    if "length_km" in record and record["length_km"] is not None:
        try:
            length = float(record["length_km"])
            if not validate_numeric_range(length, min_val=0, max_val=1000):  # 1000km seems reasonable max
                errors.append("Length must be between 0 and 1000 km")
        except (ValueError, TypeError):
            errors.append("Length must be a valid number")
    
    # Validate name length
    if "name" in record and record["name"]:
        if len(str(record["name"])) > 500:  # Reasonable limit for path names
            errors.append("Name exceeds maximum length of 500 characters")
    
    return errors


class RecordValidator:
    """Class-based validator for bike path records with configurable rules."""
    
    def __init__(self, strict_mode: bool = False):
        """Initialize validator.
        
        Args:
            strict_mode: If True, applies stricter validation rules
        """
        self.strict_mode = strict_mode
        self.validation_stats = {
            "total_validated": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "common_errors": {},
        }
    
    def validate_record(self, record: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate a single record.
        
        Args:
            record: Record to validate
            
        Returns:
            Tuple of (is_valid, error_list)
        """
        self.validation_stats["total_validated"] += 1
        errors = validate_bike_path_record(record)
        
        if self.strict_mode:
            # Additional strict validations
            if not record.get("name"):
                errors.append("Name is required in strict mode")
            
            if not record.get("geometry"):
                errors.append("Geometry is required in strict mode")
        
        # Track common errors
        for error in errors:
            self.validation_stats["common_errors"][error] = (
                self.validation_stats["common_errors"].get(error, 0) + 1
            )
        
        is_valid = len(errors) == 0
        if is_valid:
            self.validation_stats["valid_records"] += 1
        else:
            self.validation_stats["invalid_records"] += 1
            
        return is_valid, errors
    
    def validate_batch(self, records: List[Dict[str, Any]]) -> List[tuple[bool, List[str]]]:
        """Validate a batch of records.
        
        Args:
            records: List of records to validate
            
        Returns:
            List of (is_valid, error_list) tuples
        """
        return [self.validate_record(record) for record in records]
    
    def get_validation_report(self) -> Dict[str, Any]:
        """Get validation statistics report.
        
        Returns:
            Dictionary with validation statistics
        """
        return {
            **self.validation_stats,
            "validation_rate": (
                self.validation_stats["valid_records"] / 
                max(self.validation_stats["total_validated"], 1)
            ),
        }