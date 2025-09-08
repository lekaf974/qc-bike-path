"""Tests for utility modules."""

from unittest.mock import MagicMock
from unittest.mock import patch

from qc_bike_path.utils.logging import LoggerMixin
from qc_bike_path.utils.logging import get_logger
from qc_bike_path.utils.logging import log_data_operation
from qc_bike_path.utils.logging import log_error_with_context
from qc_bike_path.utils.logging import setup_logging
from qc_bike_path.utils.validators import RecordValidator
from qc_bike_path.utils.validators import sanitize_string_field
from qc_bike_path.utils.validators import validate_bike_path_record
from qc_bike_path.utils.validators import validate_coordinates
from qc_bike_path.utils.validators import validate_geojson_geometry
from qc_bike_path.utils.validators import validate_mongodb_connection_string
from qc_bike_path.utils.validators import validate_numeric_range
from qc_bike_path.utils.validators import validate_record_structure
from qc_bike_path.utils.validators import validate_url


class TestLoggingUtils:
    """Test logging utility functions."""

    def test_setup_logging_json_format(self):
        """Test logging setup with JSON format."""
        with patch("qc_bike_path.utils.logging.settings") as mock_settings:
            mock_settings.log_level = "DEBUG"
            mock_settings.log_format = "json"

            with patch(
                "qc_bike_path.utils.logging.structlog.configure"
            ) as mock_configure:
                setup_logging()
                mock_configure.assert_called_once()

    def test_setup_logging_text_format(self):
        """Test logging setup with text format."""
        with patch("qc_bike_path.utils.logging.settings") as mock_settings:
            mock_settings.log_level = "INFO"
            mock_settings.log_format = "text"

            with patch(
                "qc_bike_path.utils.logging.structlog.configure"
            ) as mock_configure:
                setup_logging()
                mock_configure.assert_called_once()

    def test_get_logger(self):
        """Test logger creation."""
        with patch(
            "qc_bike_path.utils.logging.structlog.get_logger"
        ) as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger

            logger = get_logger("test_module")

            assert logger == mock_logger
            mock_get_logger.assert_called_once_with("test_module")

    def test_logger_mixin(self):
        """Test LoggerMixin class."""

        class TestClass(LoggerMixin):
            pass

        test_instance = TestClass()

        with patch(
            "qc_bike_path.utils.logging.structlog.get_logger"
        ) as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger

            logger = test_instance.logger

            assert logger == mock_logger
            mock_get_logger.assert_called_once_with("TestClass")

    def test_log_data_operation(self):
        """Test data operation logging."""
        with patch(
            "qc_bike_path.utils.logging.structlog.get_logger"
        ) as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger

            log_data_operation("transform", 100, batch_id="test_batch")

            mock_logger.info.assert_called_once_with(
                "Data operation completed",
                operation="transform",
                record_count=100,
                batch_id="test_batch",
            )

    def test_log_error_with_context(self):
        """Test error logging with context."""
        mock_logger = MagicMock()
        test_error = ValueError("Test error")
        context = {"user_id": "test_user", "request_id": "req_123"}

        log_error_with_context(mock_logger, test_error, context, "test_operation")

        mock_logger.error.assert_called_once_with(
            "Operation failed with error",
            operation="test_operation",
            error_type="ValueError",
            error_message="Test error",
            user_id="test_user",
            request_id="req_123",
        )


class TestValidatorUtils:
    """Test validation utility functions."""

    def test_validate_geojson_geometry_valid_point(self):
        """Test validation of valid Point geometry."""
        valid_point = {"type": "Point", "coordinates": [-71.2080, 46.8139]}
        assert validate_geojson_geometry(valid_point) is True

    def test_validate_geojson_geometry_valid_linestring(self):
        """Test validation of valid LineString geometry."""
        valid_linestring = {
            "type": "LineString",
            "coordinates": [[-71.2080, 46.8139], [-71.2070, 46.8145]],
        }
        assert validate_geojson_geometry(valid_linestring) is True

    def test_validate_geojson_geometry_invalid(self):
        """Test validation of invalid geometry."""
        # Missing required fields
        assert validate_geojson_geometry({}) is False
        assert validate_geojson_geometry({"type": "Point"}) is False
        assert validate_geojson_geometry({"coordinates": []}) is False

        # Invalid type
        invalid_geometry = {"type": "InvalidType", "coordinates": []}
        assert validate_geojson_geometry(invalid_geometry) is False

        # Not a dictionary
        assert validate_geojson_geometry("not a dict") is False

    def test_validate_coordinates_valid(self):
        """Test validation of valid coordinates."""
        # Single coordinate
        assert validate_coordinates(-71.2080) is True
        assert validate_coordinates(46.8139) is True

        # Point coordinates
        assert validate_coordinates([-71.2080, 46.8139]) is True

        # LineString coordinates
        assert validate_coordinates([[-71.2080, 46.8139], [-71.2070, 46.8145]]) is True

    def test_validate_coordinates_invalid(self):
        """Test validation of invalid coordinates."""
        # Out of range
        assert validate_coordinates(200) is False
        assert validate_coordinates(-200) is False

        # Empty list
        assert validate_coordinates([]) is False

        # Invalid type
        assert validate_coordinates("not a number") is False

    def test_validate_url_valid(self):
        """Test validation of valid URLs."""
        assert validate_url("https://example.com") is True
        assert validate_url("http://localhost:8080") is True
        assert validate_url("https://api.example.com/v1/data") is True

    def test_validate_url_invalid(self):
        """Test validation of invalid URLs."""
        assert validate_url("") is False
        assert validate_url("not a url") is False
        assert validate_url("ftp://example.com") is False
        assert validate_url(None) is False

    def test_validate_mongodb_connection_string_valid(self):
        """Test validation of valid MongoDB connection strings."""
        assert validate_mongodb_connection_string("mongodb://localhost:27017") is True
        assert (
            validate_mongodb_connection_string(
                "mongodb+srv://user:pass@cluster.example.com"
            )
            is True
        )

    def test_validate_mongodb_connection_string_invalid(self):
        """Test validation of invalid MongoDB connection strings."""
        assert validate_mongodb_connection_string("") is False
        assert validate_mongodb_connection_string("mysql://localhost") is False
        assert validate_mongodb_connection_string("localhost:27017") is False
        assert validate_mongodb_connection_string(None) is False

    def test_validate_record_structure_valid(self):
        """Test validation of valid record structure."""
        record = {"id": "1", "name": "test", "type": "path"}
        required_fields = ["id", "name"]

        assert validate_record_structure(record, required_fields) is True

    def test_validate_record_structure_invalid(self):
        """Test validation of invalid record structure."""
        record = {"id": "1"}  # Missing required field
        required_fields = ["id", "name"]

        assert validate_record_structure(record, required_fields) is False
        assert validate_record_structure("not a dict", required_fields) is False

    def test_sanitize_string_field_valid(self):
        """Test string field sanitization with valid input."""
        assert sanitize_string_field("Valid Text") == "Valid Text"
        assert sanitize_string_field("  Spaced  ") == "Spaced"
        assert sanitize_string_field(123) == "123"

    def test_sanitize_string_field_invalid(self):
        """Test string field sanitization with invalid input."""
        assert sanitize_string_field("") is None
        assert sanitize_string_field("null") is None
        assert sanitize_string_field("N/A") is None
        assert sanitize_string_field(None) is None

    def test_sanitize_string_field_with_length_limit(self):
        """Test string field sanitization with length limit."""
        long_text = "A" * 100
        truncated = sanitize_string_field(long_text, max_length=50)

        assert len(truncated) == 50
        assert truncated == "A" * 50

    def test_validate_numeric_range_valid(self):
        """Test numeric range validation with valid values."""
        assert validate_numeric_range(50, min_val=0, max_val=100) is True
        assert validate_numeric_range(0, min_val=0, max_val=100) is True
        assert validate_numeric_range(100, min_val=0, max_val=100) is True
        assert validate_numeric_range(50.5, min_val=0, max_val=100) is True

    def test_validate_numeric_range_invalid(self):
        """Test numeric range validation with invalid values."""
        assert validate_numeric_range(-1, min_val=0, max_val=100) is False
        assert validate_numeric_range(101, min_val=0, max_val=100) is False
        assert validate_numeric_range("not a number", min_val=0, max_val=100) is False

    def test_validate_bike_path_record_valid(self):
        """Test bike path record validation with valid record."""
        valid_record = {
            "id": "1",
            "name": "Test Path",
            "geometry": {"type": "Point", "coordinates": [-71.2080, 46.8139]},
            "length_km": 2.5,
        }

        errors = validate_bike_path_record(valid_record)
        assert len(errors) == 0

    def test_validate_bike_path_record_invalid_geometry(self):
        """Test bike path record validation with invalid geometry."""
        invalid_record = {
            "id": "1",
            "name": "Test Path",
            "geometry": {"type": "InvalidType", "coordinates": []},
            "length_km": 2.5,
        }

        errors = validate_bike_path_record(invalid_record)
        assert len(errors) > 0
        assert any("Invalid GeoJSON geometry" in error for error in errors)

    def test_validate_bike_path_record_invalid_length(self):
        """Test bike path record validation with invalid length."""
        invalid_record = {"id": "1", "name": "Test Path", "length_km": "not a number"}

        errors = validate_bike_path_record(invalid_record)
        assert len(errors) > 0
        assert any("Length must be a valid number" in error for error in errors)

    def test_validate_bike_path_record_long_name(self):
        """Test bike path record validation with overly long name."""
        invalid_record = {
            "id": "1",
            "name": "A" * 600,  # Exceeds 500 character limit
        }

        errors = validate_bike_path_record(invalid_record)
        assert len(errors) > 0
        assert any("Name exceeds maximum length" in error for error in errors)

    def test_validate_bike_path_record_not_dict(self):
        """Test bike path record validation with non-dict input."""
        errors = validate_bike_path_record("not a dict")
        assert len(errors) > 0
        assert "Record must be a dictionary" in errors


class TestRecordValidator:
    """Test RecordValidator class."""

    def test_validator_initialization(self):
        """Test validator initialization."""
        validator = RecordValidator(strict_mode=True)
        assert validator.strict_mode is True
        assert validator.validation_stats["total_validated"] == 0

    def test_validate_record_valid(self):
        """Test validating a valid record."""
        validator = RecordValidator()
        valid_record = {
            "id": "1",
            "name": "Test Path",
            "geometry": {"type": "Point", "coordinates": [-71.2080, 46.8139]},
        }

        is_valid, errors = validator.validate_record(valid_record)

        assert is_valid is True
        assert len(errors) == 0
        assert validator.validation_stats["valid_records"] == 1

    def test_validate_record_invalid(self):
        """Test validating an invalid record."""
        validator = RecordValidator()
        invalid_record = {"id": "1", "length_km": "not a number"}

        is_valid, errors = validator.validate_record(invalid_record)

        assert is_valid is False
        assert len(errors) > 0
        assert validator.validation_stats["invalid_records"] == 1

    def test_validate_record_strict_mode(self):
        """Test validation in strict mode."""
        validator = RecordValidator(strict_mode=True)
        record = {"id": "1"}  # Missing name and geometry

        is_valid, errors = validator.validate_record(record)

        assert is_valid is False
        assert any("Name is required in strict mode" in error for error in errors)
        assert any("Geometry is required in strict mode" in error for error in errors)

    def test_validate_batch(self):
        """Test batch validation."""
        validator = RecordValidator()
        records = [
            {
                "id": "1",
                "name": "Valid Path",
                "geometry": {"type": "Point", "coordinates": [-71.2080, 46.8139]},
            },
            {"id": "2", "length_km": "invalid"},
            {"id": "3", "name": "Another Valid Path"},
        ]

        results = validator.validate_batch(records)

        assert len(results) == 3
        assert all(isinstance(result, tuple) for result in results)
        assert all(len(result) == 2 for result in results)  # (is_valid, errors)

    def test_get_validation_report(self):
        """Test validation report generation."""
        validator = RecordValidator()

        # Validate some records to generate stats
        validator.validate_record({"id": "1", "name": "Valid"})
        validator.validate_record({"id": "2", "length_km": "invalid"})

        report = validator.get_validation_report()

        assert "total_validated" in report
        assert "valid_records" in report
        assert "invalid_records" in report
        assert "validation_rate" in report
        assert "common_errors" in report

        assert report["total_validated"] == 2
        assert isinstance(report["validation_rate"], float)
        assert 0 <= report["validation_rate"] <= 1
