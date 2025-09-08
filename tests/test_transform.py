"""Tests for the transform module."""

import pytest

from qc_bike_path.transform import BikePathRecord
from qc_bike_path.transform import BikePathTransformer
from qc_bike_path.transform import DataTransformationError
from qc_bike_path.transform import create_geojson_from_records
from qc_bike_path.transform import transform_bike_path_data
from tests.fixtures import get_sample_api_response
from tests.fixtures import get_transformed_record_sample


class TestBikePathRecord:
    """Test BikePathRecord Pydantic model."""

    def test_valid_record_creation(self):
        """Test creating a valid BikePathRecord."""
        data = get_transformed_record_sample()
        record = BikePathRecord(**data)

        assert record.id == "1"
        assert record.name == "Piste Cyclable du Vieux-Port"
        assert record.length_km == 2.5
        assert record.geometry is not None
        assert record.geometry["type"] == "Point"

    def test_record_with_minimal_data(self):
        """Test record creation with minimal required data."""
        minimal_data = {"id": "test", "properties": {}}
        record = BikePathRecord(**minimal_data)

        assert record.id == "test"
        assert record.name is None
        assert record.properties == {}


class TestBikePathTransformer:
    """Test BikePathTransformer class."""

    @pytest.fixture
    def transformer(self):
        """Create transformer fixture."""
        return BikePathTransformer()

    def test_clean_text_field(self, transformer):
        """Test text field cleaning."""
        # Valid text
        assert transformer.clean_text_field("Valid Text") == "Valid Text"
        assert transformer.clean_text_field("  Spaced  ") == "Spaced"

        # Invalid/empty values
        assert transformer.clean_text_field("") is None
        assert transformer.clean_text_field("  ") is None
        assert transformer.clean_text_field("null") is None
        assert transformer.clean_text_field("N/A") is None
        assert transformer.clean_text_field(None) is None

    def test_clean_numeric_field(self, transformer):
        """Test numeric field cleaning."""
        # Valid numbers
        assert transformer.clean_numeric_field(42) == 42.0
        assert transformer.clean_numeric_field("3.14") == 3.14
        assert transformer.clean_numeric_field("0") == 0.0

        # Invalid values
        assert transformer.clean_numeric_field("not a number") is None
        assert transformer.clean_numeric_field("") is None
        assert transformer.clean_numeric_field("null") is None
        assert transformer.clean_numeric_field(None) is None

    def test_validate_geometry(self, transformer):
        """Test geometry validation."""
        # Valid geometries
        valid_point = {"type": "Point", "coordinates": [-71.2080, 46.8139]}
        assert transformer.validate_geometry(valid_point) is True

        valid_linestring = {
            "type": "LineString",
            "coordinates": [[-71.2080, 46.8139], [-71.2070, 46.8145]],
        }
        assert transformer.validate_geometry(valid_linestring) is True

        # Invalid geometries
        invalid_geometry = {"type": "InvalidType", "coordinates": []}
        assert transformer.validate_geometry(invalid_geometry) is False

        assert transformer.validate_geometry({}) is False
        assert transformer.validate_geometry(None) is False

    def test_extract_coordinates_from_lat_lon(self, transformer):
        """Test coordinate extraction from latitude/longitude fields."""
        record = {"latitude": 46.8139, "longitude": -71.2080}

        geometry = transformer.extract_coordinates(record)

        assert geometry is not None
        assert geometry["type"] == "Point"
        assert geometry["coordinates"] == [-71.2080, 46.8139]

    def test_extract_coordinates_from_geometry_field(self, transformer):
        """Test coordinate extraction from geometry field."""
        geometry_data = {"type": "Point", "coordinates": [-71.2080, 46.8139]}
        record = {"geometry": geometry_data}

        geometry = transformer.extract_coordinates(record)

        assert geometry == geometry_data

    def test_transform_record_success(self, transformer):
        """Test successful record transformation."""
        raw_record = {
            "id": "1",
            "name": "Test Path",
            "type": "Piste cyclable",
            "surface": "Asphalte",
            "length_km": "2.5",
            "latitude": 46.8139,
            "longitude": -71.2080,
            "extra_field": "extra_value",
        }

        transformed = transformer.transform_record(raw_record)

        assert transformed is not None
        assert transformed.id == "1"
        assert transformed.name == "Test Path"
        assert transformed.length_km == 2.5
        assert transformed.geometry is not None
        assert "extra_field" in transformed.properties

    def test_transform_record_with_invalid_data(self, transformer):
        """Test record transformation with invalid data."""
        invalid_record = {
            "id": "invalid",
            "length_km": "not_a_number",
            "latitude": 200,  # Invalid latitude
        }

        # Should still create a record, but with cleaned data
        transformed = transformer.transform_record(invalid_record)

        assert transformed is not None
        assert transformed.id == "invalid"
        assert transformed.length_km is None  # Invalid number cleaned to None

    def test_transform_batch(self, transformer):
        """Test batch transformation."""
        records = [
            {"id": "1", "name": "Path 1", "latitude": 46.8139, "longitude": -71.2080},
            {"id": "2", "name": "Path 2", "latitude": 46.8140, "longitude": -71.2081},
            {"id": "invalid", "latitude": 300},  # Invalid record
        ]

        transformed_records = transformer.transform_batch(records)

        # Should have 2 valid records (invalid one might be filtered)
        assert len(transformed_records) >= 2
        assert all(isinstance(r, BikePathRecord) for r in transformed_records)

    def test_create_geojson_feature_collection(self, transformer):
        """Test GeoJSON FeatureCollection creation."""
        records = [
            BikePathRecord(
                id="1",
                name="Test Path",
                geometry={"type": "Point", "coordinates": [-71.2080, 46.8139]},
                properties={},
            )
        ]

        feature_collection = transformer.create_geojson_feature_collection(records)

        assert feature_collection["type"] == "FeatureCollection"
        assert len(feature_collection["features"]) == 1
        assert feature_collection["features"][0]["type"] == "Feature"
        assert "metadata" in feature_collection


class TestTransformModule:
    """Test module-level functions."""

    def test_transform_bike_path_data_success(self):
        """Test successful data transformation."""
        raw_data = get_sample_api_response()

        transformed_records = transform_bike_path_data(raw_data)

        assert len(transformed_records) > 0
        assert all(isinstance(r, BikePathRecord) for r in transformed_records)

    def test_transform_bike_path_data_invalid_structure(self):
        """Test transformation with invalid data structure."""
        invalid_data = {"invalid": "structure"}

        with pytest.raises(DataTransformationError):
            transform_bike_path_data(invalid_data)

    def test_create_geojson_from_records(self):
        """Test GeoJSON creation from records."""
        records = [
            BikePathRecord(
                id="1",
                name="Test Path",
                geometry={"type": "Point", "coordinates": [-71.2080, 46.8139]},
                properties={},
            )
        ]

        geojson = create_geojson_from_records(records)

        assert geojson["type"] == "FeatureCollection"
        assert len(geojson["features"]) == 1


# Performance test
@pytest.mark.slow
def test_transform_large_dataset_performance():
    """Test transformation performance with large dataset."""
    from tests.fixtures import SAMPLE_LARGE_DATASET

    start_time = pytest.importorskip("time").time()
    transformed_records = transform_bike_path_data(SAMPLE_LARGE_DATASET)
    end_time = pytest.importorskip("time").time()

    execution_time = end_time - start_time

    assert len(transformed_records) > 0
    assert execution_time < 10  # Should complete within 10 seconds
