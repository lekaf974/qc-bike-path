"""Tests for the load module."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from pymongo.errors import BulkWriteError
from pymongo.errors import DuplicateKeyError
from pymongo.errors import PyMongoError

from qc_bike_path.load import BikePathDataLoader
from qc_bike_path.load import DatabaseConnectionError
from qc_bike_path.load import save_bike_path_data
from qc_bike_path.load import save_geojson_data
from qc_bike_path.transform import BikePathRecord
from tests.fixtures import get_transformed_record_sample


class TestBikePathDataLoader:
    """Test BikePathDataLoader class."""

    @pytest.fixture
    def mock_client(self):
        """Create mock MongoDB client."""
        client = AsyncMock()
        client.admin.command = AsyncMock(return_value={"ok": 1})

        # Mock database and collection
        database = AsyncMock()
        collection = AsyncMock()

        client.__getitem__.return_value = database
        database.__getitem__.return_value = collection

        return client

    @pytest.fixture
    def loader(self, mock_client):
        """Create loader fixture with mocked MongoDB client."""
        with patch("qc_bike_path.load.AsyncIOMotorClient", return_value=mock_client):
            return BikePathDataLoader()

    @pytest.mark.asyncio
    async def test_connection_success(self, mock_client):
        """Test successful MongoDB connection."""
        with patch("qc_bike_path.load.AsyncIOMotorClient", return_value=mock_client):
            loader = BikePathDataLoader()
            await loader.connect()

            assert loader.client is not None
            assert loader.database is not None
            assert loader.collection is not None
            mock_client.admin.command.assert_called_once_with("ping")

    @pytest.mark.asyncio
    async def test_connection_failure(self):
        """Test MongoDB connection failure."""
        with patch("qc_bike_path.load.AsyncIOMotorClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_client.admin.command.side_effect = PyMongoError("Connection failed")
            mock_client_class.return_value = mock_client

            loader = BikePathDataLoader()

            with pytest.raises(DatabaseConnectionError):
                await loader.connect()

    @pytest.mark.asyncio
    async def test_create_indexes(self, loader, mock_client):
        """Test index creation."""
        await loader.connect()

        # Mock collection.create_indexes to succeed
        loader.collection.create_indexes = AsyncMock()

        await loader.create_indexes()

        loader.collection.create_indexes.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_record_success(self, loader, mock_client):
        """Test successful record save."""
        await loader.connect()

        # Mock successful upsert
        mock_result = MagicMock()
        mock_result.upserted_id = "new_id"
        mock_result.modified_count = 0
        loader.collection.replace_one = AsyncMock(return_value=mock_result)

        record_data = get_transformed_record_sample()
        record = BikePathRecord(**record_data)

        result = await loader.save_record(record)

        assert result is True
        loader.collection.replace_one.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_record_update_existing(self, loader, mock_client):
        """Test updating existing record."""
        await loader.connect()

        # Mock successful update
        mock_result = MagicMock()
        mock_result.upserted_id = None
        mock_result.modified_count = 1
        loader.collection.replace_one = AsyncMock(return_value=mock_result)

        record_data = get_transformed_record_sample()
        record = BikePathRecord(**record_data)

        result = await loader.save_record(record)

        assert result is True

    @pytest.mark.asyncio
    async def test_save_record_duplicate_key_error(self, loader, mock_client):
        """Test handling of duplicate key error."""
        await loader.connect()

        loader.collection.replace_one = AsyncMock(
            side_effect=DuplicateKeyError("Duplicate")
        )

        record_data = get_transformed_record_sample()
        record = BikePathRecord(**record_data)

        result = await loader.save_record(record)

        assert result is False  # Should handle gracefully

    @pytest.mark.asyncio
    async def test_save_records_batch_success(self, loader, mock_client):
        """Test successful batch save."""
        await loader.connect()

        # Mock successful bulk write
        mock_result = MagicMock()
        mock_result.upserted_count = 2
        mock_result.modified_count = 1
        loader.collection.bulk_write = AsyncMock(return_value=mock_result)

        records = [
            BikePathRecord(id="1", name="Path 1", properties={}),
            BikePathRecord(id="2", name="Path 2", properties={}),
            BikePathRecord(id="3", name="Path 3", properties={}),
        ]

        stats = await loader.save_records_batch(records)

        assert stats["inserted"] == 2
        assert stats["updated"] == 1
        assert stats["errors"] == 0

    @pytest.mark.asyncio
    async def test_save_records_batch_with_errors(self, loader, mock_client):
        """Test batch save with some errors."""
        await loader.connect()

        # Mock bulk write with errors
        bulk_error = BulkWriteError(
            {
                "nUpserted": 1,
                "nModified": 1,
                "writeErrors": [{"index": 2, "code": 11000, "errmsg": "Duplicate key"}],
            }
        )
        loader.collection.bulk_write = AsyncMock(side_effect=bulk_error)

        records = [
            BikePathRecord(id="1", name="Path 1", properties={}),
            BikePathRecord(id="2", name="Path 2", properties={}),
            BikePathRecord(id="3", name="Path 3", properties={}),
        ]

        stats = await loader.save_records_batch(records)

        assert stats["inserted"] == 1
        assert stats["updated"] == 1
        assert stats["errors"] == 1

    @pytest.mark.asyncio
    async def test_save_geojson(self, loader, mock_client):
        """Test GeoJSON save."""
        await loader.connect()

        geojson_collection = AsyncMock()
        geojson_collection.replace_one = AsyncMock()
        loader.database.__getitem__.return_value = geojson_collection

        geojson_data = {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {"processing_timestamp": "2024-01-01T00:00:00Z"},
        }

        result = await loader.save_geojson(geojson_data)

        assert result is True
        geojson_collection.replace_one.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_collection_stats(self, loader, mock_client):
        """Test collection statistics retrieval."""
        await loader.connect()

        # Mock database command and collection operations
        loader.database.command = AsyncMock(
            return_value={"storageSize": 1024, "nindexes": 5}
        )
        loader.collection.count_documents = AsyncMock(return_value=100)
        loader.collection.find_one = AsyncMock(
            return_value={"extraction_timestamp": "2024-01-01T00:00:00Z"}
        )

        stats = await loader.get_collection_stats()

        assert stats["total_documents"] == 100
        assert stats["storage_size_bytes"] == 1024
        assert stats["index_count"] == 5
        assert stats["latest_extraction"] is not None

    @pytest.mark.asyncio
    async def test_cleanup_old_records(self, loader, mock_client):
        """Test cleanup of old records."""
        await loader.connect()

        # Mock delete operation
        mock_result = MagicMock()
        mock_result.deleted_count = 50
        loader.collection.delete_many = AsyncMock(return_value=mock_result)

        deleted_count = await loader.cleanup_old_records(days_to_keep=7)

        assert deleted_count == 50
        loader.collection.delete_many.assert_called_once()


class TestConvenienceFunctions:
    """Test module convenience functions."""

    @pytest.mark.asyncio
    async def test_save_bike_path_data_function(self):
        """Test save_bike_path_data convenience function."""
        records = [BikePathRecord(id="1", name="Test", properties={})]

        with patch("qc_bike_path.load.BikePathDataLoader") as mock_loader:
            mock_instance = AsyncMock()
            mock_instance.save_records_batch = AsyncMock(
                return_value={"inserted": 1, "updated": 0, "errors": 0}
            )
            mock_loader.return_value.__aenter__.return_value = mock_instance

            result = await save_bike_path_data(records)

            assert result["inserted"] == 1
            mock_instance.save_records_batch.assert_called_once_with(records)

    @pytest.mark.asyncio
    async def test_save_geojson_data_function(self):
        """Test save_geojson_data convenience function."""
        geojson_data = {"type": "FeatureCollection", "features": []}

        with patch("qc_bike_path.load.BikePathDataLoader") as mock_loader:
            mock_instance = AsyncMock()
            mock_instance.save_geojson = AsyncMock(return_value=True)
            mock_loader.return_value.__aenter__.return_value = mock_instance

            result = await save_geojson_data(geojson_data)

            assert result is True
            mock_instance.save_geojson.assert_called_once_with(geojson_data)


# Integration test (requires actual MongoDB)
@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_mongodb_integration():
    """Integration test with real MongoDB."""
    # This test would be skipped unless running integration tests
    pytest.skip("Integration test - requires real MongoDB connection")
