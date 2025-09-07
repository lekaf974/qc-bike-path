"""Tests for the extract module."""

import pytest
from unittest.mock import AsyncMock, patch
from aiohttp import ClientError, ClientResponseError

from qc_bike_path.extract import BikePathDataExtractor, DataExtractionError, extract_bike_path_data
from tests.fixtures import get_sample_api_response, create_mock_aiohttp_response


class TestBikePathDataExtractor:
    """Test cases for BikePathDataExtractor class."""

    @pytest.fixture
    async def extractor(self):
        """Create extractor fixture."""
        async with BikePathDataExtractor() as extractor_instance:
            yield extractor_instance

    @pytest.mark.asyncio
    async def test_successful_data_extraction(self, extractor):
        """Test successful data extraction from API."""
        sample_data = get_sample_api_response()
        mock_response = create_mock_aiohttp_response(sample_data)
        
        with patch.object(extractor.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response
            
            result = await extractor.fetch_bike_path_data()
            
            assert result == sample_data
            assert "result" in result
            assert "records" in result["result"]
            assert len(result["result"]["records"]) == 3

    @pytest.mark.asyncio
    async def test_extraction_with_limit(self, extractor):
        """Test data extraction with record limit."""
        sample_data = get_sample_api_response()
        mock_response = create_mock_aiohttp_response(sample_data)
        
        with patch.object(extractor.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response
            
            result = await extractor.fetch_bike_path_data(limit=5)
            
            # Verify limit parameter was passed
            mock_get.assert_called_once()
            call_kwargs = mock_get.call_args[1]
            assert call_kwargs['params']['limit'] == 5

    @pytest.mark.asyncio
    async def test_api_timeout_error(self, extractor):
        """Test handling of API timeout."""
        with patch.object(extractor.session, 'get') as mock_get:
            mock_get.side_effect = asyncio.TimeoutError("Request timed out")
            
            with pytest.raises(DataExtractionError) as exc_info:
                await extractor.fetch_bike_path_data()
            
            assert "Timeout" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_api_client_error(self, extractor):
        """Test handling of HTTP client errors."""
        with patch.object(extractor.session, 'get') as mock_get:
            mock_get.side_effect = ClientError("Connection failed")
            
            with pytest.raises(DataExtractionError) as exc_info:
                await extractor.fetch_bike_path_data()
            
            assert "Failed to fetch data" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_invalid_response_format(self, extractor):
        """Test handling of invalid API response format."""
        # Response missing required fields
        invalid_response = {"success": True, "data": []}  # Missing 'result' field
        mock_response = create_mock_aiohttp_response(invalid_response)
        
        with patch.object(extractor.session, 'get') as mock_get:
            mock_get.return_value.__aenter__.return_value = mock_response
            
            with pytest.raises(DataExtractionError) as exc_info:
                await extractor.fetch_bike_path_data()
            
            assert "Invalid response format" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_missing_resource_id(self, extractor):
        """Test error when resource ID is not configured."""
        with patch('qc_bike_path.extract.settings') as mock_settings:
            mock_settings.bike_path_resource_id = ""
            
            with pytest.raises(DataExtractionError) as exc_info:
                await extractor.fetch_bike_path_data()
            
            assert "resource ID not configured" in str(exc_info.value)

    def test_validate_response_structure(self, extractor):
        """Test response structure validation."""
        # Valid response
        valid_response = get_sample_api_response()
        assert extractor.validate_response_structure(valid_response) is True
        
        # Invalid responses
        assert extractor.validate_response_structure({}) is False
        assert extractor.validate_response_structure({"result": "not a dict"}) is False
        assert extractor.validate_response_structure({"result": {}}) is False
        assert extractor.validate_response_structure({"result": {"records": "not a list"}}) is False

    @pytest.mark.asyncio
    async def test_retry_logic(self, extractor):
        """Test that retry logic is applied on failures."""
        with patch.object(extractor, '_fetch_data') as mock_fetch:
            # First two calls fail, third succeeds
            mock_fetch.side_effect = [
                ClientError("First failure"),
                ClientError("Second failure"),
                get_sample_api_response()
            ]
            
            result = await extractor.fetch_bike_path_data()
            
            # Should have been called 3 times due to retries
            assert mock_fetch.call_count == 3
            assert result == get_sample_api_response()


class TestConvenienceFunctions:
    """Test convenience functions in extract module."""

    @pytest.mark.asyncio
    async def test_extract_bike_path_data_function(self):
        """Test the extract_bike_path_data convenience function."""
        sample_data = get_sample_api_response()
        
        with patch('qc_bike_path.extract.BikePathDataExtractor') as MockExtractor:
            mock_instance = AsyncMock()
            mock_instance.fetch_bike_path_data.return_value = sample_data
            MockExtractor.return_value.__aenter__.return_value = mock_instance
            
            result = await extract_bike_path_data(limit=10)
            
            assert result == sample_data
            mock_instance.fetch_bike_path_data.assert_called_once_with(limit=10)

    @pytest.mark.asyncio
    async def test_extract_geojson_data_function(self):
        """Test the extract_geojson_data convenience function."""
        from qc_bike_path.extract import extract_geojson_data
        
        sample_data = get_sample_api_response()
        
        with patch('qc_bike_path.extract.BikePathDataExtractor') as MockExtractor:
            mock_instance = AsyncMock()
            mock_instance.fetch_geojson_data.return_value = sample_data
            MockExtractor.return_value.__aenter__.return_value = mock_instance
            
            result = await extract_geojson_data()
            
            assert result == sample_data
            mock_instance.fetch_geojson_data.assert_called_once()


# Integration test (would require actual API access in real scenario)
@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_api_integration():
    """Integration test with real API (requires configuration)."""
    # This test would be skipped in normal test runs
    # and only run when specifically testing against real API
    pytest.skip("Integration test - requires real API configuration")


import asyncio