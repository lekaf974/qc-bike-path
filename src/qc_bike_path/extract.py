"""Data extraction module for Quebec bike path data."""

import asyncio
from typing import Any
from typing import Dict
from typing import Optional

import aiohttp
import structlog
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_exponential

from qc_bike_path.config import settings


logger = structlog.get_logger(__name__)


class DataExtractionError(Exception):
    """Exception raised during data extraction."""

    pass


class BikePathDataExtractor:
    """Extractor for Quebec bike path data from the open data portal."""

    def __init__(self) -> None:
        """Initialize the extractor."""
        self.session: Optional[aiohttp.ClientSession] = None
        self.base_url = settings.api_base_url
        self.timeout = aiohttp.ClientTimeout(total=settings.api_timeout)

    async def __aenter__(self) -> "BikePathDataExtractor":
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    async def _fetch_data(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Fetch data from the API with retry logic.
        
        Args:
            url: The API endpoint URL
            params: Optional query parameters
            
        Returns:
            Parsed JSON response
            
        Raises:
            DataExtractionError: If the request fails after retries
        """
        if not self.session:
            raise DataExtractionError("Session not initialized. Use async context manager.")

        try:
            logger.info("Fetching data from API", url=url, params=params)
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info("Successfully fetched data", status=response.status, size=len(str(data)))
                return data
        except aiohttp.ClientError as e:
            logger.error("HTTP client error during data fetch", error=str(e), url=url)
            raise DataExtractionError(f"Failed to fetch data from {url}: {e}") from e
        except asyncio.TimeoutError as e:
            logger.error("Timeout during data fetch", error=str(e), url=url)
            raise DataExtractionError(f"Timeout while fetching data from {url}") from e

    async def fetch_bike_path_data(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Fetch bike path data from Quebec's open data portal.
        
        Args:
            limit: Optional limit on number of records to fetch
            
        Returns:
            Raw bike path data from the API
            
        Raises:
            DataExtractionError: If extraction fails
        """
        if not settings.bike_path_resource_id:
            raise DataExtractionError(
                "Bike path resource ID not configured. "
                "Please set QC_BIKE_PATH_BIKE_PATH_RESOURCE_ID environment variable."
            )

        params = {
            "resource_id": settings.bike_path_resource_id,
            "format": "json",
        }
        
        if limit:
            params["limit"] = limit

        try:
            data = await self._fetch_data(self.base_url, params)
            
            # Validate basic response structure
            if not isinstance(data, dict):
                raise DataExtractionError("Invalid response format: expected JSON object")
            
            if "result" not in data:
                raise DataExtractionError("Invalid response format: missing 'result' key")
            
            result = data["result"]
            if "records" not in result:
                raise DataExtractionError("Invalid response format: missing 'records' in result")

            records = result["records"]
            logger.info("Successfully extracted bike path data", record_count=len(records))
            
            return data
            
        except Exception as e:
            logger.error("Failed to extract bike path data", error=str(e))
            raise DataExtractionError(f"Bike path data extraction failed: {e}") from e

    async def fetch_geojson_data(self) -> Dict[str, Any]:
        """Fetch bike path data in GeoJSON format.
        
        Returns:
            GeoJSON formatted bike path data
            
        Raises:
            DataExtractionError: If extraction fails
        """
        # For Quebec open data, we might need to construct a different URL for GeoJSON
        # This is a placeholder implementation that would need to be adjusted based on
        # the actual API structure
        geojson_params = {
            "resource_id": settings.bike_path_resource_id,
            "format": "geojson",
        }
        
        try:
            # First try to get GeoJSON directly
            data = await self._fetch_data(self.base_url, geojson_params)
            logger.info("Successfully extracted GeoJSON data")
            return data
        except DataExtractionError:
            # If GeoJSON format is not available, fetch regular data
            # and let the transform module handle conversion
            logger.warning("GeoJSON format not available, falling back to regular data format")
            return await self.fetch_bike_path_data()

    def validate_response_structure(self, data: Dict[str, Any]) -> bool:
        """Validate the basic structure of API response.
        
        Args:
            data: API response data
            
        Returns:
            True if structure is valid, False otherwise
        """
        required_keys = ["result"]
        
        if not all(key in data for key in required_keys):
            return False
            
        result = data.get("result", {})
        if not isinstance(result, dict):
            return False
            
        if "records" not in result:
            return False
            
        records = result.get("records", [])
        if not isinstance(records, list):
            return False
            
        logger.debug("Response structure validation passed", record_count=len(records))
        return True


async def extract_bike_path_data(limit: Optional[int] = None) -> Dict[str, Any]:
    """Convenience function to extract bike path data.
    
    Args:
        limit: Optional limit on number of records to fetch
        
    Returns:
        Extracted bike path data
        
    Raises:
        DataExtractionError: If extraction fails
    """
    async with BikePathDataExtractor() as extractor:
        return await extractor.fetch_bike_path_data(limit=limit)


async def extract_geojson_data() -> Dict[str, Any]:
    """Convenience function to extract GeoJSON bike path data.
    
    Returns:
        GeoJSON formatted bike path data
        
    Raises:
        DataExtractionError: If extraction fails
    """
    async with BikePathDataExtractor() as extractor:
        return await extractor.fetch_geojson_data()