"""Data loading module for saving processed bike path data to MongoDB."""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import structlog
from motor.motor_asyncio import AsyncIOMotorClient
from motor.motor_asyncio import AsyncIOMotorCollection
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import IndexModel
from pymongo import ASCENDING
from pymongo import GEOSPHERE
from pymongo.errors import BulkWriteError
from pymongo.errors import DuplicateKeyError
from pymongo.errors import PyMongoError

from qc_bike_path.config import settings
from qc_bike_path.transform import BikePathRecord


logger = structlog.get_logger(__name__)


class DatabaseConnectionError(Exception):
    """Exception raised for database connection issues."""

    pass


class DataLoadError(Exception):
    """Exception raised during data loading."""

    pass


class BikePathDataLoader:
    """Loader for saving bike path data to MongoDB."""

    def __init__(self) -> None:
        """Initialize the loader."""
        self.client: Optional[AsyncIOMotorClient] = None
        self.database: Optional[AsyncIOMotorDatabase] = None
        self.collection: Optional[AsyncIOMotorCollection] = None

    async def __aenter__(self) -> "BikePathDataLoader":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.disconnect()

    async def connect(self) -> None:
        """Connect to MongoDB."""
        try:
            self.client = AsyncIOMotorClient(
                settings.mongodb_url,
                serverSelectionTimeoutMS=settings.mongodb_timeout,
            )
            
            # Test the connection
            await self.client.admin.command("ping")
            
            self.database = self.client[settings.mongodb_database]
            self.collection = self.database[settings.mongodb_collection]
            
            logger.info(
                "Connected to MongoDB",
                database=settings.mongodb_database,
                collection=settings.mongodb_collection,
            )
            
            # Ensure indexes are created
            await self.create_indexes()
            
        except Exception as e:
            logger.error("Failed to connect to MongoDB", error=str(e))
            raise DatabaseConnectionError(f"MongoDB connection failed: {e}") from e

    async def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")

    async def create_indexes(self) -> None:
        """Create database indexes for optimization.
        
        Creates indexes for:
        - id field (unique)
        - geometry field (2dsphere for geospatial queries)  
        - extraction_timestamp field
        - name field (text search)
        """
        if not self.collection:
            raise DatabaseConnectionError("Collection not initialized")

        indexes = [
            IndexModel([("id", ASCENDING)], unique=True, sparse=True),
            IndexModel([("geometry", GEOSPHERE)]),  # For geospatial queries
            IndexModel([("extraction_timestamp", ASCENDING)]),
            IndexModel([("name", "text"), ("type", "text")]),  # Text search
            IndexModel([("type", ASCENDING)]),
            IndexModel([("surface", ASCENDING)]),
        ]

        try:
            await self.collection.create_indexes(indexes)
            logger.info("Database indexes created successfully")
        except Exception as e:
            logger.warning("Failed to create some indexes", error=str(e))

    async def save_record(self, record: BikePathRecord) -> bool:
        """Save a single bike path record to MongoDB.
        
        Args:
            record: BikePathRecord to save
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            DataLoadError: If save operation fails
        """
        if not self.collection:
            raise DatabaseConnectionError("Collection not initialized")

        try:
            # Convert Pydantic model to dict
            record_dict = record.dict(exclude_none=True)
            
            # Use upsert to handle duplicates
            filter_criteria = {"id": record.id} if record.id else {"_id": record_dict.get("_id")}
            
            result = await self.collection.replace_one(
                filter_criteria,
                record_dict,
                upsert=True,
            )
            
            if result.upserted_id:
                logger.debug("Inserted new record", id=record.id, upserted_id=str(result.upserted_id))
            elif result.modified_count > 0:
                logger.debug("Updated existing record", id=record.id)
            else:
                logger.debug("Record unchanged", id=record.id)
                
            return True
            
        except DuplicateKeyError as e:
            logger.warning("Duplicate record found", id=record.id, error=str(e))
            return False
        except PyMongoError as e:
            logger.error("MongoDB error during record save", id=record.id, error=str(e))
            raise DataLoadError(f"Failed to save record {record.id}: {e}") from e

    async def save_records_batch(self, records: List[BikePathRecord]) -> Dict[str, int]:
        """Save a batch of bike path records to MongoDB.
        
        Args:
            records: List of BikePathRecord objects to save
            
        Returns:
            Dictionary with save statistics
            
        Raises:
            DataLoadError: If batch save operation fails
        """
        if not self.collection:
            raise DatabaseConnectionError("Collection not initialized")

        if not records:
            return {"inserted": 0, "updated": 0, "errors": 0}

        try:
            operations = []
            
            for record in records:
                record_dict = record.dict(exclude_none=True)
                
                # Create upsert operation
                filter_criteria = {"id": record.id} if record.id else {"_id": record_dict.get("_id")}
                
                operations.append({
                    "replaceOne": {
                        "filter": filter_criteria,
                        "replacement": record_dict,
                        "upsert": True,
                    }
                })
            
            # Execute bulk write with ordered=False for better performance
            result = await self.collection.bulk_write(operations, ordered=False)
            
            stats = {
                "inserted": result.upserted_count,
                "updated": result.modified_count,
                "errors": 0,
            }
            
            logger.info(
                "Batch save completed",
                total_records=len(records),
                inserted=stats["inserted"],
                updated=stats["updated"],
            )
            
            return stats
            
        except BulkWriteError as e:
            # Handle partial success in bulk operations
            stats = {
                "inserted": e.details.get("nUpserted", 0),
                "updated": e.details.get("nModified", 0),
                "errors": len(e.details.get("writeErrors", [])),
            }
            
            logger.warning(
                "Bulk write completed with errors",
                inserted=stats["inserted"],
                updated=stats["updated"],
                errors=stats["errors"],
                error_details=e.details.get("writeErrors", [])[:5],  # Log first 5 errors
            )
            
            return stats
            
        except PyMongoError as e:
            logger.error("MongoDB error during batch save", error=str(e))
            raise DataLoadError(f"Failed to save batch records: {e}") from e

    async def save_geojson(self, geojson_data: Dict[str, Any]) -> bool:
        """Save GeoJSON data to a separate collection.
        
        Args:
            geojson_data: GeoJSON FeatureCollection
            
        Returns:
            True if successful
            
        Raises:
            DataLoadError: If save operation fails
        """
        if not self.database:
            raise DatabaseConnectionError("Database not initialized")

        try:
            geojson_collection = self.database[f"{settings.mongodb_collection}_geojson"]
            
            # Add timestamp to the GeoJSON document
            geojson_document = {
                **geojson_data,
                "stored_at": geojson_data.get("metadata", {}).get("processing_timestamp"),
            }
            
            # Replace the entire GeoJSON document (only keep latest version)
            await geojson_collection.replace_one(
                {},  # Empty filter to replace the single document
                geojson_document,
                upsert=True,
            )
            
            logger.info("GeoJSON data saved successfully")
            return True
            
        except PyMongoError as e:
            logger.error("Failed to save GeoJSON data", error=str(e))
            raise DataLoadError(f"Failed to save GeoJSON data: {e}") from e

    async def get_collection_stats(self) -> Dict[str, Any]:
        """Get statistics about the collection.
        
        Returns:
            Dictionary with collection statistics
        """
        if not self.collection:
            raise DatabaseConnectionError("Collection not initialized")

        try:
            stats = await self.database.command("collStats", settings.mongodb_collection)
            
            # Get count and latest record
            count = await self.collection.count_documents({})
            latest_record = await self.collection.find_one(
                {},
                sort=[("extraction_timestamp", -1)],
            )
            
            return {
                "total_documents": count,
                "storage_size_bytes": stats.get("storageSize", 0),
                "index_count": stats.get("nindexes", 0),
                "latest_extraction": latest_record.get("extraction_timestamp") if latest_record else None,
            }
            
        except PyMongoError as e:
            logger.error("Failed to get collection stats", error=str(e))
            return {}

    async def cleanup_old_records(self, days_to_keep: int = 30) -> int:
        """Remove records older than specified days.
        
        Args:
            days_to_keep: Number of days of records to keep
            
        Returns:
            Number of records deleted
        """
        if not self.collection:
            raise DatabaseConnectionError("Collection not initialized")

        from datetime import datetime, timedelta
        
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        try:
            result = await self.collection.delete_many(
                {"extraction_timestamp": {"$lt": cutoff_date}}
            )
            
            deleted_count = result.deleted_count
            logger.info(
                "Cleaned up old records",
                deleted_count=deleted_count,
                cutoff_date=cutoff_date.isoformat(),
            )
            
            return deleted_count
            
        except PyMongoError as e:
            logger.error("Failed to cleanup old records", error=str(e))
            raise DataLoadError(f"Failed to cleanup old records: {e}") from e


async def save_bike_path_data(records: List[BikePathRecord]) -> Dict[str, int]:
    """Convenience function to save bike path data.
    
    Args:
        records: List of BikePathRecord objects to save
        
    Returns:
        Dictionary with save statistics
        
    Raises:
        DataLoadError: If save operation fails
    """
    async with BikePathDataLoader() as loader:
        return await loader.save_records_batch(records)


async def save_geojson_data(geojson_data: Dict[str, Any]) -> bool:
    """Convenience function to save GeoJSON data.
    
    Args:
        geojson_data: GeoJSON FeatureCollection
        
    Returns:
        True if successful
        
    Raises:
        DataLoadError: If save operation fails
    """
    async with BikePathDataLoader() as loader:
        return await loader.save_geojson(geojson_data)