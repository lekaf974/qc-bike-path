"""Main ETL pipeline for Quebec bike path data."""

import asyncio
import sys

import structlog

from qc_bike_path.config import settings
from qc_bike_path.extract import extract_bike_path_data
from qc_bike_path.load import save_bike_path_data
from qc_bike_path.load import save_geojson_data
from qc_bike_path.transform import create_geojson_from_records
from qc_bike_path.transform import transform_bike_path_data
from qc_bike_path.utils.logging import setup_logging

logger = structlog.get_logger(__name__)


class ETLPipelineError(Exception):
    """Exception raised during ETL pipeline execution."""

    pass


class BikePathETLPipeline:
    """Main ETL pipeline for Quebec bike path data."""

    def __init__(self) -> None:
        """Initialize the ETL pipeline."""
        self.setup_complete = False

    async def setup(self) -> None:
        """Setup the ETL pipeline."""
        if not self.setup_complete:
            setup_logging()
            logger.info("ETL Pipeline initialized", environment=settings.environment)
            self.setup_complete = True

    async def run_extract_phase(self, limit: int | None = None) -> dict:
        """Run the data extraction phase.

        Args:
            limit: Optional limit on number of records to extract

        Returns:
            Extracted raw data

        Raises:
            ETLPipelineError: If extraction fails
        """
        try:
            logger.info("Starting data extraction phase")
            raw_data = await extract_bike_path_data(limit=limit)

            # Log extraction statistics
            if "result" in raw_data and "records" in raw_data["result"]:
                record_count = len(raw_data["result"]["records"])
                logger.info("Data extraction completed", record_count=record_count)

            return raw_data

        except Exception as e:
            logger.error("Data extraction phase failed", error=str(e))
            raise ETLPipelineError(f"Extraction phase failed: {e}") from e

    async def run_transform_phase(self, raw_data: dict) -> tuple[list, dict]:
        """Run the data transformation phase.

        Args:
            raw_data: Raw data from extraction phase

        Returns:
            Tuple of (transformed_records, geojson_data)

        Raises:
            ETLPipelineError: If transformation fails
        """
        try:
            logger.info("Starting data transformation phase")

            # Transform records
            transformed_records = transform_bike_path_data(raw_data)

            # Create GeoJSON
            geojson_data = create_geojson_from_records(transformed_records)

            logger.info(
                "Data transformation completed",
                transformed_count=len(transformed_records),
                geojson_features=len(geojson_data.get("features", [])),
            )

            return transformed_records, geojson_data

        except Exception as e:
            logger.error("Data transformation phase failed", error=str(e))
            raise ETLPipelineError(f"Transformation phase failed: {e}") from e

    async def run_load_phase(
        self, transformed_records: list, geojson_data: dict
    ) -> dict:
        """Run the data loading phase.

        Args:
            transformed_records: Transformed bike path records
            geojson_data: GeoJSON FeatureCollection

        Returns:
            Dictionary with loading statistics

        Raises:
            ETLPipelineError: If loading fails
        """
        try:
            logger.info("Starting data loading phase")

            # Save transformed records to MongoDB
            save_stats = await save_bike_path_data(transformed_records)

            # Save GeoJSON data
            geojson_saved = await save_geojson_data(geojson_data)

            save_stats["geojson_saved"] = geojson_saved

            logger.info(
                "Data loading completed",
                inserted=save_stats["inserted"],
                updated=save_stats["updated"],
                errors=save_stats["errors"],
                geojson_saved=geojson_saved,
            )

            return save_stats

        except Exception as e:
            logger.error("Data loading phase failed", error=str(e))
            raise ETLPipelineError(f"Loading phase failed: {e}") from e

    async def run_full_pipeline(self, limit: int | None = None) -> dict:
        """Run the complete ETL pipeline.

        Args:
            limit: Optional limit on number of records to process

        Returns:
            Dictionary with pipeline execution statistics

        Raises:
            ETLPipelineError: If any phase fails
        """
        await self.setup()

        start_time = asyncio.get_event_loop().time()

        try:
            logger.info("Starting complete ETL pipeline")

            # Phase 1: Extract
            raw_data = await self.run_extract_phase(limit=limit)

            # Phase 2: Transform
            transformed_records, geojson_data = await self.run_transform_phase(raw_data)

            # Phase 3: Load
            load_stats = await self.run_load_phase(transformed_records, geojson_data)

            end_time = asyncio.get_event_loop().time()
            execution_time = end_time - start_time

            pipeline_stats = {
                "success": True,
                "execution_time_seconds": round(execution_time, 2),
                "records_processed": len(transformed_records),
                "records_inserted": load_stats["inserted"],
                "records_updated": load_stats["updated"],
                "load_errors": load_stats["errors"],
                "geojson_saved": load_stats["geojson_saved"],
            }

            logger.info("ETL pipeline completed successfully", **pipeline_stats)
            return pipeline_stats

        except Exception as e:
            end_time = asyncio.get_event_loop().time()
            execution_time = end_time - start_time

            error_stats = {
                "success": False,
                "execution_time_seconds": round(execution_time, 2),
                "error": str(e),
            }

            logger.error("ETL pipeline failed", **error_stats)
            raise ETLPipelineError(f"Pipeline execution failed: {e}") from e

    async def health_check(self) -> dict:
        """Perform a health check of the ETL pipeline components.

        Returns:
            Dictionary with health check results
        """
        await self.setup()

        health_status = {
            "pipeline": "healthy",
            "components": {},
            "timestamp": asyncio.get_event_loop().time(),
        }

        try:
            # Test extraction (with limit to avoid processing too much data)
            logger.info("Running health check - testing extraction")
            await self.run_extract_phase(limit=1)
            health_status["components"]["extraction"] = "healthy"
        except Exception as e:
            logger.warning("Health check failed for extraction", error=str(e))
            health_status["components"]["extraction"] = f"unhealthy: {e}"
            health_status["pipeline"] = "degraded"

        try:
            # Test database connection
            from qc_bike_path.load import BikePathDataLoader

            logger.info("Running health check - testing database connection")
            async with BikePathDataLoader() as loader:
                await loader.get_collection_stats()
            health_status["components"]["database"] = "healthy"
        except Exception as e:
            logger.warning("Health check failed for database", error=str(e))
            health_status["components"]["database"] = f"unhealthy: {e}"
            health_status["pipeline"] = "unhealthy"

        logger.info("Health check completed", status=health_status["pipeline"])
        return health_status


async def main() -> None:
    """Main entry point for the ETL pipeline."""
    # Parse command line arguments
    limit = None
    if len(sys.argv) > 1:
        try:
            limit = int(sys.argv[1])
        except ValueError:
            sys.exit(1)

    # Check if we should run health check
    if len(sys.argv) > 1 and sys.argv[1] == "health":
        pipeline = BikePathETLPipeline()
        health_status = await pipeline.health_check()

        if health_status["pipeline"] == "healthy":
            sys.exit(0)
        else:
            sys.exit(1)

    # Run the ETL pipeline
    pipeline = BikePathETLPipeline()

    try:
        await pipeline.run_full_pipeline(limit=limit)

    except ETLPipelineError:
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
