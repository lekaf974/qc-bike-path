"""Configuration management for QC Bike Path ETL service."""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""

    # API Configuration
    api_base_url: str = Field(
        default="https://www.donneesquebec.ca/recherche/api/3/action/datastore_search",
        description="Base URL for Quebec data API",
    )
    bike_path_resource_id: str = Field(
        default="",  # This will need to be set based on actual resource ID
        description="Resource ID for bike path dataset",
    )
    api_timeout: int = Field(default=30, description="API request timeout in seconds")
    api_retry_attempts: int = Field(
        default=3, description="Number of API retry attempts"
    )

    # MongoDB Configuration
    mongodb_url: str = Field(
        default="mongodb://localhost:27017",
        description="MongoDB connection URL",
    )
    mongodb_database: str = Field(
        default="qc_bike_path",
        description="MongoDB database name",
    )
    mongodb_collection: str = Field(
        default="bike_paths",
        description="MongoDB collection name for bike path data",
    )
    mongodb_timeout: int = Field(
        default=5000,
        description="MongoDB connection timeout in milliseconds",
    )

    # Application Configuration
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Logging format (json or text)")
    batch_size: int = Field(
        default=1000,
        description="Batch size for processing records",
    )

    # Environment
    environment: str = Field(default="development", description="Environment name")
    debug: bool = Field(default=False, description="Debug mode")

    # Optional features
    enable_caching: bool = Field(default=True, description="Enable response caching")
    cache_ttl_seconds: int = Field(default=3600, description="Cache TTL in seconds")

    class Config:
        """Pydantic configuration."""

        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        env_prefix = "QC_BIKE_PATH_"


def get_settings() -> Settings:
    """Get application settings instance."""
    return Settings()


# Global settings instance
settings = get_settings()
