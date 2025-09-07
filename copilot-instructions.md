# Copilot Instructions - Quebec Bike Path ETL Service

## Project Overview

This repository contains ETL (Extract, Transform, Load) services for processing Quebec City's public bike path data. The service fetches GeoJSON data from the official Quebec government data portal, processes it, and stores it in MongoDB.

**Data Source**: https://www.donneesquebec.ca/recherche/dataset/vque_24

## Development Guidelines

### Technology Stack

- **Python**: Latest stable version (3.11+)
- **Database**: MongoDB
- **Testing**: pytest
- **Environment Management**: venv or similar
- **Data Format**: GeoJSON
- **Linting/Formatting**: ruff, black
- **Type Checking**: mypy

### Project Structure

```
qc-bike-path/
├── src/
│   ├── qc_bike_path/
│   │   ├── __init__.py
│   │   ├── config.py          # Configuration management
│   │   ├── extract.py         # Data extraction from API
│   │   ├── transform.py       # Data cleaning and transformation
│   │   ├── load.py            # Data loading to MongoDB
│   │   ├── main.py            # Main ETL pipeline
│   │   └── utils/
│   │       ├── __init__.py
│   │       ├── logging.py     # Logging setup
│   │       └── validators.py  # Data validation utilities
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   └── fixtures/              # Test data fixtures
├── config/
│   ├── .env.example          # Environment variables template
│   └── logging.yaml          # Logging configuration
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── .github/
│   └── workflows/
│       └── ci.yml            # CI/CD pipeline
├── pyproject.toml            # Project dependencies and config
├── README.md
└── copilot-instructions.md
```

### Security Best Practices

1. **Never commit secrets**: Use environment variables for sensitive data
   - MongoDB connection strings
   - API keys
   - Passwords or tokens

2. **Use .env files**: Store configuration in `.env` files (excluded from git)
   - Provide `.env.example` as a template
   - Load environment variables using `python-dotenv`

3. **Input validation**: Validate all external data
   - Use pydantic for data models
   - Sanitize inputs before database operations

### Code Quality Standards

1. **Linting and Formatting**:
   - Use `ruff` for linting (replaces flake8, isort, etc.)
   - Use `black` for code formatting
   - Configure in `pyproject.toml`

2. **Type Hints**:
   - Use type hints for all functions and methods
   - Use `mypy` for static type checking
   - Import types from `typing` module

3. **Testing**:
   - Write unit tests for all core functions
   - Use `pytest` as the testing framework
   - Aim for >80% code coverage
   - Use fixtures for test data
   - Mock external dependencies (MongoDB, HTTP calls)

4. **Documentation**:
   - Use docstrings for all modules, classes, and functions
   - Follow Google or NumPy docstring style
   - Keep README.md updated

### ETL Pipeline Guidelines

#### Extract Phase (`extract.py`)
- Fetch GeoJSON data from Quebec's open data portal
- Handle HTTP errors gracefully
- Implement retry logic with exponential backoff
- Log all extraction activities
- Cache data when appropriate

```python
# Example structure
async def fetch_bike_path_data(url: str) -> dict:
    """Fetch bike path data from Quebec's open data portal."""
    pass

def validate_geojson(data: dict) -> bool:
    """Validate GeoJSON format."""
    pass
```

#### Transform Phase (`transform.py`)
- Clean and normalize GeoJSON data
- Handle missing or invalid data points
- Convert coordinate systems if needed
- Add metadata (extraction timestamp, source, etc.)
- Validate transformed data structure

```python
# Example structure
def clean_bike_path_data(raw_data: dict) -> dict:
    """Clean and normalize bike path GeoJSON data."""
    pass

def add_metadata(data: dict, extraction_time: datetime) -> dict:
    """Add metadata to processed data."""
    pass
```

#### Load Phase (`load.py`)
- Connect to MongoDB using connection pooling
- Handle duplicate data (upsert operations)
- Create appropriate indexes
- Log loading statistics
- Handle database errors gracefully

```python
# Example structure
async def save_to_mongodb(data: dict, collection_name: str) -> bool:
    """Save processed data to MongoDB."""
    pass

def create_indexes(collection: Collection) -> None:
    """Create database indexes for optimization."""
    pass
```

### Configuration Management

Use a centralized configuration system:

```python
# config.py example
from pydantic import BaseSettings

class Settings(BaseSettings):
    mongodb_url: str
    mongodb_database: str
    api_base_url: str
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
```

### Error Handling

1. **Use custom exceptions** for different error types:
   ```python
   class DataExtractionError(Exception):
       pass
   
   class DataValidationError(Exception):
       pass
   
   class DatabaseConnectionError(Exception):
       pass
   ```

2. **Implement comprehensive logging**:
   - Log all major operations
   - Use structured logging (JSON format)
   - Include correlation IDs for tracing

3. **Graceful degradation**:
   - Continue processing when individual records fail
   - Provide meaningful error messages
   - Implement health checks

### Testing Strategy

1. **Unit Tests**:
   - Test individual functions in isolation
   - Mock external dependencies
   - Use parametrized tests for different scenarios

2. **Integration Tests**:
   - Test ETL pipeline end-to-end
   - Use test database for integration tests
   - Test with real (but sanitized) data samples

3. **Fixtures**:
   - Create reusable test data fixtures
   - Store sample GeoJSON data for testing
   - Use factory pattern for test objects

### Development Workflow

1. **Environment Setup**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # or `venv\Scripts\activate` on Windows
   pip install -e .[dev]
   ```

2. **Pre-commit Checks**:
   ```bash
   ruff check .
   black --check .
   mypy src/
   pytest
   ```

3. **Running the ETL Pipeline**:
   ```bash
   python -m qc_bike_path.main
   ```

### Deployment Considerations

1. **Docker**: Provide Dockerfile for containerized deployment
2. **Environment Variables**: Use environment-specific configuration
3. **Health Checks**: Implement health check endpoints
4. **Monitoring**: Add metrics and observability
5. **Scheduling**: Consider using cron jobs or task schedulers

### Contributing Guidelines

1. **Branch Naming**: Use descriptive branch names (e.g., `feature/add-data-validation`)
2. **Commit Messages**: Use conventional commits format
3. **Pull Requests**: Include description of changes and testing approach
4. **Code Review**: All changes require review before merging

### Performance Considerations

1. **Async Operations**: Use async/await for I/O operations
2. **Connection Pooling**: Use connection pools for database operations
3. **Batch Processing**: Process data in batches when dealing with large datasets
4. **Caching**: Implement appropriate caching strategies
5. **Memory Management**: Be mindful of memory usage with large GeoJSON files

## Helpful Commands

```bash
# Setup development environment
python -m venv venv && source venv/bin/activate
pip install -e .[dev]

# Run linting and formatting
ruff check . --fix
black .
mypy src/

# Run tests
pytest --cov=src/qc_bike_path --cov-report=html

# Run the ETL pipeline
python -m qc_bike_path.main

# Build Docker image
docker build -f docker/Dockerfile -t qc-bike-path .

# Run with docker-compose
docker-compose -f docker/docker-compose.yml up
```

This ETL service should be robust, maintainable, and follow modern Python development practices while handling Quebec's bike path data efficiently.