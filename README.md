# Quebec City Bike Path ETL Service

[![CI/CD Pipeline](https://github.com/lekaf974/qc-bike-path/actions/workflows/ci.yml/badge.svg)](https://github.com/lekaf974/qc-bike-path/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/lekaf974/qc-bike-path/branch/main/graph/badge.svg)](https://codecov.io/gh/lekaf974/qc-bike-path)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

A robust ETL (Extract, Transform, Load) service for processing Quebec City's public bike path data. The service fetches GeoJSON data from the official Quebec government data portal, processes and cleans it, and stores it in MongoDB for analysis and visualization.

## 🚴‍♀️ Overview

This ETL service automates the collection and processing of Quebec City's bike path infrastructure data from the [Quebec Open Data Portal](https://www.donneesquebec.ca/recherche/dataset/vque_24). It provides:

- **Automated data extraction** from government APIs
- **Data cleaning and transformation** with validation
- **MongoDB storage** with geospatial indexing
- **GeoJSON export** for mapping applications
- **Comprehensive logging** and monitoring
- **Docker deployment** ready

## 📊 Data Source

**Source**: [Quebec Government Open Data Portal](https://www.donneesquebec.ca/recherche/dataset/vque_24)
- **Format**: GeoJSON / JSON API
- **Content**: Bike paths, cycle lanes, and recreational trails
- **Coverage**: Quebec City metropolitan area
- **Updates**: Regular updates from municipal data

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   EXTRACT   │    │ TRANSFORM   │    │    LOAD     │
│             │    │             │    │             │
│ Quebec API  │───▶│ Clean &     │───▶│  MongoDB    │
│ GeoJSON     │    │ Validate    │    │ + GeoJSON   │
│ Data        │    │ Data        │    │ Export      │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Key Components

- **Extract** (`extract.py`): Fetches data from Quebec's API with retry logic and error handling
- **Transform** (`transform.py`): Cleans, validates, and normalizes bike path data
- **Load** (`load.py`): Stores processed data in MongoDB with geospatial indexes
- **Main** (`main.py`): Orchestrates the complete ETL pipeline

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+**
- **MongoDB 4.4+**
- **Git**

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/lekaf974/qc-bike-path.git
   cd qc-bike-path
   ```

2. **Set up virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -e .[dev]
   ```

4. **Configure environment**:
   ```bash
   cp config/.env.example .env
   # Edit .env with your configuration
   ```

5. **Run the ETL pipeline**:
   ```bash
   python -m qc_bike_path.main
   ```

### Docker Deployment

1. **Using Docker Compose** (recommended):
   ```bash
   docker-compose -f docker/docker-compose.yml up -d
   ```

2. **Build and run manually**:
   ```bash
   docker build -f docker/Dockerfile -t qc-bike-path .
   docker run -d --name qc-bike-path --env-file .env qc-bike-path
   ```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `QC_BIKE_PATH_MONGODB_URL` | MongoDB connection string | `mongodb://localhost:27017` |
| `QC_BIKE_PATH_MONGODB_DATABASE` | Database name | `qc_bike_path` |
| `QC_BIKE_PATH_API_BASE_URL` | Quebec API endpoint | `https://www.donneesquebec.ca/recherche/api/...` |
| `QC_BIKE_PATH_BIKE_PATH_RESOURCE_ID` | Resource ID for bike path data | *(required)* |
| `QC_BIKE_PATH_LOG_LEVEL` | Logging level | `INFO` |
| `QC_BIKE_PATH_BATCH_SIZE` | Processing batch size | `1000` |

### Sample Configuration

```bash
# .env file
QC_BIKE_PATH_MONGODB_URL=mongodb://localhost:27017
QC_BIKE_PATH_MONGODB_DATABASE=qc_bike_path
QC_BIKE_PATH_BIKE_PATH_RESOURCE_ID=your-resource-id
QC_BIKE_PATH_LOG_LEVEL=INFO
QC_BIKE_PATH_ENVIRONMENT=production
```

## 🔧 Development

### Setup Development Environment

```bash
# Install development dependencies
pip install -e .[dev]

# Setup pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# Run unit tests
pytest tests/ -v

# Run with coverage
pytest --cov=src/qc_bike_path --cov-report=html

# Run specific test types
pytest -m "not integration"  # Skip integration tests
pytest -m "integration"      # Run only integration tests
```

### Code Quality

```bash
# Linting and formatting
ruff check .                 # Linting
ruff check . --fix          # Fix auto-fixable issues
black .                     # Code formatting
mypy src/                   # Type checking

# All quality checks
ruff check . && black --check . && mypy src/
```

### Project Structure

```
qc-bike-path/
├── src/qc_bike_path/          # Main application code
│   ├── __init__.py
│   ├── config.py              # Configuration management
│   ├── extract.py             # Data extraction
│   ├── transform.py           # Data transformation
│   ├── load.py               # Data loading
│   ├── main.py               # Main ETL pipeline
│   └── utils/                # Utility modules
│       ├── logging.py        # Logging setup
│       └── validators.py     # Data validation
├── tests/                    # Test suite
│   ├── fixtures/             # Test data
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── config/                   # Configuration files
│   ├── .env.example
│   └── logging.yaml
├── docker/                   # Docker configuration
│   ├── Dockerfile
│   └── docker-compose.yml
├── .github/workflows/        # CI/CD pipelines
├── pyproject.toml           # Project configuration
├── README.md
└── copilot-instructions.md  # Development guidelines
```

## 📖 Usage Examples

### Basic ETL Run

```bash
# Run complete ETL pipeline
python -m qc_bike_path.main

# Run with record limit (for testing)
python -m qc_bike_path.main 100

# Health check
python -m qc_bike_path.main health
```

### Programmatic Usage

```python
from qc_bike_path.main import BikePathETLPipeline

# Run ETL pipeline
pipeline = BikePathETLPipeline()
stats = await pipeline.run_full_pipeline()

print(f"Processed {stats['records_processed']} records")
```

### Custom Processing

```python
from qc_bike_path import extract, transform, load

# Extract data
raw_data = await extract.extract_bike_path_data(limit=1000)

# Transform data
transformed_records = transform.transform_bike_path_data(raw_data)

# Load to MongoDB
save_stats = await load.save_bike_path_data(transformed_records)
```

## 📊 Data Schema

### Input Data Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique path identifier |
| `name` | string | Path name (French) |
| `type` | string | Path type (piste cyclable, voie cyclable, etc.) |
| `surface` | string | Surface material |
| `length_km` | float | Length in kilometers |
| `latitude` | float | Latitude coordinate |
| `longitude` | float | Longitude coordinate |

### Output Data Structure

```json
{
  "id": "path_123",
  "name": "Piste Cyclable du Vieux-Port",
  "type": "Piste cyclable",
  "surface": "Asphalte",
  "length_km": 2.5,
  "geometry": {
    "type": "Point",
    "coordinates": [-71.2080, 46.8139]
  },
  "properties": {
    "description": "Belle piste le long du fleuve",
    "status": "Active"
  },
  "source_url": "https://www.donneesquebec.ca/...",
  "extraction_timestamp": "2024-01-15T10:30:00Z"
}
```

## 🐛 Troubleshooting

### Common Issues

1. **MongoDB Connection Failed**
   ```bash
   # Check MongoDB is running
   mongosh --eval "db.runCommand('ping')"
   
   # Verify connection string
   echo $QC_BIKE_PATH_MONGODB_URL
   ```

2. **API Resource ID Not Found**
   - Check the Quebec Open Data portal for the correct resource ID
   - Verify the API endpoint is accessible
   - Ensure resource ID is set in environment variables

3. **Import Errors**
   ```bash
   # Reinstall dependencies
   pip install -e .[dev] --force-reinstall
   
   # Check Python path
   python -c "import qc_bike_path; print(qc_bike_path.__file__)"
   ```

### Logging and Debugging

```bash
# Enable debug logging
export QC_BIKE_PATH_LOG_LEVEL=DEBUG

# Run with verbose output
python -m qc_bike_path.main --verbose

# Check logs
tail -f logs/qc_bike_path.log
```

## 🤝 Contributing

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Make your changes** following the coding standards
4. **Run tests**: `pytest`
5. **Commit changes**: `git commit -m 'Add amazing feature'`
6. **Push to branch**: `git push origin feature/amazing-feature`
7. **Open a Pull Request**

### Development Guidelines

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guidelines
- Write comprehensive tests for new features
- Update documentation for API changes
- Use type hints for all functions
- Add docstrings following Google style
- Ensure all tests pass before submitting PR

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Government of Quebec** for providing open bike path data
- **MongoDB Community** for geospatial database capabilities
- **Python Community** for excellent ETL libraries
- **Contributors** who help improve this project

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/lekaf974/qc-bike-path/issues)
- **Discussions**: [GitHub Discussions](https://github.com/lekaf974/qc-bike-path/discussions)
- **Email**: team@example.com

---

**Made with ❤️ for Quebec City's cycling community** 🚴‍♀️🚴‍♂️