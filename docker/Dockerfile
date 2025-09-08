FROM python:3.11-slim

LABEL maintainer="QC Bike Path Team <team@example.com>"
LABEL description="ETL service for Quebec City bike path data"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create non-root user
RUN groupadd -r qcbikepath && useradd -r -g qcbikepath qcbikepath

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libc6-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY pyproject.toml ./
COPY src/ ./src/

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Create directories for logs and data
RUN mkdir -p logs data \
    && chown -R qcbikepath:qcbikepath /app

# Switch to non-root user
USER qcbikepath

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -m qc_bike_path.main health

# Default command
CMD ["python", "-m", "qc_bike_path.main"]