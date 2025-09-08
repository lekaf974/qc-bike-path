"""Logging configuration and setup."""

import asyncio
import logging
import logging.config
import sys
from typing import Any

import structlog

from qc_bike_path.config import settings


def setup_logging() -> None:
    """Set up structured logging configuration."""
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
    )

    # Configure structlog
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if settings.log_format.lower() == "json":
        processors.extend(
            [
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ]
        )
    else:
        processors.extend(
            [
                structlog.processors.CallsiteParameterAdder(
                    {
                        structlog.processors.CallsiteParameter.FILENAME,
                        structlog.processors.CallsiteParameter.FUNC_NAME,
                        structlog.processors.CallsiteParameter.LINENO,
                    }
                ),
                structlog.dev.ConsoleRenderer(colors=True),
            ]
        )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Get a structured logger instance.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


class LoggerMixin:
    """Mixin class to add logging capabilities to any class."""

    @property
    def logger(self) -> structlog.BoundLogger:
        """Get logger for this class."""
        return structlog.get_logger(self.__class__.__name__)


def log_execution_time(func_name: str):
    """Decorator to log execution time of functions.

    Args:
        func_name: Name of the function for logging

    Returns:
        Decorator function
    """
    import functools
    import time

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            logger = structlog.get_logger(func_name)
            start_time = time.time()

            try:
                logger.debug("Function execution started")
                result = await func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(
                    "Function execution completed", execution_time=execution_time
                )
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(
                    "Function execution failed",
                    execution_time=execution_time,
                    error=str(e),
                )
                raise

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger = structlog.get_logger(func_name)
            start_time = time.time()

            try:
                logger.debug("Function execution started")
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(
                    "Function execution completed", execution_time=execution_time
                )
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(
                    "Function execution failed",
                    execution_time=execution_time,
                    error=str(e),
                )
                raise

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


# Additional utility functions for common logging patterns


def log_data_operation(operation: str, count: int, **kwargs: Any) -> None:
    """Log data operation with standardized format.

    Args:
        operation: Type of operation (extract, transform, load)
        count: Number of records processed
        **kwargs: Additional context data
    """
    logger = structlog.get_logger("data_operation")
    logger.info(
        "Data operation completed", operation=operation, record_count=count, **kwargs
    )


def log_error_with_context(
    logger: structlog.BoundLogger,
    error: Exception,
    context: dict[str, Any],
    operation: str = "unknown",
) -> None:
    """Log error with additional context information.

    Args:
        logger: Structlog logger instance
        error: Exception that occurred
        context: Additional context information
        operation: Name of the operation that failed
    """
    logger.error(
        "Operation failed with error",
        operation=operation,
        error_type=type(error).__name__,
        error_message=str(error),
        **context,
    )
