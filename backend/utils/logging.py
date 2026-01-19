"""
Centralized logging configuration for the DCL backend.
"""

import logging
import sys
from typing import Optional


def setup_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging for the DCL application.

    Args:
        level: The logging level (default: INFO)
        format_string: Optional custom format string

    Returns:
        The root logger instance
    """
    if format_string is None:
        format_string = (
            "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
        )

    # Configure root logger
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Set specific log levels for noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logging.getLogger("dcl")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a specific module.

    Args:
        name: The module name (typically __name__)

    Returns:
        A configured logger instance
    """
    return logging.getLogger(f"dcl.{name}")


# Initialize logging on module import
logger = setup_logging()
