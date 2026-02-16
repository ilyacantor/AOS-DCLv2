"""
Centralized logging configuration for the DCL backend.
"""

import logging
import sys
from typing import Optional

_initialized = False


def setup_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging for the DCL application.
    Only runs once, subsequent calls return existing logger.
    """
    global _initialized

    if _initialized:
        return logging.getLogger("dcl")

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

    _initialized = True
    return logging.getLogger("dcl")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a specific module.
    Initializes logging on first call.
    """
    setup_logging()  # Ensure initialized
    return logging.getLogger(f"dcl.{name}")
