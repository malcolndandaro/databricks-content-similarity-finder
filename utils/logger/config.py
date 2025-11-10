"""
Logging configuration for the application.
"""

import logging
import sys
from typing import Optional, Dict, Any
from pathlib import Path


def setup_logger(
    name: str,
    level: str = "INFO",
    log_file: Optional[str] = None,
    log_format: Optional[str] = None,
) -> logging.Logger:
    """
    Set up a logger with the specified configuration.

    Args:
        name: Name of the logger
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path to write logs to
        log_format: Optional custom log format string

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # If the logger already has handlers, assume it's configured
    if logger.handlers:
        return logger
    
    # Set the logging level
    level = getattr(logging, level.upper())
    logger.setLevel(level)

    # Create formatters
    if not log_format:
        log_format = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    formatter = logging.Formatter(log_format)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Create file handler if log_file is specified
    if log_file:
        # Ensure the log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Prevent propagation to root logger to avoid duplicate messages
    logger.propagate = False

    return logger


def get_finder_logger(
    level: str = "INFO",
    log_file: Optional[str] = None,
    include_debug: bool = False
) -> logging.Logger:
    """
    Get a configured logger for the finder module.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path to write logs to
        include_debug: If True, includes additional debug information in log format

    Returns:
        Configured logger instance for finder module
    """
    if include_debug:
        log_format = "%(asctime)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s"
    else:
        log_format = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"

    return setup_logger(
        name="finder",
        level=level,
        log_file=log_file,
        log_format=log_format
    ) 