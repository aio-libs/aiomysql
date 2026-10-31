"""Logging configuration."""
import logging

# Name the logger after the package.
logger = logging.getLogger(__package__)
logger.setLevel(logging.WARNING)
