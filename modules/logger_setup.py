import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime


def setup_logger(log_dir="logs", log_level=logging.INFO):
    """
    Sets up a logger that logs messages to a specified directory with daily log files.

    Parameters:
    log_dir (str): The directory where log files will be stored.
    log_level (int): The logging level (e.g., logging.INFO, logging.DEBUG).
    """
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Configure the logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Create file handler with TimedRotatingFileHandler
    log_file = os.path.join(log_dir, "app.log")
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=30,  # Keep logs for 30 days
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(file_handler)

    return logger
