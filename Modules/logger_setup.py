import logging
import os


def setup_logger(log_file="app.log", log_level=logging.INFO):
    """
    Sets up a logger that logs messages to a specified file.

    Parameters:
    log_file (str): The name of the log file.
    log_level (int): The logging level (e.g., logging.INFO, logging.DEBUG).
    """
    # Create logs directory if it doesn't exist
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Configure the logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Create file handler
    file_handler = logging.FileHandler(os.path.join("logs", log_file))
    file_handler.setLevel(log_level)

    # Create formatter and add it to the handler
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(file_handler)

    return logger
