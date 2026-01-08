import logging
import sys
from logging.handlers import RotatingFileHandler
import os

def setup_logger(name: str = "trading_bot", log_level: int = logging.INFO, log_file: str = "bot.log") -> logging.Logger:
    """
    Sets up a logger with both console and file output.
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler
    try:
        file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Failed to setup file logging: {e}")

    return logger
