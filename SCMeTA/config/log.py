import logging
import os
from pathlib import Path

def setup_logger(name = __name__, log_file : str | None = None, level=logging.INFO):
    """
    Set up a logger that outputs to both console and file
    
    Args:
        name (str): Logger name (used to distinguish different scripts)
        log_file (str): Path to the log file
        level (int): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create log directory if it doesn't exist\
    if log_file is None:
        log_file = str(Path.home() / ".scmeta" / "scmeta.log")
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Create logger with specified name
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger
    
    # Define log format: timestamp - logger name - level - message
    formatter = logging.Formatter(
    fmt="[%(asctime)s][\033[32m%(levelname)s\033[0m] %(message)s",
    datefmt="%Y.%m.%d %H:%M"
)
    # File handler (without rotation)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
    