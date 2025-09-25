import logging
import os
from pathlib import Path

def setup_logger(name = __name__, log_file : str = '', level=logging.INFO):
    """
    Set up a logger that outputs to both console and file
    
    Args:
        name (str): Logger name (used to distinguish different scripts)
        log_file (str): Path to the log file. If None, only console logging is used.
                        If 'home', log file is set to user's home directory as ~/.scmeta/scmeta.log
                        If 'cwd', log file is set to current working directory as ./scmeta.log
        level (int): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create log directory if it doesn't exist\
    if log_file != '':
        if log_file == 'home':
            log_file = str(Path.home() / ".scmeta" / "scmeta.log")
        elif log_file == 'cwd':
            log_file = os.path.join(os.getcwd(), "scmeta.log")
        else:
            if os.path.isfile(log_file):
                log_file = os.path.abspath(log_file)
            elif os.path.isdir(log_file):
                log_file = os.path.join(os.path.abspath(log_file), "scmeta.log")
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
    else:
        pass  # No file logging if log_file is None

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
    formatter.converter = lambda *args: __import__('time').localtime(*args)
    # File handler (without rotation)
    if log_file != '':
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.info(f"Save log in {log_file}.")

    return logger
    