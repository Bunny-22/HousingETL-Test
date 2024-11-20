import logging
from typing import Optional
import sys
from datetime import datetime

def setup_logging(log_file: Optional[str] = None) -> logging.Logger:
    """Configure logging with both file and console handlers."""
    logger = logging.getLogger('housing_pipeline')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def timer_decorator(func):
    """Decorator to measure and log function execution time."""
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        execution_time = datetime.now() - start_time
        logging.getLogger('housing_pipeline').info(
            f"Function {func.__name__} executed in {execution_time}"
        )
        return result
    return wrapper