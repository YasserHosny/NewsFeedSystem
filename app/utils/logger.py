import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_logging(name, log_level=logging.INFO):
    """Configure logging with rotating file handler"""
    
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Configure logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create rotating file handler
    today = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(logs_dir, f'{name}_{today}.log')
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(file_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    
    return logger 