import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

def configure_logging():
    """Configure global logging settings"""
    
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(file_formatter)
    root_logger.addHandler(console_handler)
    
    # Create rotating file handler for root logger
    root_log_file = os.path.join(logs_dir, 'app.log')
    root_file_handler = RotatingFileHandler(
        root_log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    root_file_handler.setFormatter(file_formatter)
    root_logger.addHandler(root_file_handler)
    
    # Configure component loggers
    components = ['worker', 'proxy_manager', 'message_queue', 'connection_manager']
    loggers = {}
    
    for component in components:
        logger = logging.getLogger(component)
        logger.setLevel(logging.INFO)
        logger.propagate = False  # Don't propagate to root logger
        
        # Create component-specific file handler
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(logs_dir, f'{component}_{today}.log')
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,
            backupCount=5
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)  # Also log to console
        
        loggers[component] = logger
    
    return loggers

# Initialize all loggers
LOGGERS = configure_logging()

def get_logger(log_file):
    """Get a configured logger by name"""
    if log_file not in LOGGERS:
        logger = logging.getLogger(log_file)
        logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        # Create component-specific file handler
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(logs_dir, f'{log_file}_{today}.log')
        
        # Check if logger already has handlers to avoid duplicates
        if not logger.handlers:
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10*1024*1024,
                backupCount=5
            )
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            # Add console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            
        LOGGERS[log_file] = logger
        
    return LOGGERS[log_file]

# For backwards compatibility (if needed)
def setup_logging(name='app'):
    """Backwards compatibility wrapper"""
    return get_logger(name)
