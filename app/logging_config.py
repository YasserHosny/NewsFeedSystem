import logging
import os
from logging.handlers import RotatingFileHandler
import time

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def configure_logging():
    # Root logger
    root_logger = logging.getLogger()
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))

    log_format = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    log_format.converter = time.gmtime

    # Console handler (only for root logger)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)

    # Named component loggers
    loggers = {}

    for name in [
        "app",
        "worker",
        "proxy_manager",
        "orchestrator",
        "scheduler_service",
        "config_api",
        "dashboard",
        "task_executor",
        "task_tracker",
        "monitoring_service",
        "alert_service",
        "data_storage_service",
        "message_queue_service",
        "integration_test"
    ]:
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, log_level, logging.INFO))

        if not logger.handlers:
            log_file = os.path.join(LOG_DIR, f"{name}.log")
            file_handler = RotatingFileHandler(
                log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8'
            )
            file_handler.setFormatter(log_format)
            logger.addHandler(file_handler)

        loggers[name] = logger

    return loggers

LOGGERS = configure_logging()

def get_logger(name):
    return LOGGERS.get(name, logging.getLogger(name))
