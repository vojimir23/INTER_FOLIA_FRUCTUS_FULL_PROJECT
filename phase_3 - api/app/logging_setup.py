

import logging
import logging.config
from typing import Dict, Any

# Structured JSON logging configuration.
LOGGING_CONFIG: Dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "correlation_id": {
            "()": "asgi_correlation_id.CorrelationIdFilter",
            "uuid_length": 32,
            "default_value": "-",
        },
    },
    "formatters": {
        "json": {
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(asctime)s %(levelname)s [%(correlation_id)s] %(name)s: %(message)s",
        },
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "json",
            "filters": ["correlation_id"],
            "level": "INFO",
        },
    },
    "loggers": {
        "fastapi": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "api": {"handlers": ["default"], "level": "INFO", "propagate": False},
    },
}

# Module-level logger, will be configured by setup_logging.
logger = logging.getLogger("api")

def setup_logging():
    """
    Applies the logging configuration from the LOGGING_CONFIG dictionary.
    """
    logging.config.dictConfig(LOGGING_CONFIG)