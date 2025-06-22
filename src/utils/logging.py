"""Logging configuration utilities."""

import sys
from pathlib import Path
from typing import Optional

# Add common library to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "common"))

from utils.logger import get_logger


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Set up logging configuration using common logger."""
    # Always enable file logging to app.log for consistency
    logger_config = {
        "level": level,
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "json_format": False,
        "console": {
            "enabled": True,
            "level": level,
        },
        "file": {
            "enabled": True,
            "path": log_file or "logs/app.log",
            "max_bytes": 10 * 1024 * 1024,  # 10MB
            "backup_count": 5,
            "rotation": "size",
        }
    }
    
    # Get logger instance and configure root logger
    logger_instance = get_logger("dealdriver", logger_config)
    
    # Set up root logger to use the same configuration
    import logging
    root_logger = logging.getLogger()
    root_logger.handlers = logger_instance.logger.handlers
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Quiet down noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)