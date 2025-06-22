"""Application settings and configuration."""

import os
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

# Add common library to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "common"))

from utils.config import Config


@dataclass
class Settings:
    """Application settings."""
    hubspot_token: str
    log_level: str = "INFO"
    log_file: Optional[str] = None
    scraping_timeout: int = 30
    max_content_length: int = 32000
    deepseek_api_key: Optional[str] = None
    deepseek_endpoint: str = "https://api.deepseek.com"
    scraping_depth: int = 2  # Default scraping depth
    
    @classmethod
    def from_config(cls, config_file: Optional[str] = None) -> "Settings":
        """Create settings from configuration file and environment."""
        # Load configuration
        config_path = config_file or "config/app_config.yaml"
        config = Config(config_file=config_path)
        
        return cls(
            hubspot_token=config.get("hubspot.token", os.environ.get("HUBSPOT_TOKEN", "")),
            log_level=config.get("logging.level", os.environ.get("LOG_LEVEL", "INFO")),
            log_file=config.get("logging.file", os.environ.get("LOG_FILE")),
            scraping_timeout=config.get_int("scraping.timeout", int(os.environ.get("SCRAPING_TIMEOUT", "30"))),
            max_content_length=config.get_int("pipeline.max_content_length", int(os.environ.get("MAX_CONTENT_LENGTH", "50000"))),
            deepseek_api_key=config.get("deepseek.api_key", os.environ.get("DEEPSEEK_API_KEY")),
            deepseek_endpoint=config.get("deepseek.endpoint", os.environ.get("DEEPSEEK_ENDPOINT", "https://api.deepseek.com")),
            scraping_depth=config.get_int("scraping.depth", int(os.environ.get("SCRAPING_DEPTH", "2")))
        )
    
    @classmethod
    def from_env(cls) -> "Settings":
        """Create settings from environment variables (backward compatibility)."""
        return cls(
            hubspot_token=os.environ.get("HUBSPOT_TOKEN", ""),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            log_file=os.environ.get("LOG_FILE"),
            scraping_timeout=int(os.environ.get("SCRAPING_TIMEOUT", "30")),
            max_content_length=int(os.environ.get("MAX_CONTENT_LENGTH", "50000")),
            deepseek_api_key=os.environ.get("DEEPSEEK_API_KEY"),
            deepseek_endpoint=os.environ.get("DEEPSEEK_ENDPOINT", "https://api.deepseek.com"),
            scraping_depth=int(os.environ.get("SCRAPING_DEPTH", "2"))
        )