"""Base Luigi task configurations."""

import luigi
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


class BaseTask(luigi.Task):
    """Base task with common functionality."""
    
    domain = luigi.Parameter()
    
    @property
    def data_dir(self) -> Path:
        """Get base data directory."""
        return Path("data")
    
    @property
    def domain_safe(self) -> str:
        """Get filesystem-safe version of domain."""
        return self.domain.replace("/", "_").replace(":", "")
    
    def get_output_path(self, subfolder: str, extension: str) -> Path:
        """Get output path for a specific subfolder."""
        return self.data_dir / subfolder / "raw" / f"{self.domain_safe}.{extension}"