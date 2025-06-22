"""Luigi task for web scraping."""

import json
import luigi
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from src.tasks.base import BaseTask
from src.services.scraper import WebScraper
from src.services.multi_page_scraper import MultiPageScraper
from src.models.enrichment import ScrapedContent
from src.constants import ProcessingConfig

logger = logging.getLogger(__name__)


class ScrapeWebsiteTask(BaseTask):
    """Task to scrape a website and save content to file."""
    
    # Add scraping depth parameter
    scraping_depth = luigi.IntParameter(default=ProcessingConfig.DEFAULT_SCRAPING_DEPTH)
    
    def output(self):
        """Define output target."""
        output_path = self.get_output_path("site_content", "json")
        return luigi.LocalTarget(str(output_path))
    
    def run(self):
        """Execute the scraping task."""
        logger.info(f"Starting scrape task for domain: {self.domain}")
        
        try:
            scraped_content = self._scrape_domain()
            output_data = self._prepare_output_data(scraped_content)
            self._save_output(output_data)
            logger.info(f"Successfully scraped and saved content for {self.domain}")
            
        except Exception as e:
            logger.error(f"Failed to scrape {self.domain}: {str(e)}")
            self._save_error_output(str(e))
    
    def _scrape_domain(self):
        """Scrape the domain and return content."""
        # Use multi-page scraper if depth > 0
        if self.scraping_depth > 0:
            logger.info(f"Using multi-page scraper with depth={self.scraping_depth}")
            scraper = MultiPageScraper(use_browser_pool=False, max_depth=self.scraping_depth)
            
            # Scrape multiple pages
            scraped_pages = scraper.scrape_domain_multi_page(self.domain, max_pages=10)
            
            # Combine into single content
            return scraper.create_combined_content(scraped_pages)
        else:
            logger.info("Using single-page scraper")
            scraper = WebScraper()
            return scraper.scrape_domain(self.domain)
    
    def _prepare_output_data(self, scraped_content) -> dict:
        """Prepare data for output file."""
        return {
            "domain": self.domain,
            "url": scraped_content.url,
            "success": scraped_content.success,
            "scraped_at": datetime.now().isoformat(),
            "content": scraped_content.content if scraped_content.success else "",
            "emails": scraped_content.emails,
            "error": scraped_content.error if not scraped_content.success else None
        }
    
    def _save_output(self, output_data: dict):
        """Save output data to file."""
        output_path = Path(self.output().path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)
    
    def _save_error_output(self, error_message: str):
        """Save error output to mark task as complete."""
        error_data = {
            "domain": self.domain,
            "url": f"https://{self.domain}",
            "success": False,
            "scraped_at": datetime.now().isoformat(),
            "content": "",
            "emails": [],
            "error": error_message
        }
        self._save_output(error_data)