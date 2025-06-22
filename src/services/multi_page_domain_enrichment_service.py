"""Domain enrichment service with multi-page scraping support."""

import logging
from typing import Dict, Any, Optional

from src.constants import (
    DOMAIN_PROCESSING_DELAY_SECONDS,
    ENRICHMENT_STATUS_COMPLETED,
    ENRICHMENT_STATUS_FAILED,
    COMPANY_NAME_WITH_OWNER_FORMAT,
    ProcessingConfig
)
from src.models.enrichment import CompanyAnalysis
from src.services.domain_enrichment_service import DomainEnrichmentService
from src.services.analyzer import AIAnalyzer
from src.services.multi_page_scraper import MultiPageScraper

logger = logging.getLogger(__name__)


class MultiPageDomainEnrichmentService(DomainEnrichmentService):
    """Domain enrichment service with multi-page crawling support."""
    
    def __init__(self, analyzer: AIAnalyzer, scraping_depth: Optional[int] = None):
        """
        Initialize multi-page domain enrichment service.
        
        Args:
            analyzer: AI analyzer instance
            scraping_depth: Maximum scraping depth (None uses default)
        """
        # Set scraping depth
        self.scraping_depth = scraping_depth if scraping_depth is not None else ProcessingConfig.DEFAULT_SCRAPING_DEPTH
        
        # Create multi-page scraper
        scraper = MultiPageScraper(use_browser_pool=False, max_depth=self.scraping_depth)
        
        # Initialize parent with our scraper
        super().__init__(scraper, analyzer)
        
        logger.info("Initialized MultiPageDomainEnrichmentService with depth=%d", self.scraping_depth)
    
    def enrich_domain(self, domain: str) -> Dict[str, Any]:
        """
        Enrich a single domain with multi-page scraped and analyzed data.
        
        Args:
            domain: Domain to enrich
            
        Returns:
            Dictionary containing enrichment results
        """
        logger.info(f"Starting multi-page enrichment for domain: {domain} (depth={self.scraping_depth})")
        result = {
            "name": domain,
            "enrichment_status": ENRICHMENT_STATUS_FAILED
        }
        
        try:
            # Perform multi-page scraping if depth > 0
            if self.scraping_depth > 0:
                logger.info(f"Performing multi-page scrape for {domain} (depth={self.scraping_depth})")
                
                # Cast scraper to MultiPageScraper to access multi-page methods
                if isinstance(self.scraper, MultiPageScraper):
                    # Scrape multiple pages
                    scraped_pages = self.scraper.scrape_domain_multi_page(domain, max_pages=10)
                    
                    if not scraped_pages:
                        result["enrichment_error"] = "No pages could be scraped"
                        logger.error(f"No pages scraped for {domain}")
                        return result
                    
                    # Log pages scraped
                    logger.info(f"Scraped {len(scraped_pages)} pages from {domain}")
                    for url in scraped_pages:
                        logger.debug(f"  - {url}: {len(scraped_pages[url].content)} chars")
                    
                    # Combine all scraped content
                    scraped = self.scraper.create_combined_content(scraped_pages)
                    
                    # Add metadata about multi-page scraping
                    result["pages_scraped"] = len(scraped_pages)
                    result["scraped_urls"] = list(scraped_pages.keys())
                else:
                    # Fallback to single page
                    logger.warning("Scraper doesn't support multi-page, falling back to single page")
                    scraped = self.scraper.scrape_domain(domain)
            else:
                # Single page scraping (depth = 0)
                logger.info(f"Performing single-page scrape for {domain}")
                scraped = self.scraper.scrape_domain(domain)
                result["pages_scraped"] = 1
            
            if not scraped.success:
                result["enrichment_error"] = scraped.error or "Scraping failed"
                logger.error(f"Failed to scrape {domain}: {result['enrichment_error']}")
                return result
            
            logger.info(f"Successfully scraped {len(scraped.content)} total characters from {domain}")
            result["site_content"] = scraped.content
            
            # Include scraped emails
            if scraped.emails:
                result["scraped_emails"] = scraped.emails
                logger.info(f"Found {len(scraped.emails)} unique emails for {domain}: {scraped.emails}")
            
            # Analyze content
            logger.info(f"Analyzing content for {domain} using AI")
            analysis = self.analyzer.analyze_company(scraped.content, domain=domain, emails=scraped.emails)
            if not analysis:
                result["enrichment_error"] = "Analysis failed"
                logger.error(f"Failed to analyze content for {domain}")
                return result
            
            logger.info(f"AI analysis completed for {domain}")
            
            # Build successful result
            result = self._build_enrichment_result(domain, analysis, scraped.content)
            
            # Preserve scraped emails and multi-page metadata
            if scraped.emails:
                result["scraped_emails"] = scraped.emails
            if "pages_scraped" not in result:
                result["pages_scraped"] = 1
            if "scraped_urls" in locals():
                result["scraped_urls"] = list(scraped_pages.keys())
            
            logger.info(f"Successfully enriched {domain} - Company: {result.get('name', domain)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Unexpected error enriching {domain}: {e}", exc_info=True)
            result["enrichment_error"] = f"Unexpected error: {str(e)[:200]}"
            return result