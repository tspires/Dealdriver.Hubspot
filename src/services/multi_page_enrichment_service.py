"""Enrichment service with multi-page scraping support."""

import logging
from typing import Optional, Set

from src.models.hubspot import Company
from src.models.enrichment import ScrapedContent
from src.services.enrichment_service import EnrichmentService
from src.services.hubspot_service import HubSpotService
from src.services.multi_page_scraper import MultiPageScraper
from src.constants import ProcessingConfig

logger = logging.getLogger(__name__)


class MultiPageEnrichmentService(EnrichmentService):
    """Enrichment service that supports multi-page scraping."""
    
    def __init__(self, hubspot_service: HubSpotService, scraping_depth: Optional[int] = None):
        """
        Initialize multi-page enrichment service.
        
        Args:
            hubspot_service: HubSpot service instance
            scraping_depth: Maximum scraping depth (None uses default)
        """
        # Initialize parent class but we'll override the scraper
        super().__init__(hubspot_service)
        
        # Replace scraper with multi-page version
        self.scraping_depth = scraping_depth if scraping_depth is not None else ProcessingConfig.DEFAULT_SCRAPING_DEPTH
        self.scraper = MultiPageScraper(use_browser_pool=False, max_depth=self.scraping_depth)
        
        logger.info("Initialized MultiPageEnrichmentService with depth=%d", self.scraping_depth)
    
    def enrich_company(self, company: Company) -> bool:
        """
        Enrich a single company with multi-page scraping.
        
        Args:
            company: Company to enrich
            
        Returns:
            True if successful
        """
        try:
            domain = company.domain or self._extract_domain_from_website(company.website)
            if not domain:
                logger.warning(f"No domain found for company {company.id} ({company.name})")
                self._mark_company_failed(company.id, "No domain found")
                return False
            
            # Skip if domain looks invalid
            from src.constants import MIN_DOMAIN_LENGTH
            if len(domain) < MIN_DOMAIN_LENGTH or '.' not in domain:
                logger.warning(f"Invalid domain '{domain}' for company {company.id} ({company.name})")
                self._mark_company_failed(company.id, f"Invalid domain: {domain}")
                return False
            
            if domain in self.processed_domains:
                logger.info("Domain %s already processed, using cached content", domain)
                return True
            
            # Perform multi-page scraping if depth > 0
            if self.scraping_depth > 0:
                logger.info("Performing multi-page scrape for %s (depth=%d)", domain, self.scraping_depth)
                
                # Scrape multiple pages
                scraped_pages = self.scraper.scrape_domain_multi_page(domain, max_pages=10)
                
                if not scraped_pages:
                    logger.error(f"No pages scraped for {domain}")
                    self._mark_company_failed(company.id, "No pages could be scraped")
                    return False
                
                # Combine all scraped content
                scraped = self.scraper.create_combined_content(scraped_pages)
                
                logger.info("Multi-page scrape completed for %s: %d pages, %d total chars, %d emails",
                           domain, len(scraped_pages), len(scraped.content), len(scraped.emails))
            else:
                # Single page scraping (depth = 0)
                logger.info("Performing single-page scrape for %s", domain)
                scraped = self.scraper.scrape_domain(domain)
            
            if not scraped.success:
                logger.error(f"Failed to scrape {domain}: {scraped.error}")
                self._mark_company_failed(company.id, f"Scraping failed: {scraped.error}")
                return False
            
            self.processed_domains.add(domain)
            
            # Analyze the combined content
            analysis = self.analyzer.analyze_company(scraped.content, domain=domain, emails=scraped.emails)
            if not analysis:
                logger.error(f"Failed to analyze content for company {company.id}")
                self._mark_company_failed(company.id, "Analysis failed")
                return False
            
            # Continue with normal enrichment process...
            # (rest of the method remains the same as parent class)
            return super().enrich_company(company)
            
        except Exception as e:
            logger.error(f"Failed to enrich company {company.id}: {e}", exc_info=True)
            self._mark_company_failed(company.id, str(e))
            return False