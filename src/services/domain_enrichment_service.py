"""Service for enriching domains from files."""

import logging
import time
from typing import Dict, Any

from src.constants import (
    DOMAIN_PROCESSING_DELAY_SECONDS,
    ENRICHMENT_STATUS_COMPLETED,
    ENRICHMENT_STATUS_FAILED,
    COMPANY_NAME_WITH_OWNER_FORMAT
)
from src.models.enrichment import CompanyAnalysis
from src.services.analyzer import AIAnalyzer
from src.services.scraper import WebScraper

logger = logging.getLogger(__name__)


class DomainEnrichmentService:
    """Service for enriching individual domains."""
    
    def __init__(self, scraper: WebScraper, analyzer: AIAnalyzer):
        """Initialize domain enrichment service."""
        self.scraper = scraper
        self.analyzer = analyzer
    
    def enrich_domain(self, domain: str) -> Dict[str, Any]:
        """
        Enrich a single domain with scraped and analyzed data.
        
        Args:
            domain: Domain to enrich
            
        Returns:
            Dictionary containing enrichment results
        """
        logger.info(f"Starting enrichment for domain: {domain}")
        result = {
            "name": domain,
            "enrichment_status": ENRICHMENT_STATUS_FAILED
        }
        
        try:
            # Scrape domain
            logger.info(f"Scraping website for {domain}")
            scraped = self.scraper.scrape_domain(domain)
            if not scraped.success:
                result["enrichment_error"] = scraped.error or "Scraping failed"
                logger.error(f"Failed to scrape {domain}: {result['enrichment_error']}")
                return result
            
            logger.info(f"Successfully scraped {len(scraped.content)} characters from {domain}")
            result["site_content"] = scraped.content
            
            # Include scraped emails
            if scraped.emails:
                result["scraped_emails"] = scraped.emails
                logger.info(f"Found {len(scraped.emails)} emails for {domain}: {scraped.emails}")
            
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
            
            # Preserve scraped emails in result
            if scraped.emails:
                result["scraped_emails"] = scraped.emails
            
            logger.info(f"Successfully enriched {domain} - Company: {result.get('name', domain)}")
            
            return result
            
        except Exception as e:
            logger.error(f"Unexpected error enriching {domain}: {e}", exc_info=True)
            result["enrichment_error"] = f"Unexpected error: {str(e)[:200]}"
            return result
    
    def _build_enrichment_result(
        self, 
        domain: str, 
        analysis: CompanyAnalysis, 
        content: str
    ) -> Dict[str, Any]:
        """Build enrichment result from analysis."""
        result = analysis.to_dict()
        result["site_content"] = content
        result["enrichment_status"] = ENRICHMENT_STATUS_COMPLETED
        
        # Set company name
        if analysis.company_owner:
            result["name"] = COMPANY_NAME_WITH_OWNER_FORMAT.format(
                domain=domain, 
                owner=analysis.company_owner
            )
        else:
            result["name"] = domain
        
        return result
    
    @staticmethod
    def add_processing_delay(is_last: bool) -> None:
        """Add delay between domain processing to avoid rate limiting."""
        if not is_last:
            time.sleep(DOMAIN_PROCESSING_DELAY_SECONDS)