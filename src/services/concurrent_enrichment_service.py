"""Concurrent enrichment service for processing multiple domains in parallel."""

import logging
from typing import Dict, Any, List, Tuple, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
import threading

from src.services.domain_enrichment_service import DomainEnrichmentService
from src.services.multi_page_domain_enrichment_service import MultiPageDomainEnrichmentService
from src.services.analyzer import AIAnalyzer
from src.services.scraper import WebScraper
from src.constants import ProcessingConfig
from src.utils.rate_limiter import rate_limit_manager
from src.utils.multiprocessing_manager import batch_process_with_progress

logger = logging.getLogger(__name__)


class ConcurrentEnrichmentService:
    """Service for enriching multiple domains concurrently."""
    
    def __init__(self, num_workers: int = 4, scraping_depth: Optional[int] = None):
        """
        Initialize concurrent enrichment service.
        
        Args:
            num_workers: Number of concurrent workers
            scraping_depth: Maximum scraping depth (None uses default)
        """
        self.num_workers = num_workers
        self.scraping_depth = scraping_depth if scraping_depth is not None else ProcessingConfig.DEFAULT_SCRAPING_DEPTH
        self._local = threading.local()
    
    def _get_enrichment_service(self) -> DomainEnrichmentService:
        """Get thread-local enrichment service instance."""
        if not hasattr(self._local, 'service'):
            # Create thread-local instances
            self._local.analyzer = AIAnalyzer()
            
            # Use multi-page enrichment service if depth > 0
            if self.scraping_depth > 0:
                self._local.service = MultiPageDomainEnrichmentService(
                    self._local.analyzer,
                    self.scraping_depth
                )
            else:
                # Each thread gets its own WebScraper
                self._local.scraper = WebScraper()
                self._local.service = DomainEnrichmentService(
                    self._local.scraper,
                    self._local.analyzer
                )
        return self._local.service
    
    def enrich_domain_with_rate_limit(self, domain: str) -> Dict[str, Any]:
        """
        Enrich a single domain with rate limiting.
        
        Args:
            domain: Domain to enrich
            
        Returns:
            Enrichment result dictionary
        """
        # Acquire rate limit tokens
        rate_limit_manager.acquire('selenium')  # For web scraping
        rate_limit_manager.acquire('deepseek')  # For AI analysis
        
        # Get thread-local service
        service = self._get_enrichment_service()
        
        # Perform enrichment
        return service.enrich_domain(domain)
    
    def enrich_domains(
        self, 
        domains: List[str],
        progress_callback: Optional[Callable[[int, int, Dict, Optional[Dict]], None]] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict]]:
        """
        Enrich multiple domains concurrently.
        
        Args:
            domains: List of domains to enrich
            progress_callback: Optional callback for progress updates
            
        Returns:
            Tuple of (results, errors)
        """
        logger.info(f"Starting concurrent enrichment of {len(domains)} domains with {self.num_workers} workers")
        
        # Process domains in parallel with rate limiting
        results, errors = batch_process_with_progress(
            items=domains,
            process_func=self.enrich_domain_with_rate_limit,
            num_workers=self.num_workers,
            progress_callback=progress_callback,
            rate_limit_apis=['selenium', 'deepseek']
        )
        
        logger.info(f"Concurrent enrichment completed: {len(results)} successful, {len(errors)} errors")
        
        return results, errors
    
    def enrich_companies(
        self,
        companies: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int, Dict, Optional[Dict]], None]] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict]]:
        """
        Enrich multiple companies concurrently.
        
        Args:
            companies: List of company dictionaries with 'domain' field
            progress_callback: Optional callback for progress updates
            
        Returns:
            Tuple of (results, errors)
        """
        def enrich_company(company: Dict[str, Any]) -> Dict[str, Any]:
            """Enrich a single company."""
            domain = company.get('domain')
            if not domain:
                raise ValueError(f"Company missing domain: {company}")
            
            # Enrich the domain
            result = self.enrich_domain_with_rate_limit(domain)
            
            # Merge with existing company data
            result['id'] = company.get('id')
            result['original_data'] = company
            
            return result
        
        logger.info(f"Starting concurrent enrichment of {len(companies)} companies with {self.num_workers} workers")
        
        # Process companies in parallel
        results, errors = batch_process_with_progress(
            items=companies,
            process_func=enrich_company,
            num_workers=self.num_workers,
            progress_callback=progress_callback,
            rate_limit_apis=['selenium', 'deepseek', 'hubspot']
        )
        
        logger.info(f"Concurrent company enrichment completed: {len(results)} successful, {len(errors)} errors")
        
        return results, errors
    
    def enrich_leads(
        self,
        leads: List[Dict[str, Any]],
        progress_callback: Optional[Callable[[int, int, Dict, Optional[Dict]], None]] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict]]:
        """
        Enrich multiple leads concurrently.
        
        Args:
            leads: List of lead dictionaries with 'email' field
            progress_callback: Optional callback for progress updates
            
        Returns:
            Tuple of (results, errors)
        """
        def enrich_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
            """Enrich a single lead."""
            from src.utils.domain import extract_domain
            
            email = lead.get('email')
            if not email:
                raise ValueError(f"Lead missing email: {lead}")
            
            # Extract domain from email
            domain = extract_domain(email)
            if not domain:
                raise ValueError(f"Could not extract domain from email: {email}")
            
            # Get thread-local service instances
            service = self._get_enrichment_service()
            
            # Acquire rate limits
            rate_limit_manager.acquire('selenium')
            rate_limit_manager.acquire('deepseek')
            
            # Scrape domain
            scraped = service.scraper.scrape_domain(domain)
            if not scraped.success:
                raise ValueError(f"Failed to scrape {domain}: {scraped.error}")
            
            # Analyze for lead
            analysis = service.analyzer.analyze_lead(scraped.content, email)
            if not analysis:
                raise ValueError(f"Failed to analyze content for {email}")
            
            # Build result
            result = {
                'id': lead.get('id'),
                'email': email,
                'site_content': scraped.content,
                'enrichment_status': 'completed',
                'buyer_persona': analysis.buyer_persona,
                'lead_score_adjustment': analysis.lead_score_adjustment,
                'original_data': lead
            }
            
            return result
        
        logger.info(f"Starting concurrent enrichment of {len(leads)} leads with {self.num_workers} workers")
        
        # Process leads in parallel
        results, errors = batch_process_with_progress(
            items=leads,
            process_func=enrich_lead,
            num_workers=self.num_workers,
            progress_callback=progress_callback,
            rate_limit_apis=['selenium', 'deepseek', 'hubspot']
        )
        
        logger.info(f"Concurrent lead enrichment completed: {len(results)} successful, {len(errors)} errors")
        
        return results, errors