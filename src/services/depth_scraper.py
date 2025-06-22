"""Web scraper with depth support."""

import logging
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
from collections import deque

from src.models.enrichment import ScrapedContent
from src.services.scraper import WebScraper

logger = logging.getLogger(__name__)


class DepthAwareScraper(WebScraper):
    """Web scraper that supports depth-based crawling."""
    
    # Default crawling depth
    DEFAULT_DEPTH = 2
    
    def __init__(self, use_browser_pool: bool = True, max_depth: int = DEFAULT_DEPTH):
        """
        Initialize depth-aware scraper.
        
        Args:
            use_browser_pool: Whether to use browser pool
            max_depth: Maximum depth to crawl (default: 2)
        """
        super().__init__(use_browser_pool)
        self.max_depth = max_depth
        logger.info("Initialized DepthAwareScraper with max_depth=%d", max_depth)
    
    def scrape_with_depth(self, start_url: str, max_pages: int = 10) -> Dict[str, ScrapedContent]:
        """
        Scrape a website up to a specified depth.
        
        Args:
            start_url: Starting URL to scrape
            max_pages: Maximum number of pages to scrape (default: 10)
            
        Returns:
            Dictionary mapping URLs to ScrapedContent
        """
        logger.info("Starting depth-based scraping from %s (max_depth=%d, max_pages=%d)", 
                   start_url, self.max_depth, max_pages)
        
        # Parse base domain for link filtering
        parsed_start = urlparse(start_url)
        base_domain = parsed_start.netloc
        if base_domain.startswith('www.'):
            base_domain = base_domain[4:]
        
        # Queue of (url, depth) tuples
        url_queue = deque([(start_url, 0)])
        visited_urls = set()
        scraped_pages = {}
        
        while url_queue and len(scraped_pages) < max_pages:
            current_url, current_depth = url_queue.popleft()
            
            # Skip if already visited
            if current_url in visited_urls:
                continue
            
            visited_urls.add(current_url)
            
            logger.info("Scraping %s at depth %d", current_url, current_depth)
            
            # Scrape the page
            scraped_content = self.scrape_url(current_url)
            scraped_pages[current_url] = scraped_content
            
            # If scraping was successful and we haven't reached max depth, extract links
            if scraped_content.success and current_depth < self.max_depth:
                links = self._extract_links_from_content(scraped_content.content, current_url, base_domain)
                
                # Add links to queue with incremented depth
                for link in links:
                    if link not in visited_urls:
                        url_queue.append((link, current_depth + 1))
                        logger.debug("Added %s to queue at depth %d", link, current_depth + 1)
        
        logger.info("Depth scraping completed: %d pages scraped", len(scraped_pages))
        return scraped_pages
    
    def _extract_links_from_content(self, content: str, current_url: str, base_domain: str) -> List[str]:
        """
        Extract internal links from scraped content.
        
        Args:
            content: Page content (text)
            current_url: Current page URL
            base_domain: Base domain to filter internal links
            
        Returns:
            List of internal URLs
        """
        # Since we only have text content, we need to get the raw HTML
        # For now, we'll return empty list as we can't extract links from plain text
        # In a real implementation, we'd need to modify the scraper to return HTML
        return []
    
    def scrape_domain_with_depth(self, domain: str, max_pages: int = 10) -> Dict[str, ScrapedContent]:
        """
        Scrape a domain up to the specified depth.
        
        Args:
            domain: Domain to scrape
            max_pages: Maximum number of pages to scrape
            
        Returns:
            Dictionary mapping URLs to ScrapedContent
        """
        start_url = f"https://{domain}"
        return self.scrape_with_depth(start_url, max_pages)