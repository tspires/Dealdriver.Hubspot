"""Multi-page web scraper with depth support."""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
from collections import deque
from dataclasses import dataclass
import time

from bs4 import BeautifulSoup

from src.models.enrichment import ScrapedContent
from src.services.scraper import WebScraper
from src.services.html_aware_scraper import HTMLAwareScraper, ScrapedPage
from src.constants import ProcessingConfig

logger = logging.getLogger(__name__)


@dataclass
class PageInfo:
    """Information about a scraped page."""
    url: str
    depth: int
    content: str
    emails: List[str]
    links: List[str]
    success: bool
    error: Optional[str] = None


class MultiPageScraper(HTMLAwareScraper):
    """Web scraper that supports multi-page crawling with configurable depth."""
    
    def __init__(self, use_browser_pool: bool = False, max_depth: Optional[int] = None):
        """
        Initialize multi-page scraper.
        
        Args:
            use_browser_pool: Whether to use browser pool (disabled for multi-page)
            max_depth: Maximum crawling depth (None uses default from settings)
        """
        # Disable browser pool for multi-page crawling to avoid session issues
        super().__init__(use_browser_pool=False)
        
        # Set max depth from parameter or use default
        self.max_depth = max_depth if max_depth is not None else ProcessingConfig.DEFAULT_SCRAPING_DEPTH
        
        logger.info("Initialized MultiPageScraper with max_depth=%d", self.max_depth)
        
    def scrape_domain_multi_page(self, domain: str, max_pages: int = 10) -> Dict[str, ScrapedContent]:
        """
        Scrape a domain with multiple pages up to specified depth.
        
        Args:
            domain: Domain to scrape
            max_pages: Maximum number of pages to scrape
            
        Returns:
            Dictionary mapping URLs to ScrapedContent
        """
        start_url = f"https://{domain}"
        return self.scrape_multi_page(start_url, max_pages)
    
    def scrape_multi_page(self, start_url: str, max_pages: int = 10) -> Dict[str, ScrapedContent]:
        """
        Scrape multiple pages starting from a URL up to specified depth.
        
        Args:
            start_url: Starting URL
            max_pages: Maximum number of pages to scrape
            
        Returns:
            Dictionary mapping URLs to ScrapedContent
        """
        logger.info("Starting multi-page scrape from %s (depth=%d, max_pages=%d)", 
                   start_url, self.max_depth, max_pages)
        
        # Parse base domain for filtering internal links
        parsed_start = urlparse(start_url)
        base_domain = parsed_start.netloc
        if base_domain.startswith('www.'):
            base_domain = base_domain[4:]
        
        # Queue of (url, depth) tuples
        url_queue = deque([(start_url, 0)])
        visited_urls = set()
        scraped_pages = {}
        
        # Track statistics
        total_emails_found = set()
        pages_by_depth = {i: 0 for i in range(self.max_depth + 1)}
        
        while url_queue and len(scraped_pages) < max_pages:
            current_url, current_depth = url_queue.popleft()
            
            # Skip if already visited
            if current_url in visited_urls:
                continue
                
            visited_urls.add(current_url)
            pages_by_depth[current_depth] += 1
            
            logger.info("Scraping page %d/%d: %s (depth=%d)", 
                       len(scraped_pages) + 1, max_pages, current_url, current_depth)
            
            # Scrape the page with HTML preservation
            try:
                scraped_page = self.scrape_url_with_html(current_url)
            except AttributeError:
                # Fallback to regular scraping if HTML-aware scraping not available
                logger.debug("HTML-aware scraping not available, using regular scraping")
                scraped_content = self.scrape_url(current_url)
                scraped_pages[current_url] = scraped_content
                
                # Collect emails
                if scraped_content.emails:
                    total_emails_found.update(scraped_content.emails)
                    
                # Can't extract links without HTML, so continue
                continue
            
            # Convert to ScrapedContent for compatibility
            scraped_content = ScrapedContent(
                url=scraped_page.url,
                content=scraped_page.text_content,
                success=scraped_page.success,
                error=scraped_page.error,
                emails=scraped_page.emails
            )
            scraped_pages[current_url] = scraped_content
            
            # Collect emails
            if scraped_content.emails:
                total_emails_found.update(scraped_content.emails)
            
            # Extract and queue links if we haven't reached max depth
            if scraped_page.success and current_depth < self.max_depth:
                # Extract links from HTML
                links = self.extract_links_from_html(
                    scraped_page.html_content, 
                    current_url, 
                    base_domain
                )
                
                # Add unvisited links to queue
                new_links_added = 0
                for link in links:
                    if link not in visited_urls and (link, current_depth + 1) not in url_queue:
                        url_queue.append((link, current_depth + 1))
                        new_links_added += 1
                
                if new_links_added > 0:
                    logger.debug("Added %d new links to queue from %s", new_links_added, current_url)
            
            # Add small delay between pages to be respectful
            if url_queue:
                time.sleep(1)
        
        # Log statistics
        logger.info("Multi-page scraping completed:")
        logger.info("  Total pages scraped: %d", len(scraped_pages))
        logger.info("  Total unique emails found: %d", len(total_emails_found))
        for depth, count in pages_by_depth.items():
            if count > 0:
                logger.info("  Pages at depth %d: %d", depth, count)
        
        return scraped_pages
    
    def _extract_links_from_content(self, content: str, current_url: str, base_domain: str) -> List[str]:
        """
        Extract links from page content.
        
        Since we only have text content, this is limited. In a real implementation,
        we would need the raw HTML. For now, we'll try to find URLs in the text.
        
        Args:
            content: Page text content
            current_url: Current page URL
            base_domain: Base domain for filtering
            
        Returns:
            List of internal URLs found
        """
        links = []
        
        # Try to find URLs in the content using regex
        # This is not ideal but works for simple cases
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+[^\s<>"{}|\\^`\[\].,;:!?\'")\]}]'
        potential_urls = re.findall(url_pattern, content)
        
        for url in potential_urls:
            # Clean up the URL
            url = url.rstrip('.,;:!?\'"')
            
            # Parse and check if it's an internal link
            try:
                parsed = urlparse(url)
                url_domain = parsed.netloc
                if url_domain.startswith('www.'):
                    url_domain = url_domain[4:]
                
                # Check if internal link
                if url_domain == base_domain or url_domain.endswith(f'.{base_domain}'):
                    # Normalize the URL
                    normalized = url.rstrip('/')
                    if normalized not in links:
                        links.append(normalized)
            except Exception as e:
                logger.debug("Error parsing potential URL %s: %s", url, e)
        
        logger.debug("Found %d potential internal links in content", len(links))
        return links
    
    
    def create_combined_content(self, scraped_pages: Dict[str, ScrapedContent]) -> ScrapedContent:
        """
        Combine multiple scraped pages into a single ScrapedContent object.
        
        Args:
            scraped_pages: Dictionary of URL to ScrapedContent
            
        Returns:
            Combined ScrapedContent with all content and emails
        """
        if not scraped_pages:
            return ScrapedContent(
                url="",
                content="",
                success=False,
                error="No pages scraped",
                emails=[]
            )
        
        # Get the first URL as the main URL
        main_url = next(iter(scraped_pages.keys()))
        
        # Combine all content with URL headers
        combined_content_parts = []
        all_emails = set()
        successful_pages = 0
        
        for url, scraped in scraped_pages.items():
            if scraped.success and scraped.content:
                # Add URL as section header
                combined_content_parts.append(f"\n\n=== Page: {url} ===\n")
                combined_content_parts.append(scraped.content)
                successful_pages += 1
                
                # Collect emails
                if scraped.emails:
                    all_emails.update(scraped.emails)
        
        # Create combined content
        combined_content = "".join(combined_content_parts).strip()
        
        logger.info("Combined %d successful pages into single content (%d chars, %d emails)", 
                   successful_pages, len(combined_content), len(all_emails))
        
        return ScrapedContent(
            url=main_url,
            content=combined_content,
            success=successful_pages > 0,
            error=None if successful_pages > 0 else "No successful pages",
            emails=list(all_emails)
        )