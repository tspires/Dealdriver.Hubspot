"""Web scraper that preserves HTML content for link extraction."""

import logging
import re
import time
from typing import List, Tuple, Optional, Dict
from urllib.parse import urlparse, urljoin
from dataclasses import dataclass

from bs4 import BeautifulSoup

from src.models.enrichment import ScrapedContent
from src.services.scraper import WebScraper
from src.utils.domain import normalize_url

logger = logging.getLogger(__name__)


@dataclass 
class ScrapedPage:
    """Scraped page with both text and HTML content."""
    url: str
    text_content: str
    html_content: str
    emails: List[str]
    success: bool
    error: Optional[str] = None


class HTMLAwareScraper(WebScraper):
    """Web scraper that preserves HTML for link extraction."""
    
    def scrape_url_with_html(self, url: str) -> ScrapedPage:
        """
        Scrape a URL and preserve both text and HTML content.
        
        Args:
            url: URL to scrape
            
        Returns:
            ScrapedPage with both text and HTML
        """
        logger.debug("Scraping URL with HTML preservation: %s", url)
        
        # Try requests-based scraping first
        if self.requests_scraper:
            logger.info("Attempting HTML-aware requests scraping for %s", url)
            try:
                scraped_page = self._scrape_with_requests_html(url)
                if scraped_page.success and len(scraped_page.text_content) > self.MIN_CONTENT_LENGTH:
                    return scraped_page
                else:
                    logger.info("Requests returned insufficient content, trying Selenium")
            except Exception as e:
                logger.warning("Requests scraping failed: %s", e)
        
        # Fall back to Selenium
        logger.info("Using Selenium for %s", url)
        return self._scrape_with_selenium_html(url)
    
    def _scrape_with_requests_html(self, url: str) -> ScrapedPage:
        """Scrape using requests and preserve HTML."""
        try:
            normalized_url = normalize_url(url)
            
            # Initialize scraper if needed
            if not hasattr(self.requests_scraper, 'session') or self.requests_scraper.session is None:
                self.requests_scraper.initialize()
            
            # Fetch the page
            status_code, html_content, headers = self.requests_scraper.fetch_page(normalized_url)
            
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text content
            text_content = soup.get_text(separator=' ', strip=True)
            
            # Extract emails
            parsed_url = urlparse(normalized_url)
            domain = parsed_url.hostname
            if domain and domain.startswith('www.'):
                domain = domain[4:]
            
            emails = self.extract_emails_from_html(html_content, domain) if domain else []
            
            return ScrapedPage(
                url=normalized_url,
                text_content=text_content,
                html_content=html_content,
                emails=emails,
                success=True,
                error=None
            )
            
        except Exception as e:
            logger.error("Requests scraping failed: %s", e)
            return ScrapedPage(
                url=url,
                text_content="",
                html_content="",
                emails=[],
                success=False,
                error=str(e)
            )
    
    def _scrape_with_selenium_html(self, url: str) -> ScrapedPage:
        """Scrape using Selenium and preserve HTML."""
        scraper = self.fallback_scraper or self._create_scraper()
        if not scraper:
            return ScrapedPage(
                url=url,
                text_content="",
                html_content="",
                emails=[],
                success=False,
                error="Failed to create scraper"
            )
        
        try:
            normalized_url = normalize_url(url)
            
            # Initialize and navigate
            scraper.initialize()
            scraper.driver.get(normalized_url)
            
            # Wait for content
            time.sleep(self.DEFAULT_WAIT_TIME)
            
            # Get HTML and text
            html_content = scraper.driver.page_source
            text_content = scraper.driver.find_element("tag name", "body").text
            
            # Extract emails
            parsed_url = urlparse(normalized_url)
            domain = parsed_url.hostname
            if domain and domain.startswith('www.'):
                domain = domain[4:]
            
            emails = self.extract_emails_from_html(html_content, domain) if domain else []
            
            # Cleanup
            if hasattr(scraper, 'safe_cleanup'):
                scraper.safe_cleanup()
            else:
                scraper.cleanup()
            
            return ScrapedPage(
                url=normalized_url,
                text_content=text_content,
                html_content=html_content,
                emails=emails,
                success=len(text_content) > self.MIN_CONTENT_LENGTH,
                error=None if len(text_content) > self.MIN_CONTENT_LENGTH else "Insufficient content"
            )
            
        except Exception as e:
            logger.error("Selenium scraping failed: %s", e)
            # Ensure cleanup
            try:
                if scraper and scraper != self.fallback_scraper:
                    if hasattr(scraper, 'safe_cleanup'):
                        scraper.safe_cleanup()
                    else:
                        scraper.cleanup()
            except:
                pass
            
            return ScrapedPage(
                url=url,
                text_content="",
                html_content="",
                emails=[],
                success=False,
                error=str(e)
            )
    
    def extract_links_from_html(self, html: str, base_url: str, target_domain: str) -> List[str]:
        """
        Extract internal links from HTML content.
        
        Args:
            html: HTML content
            base_url: Base URL for resolving relative links
            target_domain: Domain to filter internal links
            
        Returns:
            List of internal URLs
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = set()
            
            # Find all links
            for tag in soup.find_all(['a', 'link']):
                href = tag.get('href')
                if not href:
                    continue
                
                # Skip anchors and javascript
                if href.startswith('#') or href.startswith('javascript:'):
                    continue
                
                # Resolve relative URLs
                absolute_url = urljoin(base_url, href)
                
                # Parse and check domain
                parsed = urlparse(absolute_url)
                url_domain = parsed.hostname
                if not url_domain:
                    continue
                    
                if url_domain.startswith('www.'):
                    url_domain = url_domain[4:]
                
                # Check if internal link
                if url_domain == target_domain or url_domain.endswith(f'.{target_domain}'):
                    # Normalize URL
                    normalized = absolute_url.split('#')[0].rstrip('/')
                    if normalized:
                        links.add(normalized)
            
            return list(links)
            
        except Exception as e:
            logger.error(f"Error extracting links: {e}")
            return []
    
