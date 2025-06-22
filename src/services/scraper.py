"""Web scraping service."""

import logging
import re
import time
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

from src.models.enrichment import ScrapedContent
from src.utils.domain import extract_domain, normalize_url


logger = logging.getLogger(__name__)


class WebScraper:
    """Service for scraping website content."""
    
    # Configuration constants
    MIN_CONTENT_LENGTH = 100
    DEFAULT_WAIT_TIME = 2
    SCRAPER_TIMEOUT = 10
    
    def __init__(self, use_browser_pool: bool = True):
        """Initialize scraper."""
        logger.info("Initializing WebScraper with use_browser_pool=%s", use_browser_pool)
        self._scraper_config = None
        self._selenium_scraper_class = None
        self._requests_scraper_class = None
        self.use_browser_pool = use_browser_pool
        self.browser_pool = None
        self.requests_scraper = None
        
        try:
            import sys
            # Add the common directory to Python path
            logger.debug("Adding common directory to Python path")
            sys.path.insert(0, '/home/tspires/Development/common')
            
            # Import scrapers
            logger.debug("Importing scraper classes from common library")
            from scrape.selenium_scraper import SeleniumScraper
            from scrape.requests_scraper import RequestsScraper
            from scrape.base_scraper import ScraperConfig
            logger.debug("Successfully imported scraper classes")
            
            # Store config for creating scrapers
            logger.debug("Creating scraper configuration")
            # Get scraping depth from settings if available
            from src.constants import ProcessingConfig
            scraping_depth = getattr(ProcessingConfig, 'DEFAULT_SCRAPING_DEPTH', 2)
            
            self._scraper_config = ScraperConfig(
                timeout=self.SCRAPER_TIMEOUT,
                max_pages=10,
                enable_javascript=True,
                load_images=False,  # Faster without images
                load_css=True,
                delay_between_requests=1.0,
                max_retries=1  # Reduced retries for faster failure
            )
            # Store depth for future use
            self.scraping_depth = scraping_depth
            logger.debug("Scraper config: timeout=%d, javascript=%s, images=%s", 
                        self.SCRAPER_TIMEOUT, True, False)
            self._selenium_scraper_class = SeleniumScraper
            self._requests_scraper_class = RequestsScraper
            
            # Create a fallback scraper
            logger.debug("Creating fallback Selenium scraper")
            self.fallback_scraper = SeleniumScraper(config=self._scraper_config, headless=True)
            logger.info("Fallback Selenium scraper initialized")
            
            # Initialize requests scraper
            logger.debug("Creating requests scraper")
            self.requests_scraper = RequestsScraper(
                timeout=self.SCRAPER_TIMEOUT,
                delay_between_requests=0.5,  # Faster for requests
                max_retries=2
            )
            logger.info("Requests scraper initialized")
            
            # Initialize browser pool if enabled
            if use_browser_pool:
                try:
                    from src.services.browser_pool import get_browser_pool
                    self.browser_pool = get_browser_pool()
                    logger.info("Browser pool initialized successfully")
                except Exception as pool_error:
                    logger.warning("Failed to initialize browser pool: %s", pool_error)
                    logger.debug("Browser pool initialization error details", exc_info=True)
                    self.browser_pool = None
                
        except Exception as e:
            logger.error("Failed to import scrapers: %s", e)
            logger.debug("Scraper import error details", exc_info=True)
            self._selenium_scraper_class = None
            self._requests_scraper_class = None
            self.fallback_scraper = None
            self.requests_scraper = None
            logger.critical("WebScraper initialization failed - scraping will not work")
    
    def _create_scraper(self):
        """Factory method to create a new scraper instance."""
        logger.debug("Creating new scraper instance")
        if self._selenium_scraper_class and self._scraper_config:
            logger.debug("Creating Selenium scraper with headless=True")
            return self._selenium_scraper_class(config=self._scraper_config, headless=True)
        logger.warning("Cannot create scraper - missing Selenium class or config")
        return None
    
    def extract_emails_from_html(self, html: str, domain: str) -> List[str]:
        """Extract email addresses from HTML that match the given domain."""
        # Email regex pattern
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        # Find all email addresses in the HTML
        all_emails = re.findall(email_pattern, html)
        
        # Filter emails to only include those with the same domain
        domain_emails = []
        for email in all_emails:
            email_domain = email.split('@')[1].lower()
            # Check if email domain matches or is a subdomain of the target domain
            if email_domain == domain.lower() or email_domain.endswith('.' + domain.lower()):
                if email.lower() not in [e.lower() for e in domain_emails]:
                    domain_emails.append(email)
        
        logger.debug("Found %d emails for domain %s", len(domain_emails), domain)
        if domain_emails:
            logger.debug("Email addresses found: %s", domain_emails)
        return domain_emails
    
    def scrape_url(self, url: str) -> ScrapedContent:
        """Scrape content from a URL - tries requests first, then falls back to Selenium."""
        # Add performance monitoring
        from src.utils.performance_monitor import get_performance_monitor
        monitor = get_performance_monitor()
        
        # Extract domain for monitoring
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.hostname or url
        
        with monitor.track_scrape(domain) as metric:
            # First, try requests-based scraping (faster and lighter)
            if self.requests_scraper:
                logger.info("Attempting requests-based scraping for %s", url)
                logger.debug("Request scraper available: %s", self.requests_scraper is not None)
                try:
                    result = self._scrape_with_requests(url)
                    if result.success and result.content and len(result.content) > self.MIN_CONTENT_LENGTH:
                        logger.info("Successfully scraped %s using requests", url)
                        logger.debug("Content length: %d, Emails found: %d", 
                                   len(result.content), len(result.emails) if result.emails else 0)
                        metric.complete(
                            success=result.success,
                            content_size=len(result.content) if result.content else 0,
                            emails_found=len(result.emails) if result.emails else 0,
                            error=result.error
                        )
                        return result
                    else:
                        logger.info("Requests scraping returned insufficient content (%d chars), falling back to Selenium", 
                                   len(result.content) if result.content else 0)
                except Exception as e:
                    logger.warning("Requests scraping failed for %s: %s, falling back to Selenium", url, e)
                    logger.debug("Requests scraping error details", exc_info=True)
            
            # Fall back to Selenium-based scraping
            logger.info("Using Selenium for %s", url)
            logger.debug("Browser pool enabled: %s, Browser pool available: %s", 
                        self.use_browser_pool, self.browser_pool is not None)
            
            # Check if we have the necessary components
            if not self._selenium_scraper_class:
                logger.warning("SeleniumScraper not available")
                result = ScrapedContent(
                    url=url,
                    content="",
                    success=False,
                    error="SeleniumScraper not available",
                    emails=[]
                )
                metric.complete(success=False, error="SeleniumScraper not available")
                return result
            
            # Try browser pool first if enabled
            if self.use_browser_pool and self.browser_pool:
                try:
                    result = self._scrape_with_browser_pool(url)
                except Exception as e:
                    logger.warning("Browser pool scraping failed: %s, falling back to new browser", e)
                    logger.debug("Browser pool error details", exc_info=True)
                    result = self._scrape_with_new_browser(url)
            else:
                # Use new browser instances
                result = self._scrape_with_new_browser(url)
            
            # Update metrics
            metric.complete(
                success=result.success,
                content_size=len(result.content) if result.content else 0,
                emails_found=len(result.emails) if result.emails else 0,
                error=result.error
            )
            
            return result
    
    def _scrape_with_requests(self, url: str) -> ScrapedContent:
        """Scrape using requests library (no JavaScript support)."""
        try:
            normalized_url = normalize_url(url)
            logger.info("Starting requests scrape of %s", normalized_url)
            logger.debug("Original URL: %s, Normalized URL: %s", url, normalized_url)
            
            # Initialize the requests scraper if not already done
            if not hasattr(self.requests_scraper, 'session') or self.requests_scraper.session is None:
                self.requests_scraper.initialize()
            
            # Fetch the page
            status_code, html_content, headers = self.requests_scraper.fetch_page(normalized_url)
            
            # Extract text content from HTML
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text content
            content = soup.get_text(separator=' ', strip=True)
            
            # Extract emails from raw HTML
            parsed_url = urlparse(normalized_url)
            domain = parsed_url.hostname
            if domain and domain.startswith('www.'):
                domain = domain[4:]
            
            emails = self.extract_emails_from_html(html_content, domain) if domain else []
            
            logger.info("Requests scrape completed: %d chars, %d emails", 
                       len(content), len(emails))
            logger.debug("Request scrape success for %s", normalized_url)
            
            return ScrapedContent(
                url=normalized_url,
                content=content,
                success=True,
                emails=emails,
                error=None
            )
            
        except Exception as e:
            logger.error("Requests scraping failed for %s: %s", url, str(e))
            logger.debug("Full requests scraping error", exc_info=True)
            return ScrapedContent(
                url=url,
                content="",
                success=False,
                error=str(e),
                emails=[]
            )
    
    def _scrape_with_browser_pool(self, url: str) -> ScrapedContent:
        """Scrape using browser from pool."""
        from src.services.browser_pool import BrowserPool
        
        with self.browser_pool.get_browser() as scraper:
            if not scraper:
                raise Exception("No browser available from pool")
            
            try:
                normalized_url = normalize_url(url)
                logger.info("Starting scrape of %s (browser pool)", normalized_url)
                logger.debug("Using browser from pool")
                
                # Navigate to the page
                scraper.driver.get(normalized_url)
                
                # Wait for content
                time.sleep(self.DEFAULT_WAIT_TIME)
                
                # Get content and emails
                from selenium.webdriver.common.by import By
                raw_html = scraper.driver.page_source
                content = scraper.driver.find_element(By.TAG_NAME, "body").text
                
                # Extract emails
                parsed_url = urlparse(normalized_url)
                domain = parsed_url.hostname
                if domain and domain.startswith('www.'):
                    domain = domain[4:]
                
                emails = self.extract_emails_from_html(raw_html, domain) if domain else []
                
                success = len(content) > self.MIN_CONTENT_LENGTH
                return ScrapedContent(
                    url=normalized_url,
                    content=content,
                    success=success,
                    emails=emails,
                    error=None if success else "Insufficient content"
                )
                
            except Exception as e:
                logger.error("Browser pool scraping failed for %s: %s", url, e)
                logger.debug("Browser pool scraping error details", exc_info=True)
                raise
    
    def _scrape_with_new_browser(self, url: str) -> ScrapedContent:
        """Scrape using a new browser instance (fallback mode)."""
        scraper = self.fallback_scraper or self._create_scraper()
        if not scraper:
            return ScrapedContent(
                url=url,
                content="",
                success=False,
                error="Failed to create scraper",
                emails=[]
            )
        
        # Validate domain before attempting to scrape
        if hasattr(scraper, 'validate_domain'):
            is_valid, error_msg = scraper.validate_domain(url)
            if not is_valid:
                logger.warning("Domain validation failed for %s: %s", url, error_msg)
                logger.debug("Skipping scrape due to invalid domain")
                return ScrapedContent(
                    url=url,
                    content="",
                    success=False,
                    error=error_msg,
                    emails=[]
                )
        
        try:
            normalized_url = normalize_url(url)
            logger.info("Starting scrape of %s (new browser mode)", normalized_url)
            logger.debug("Creating new browser instance for scraping")
            
            # Initialize the scraper (creates driver)
            logger.debug("Initializing Selenium driver")
            scraper.initialize()
            
            # Import Selenium components
            from selenium.webdriver.common.by import By
            
            # Navigate to the page
            logger.info("Loading page: %s", normalized_url)
            start_time = time.time()
            scraper.driver.get(normalized_url)
            load_time = time.time() - start_time
            logger.info("Page loaded in %.2f seconds", load_time)
            logger.debug("Page title: %s", scraper.driver.title)
            
            # Wait for JavaScript to load
            logger.debug("Waiting %d seconds for JavaScript to execute", self.DEFAULT_WAIT_TIME)
            js_start = time.time()
            time.sleep(self.DEFAULT_WAIT_TIME)
            logger.debug("JavaScript wait completed in %.2f seconds", time.time() - js_start)
            
            # Get the raw HTML for email extraction
            logger.debug("Extracting raw HTML for email detection")
            emails = []
            try:
                raw_html = scraper.driver.page_source
                # Extract domain from URL for email filtering
                parsed_url = urlparse(normalized_url)
                domain = parsed_url.hostname
                if domain and domain.startswith('www.'):
                    domain = domain[4:]  # Remove www. prefix
                
                # Extract emails from raw HTML
                emails = self.extract_emails_from_html(raw_html, domain) if domain else []
                if emails:
                    logger.info("Found %d email(s) for domain %s: %s", 
                               len(emails), domain, emails)
                else:
                    logger.debug("No emails found for domain %s", domain)
            except Exception as e:
                logger.error("Failed to extract emails: %s", e)
                logger.debug("Email extraction error details", exc_info=True)
            
            # Get the page content
            logger.debug("Extracting page content")
            content = ""
            try:
                content = scraper.driver.find_element(By.TAG_NAME, "body").text
                logger.info("Successfully scraped %d characters from %s", 
                           len(content), normalized_url)
                logger.debug("First 200 chars of content: %s", content[:200] if content else "")
            except Exception as e:
                logger.error("Failed to extract body text: %s", e)
                logger.debug("Body text extraction error details", exc_info=True)
            
            # Cleanup the scraper (closes driver)
            logger.debug("Cleaning up Selenium driver")
            if hasattr(scraper, 'safe_cleanup'):
                scraper.safe_cleanup()
            else:
                scraper.cleanup()
            
            # Determine success based on content
            if content and len(content) > self.MIN_CONTENT_LENGTH:
                logger.info("Scrape successful for %s", normalized_url)
                logger.debug("Final result: %d chars content, %d emails", 
                           len(content), len(emails))
                return ScrapedContent(
                    url=normalized_url,
                    content=content,
                    success=True,
                    emails=emails
                )
            else:
                logger.warning("Insufficient content scraped from %s (only %d characters, minimum: %d)", 
                             normalized_url, len(content), self.MIN_CONTENT_LENGTH)
                return ScrapedContent(
                    url=normalized_url,
                    content=content,
                    success=False,
                    error=f"Insufficient content scraped (only {len(content)} characters)",
                    emails=emails
                )
                
        except Exception as e:
            logger.error("Failed to scrape %s: %s", url, str(e))
            logger.debug("Full scraping error details", exc_info=True)
            # Ensure cleanup on error
            try:
                if scraper and scraper != self.fallback_scraper:
                    logger.debug("Cleaning up driver after error")
                    if hasattr(scraper, 'safe_cleanup'):
                        scraper.safe_cleanup()
                    else:
                        scraper.cleanup()
            except Exception as cleanup_error:
                logger.error("Error during cleanup: %s", cleanup_error)
                logger.debug("Cleanup error details", exc_info=True)
            return ScrapedContent(
                url=url,
                content="",
                success=False,
                error=str(e),
                emails=[]
            )
    
    def scrape_domain(self, domain: str) -> ScrapedContent:
        """Scrape content from a domain."""
        logger.debug("Scraping domain: %s", domain)
        url = f"https://{domain}"
        logger.debug("Converted domain to URL: %s", url)
        return self.scrape_url(url)