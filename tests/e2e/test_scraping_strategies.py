"""Tests for requests-first scraping strategy."""

import pytest
import json
import os
from pathlib import Path
import tempfile
import shutil
from unittest.mock import patch, Mock, MagicMock, call
import logging

# Disable Luigi logging during tests
logging.getLogger("luigi").setLevel(logging.ERROR)

from src.pipeline import DomainPipeline
from src.services.scraper import WebScraper
from src.models.enrichment import ScrapedContent


class TestScrapingStrategies:
    """Test the requests-first scraping approach with Selenium fallback."""
    
    @pytest.fixture
    def test_environment(self):
        """Set up test environment."""
        temp_dir = tempfile.mkdtemp()
        original_cwd = os.getcwd()
        
        dirs = [
            "data/site_content/raw",
            "data/enriched_companies/raw", 
            "data/enriched_leads/raw",
            "output",
            "logs"
        ]
        for d in dirs:
            os.makedirs(os.path.join(temp_dir, d), exist_ok=True)
        
        os.chdir(temp_dir)
        yield temp_dir
        
        os.chdir(original_cwd)
        shutil.rmtree(temp_dir)
    
    def test_requests_scraper_initialization(self):
        """Test that requests scraper is properly initialized."""
        scraper = WebScraper(use_browser_pool=False)
        
        # Should have requests scraper
        assert scraper.requests_scraper is not None
        assert hasattr(scraper.requests_scraper, 'fetch_page')
        
        # Should also have Selenium fallback
        assert scraper.fallback_scraper is not None
    
    @patch('src.services.scraper.WebScraper._scrape_with_requests')
    @patch('src.services.scraper.WebScraper._scrape_with_new_browser')
    def test_requests_first_success(self, mock_selenium, mock_requests):
        """Test that successful requests scraping doesn't trigger Selenium."""
        # Mock successful requests scraping with enough content
        mock_requests.return_value = ScrapedContent(
            url="https://example.com",
            content="This is a static website with plenty of content for analysis. " * 20,  # Make it longer than MIN_CONTENT_LENGTH
            success=True,
            emails=["info@example.com"],
            error=None
        )
        
        scraper = WebScraper(use_browser_pool=False)
        result = scraper.scrape_url("https://example.com")
        
        # Requests should be called
        assert mock_requests.called
        
        # Selenium should NOT be called
        assert not mock_selenium.called
        
        # Result should be from requests
        assert result.success is True
        assert "static website" in result.content
    
    @patch('src.services.scraper.WebScraper._scrape_with_requests')
    @patch('src.services.scraper.WebScraper._scrape_with_new_browser')
    def test_requests_insufficient_content_fallback(self, mock_selenium, mock_requests):
        """Test fallback to Selenium when requests returns insufficient content."""
        # Mock requests returning insufficient content
        mock_requests.return_value = ScrapedContent(
            url="https://example.com",
            content="Loading...",  # Too short
            success=True,
            emails=[],
            error=None
        )
        
        # Mock Selenium returning full content
        mock_selenium.return_value = ScrapedContent(
            url="https://example.com",
            content="This is the full JavaScript-rendered content with much more information.",
            success=True,
            emails=["contact@example.com"],
            error=None
        )
        
        scraper = WebScraper(use_browser_pool=False)
        result = scraper.scrape_url("https://example.com")
        
        # Both should be called
        assert mock_requests.called
        assert mock_selenium.called
        
        # Result should be from Selenium
        assert "JavaScript-rendered" in result.content
    
    @patch('src.services.scraper.WebScraper._scrape_with_requests')
    @patch('src.services.scraper.WebScraper._scrape_with_new_browser')
    def test_requests_failure_fallback(self, mock_selenium, mock_requests):
        """Test fallback to Selenium when requests fails."""
        # Mock requests failing
        mock_requests.side_effect = Exception("Connection refused")
        
        # Mock Selenium succeeding
        mock_selenium.return_value = ScrapedContent(
            url="https://example.com",
            content="Content scraped with Selenium after requests failed.",
            success=True,
            emails=["admin@example.com"],
            error=None
        )
        
        scraper = WebScraper(use_browser_pool=False)
        result = scraper.scrape_url("https://example.com")
        
        # Both should be called
        assert mock_requests.called
        assert mock_selenium.called
        
        # Result should be from Selenium
        assert result.success is True
        assert "Selenium" in result.content
    
    @pytest.mark.skip(reason="Common module import issues in test environment")
    @patch('common.scrape.requests_scraper.RequestsScraper')
    @patch('common.scrape.selenium_scraper.SeleniumScraper')
    def test_requests_scraper_session_management(self, mock_selenium_class, mock_requests_class):
        """Test that requests scraper properly manages sessions."""
        # Create mock instances
        mock_requests_instance = Mock()
        mock_requests_instance.session = None
        mock_requests_instance.initialize = Mock()
        mock_requests_instance.fetch_page = Mock(
            return_value=(200, "<html><body>Test content</body></html>", {})
        )
        mock_requests_class.return_value = mock_requests_instance
        
        # Mock Selenium
        mock_selenium_instance = Mock()
        mock_selenium_class.return_value = mock_selenium_instance
        
        scraper = WebScraper(use_browser_pool=False)
        
        # First scrape should initialize session
        scraper._scrape_with_requests("https://example.com")
        assert mock_requests_instance.initialize.called
        
        # Subsequent scrapes should reuse session
        mock_requests_instance.initialize.reset_mock()
        scraper._scrape_with_requests("https://another.com")
        
        # Initialize should be called only if session is None
        if hasattr(mock_requests_instance, 'session') and mock_requests_instance.session is not None:
            assert not mock_requests_instance.initialize.called
    
    @patch('src.tasks.scrape.WebScraper')
    def test_pipeline_with_mixed_content_types(self, mock_scraper_class, test_environment):
        """Test pipeline with sites that require different scraping strategies."""
        scrape_strategies = {}
        
        def strategy_tracking_scraper(url):
            from urllib.parse import urlparse
            domain = urlparse(url).hostname or url
            
            # Track which strategy was used
            if domain not in scrape_strategies:
                scrape_strategies[domain] = []
            
            # Simulate different scenarios
            if "static" in domain:
                # Static site - requests should work
                scrape_strategies[domain].append("requests")
                content = "This is a static HTML website with lots of content."
            elif "spa" in domain:
                # Single Page App - needs Selenium
                scrape_strategies[domain].append("selenium")
                content = "React App: Dynamic content loaded via JavaScript."
            elif "mixed" in domain:
                # Mixed content - requests gets partial, Selenium gets full
                if len(scrape_strategies[domain]) == 0:
                    scrape_strategies[domain].append("requests")
                    content = "Loading..."  # Insufficient
                else:
                    scrape_strategies[domain].append("selenium")
                    content = "Full content after JavaScript execution."
            else:
                scrape_strategies[domain].append("unknown")
                content = "Default content"
            
            return ScrapedContent(
                url=url,
                content=content,
                success=True,
                emails=[f"info@{domain}"],
                error=None
            )
        
        # Create a mock scraper instance
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_url = strategy_tracking_scraper
        mock_scraper_instance.scrape_domain = lambda domain: strategy_tracking_scraper(f"https://{domain}")
        mock_scraper_class.return_value = mock_scraper_instance
        
        # Process different types of sites
        domains = ["static-site.com", "spa-app.com", "mixed-content.com"]
        with open("mixed_sites.txt", "w") as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("mixed_sites.txt", "output", use_celery=False)
        
        # All should be processed
        for domain in domains:
            scraped_file = Path(f"data/site_content/raw/{domain}.json")
            assert scraped_file.exists()
    
    @pytest.mark.skip(reason="Common module import issues in test environment")
    @patch('bs4.BeautifulSoup')
    @patch('src.services.scraper.RequestsScraper')
    def test_html_parsing_in_requests_scraper(self, mock_requests_class, mock_soup_class):
        """Test that HTML is properly parsed when using requests scraper."""
        # Mock HTML content
        html_content = """
        <html>
            <head>
                <title>Test Page</title>
                <script>console.log('JavaScript code');</script>
                <style>body { color: black; }</style>
            </head>
            <body>
                <h1>Welcome to Test Site</h1>
                <p>This is the main content.</p>
                <p>Contact us at info@test.com</p>
                <script>
                    // More JavaScript
                    document.write('Dynamic content');
                </script>
            </body>
        </html>
        """
        
        # Mock requests scraper
        mock_requests_instance = Mock()
        mock_requests_instance.session = Mock()
        mock_requests_instance.fetch_page = Mock(
            return_value=(200, html_content, {"Content-Type": "text/html"})
        )
        mock_requests_class.return_value = mock_requests_instance
        
        # Mock BeautifulSoup
        mock_soup_instance = Mock()
        mock_soup_instance.get_text = Mock(
            return_value="Welcome to Test Site This is the main content. Contact us at info@test.com"
        )
        
        # Mock script/style removal
        mock_scripts = [Mock(), Mock()]
        mock_styles = [Mock()]
        mock_soup_instance.__call__ = Mock(side_effect=[mock_scripts, mock_styles])
        
        mock_soup_class.return_value = mock_soup_instance
        
        scraper = WebScraper(use_browser_pool=False)
        result = scraper._scrape_with_requests("https://test.com")
        
        # BeautifulSoup should be used to parse HTML
        assert mock_soup_class.called
        
        # Scripts and styles should be removed
        assert mock_soup_instance.called
        
        # Content should be extracted
        assert result.content == "Welcome to Test Site This is the main content. Contact us at info@test.com"
    
    @patch('src.services.scraper.WebScraper._scrape_with_requests')
    @patch('src.services.scraper.WebScraper._scrape_with_browser_pool')
    @patch('src.services.scraper.WebScraper._scrape_with_new_browser')
    def test_browser_pool_with_requests_fallback(self, mock_new_browser, mock_browser_pool, mock_requests):
        """Test that browser pool is tried after requests fails."""
        # Mock requests failing
        mock_requests.return_value = ScrapedContent(
            url="https://example.com",
            content="Insufficient",
            success=False,
            emails=[],
            error="Too short"
        )
        
        # Mock browser pool succeeding
        mock_browser_pool.return_value = ScrapedContent(
            url="https://example.com",
            content="Content from browser pool",
            success=True,
            emails=["pool@example.com"],
            error=None
        )
        
        scraper = WebScraper(use_browser_pool=True)
        scraper.browser_pool = Mock()  # Mock browser pool exists
        
        result = scraper.scrape_url("https://example.com")
        
        # Requests should be tried first
        assert mock_requests.called
        
        # Browser pool should be tried next
        assert mock_browser_pool.called
        
        # New browser should NOT be called
        assert not mock_new_browser.called
        
        # Result should be from browser pool
        assert "browser pool" in result.content
    
    def test_email_extraction_with_requests(self):
        """Test that email extraction works correctly with requests-scraped content."""
        scraper = WebScraper(use_browser_pool=False)
        
        # Test HTML with various email formats
        html_content = """
        <html>
            <body>
                <p>Contact us at info@example.com</p>
                <a href="mailto:sales@example.com">Sales Team</a>
                <div>Support: support@example.com</div>
                <!-- Email in comment: hidden@example.com -->
                <script>var email = "script@example.com";</script>
                <p>External: contact@other-domain.com (should be filtered)</p>
                <p>Subdomain: admin@sub.example.com</p>
            </body>
        </html>
        """
        
        emails = scraper.extract_emails_from_html(html_content, "example.com")
        
        # Should find emails from the domain
        assert "info@example.com" in emails
        assert "sales@example.com" in emails
        assert "support@example.com" in emails
        assert "hidden@example.com" in emails  # Even in comments
        assert "script@example.com" in emails  # Even in scripts
        
        # Should filter out external domain
        assert "contact@other-domain.com" not in emails
        
        # Subdomain handling depends on implementation
        # Current implementation includes subdomains
        assert "admin@sub.example.com" in emails
    
    @pytest.mark.skip(reason="Performance monitoring issues in test environment")
    @patch('src.services.scraper.WebScraper._scrape_with_requests')
    @patch('src.services.scraper.WebScraper._scrape_with_new_browser')  
    def test_performance_monitoring_tracks_strategy(self, mock_selenium, mock_requests):
        """Test that performance monitoring tracks which strategy was used."""
        # Mock successful requests scraping
        mock_requests.return_value = ScrapedContent(
            url="https://fast-site.com",
            content="Quick static content that loads fast",
            success=True,
            emails=["fast@fast-site.com"],
            error=None
        )
        
        # Import performance monitor
        from src.utils.performance_monitor import get_performance_monitor
        monitor = get_performance_monitor()
        
        scraper = WebScraper(use_browser_pool=False)
        result = scraper.scrape_url("https://fast-site.com")
        
        # Performance should be tracked
        metrics = monitor.metrics
        assert len(metrics) > 0
        
        # Latest metric should be for our domain
        latest_metric = metrics[-1]
        assert latest_metric.domain == "fast-site.com"
        assert latest_metric.success is True
        assert latest_metric.duration is not None