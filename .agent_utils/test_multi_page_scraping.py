#!/usr/bin/env python3
"""Test multi-page scraping functionality."""

import sys
import logging
from pathlib import Path

# Add project to path
sys.path.insert(0, '/home/tspires/Development/Dealdriver/Dealdriver.Hubspot')

from src.services.multi_page_scraper import MultiPageScraper
from src.utils.logging import setup_logging

# Setup logging
setup_logging(level="DEBUG")
logger = logging.getLogger(__name__)


def test_multi_page_scraping():
    """Test multi-page scraping with different depths."""
    
    # Test domain
    test_domain = "python.org"
    
    print("\n" + "=" * 80)
    print(f"Testing Multi-Page Scraping for {test_domain}")
    print("=" * 80)
    
    # Test different depths
    depths = [0, 1, 2]
    
    for depth in depths:
        print(f"\n\n--- Testing with depth={depth} ---")
        
        try:
            # Create scraper with specified depth
            scraper = MultiPageScraper(use_browser_pool=False, max_depth=depth)
            
            # Scrape domain
            if depth > 0:
                scraped_pages = scraper.scrape_domain_multi_page(test_domain, max_pages=5)
                
                print(f"\nScraped {len(scraped_pages)} pages:")
                for url, content in scraped_pages.items():
                    print(f"  - {url}: {len(content.content)} chars, {len(content.emails)} emails")
                
                # Combine content
                combined = scraper.create_combined_content(scraped_pages)
                print(f"\nCombined content: {len(combined.content)} chars total")
                print(f"Total unique emails: {len(combined.emails)}")
                
            else:
                # Single page
                scraped = scraper.scrape_domain(test_domain)
                print(f"\nSingle page scraped: {len(scraped.content)} chars")
                print(f"Emails found: {len(scraped.emails)}")
                
        except Exception as e:
            print(f"Error: {e}")
            logger.error("Test failed", exc_info=True)


def test_link_extraction():
    """Test link extraction from content."""
    
    print("\n\n" + "=" * 80)
    print("Testing Link Extraction")
    print("=" * 80)
    
    # Sample content with links
    test_content = """
    Welcome to our website. Check out these pages:
    
    Visit our about page at https://example.com/about
    Learn more at https://example.com/services/consulting
    External link: https://google.com
    Contact us: https://example.com/contact
    
    Also see http://example.com/blog for updates.
    """
    
    scraper = MultiPageScraper()
    links = scraper._extract_links_from_content(test_content, "https://example.com", "example.com")
    
    print(f"\nFound {len(links)} internal links:")
    for link in links:
        print(f"  - {link}")


if __name__ == "__main__":
    test_multi_page_scraping()
    test_link_extraction()