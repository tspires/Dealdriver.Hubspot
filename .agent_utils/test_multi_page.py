#!/usr/bin/env python3
"""Test multi-page scraping functionality."""

import sys
sys.path.insert(0, '/home/tspires/Development/Dealdriver/Dealdriver.Hubspot')

from src.services.multi_page_scraper import MultiPageScraper

# Test multi-page scraping
print("Testing MultiPageScraper...")
scraper = MultiPageScraper(max_depth=2)

# Test with a real domain
domain = "example.com"
print(f"\nScraping {domain} with depth=2...")

try:
    scraped_pages = scraper.scrape_domain_multi_page(domain, max_pages=5)
    
    print(f"\nScraped {len(scraped_pages)} pages:")
    for url, content in scraped_pages.items():
        print(f"  - {url}: {len(content.content)} chars, {len(content.emails)} emails")
    
    # Test combining content
    print("\nTesting content combination...")
    combined = scraper.create_combined_content(scraped_pages)
    print(f"Combined content: {len(combined.content)} chars, {len(combined.emails)} unique emails")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()