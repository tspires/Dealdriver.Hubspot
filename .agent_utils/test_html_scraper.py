#!/usr/bin/env python3
"""Test HTML-aware scraper."""

import sys
sys.path.insert(0, '/home/tspires/Development/Dealdriver/Dealdriver.Hubspot')

from src.services.html_aware_scraper import HTMLAwareScraper

# Test HTML scraper
print("Testing HTMLAwareScraper...")
scraper = HTMLAwareScraper(use_browser_pool=False)

# Test URL
url = "https://example.com"

try:
    result = scraper.scrape_url_with_html(url)
    print(f"Result type: {type(result)}")
    print(f"URL: {result.url}")
    print(f"Success: {result.success}")
    print(f"Text length: {len(result.text_content)}")
    print(f"HTML length: {len(result.html_content)}")
    print(f"Emails: {result.emails}")
    
    # Test link extraction
    links = scraper.extract_links_from_html(result.html_content, url, "example.com")
    print(f"Links found: {links}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()