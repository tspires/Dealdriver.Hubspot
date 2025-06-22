#!/usr/bin/env python3
"""Debug multi-page scraping."""

import sys
sys.path.insert(0, '/home/tspires/Development/Dealdriver/Dealdriver.Hubspot')

from src.services.multi_page_scraper import MultiPageScraper

# Create scraper
print("Creating MultiPageScraper...")
scraper = MultiPageScraper(max_depth=1)

# Check method
print(f"scrape_url_with_html method: {scraper.scrape_url_with_html}")
print(f"Method from class: {scraper.__class__.scrape_url_with_html}")

# Try to call the method directly
url = "https://example.com"
print(f"\nTrying to scrape {url}...")

try:
    result = scraper.scrape_url_with_html(url)
    print(f"Success! Result type: {type(result)}")
    print(f"Result attributes: {dir(result)}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    
# Also test the parent class directly
print("\n\nTesting HTMLAwareScraper directly...")
from src.services.html_aware_scraper import HTMLAwareScraper
html_scraper = HTMLAwareScraper()
try:
    result2 = html_scraper.scrape_url_with_html(url)
    print(f"Success! Result type: {type(result2)}")
except Exception as e:
    print(f"Error: {e}")