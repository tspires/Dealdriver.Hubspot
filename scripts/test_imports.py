#!/usr/bin/env python3
import sys
sys.path.insert(0, '/home/tspires/Development')

print("Testing imports...")

try:
    # Import HubSpot client directly without going through common.__init__
    import common.clients.hubspot as hubspot_module
    print("✓ HubSpot client module imported")
    
    # Try to import the client class directly
    HubSpotClient = hubspot_module.HubSpotClient
    print("✓ HubSpotClient class imported")
    
except Exception as e:
    print(f"✗ Import failed: {e}")
    import traceback
    traceback.print_exc()

# Browser manager was removed - no longer testing this import
# try:
#     from Dealdriver.Enrich.browser_selenium_manager import BrowserSeleniumManager
#     print("✓ BrowserSeleniumManager imported")
# except Exception as e:
#     print(f"✗ BrowserSeleniumManager import failed: {e}")

try:
    import common.clients.deepseek as deepseek_module
    DeepSeekClient = deepseek_module.DeepSeekClient
    print("✓ DeepSeekClient imported")
except Exception as e:
    print(f"✗ DeepSeekClient import failed: {e}")

# Test scraper imports
try:
    import common.scrape.selenium_scraper as selenium_module
    SeleniumScraper = selenium_module.SeleniumScraper
    print("✓ SeleniumScraper imported")
except Exception as e:
    print(f"✗ SeleniumScraper import failed: {e}")

try:
    import common.scrape.requests_scraper as requests_module
    RequestsScraper = requests_module.RequestsScraper
    print("✓ RequestsScraper imported")
except Exception as e:
    print(f"✗ RequestsScraper import failed: {e}")