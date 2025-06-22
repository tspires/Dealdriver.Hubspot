#!/usr/bin/env python3
"""Test full multi-page enrichment pipeline."""

import sys
sys.path.insert(0, '/home/tspires/Development/Dealdriver/Dealdriver.Hubspot')

from src.services.multi_page_domain_enrichment_service import MultiPageDomainEnrichmentService
from src.services.analyzer import AIAnalyzer

# Create services
print("Creating AI analyzer...")
analyzer = AIAnalyzer()

print("Creating multi-page domain enrichment service...")
service = MultiPageDomainEnrichmentService(analyzer, scraping_depth=2)

# Test domain
domain = "python.org"
print(f"\nEnriching {domain} with depth=2...")

try:
    result = service.enrich_domain(domain)
    
    print(f"\nEnrichment Status: {result.get('enrichment_status')}")
    print(f"Pages Scraped: {result.get('pages_scraped', 0)}")
    
    if result.get('scraped_urls'):
        print(f"\nScraped URLs:")
        for url in result['scraped_urls'][:5]:  # Show first 5
            print(f"  - {url}")
        if len(result['scraped_urls']) > 5:
            print(f"  ... and {len(result['scraped_urls']) - 5} more")
    
    print(f"\nContent Length: {len(result.get('site_content', ''))} chars")
    print(f"Emails Found: {len(result.get('scraped_emails', []))}")
    
    if result.get('enrichment_status') == 'completed':
        print(f"\nCompany Name: {result.get('name')}")
        print(f"Industry: {result.get('industry')}")
        print(f"Business Type: {result.get('business_type_description', '')[:100]}...")
        print(f"Confidence Score: {result.get('confidence_score', 0):.1%}")
    else:
        print(f"\nError: {result.get('enrichment_error')}")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()