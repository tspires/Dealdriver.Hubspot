#!/usr/bin/env python3
"""Generate a summary of enrichment results."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.hubspot_service import HubSpotService
from src.config.settings import Settings
from collections import defaultdict

def main():
    token = os.environ.get('HUBSPOT_ACCESS_TOKEN')
    if not token:
        print("HUBSPOT_ACCESS_TOKEN not set")
        return 1
    
    settings = Settings(hubspot_token=token)
    hubspot = HubSpotService(settings.hubspot_token)
    
    stats = defaultdict(int)
    enriched_companies = []
    
    print("\nScanning companies for enrichment data...\n")
    
    for company in hubspot.get_companies():
        stats['total'] += 1
        props = company.__dict__
        
        # Check if enriched
        if props.get('enrichment_status') == 'completed':
            stats['enriched'] += 1
            enriched_companies.append(company)
            
            # Count what data we have
            if props.get('site_content'):
                stats['has_content'] += 1
            if props.get('description'):
                stats['has_description'] += 1
            if props.get('city') or props.get('state') or props.get('country'):
                stats['has_location'] += 1
            if props.get('numberofemployees'):
                stats['has_employees'] += 1
            if props.get('annualrevenue'):
                stats['has_revenue'] += 1
            if props.get('industry'):
                stats['has_industry'] += 1
        elif not props.get('domain') and not props.get('website'):
            stats['no_domain'] += 1
    
    # Print summary
    print("="*60)
    print("HUBSPOT ENRICHMENT STATUS SUMMARY")
    print("="*60)
    print(f"Total companies: {stats['total']}")
    print(f"Enriched: {stats['enriched']} ({stats['enriched']/stats['total']*100:.1f}%)")
    print(f"No domain/website: {stats['no_domain']}")
    print(f"Remaining to enrich: {stats['total'] - stats['enriched'] - stats['no_domain']}")
    print()
    print("Enriched companies have:")
    print(f"  - Descriptions: {stats['has_description']}")
    print(f"  - Location data: {stats['has_location']}")
    print(f"  - Employee counts: {stats['has_employees']}")
    print(f"  - Revenue data: {stats['has_revenue']}")
    print(f"  - Industry classification: {stats['has_industry']}")
    print(f"  - Scraped content: {stats['has_content']}")
    
    # Show some enriched examples
    if enriched_companies:
        print("\nRecently enriched companies:")
        for company in enriched_companies[-5:]:
            print(f"\n✓ {company.name}")
            if company.description:
                print(f"  {company.description[:100]}...")
            if company.city or company.state:
                print(f"  Location: {company.city}, {company.state}")
            if company.industry:
                print(f"  Industry: {company.industry}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())