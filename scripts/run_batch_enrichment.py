#!/usr/bin/env python3
"""Run batch enrichment with better error handling and summary."""

import sys
import os
import time
import logging
from datetime import datetime

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config.settings import Settings
from src.services.enrichment_service import EnrichmentService
from src.services.hubspot_service import HubSpotService

# Setup logging using common logger
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from src.utils.logging import setup_logging

setup_logging(level="INFO")
logger = logging.getLogger(__name__)

def main():
    # Get token from environment
    token = os.environ.get('HUBSPOT_ACCESS_TOKEN')
    if not token:
        logger.error("HUBSPOT_ACCESS_TOKEN not set")
        return 1
    
    # Initialize services
    settings = Settings(hubspot_token=token)
    hubspot = HubSpotService(settings.hubspot_token)
    enrichment = EnrichmentService(hubspot)
    
    # Track results
    results = {
        'total': 0,
        'enriched': 0,
        'failed': 0,
        'no_domain': 0,
        'timeout': 0,
        'errors': []
    }
    
    start_time = datetime.now()
    logger.info("Starting batch enrichment...")
    
    try:
        for company in hubspot.get_companies():
            results['total'] += 1
            
            # Skip if no domain
            if not company.domain and not company.website:
                logger.warning(f"Company {company.name} has no domain/website")
                results['no_domain'] += 1
                continue
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing company {results['total']}: {company.name}")
            logger.info(f"Domain: {company.domain or 'from website'}")
            
            try:
                if enrichment.enrich_company(company):
                    results['enriched'] += 1
                    logger.info(f"✓ Successfully enriched {company.name}")
                else:
                    results['failed'] += 1
                    logger.error(f"✗ Failed to enrich {company.name}")
            except Exception as e:
                results['failed'] += 1
                error_msg = str(e)
                if 'timeout' in error_msg.lower():
                    results['timeout'] += 1
                results['errors'].append({
                    'company': company.name,
                    'error': error_msg[:200]
                })
                logger.error(f"✗ Error enriching {company.name}: {error_msg[:200]}")
            
            # Progress update every 5 companies
            if results['total'] % 5 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = results['total'] / elapsed * 60  # per minute
                logger.info(f"\nProgress: {results['enriched']}/{results['total']} enriched "
                          f"({results['enriched']/results['total']*100:.1f}%) "
                          f"Rate: {rate:.1f} companies/min")
            
            # Delay to avoid rate limiting
            time.sleep(2)
            
    except KeyboardInterrupt:
        logger.warning("\nEnrichment interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    
    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "="*70)
    print("ENRICHMENT SUMMARY")
    print("="*70)
    print(f"Total companies processed: {results['total']}")
    print(f"Successfully enriched: {results['enriched']} ({results['enriched']/max(results['total'],1)*100:.1f}%)")
    print(f"Failed: {results['failed']}")
    print(f"No domain/website: {results['no_domain']}")
    print(f"Timeout errors: {results['timeout']}")
    print(f"Total time: {elapsed/60:.1f} minutes")
    print(f"Average time per company: {elapsed/max(results['total'],1):.1f} seconds")
    
    if results['errors']:
        print(f"\nTop errors:")
        for err in results['errors'][:5]:
            print(f"- {err['company']}: {err['error'][:100]}...")
    
    print("\n✓ Enrichment batch complete!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())