#!/usr/bin/env python3
"""
Script to dump all contacts and companies from HubSpot to CSV files.

Usage:
    python dump_hubspot_data.py --token YOUR_HUBSPOT_TOKEN
    python dump_hubspot_data.py --token YOUR_HUBSPOT_TOKEN --output-dir /path/to/output
    python dump_hubspot_data.py --token YOUR_HUBSPOT_TOKEN --contacts-only
    python dump_hubspot_data.py --token YOUR_HUBSPOT_TOKEN --companies-only
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.hubspot_service import HubSpotService
from src.utils.logging import setup_logging


def sanitize_value(value: Any) -> str:
    """Convert value to string and handle special cases."""
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        # Convert complex types to JSON string for better readability
        try:
            return json.dumps(value)
        except (TypeError, ValueError):
            return str(value)
    # Handle boolean values
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def dump_contacts(hubspot_service: HubSpotService, output_file: Path, batch_size: int = 100) -> int:
    """Dump all contacts to CSV file."""
    logger = logging.getLogger(__name__)
    logger.info(f"Dumping contacts to {output_file}")
    
    # Process in batches to handle large datasets
    all_keys = set()
    temp_file = output_file.with_suffix('.tmp')
    total_contacts = 0
    
    try:
        # Collect all contacts in memory to avoid duplicate API calls
        logger.info("Fetching all contacts from HubSpot...")
        all_contacts = []
        
        for contact in hubspot_service.get_leads():
            contact_dict = contact.to_dict()
            all_contacts.append(contact_dict)
            all_keys.update(contact_dict.keys())
            
            if len(all_contacts) % batch_size == 0:
                logger.info(f"Fetched {len(all_contacts)} contacts...")
        
        total_contacts = len(all_contacts)
        logger.info(f"Total contacts fetched: {total_contacts}")
        
        if total_contacts == 0:
            logger.warning("No contacts found in HubSpot")
            output_file.write_text("")  # Create empty file
            return 0
        
        # Sort fieldnames for consistent output
        fieldnames = sorted(list(all_keys))
        
        # Write to temporary file first
        with open(temp_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            
            for i, contact_dict in enumerate(all_contacts):
                # Ensure all fields are present and sanitized
                row = {field: sanitize_value(contact_dict.get(field)) for field in fieldnames}
                writer.writerow(row)
                
                if (i + 1) % batch_size == 0:
                    logger.info(f"Written {i + 1}/{total_contacts} contacts...")
        
        # Move temp file to final location
        temp_file.replace(output_file)
        
    except Exception as e:
        logger.error(f"Error during contact dump: {e}")
        if temp_file.exists():
            temp_file.unlink()
        raise
    
    logger.info(f"Successfully dumped {total_contacts} contacts")
    return total_contacts


def dump_companies(hubspot_service: HubSpotService, output_file: Path, batch_size: int = 100) -> int:
    """Dump all companies to CSV file."""
    logger = logging.getLogger(__name__)
    logger.info(f"Dumping companies to {output_file}")
    
    # Process in batches to handle large datasets
    all_keys = set()
    temp_file = output_file.with_suffix('.tmp')
    total_companies = 0
    
    try:
        # Collect all companies in memory to avoid duplicate API calls
        logger.info("Fetching all companies from HubSpot...")
        all_companies = []
        
        for company in hubspot_service.get_companies():
            company_dict = company.to_dict()
            all_companies.append(company_dict)
            all_keys.update(company_dict.keys())
            
            if len(all_companies) % batch_size == 0:
                logger.info(f"Fetched {len(all_companies)} companies...")
        
        total_companies = len(all_companies)
        logger.info(f"Total companies fetched: {total_companies}")
        
        if total_companies == 0:
            logger.warning("No companies found in HubSpot")
            output_file.write_text("")  # Create empty file
            return 0
        
        # Sort fieldnames for consistent output
        fieldnames = sorted(list(all_keys))
        
        # Write to temporary file first
        with open(temp_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            
            for i, company_dict in enumerate(all_companies):
                # Ensure all fields are present and sanitized
                row = {field: sanitize_value(company_dict.get(field)) for field in fieldnames}
                writer.writerow(row)
                
                if (i + 1) % batch_size == 0:
                    logger.info(f"Written {i + 1}/{total_companies} companies...")
        
        # Move temp file to final location
        temp_file.replace(output_file)
        
    except Exception as e:
        logger.error(f"Error during company dump: {e}")
        if temp_file.exists():
            temp_file.unlink()
        raise
    
    logger.info(f"Successfully dumped {total_companies} companies")
    return total_companies


def main():
    parser = argparse.ArgumentParser(
        description="Dump HubSpot contacts and companies to CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--token", required=True, help="HubSpot API token")
    parser.add_argument("--output-dir", default="output", help="Output directory (default: output)")
    parser.add_argument("--contacts-only", action="store_true", help="Only dump contacts")
    parser.add_argument("--companies-only", action="store_true", help="Only dump companies")
    parser.add_argument("--batch-size", type=int, default=100, help="Progress logging batch size (default: 100)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Initialize HubSpot service
    try:
        hubspot_service = HubSpotService(args.token)
    except Exception as e:
        logger.error(f"Failed to initialize HubSpot service: {e}")
        return 1
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    total_contacts = 0
    total_companies = 0
    
    # Dump contacts
    if not args.companies_only:
        contacts_file = output_dir / f"hubspot_contacts_{timestamp}.csv"
        try:
            total_contacts = dump_contacts(hubspot_service, contacts_file, batch_size=args.batch_size)
            logger.info(f"Contacts saved to: {contacts_file}")
        except Exception as e:
            logger.error(f"Failed to dump contacts: {e}", exc_info=True)
            return 1
    
    # Dump companies
    if not args.contacts_only:
        companies_file = output_dir / f"hubspot_companies_{timestamp}.csv"
        try:
            total_companies = dump_companies(hubspot_service, companies_file, batch_size=args.batch_size)
            logger.info(f"Companies saved to: {companies_file}")
        except Exception as e:
            logger.error(f"Failed to dump companies: {e}", exc_info=True)
            return 1
    
    # Summary
    logger.info("=" * 50)
    logger.info("DUMP COMPLETE")
    logger.info("=" * 50)
    if not args.companies_only:
        logger.info(f"Total contacts dumped: {total_contacts}")
    if not args.contacts_only:
        logger.info(f"Total companies dumped: {total_companies}")
    logger.info(f"Output directory: {output_dir.absolute()}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())