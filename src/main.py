#!/usr/bin/env python3
"""
HubSpot Enrichment Application

Main entry point for enriching HubSpot leads and companies with web-scraped data.
"""

import argparse
import logging
import os
import sys
from typing import Optional

from src.cli.commands import EnrichmentCommand
from src.config.settings import Settings
from src.utils.logging import setup_logging


def main() -> int:
    """Main application entry point."""
    # Parse arguments first to get log level
    parser = argparse.ArgumentParser(
        description="Enrich HubSpot records with scraped website data"
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("HUBSPOT_ACCESS_TOKEN"),
        help="HubSpot API token (defaults to HUBSPOT_ACCESS_TOKEN env var)",
        type=str
    )
    parser.add_argument(
        "--create-properties",
        action="store_true",
        help="Create custom properties in HubSpot"
    )
    parser.add_argument(
        "--leads",
        action="store_true",
        help="Process leads only"
    )
    parser.add_argument(
        "--companies",
        action="store_true",
        help="Process companies only"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of records to process"
    )
    parser.add_argument(
        "--lead-email",
        type=str,
        help="Process a single lead by email"
    )
    parser.add_argument(
        "--lead-id",
        type=str,
        help="Process a single lead by HubSpot ID"
    )
    parser.add_argument(
        "--company-domain",
        type=str,
        help="Process a single company by domain"
    )
    parser.add_argument(
        "--company-id",
        type=str,
        help="Process a single company by HubSpot ID"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level"
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Process domains from a file (one domain per line) and output CSV"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory for CSV files (default: output)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent workers for processing (default: 4)"
    )
    parser.add_argument(
        "--no-celery",
        action="store_true",
        help="Use Luigi local scheduler instead of Celery for processing"
    )
    parser.add_argument(
        "--import-to-hubspot",
        action="store_true",
        help="Import CSV results to HubSpot after processing (requires --file)"
    )
    parser.add_argument(
        "--scraping-depth",
        type=int,
        default=2,
        help="Maximum depth for web crawling (default: 2, single page: 0)"
    )
    
    args = parser.parse_args()
    
    # Setup logging with INFO as default
    setup_logging(level=args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("Starting HubSpot Enrichment Application")
    logger.debug("Command line arguments: %s", vars(args))
    
    if not args.token:
        logger.error("No HubSpot token provided. Use --token or set HUBSPOT_ACCESS_TOKEN environment variable.")
        return 1
    
    try:
        logger.debug("Creating application settings")
        settings = Settings(
            hubspot_token=args.token,
            log_level=args.log_level,
            scraping_depth=args.scraping_depth
        )
        settings.num_workers = args.workers
        logger.info("Settings initialized - Workers: %d, Log Level: %s", 
                   settings.num_workers, settings.log_level)
        
        logger.debug("Initializing EnrichmentCommand")
        command = EnrichmentCommand(settings)
        logger.debug("EnrichmentCommand initialized successfully")
        
        if args.create_properties:
            logger.info("Creating custom properties in HubSpot...")
            command.create_custom_properties()
            logger.info("Custom properties created successfully")
            return 0
        
        # Process single records if specified
        if args.lead_email:
            logger.info("Processing single lead by email: %s", args.lead_email)
            command.process_single_lead_by_email(args.lead_email)
            logger.info("Single lead processing completed")
            return 0
        
        if args.lead_id:
            logger.info("Processing single lead by ID: %s", args.lead_id)
            command.process_single_lead_by_id(args.lead_id)
            logger.info("Single lead processing completed")
            return 0
        
        if args.company_domain:
            logger.info("Processing single company by domain: %s", args.company_domain)
            command.process_single_company_by_domain(args.company_domain)
            logger.info("Single company processing completed")
            return 0
        
        if args.company_id:
            logger.info("Processing single company by ID: %s", args.company_id)
            command.process_single_company_by_id(args.company_id)
            logger.info("Single company processing completed")
            return 0
        
        # Process domains from file
        if args.file:
            logger.info("Processing domains from file: %s", args.file)
            logger.debug("Output directory: %s", args.output or "output")
            logger.debug("Using Celery: %s", not args.no_celery)
            logger.debug("Import to HubSpot: %s", args.import_to_hubspot)
            
            from src.pipeline import run_pipeline
            
            # Use Celery by default, unless --no-celery flag is set
            use_celery = not getattr(args, 'no_celery', False)
            output_dir = args.output or "output"
            
            # Check if import to HubSpot is requested
            import_to_hubspot = args.import_to_hubspot
            if import_to_hubspot and not args.token:
                logger.error("HubSpot import requested but no token provided")
                return 1
            
            logger.info("Starting pipeline processing")
            run_pipeline(
                args.file, 
                output_dir, 
                use_celery=use_celery,
                hubspot_token=args.token if import_to_hubspot else None,
                import_to_hubspot=import_to_hubspot
            )
            logger.info("Pipeline processing completed successfully")
            return 0
        
        # Process bulk records
        process_leads = args.leads or (not args.leads and not args.companies)
        process_companies = args.companies or (not args.leads and not args.companies)
        
        if process_leads:
            logger.info("Processing leads...")
            logger.debug("Lead processing limit: %s", args.limit or "unlimited")
            command.process_leads(limit=args.limit)
            logger.info("Lead processing completed")
        
        if process_companies:
            logger.info("Processing companies...")
            logger.debug("Company processing limit: %s", args.limit or "unlimited")
            command.process_companies(limit=args.limit)
            logger.info("Company processing completed")
        
        logger.info("Enrichment completed successfully")
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Application interrupted by user")
        logger.debug("Shutting down due to KeyboardInterrupt")
        return 130
    except Exception as e:
        logger.error("Application failed: %s", e)
        logger.debug("Application error details", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())