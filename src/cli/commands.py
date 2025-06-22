"""CLI commands implementation."""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

from src.config.settings import Settings
from src.constants import DOMAIN_PROCESSING_DELAY_SECONDS
from src.services.enrichment_service import EnrichmentService
from src.services.multi_page_enrichment_service import MultiPageEnrichmentService
from src.services.hubspot_service import HubSpotService
from src.services.concurrent_enrichment_service import ConcurrentEnrichmentService
from src.utils.csv_exporter import CSVExporter
from src.utils.lead_csv_exporter import LeadCSVExporter


logger = logging.getLogger(__name__)


class EnrichmentCommand:
    """Main enrichment command handler."""
    
    def __init__(self, settings: Settings):
        """Initialize command with settings."""
        self.settings = settings
        self.hubspot = HubSpotService(settings.hubspot_token)
        
        num_workers = getattr(settings, 'num_workers', 4)
        scraping_depth = getattr(settings, 'scraping_depth', 2)
        
        # Use multi-page enrichment service if depth > 0
        if scraping_depth > 0:
            logger.info("Using multi-page enrichment with depth=%d", scraping_depth)
            self.enrichment = MultiPageEnrichmentService(self.hubspot, scraping_depth)
        else:
            logger.info("Using single-page enrichment")
            self.enrichment = EnrichmentService(self.hubspot)
            
        self.concurrent_service = ConcurrentEnrichmentService(
            num_workers=num_workers,
            scraping_depth=scraping_depth
        )
    
    
    def create_custom_properties(self) -> None:
        """Create custom properties in HubSpot."""
        logger.info("Creating contact properties...")
        self.hubspot.create_contact_properties()
        
        logger.info("Creating company properties...")
        self.hubspot.create_company_properties()
        
        logger.info("All custom properties created successfully")
    
    def process_leads(self, limit: Optional[int] = None) -> None:
        """Process and enrich leads."""
        logger.info(f"Starting lead enrichment (limit: {limit or 'all'})")
        
        # Collect all leads first
        leads = []
        for lead in self.hubspot.get_leads(limit=limit):
            leads.append(lead.to_dict())
        
        if not leads:
            logger.info("No leads found to process")
            return
        
        logger.info(f"Found {len(leads)} leads to process")
        
        # Define progress callback
        def progress_callback(current, total, result, error):
            if error:
                logger.error(f"Lead {current}/{total} failed: {error['error']}")
            else:
                logger.info(f"Lead {current}/{total} processed: {result.get('email', 'unknown')}")
        
        # Process leads concurrently
        results, errors = self.concurrent_service.enrich_leads(
            leads,
            progress_callback=progress_callback
        )
        
        # Update HubSpot with results
        for result in results:
            try:
                lead_id = result['id']
                self.hubspot.update_lead(lead_id, {
                    'site_content': result.get('site_content', ''),
                    'enrichment_status': result.get('enrichment_status', ''),
                    'buyer_persona': result.get('buyer_persona', ''),
                    'lead_score_adjustment': result.get('lead_score_adjustment', 0)
                })
            except Exception as e:
                logger.error(f"Failed to update lead {result.get('email')}: {e}")
        
        logger.info(f"Lead enrichment complete: {len(results)}/{len(leads)} enriched, {len(errors)} failed")
    
    def process_companies(self, limit: Optional[int] = None) -> None:
        """Process and enrich companies."""
        logger.info(f"Starting company enrichment (limit: {limit or 'all'})")
        
        # Collect all companies first
        companies = []
        for company in self.hubspot.get_companies(limit=limit):
            companies.append(company.to_dict())
        
        if not companies:
            logger.info("No companies found to process")
            return
        
        logger.info(f"Found {len(companies)} companies to process")
        
        # Define progress callback
        def progress_callback(current, total, result, error):
            if error:
                logger.error(f"Company {current}/{total} failed: {error['error']}")
            else:
                logger.info(f"Company {current}/{total} processed: {result.get('name', 'unknown')}")
        
        # Process companies concurrently
        results, errors = self.concurrent_service.enrich_companies(
            companies,
            progress_callback=progress_callback
        )
        
        # Update HubSpot with results
        for result in results:
            try:
                company_id = result['id']
                # Remove fields not needed for update
                update_data = {k: v for k, v in result.items() 
                              if k not in ['id', 'original_data', 'scraped_emails']}
                self.hubspot.update_company(company_id, update_data)
                
                # Create note if content was scraped
                if result.get('site_content'):
                    self.hubspot.create_note(
                        company_id,
                        'company',
                        f"Website content scraped:\n\n{result['site_content'][:1000]}..."
                    )
            except Exception as e:
                logger.error(f"Failed to update company {result.get('name')}: {e}")
        
        logger.info(f"Company enrichment complete: {len(results)}/{len(companies)} enriched, {len(errors)} failed")
    
    def process_single_lead_by_email(self, email: str) -> None:
        """Process a single lead by email."""
        lead = self.hubspot.get_lead_by_email(email)
        if not lead:
            logger.error(f"Lead with email {email} not found")
            return
        
        logger.info(f"Found lead: {lead.firstname} {lead.lastname} ({lead.id})")
        
        if self.enrichment.enrich_lead(lead):
            logger.info(f"Successfully enriched lead {email}")
        else:
            logger.error(f"Failed to enrich lead {email}")
    
    def process_single_lead_by_id(self, lead_id: str) -> None:
        """Process a single lead by ID."""
        lead = self.hubspot.get_lead_by_id(lead_id)
        if not lead:
            logger.error(f"Lead with ID {lead_id} not found")
            return
        
        logger.info(f"Found lead: {lead.email} ({lead.firstname} {lead.lastname})")
        
        if self.enrichment.enrich_lead(lead):
            logger.info(f"Successfully enriched lead {lead_id}")
        else:
            logger.error(f"Failed to enrich lead {lead_id}")
    
    def process_single_company_by_domain(self, domain: str) -> None:
        """Process a single company by domain."""
        company = self.hubspot.get_company_by_domain(domain)
        if not company:
            logger.error(f"Company with domain {domain} not found")
            return
        
        logger.info(f"Found company: {company.name} ({company.id})")
        
        if self.enrichment.enrich_company(company):
            logger.info(f"Successfully enriched company {domain}")
        else:
            logger.error(f"Failed to enrich company {domain}")
    
    def process_single_company_by_id(self, company_id: str) -> None:
        """Process a single company by ID."""
        company = self.hubspot.get_company_by_id(company_id)
        if not company:
            logger.error(f"Company with ID {company_id} not found")
            return
        
        logger.info(f"Found company: {company.name} (domain: {company.domain})")
        
        if self.enrichment.enrich_company(company):
            logger.info(f"Successfully enriched company {company_id}")
        else:
            logger.error(f"Failed to enrich company {company_id}")
    
    def process_file_domains(self, input_file: str, output_file: str, limit: int = None) -> None:
        """Process domains from a file and export to CSV."""
        from src.utils.file_processor import DomainFileProcessor
        from src.services.domain_enrichment_service import DomainEnrichmentService
        
        # Validate input file
        is_valid, error_msg = DomainFileProcessor.validate_input_file(input_file)
        if not is_valid:
            logger.error(error_msg)
            return
        
        # Read and validate domains
        domains, read_errors = DomainFileProcessor.read_domains_from_file(
            Path(input_file)
        )
        
        if not domains:
            logger.error("No valid domains found in input file")
            return
        
        logger.info(f"Found {len(domains)} valid domains to process")
        if read_errors:
            logger.warning(f"Encountered {len(read_errors)} errors while reading file")
        
        # Apply limit if specified
        if limit and limit > 0:
            original_count = len(domains)
            domains = domains[:limit]
            logger.info(f"Limiting processing to {limit} domains (out of {original_count})")
        
        # Process domains with incremental export
        self._process_domain_list_incremental(domains, output_file)
    
    def _process_domain_list(self, domains: List[str]) -> Dict[str, Dict[str, Any]]:
        """Process a list of domains and return results."""
        from src.services.domain_enrichment_service import DomainEnrichmentService
        
        enrichment_service = DomainEnrichmentService(
            self.enrichment.scraper,
            self.enrichment.analyzer
        )
        
        results = {}
        stats = {"total": 0, "enriched": 0, "failed": 0}
        
        for idx, domain in enumerate(domains):
            stats["total"] += 1
            is_last = idx == len(domains) - 1
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing domain {stats['total']}/{len(domains)}: {domain}")
            logger.info(f"{'='*60}")
            
            # Skip if already processed
            if domain in self.enrichment.processed_domains:
                logger.info(f"Domain {domain} already processed, skipping")
                continue
            
            try:
                result = enrichment_service.enrich_domain(domain)
                results[domain] = result
                
                if result["enrichment_status"] == "completed":
                    stats["enriched"] += 1
                    logger.info(f"✓ Successfully enriched {domain}")
                else:
                    stats["failed"] += 1
                    logger.warning(f"✗ Failed to enrich {domain}")
                
                self.enrichment.processed_domains.add(domain)
                
            except Exception as e:
                logger.error(f"Failed to process {domain}: {e}", exc_info=True)
                stats["failed"] += 1
                results[domain] = {
                    "name": domain,
                    "enrichment_status": "failed",
                    "enrichment_error": str(e)[:255]
                }
            
            # Add delay between requests
            if not is_last:
                logger.debug(f"Waiting {DOMAIN_PROCESSING_DELAY_SECONDS} seconds before next domain...")
            DomainEnrichmentService.add_processing_delay(is_last)
        
        self._log_processing_stats(stats)
        return results
    
    def _export_results(
        self, 
        results: Dict[str, Dict[str, Any]], 
        output_file: str
    ) -> None:
        """Export enrichment results to CSV."""
        exporter = CSVExporter(output_file)
        
        for domain, data in results.items():
            exporter.add_company(domain, data)
        
        try:
            exporter.write()
            self._log_export_success(output_file, len(results))
        except Exception as e:
            logger.error(f"Failed to write output file: {e}")
            raise
    
    def _process_domain_list_incremental(self, domains: List[str], output_file: str) -> None:
        """Process a list of domains and write results incrementally to CSV."""
        import threading
        
        # Initialize CSV exporters for incremental writing
        exporter = CSVExporter(output_file)
        
        # Load existing domains to check for resume
        existing_domains = exporter.load_existing_domains()
        resume_mode = len(existing_domains) > 0
        
        if resume_mode:
            logger.info(f"Resume mode: Found {len(existing_domains)} already processed domains")
        
        # Filter out already processed domains
        domains_to_process = [d for d in domains if not exporter.is_domain_processed(d)]
        
        if not domains_to_process:
            logger.info("All domains already processed")
            return
        
        logger.info(f"Will process {len(domains_to_process)} domains (skipping {len(domains) - len(domains_to_process)} already processed)")
        
        exporter.open_for_writing(append=resume_mode)
        
        # Create lead exporter with filename based on company output file
        lead_output_file = output_file.replace('.csv', '_leads.csv')
        if lead_output_file == output_file:  # In case there's no .csv extension
            lead_output_file = output_file + '_leads.csv'
        lead_exporter = LeadCSVExporter(lead_output_file)
        lead_exporter.open_for_writing(append=resume_mode)
        
        stats = {"total": 0, "enriched": 0, "failed": 0, "total_leads": 0}
        stats_lock = threading.Lock()
        
        # Define progress callback that writes to CSV
        def progress_callback(current, total, result, error):
            domain = None
            
            if error:
                domain = error.get('item', 'unknown')
                logger.error(f"Domain {current}/{total} failed: {domain} - {error['error']}")
                
                # Write error result to CSV
                try:
                    error_result = {
                        "name": domain,
                        "enrichment_status": "failed",
                        "enrichment_error": error['error'][:255]
                    }
                    exporter.write_company_incremental(domain, error_result)
                except Exception as csv_error:
                    logger.error(f"Failed to write error result for {domain} to CSV: {csv_error}")
                
                with stats_lock:
                    stats["failed"] += 1
            else:
                domain = result.get('name', 'unknown')
                logger.info(f"Domain {current}/{total} processed: {domain}")
                
                # Write company to CSV immediately
                try:
                    exporter.write_company_incremental(domain, result)
                except Exception as csv_error:
                    logger.error(f"Failed to write company result for {domain} to CSV: {csv_error}")
                
                # Write leads if emails were found
                if result.get("scraped_emails"):
                    try:
                        emails = result["scraped_emails"]
                        with stats_lock:
                            stats["total_leads"] += len(emails)
                        logger.info(f"Found {len(emails)} email(s) for {domain}")
                        
                        # Write each email as a lead
                        for email in emails:
                            try:
                                lead_data = {
                                    "company": result.get("name", domain),
                                    "company_domain": domain,
                                    "lead_source": "Website Scraping",
                                    "enrichment_date": result.get("enrichment_date", ""),
                                    "company_industry": result.get("industry", ""),
                                    "company_city": result.get("city", ""),
                                    "company_state": result.get("state_region", ""),
                                    "company_country": result.get("country", ""),
                                    "company_employees": result.get("number_of_employees", ""),
                                    "company_revenue": result.get("annual_revenue", "")
                                }
                                lead_exporter.write_lead_incremental(email, domain, lead_data)
                            except Exception as lead_error:
                                logger.error(f"Failed to write lead {email} for {domain}: {lead_error}")
                    except Exception as email_error:
                        logger.error(f"Failed to process emails for {domain}: {email_error}")
                
                if result["enrichment_status"] == "completed":
                    with stats_lock:
                        stats["enriched"] += 1
                    logger.info(f"✓ Successfully enriched {domain}")
                else:
                    with stats_lock:
                        stats["failed"] += 1
                    logger.warning(f"✗ Failed to enrich {domain}")
            
            with stats_lock:
                stats["total"] += 1
        
        try:
            # Process domains concurrently
            results, errors = self.concurrent_service.enrich_domains(
                domains_to_process,
                progress_callback=progress_callback
            )
        
        finally:
            # Always close the exporters
            exporter.close()
            lead_exporter.close()
        
        self._log_processing_stats(stats)
        self._log_export_success(output_file, stats["total"])
        
        # Log lead export info
        if stats["total_leads"] > 0:
            logger.info(
                f"\nLead export complete!\n"
                f"- Lead file: {lead_output_file}\n"
                f"- Total leads exported: {stats['total_leads']}\n\n"
                f"You can now import {lead_output_file} into HubSpot using the "
                f"Contacts import tool."
            )
    
    def _log_processing_stats(self, stats: Dict[str, int]) -> None:
        """Log processing statistics."""
        message = (
            f"\nProcessing statistics:\n"
            f"- Total domains: {stats['total']}\n"
            f"- Successfully enriched: {stats['enriched']}\n"
            f"- Failed: {stats['failed']}"
        )
        if 'total_leads' in stats:
            message += f"\n- Total leads found: {stats['total_leads']}"
        logger.info(message)
    
    def _log_export_success(self, output_file: str, count: int) -> None:
        """Log successful export message."""
        logger.info(
            f"\nFile processing complete!\n"
            f"- Output file: {output_file}\n"
            f"- Records exported: {count}\n\n"
            f"You can now import {output_file} into HubSpot using the "
            f"Companies import tool."
        )