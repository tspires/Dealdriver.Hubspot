"""Main pipeline orchestrator using Luigi and Celery."""

import luigi
import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from src.tasks.celery_tasks import process_domain_pipeline
from src.utils.file_processor import DomainFileProcessor

logger = logging.getLogger(__name__)


class DomainPipeline:
    """Orchestrates the domain enrichment pipeline."""
    
    def __init__(self, use_celery: bool = True, hubspot_token: Optional[str] = None):
        """
        Initialize pipeline.
        
        Args:
            use_celery: Whether to use Celery for distributed processing
            hubspot_token: Optional HubSpot API token for bulk import
        """
        self.use_celery = use_celery
        self.file_processor = DomainFileProcessor()
        self.hubspot_token = hubspot_token
    
    def process_domains_from_file(
        self,
        input_file: str,
        output_dir: str = "output",
        use_celery: bool = True,
        import_to_hubspot: bool = False
    ) -> None:
        """
        Process domains from a file using the pipeline.
        
        Args:
            input_file: Path to file containing domains
            output_dir: Directory for CSV outputs
            use_celery: Whether to use Celery for distributed processing
            import_to_hubspot: Whether to import results to HubSpot after CSV export
        """
        logger.info(f"Processing domains from {input_file}")
        
        # Read domains from file
        domains, errors = self.file_processor.read_domains_from_file(input_file)
        if errors:
            logger.warning(f"Found {len(errors)} errors while reading domains: {errors}")
        logger.info(f"Found {len(domains)} unique domains to process")
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate output file names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        company_csv = output_path / f"companies_{timestamp}.csv"
        leads_csv = output_path / f"leads_{timestamp}.csv"
        
        if use_celery and self.use_celery:
            self._process_with_celery(domains, str(company_csv), str(leads_csv))
        else:
            self._process_with_luigi(domains, str(company_csv), str(leads_csv))
        
        # Import to HubSpot if requested
        if import_to_hubspot and self.hubspot_token:
            self._import_to_hubspot(str(company_csv), str(leads_csv))
    
    def _process_with_celery(
        self,
        domains: List[str],
        company_csv: str,
        leads_csv: str
    ) -> None:
        """Process domains using Celery for distributed execution."""
        from src.tasks.celery_tasks import scrape_domain, enrich_company, enrich_leads
        import time
        
        logger.info(f"Processing {len(domains)} domains with Celery")
        
        # Create CSV files with headers
        self._initialize_csv_files(company_csv, leads_csv)
        
        # Phase 1: Scrape all domains sequentially for simplicity
        logger.info("Phase 1: Scraping all domains...")
        scrape_results = []
        for domain in domains:
            logger.info(f"Scraping {domain}...")
            result = scrape_domain.delay(domain)
            try:
                scrape_result = result.get(timeout=300)  # 5 minute timeout per domain
                scrape_results.append(scrape_result)
                logger.info(f"Scraping completed for {domain}: {scrape_result}")
            except Exception as e:
                logger.error(f"Scraping failed for {domain}: {str(e)}")
                scrape_results.append({"domain": domain, "success": False, "error": str(e)})
        
        logger.info(f"Scraping phase completed. {len(scrape_results)} domains processed.")
        
        # Phase 2: Enrich companies and leads sequentially
        logger.info("Phase 2: Enriching companies and leads...")
        for domain in domains:
            # Enrich company
            logger.info(f"Enriching company data for {domain}...")
            try:
                company_result = enrich_company.delay(domain)
                company_result.get(timeout=600)  # 10 minute timeout
                logger.info(f"Company enrichment completed for {domain}")
            except Exception as e:
                logger.error(f"Company enrichment failed for {domain}: {str(e)}")
            
            # Enrich leads
            logger.info(f"Enriching leads data for {domain}...")
            try:
                leads_result = enrich_leads.delay(domain)
                leads_result.get(timeout=600)  # 10 minute timeout
                logger.info(f"Leads enrichment completed for {domain}")
            except Exception as e:
                logger.error(f"Leads enrichment failed for {domain}: {str(e)}")
        
        logger.info("Enrichment phase completed.")
        
        # Phase 3: Export to CSV
        logger.info("Phase 3: Exporting to CSV...")
        self._export_results_to_csv(domains, company_csv, leads_csv)
        
        logger.info(f"Processing complete. Results written to:")
        logger.info(f"  Companies: {company_csv}")
        logger.info(f"  Leads: {leads_csv}")
    
    def _process_with_luigi(
        self,
        domains: List[str],
        company_csv: str,
        leads_csv: str
    ) -> None:
        """Process domains using Luigi local scheduler."""
        from src.tasks.export import ExportAllCSVTask
        
        logger.info(f"Processing {len(domains)} domains with Luigi local scheduler")
        
        # Create Luigi tasks for all domains
        tasks = []
        for domain in domains:
            task = ExportAllCSVTask(
                domain=domain,
                company_csv=company_csv,
                leads_csv=leads_csv
            )
            tasks.append(task)
        
        # Run all tasks
        luigi.build(tasks, local_scheduler=True, log_level='INFO')
        
        logger.info(f"Processing complete. Results written to:")
        logger.info(f"  Companies: {company_csv}")
        logger.info(f"  Leads: {leads_csv}")
        
        # Log performance summary
        from src.utils.performance_monitor import get_performance_monitor
        monitor = get_performance_monitor()
        monitor.log_summary()
    
    def _import_to_hubspot(self, company_csv: str, leads_csv: str) -> None:
        """Import CSV files to HubSpot using bulk import."""
        from src.tasks.hubspot_import import ImportAllTask
        
        logger.info("Starting HubSpot bulk import...")
        
        # Create import task
        import_task = ImportAllTask(
            company_csv=company_csv,
            leads_csv=leads_csv,
            hubspot_token=self.hubspot_token
        )
        
        # Run import task
        luigi.build([import_task], local_scheduler=True, log_level='INFO')
        
        logger.info("HubSpot import completed")
    
    def _initialize_csv_files(self, company_csv: str, leads_csv: str) -> None:
        """Initialize CSV files with headers."""
        import csv
        from pathlib import Path
        
        # Company CSV headers
        company_headers = [
            "domain", "enriched_at", "success", "error", "scraped_url",
            "emails_found", "business_type", "naics_code", "target_market",
            "products_services", "value_propositions", "competitive_advantages",
            "technologies", "certifications", "pain_points", "confidence_score"
        ]
        
        # Lead CSV headers
        lead_headers = [
            "email", "first_name", "last_name", "company_domain",
            "enriched_at", "error", "buyer_persona", "lead_score_adjustment"
        ]
        
        # Create company CSV
        Path(company_csv).parent.mkdir(parents=True, exist_ok=True)
        with open(company_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(company_headers)
        
        # Create leads CSV
        Path(leads_csv).parent.mkdir(parents=True, exist_ok=True)
        with open(leads_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(lead_headers)
    
    def _export_results_to_csv(self, domains: List[str], company_csv: str, leads_csv: str) -> None:
        """Export enriched results to CSV files."""
        import json
        import csv
        from pathlib import Path
        
        companies_written = 0
        leads_written = 0
        
        for domain in domains:
            try:
                # Export company data
                company_file = Path(f"data/enriched_companies/raw/{domain}.json")
                if company_file.exists():
                    with open(company_file, 'r') as f:
                        company_data = json.load(f)
                    
                    row = self._prepare_company_row(company_data)
                    with open(company_csv, 'a', newline='', encoding='utf-8') as f:
                        fieldnames = [
                            "domain", "enriched_at", "success", "error", "scraped_url",
                            "emails_found", "business_type", "naics_code", "target_market",
                            "products_services", "value_propositions", "competitive_advantages",
                            "technologies", "certifications", "pain_points", "confidence_score"
                        ]
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writerow(row)
                    companies_written += 1
                
                # Export leads data
                leads_file = Path(f"data/enriched_leads/raw/{domain}.json")
                if leads_file.exists():
                    with open(leads_file, 'r') as f:
                        leads_data = json.load(f)
                    
                    leads = leads_data.get("leads", [])
                    if leads:
                        rows = [self._prepare_lead_row(lead, leads_data) for lead in leads]
                        with open(leads_csv, 'a', newline='', encoding='utf-8') as f:
                            fieldnames = [
                                "email", "first_name", "last_name", "company_domain",
                                "enriched_at", "error", "buyer_persona", "lead_score_adjustment"
                            ]
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            writer.writerows(rows)
                        leads_written += len(rows)
                        
            except Exception as e:
                logger.warning(f"Failed to export data for {domain}: {str(e)}")
        
        logger.info(f"Exported {companies_written} companies and {leads_written} leads to CSV")
    
    def _prepare_company_row(self, data: dict) -> dict:
        """Prepare a company row for CSV export."""
        row = {
            "domain": data.get("domain", ""),
            "enriched_at": data.get("enriched_at", ""),
            "success": data.get("success", False),
            "error": data.get("error", ""),
            "scraped_url": data.get("scraped_url", ""),
            "emails_found": ";".join(data.get("emails_found", []))
        }
        
        # Add analysis fields if available
        if data.get("analysis"):
            analysis = data["analysis"]
            row.update({
                "business_type": analysis.get("business_type_description", ""),
                "naics_code": analysis.get("naics_code", ""),
                "target_market": analysis.get("target_market", ""),
                "products_services": ";".join(analysis.get("primary_products_services", [])),
                "value_propositions": ";".join(analysis.get("value_propositions", [])),
                "competitive_advantages": ";".join(analysis.get("competitive_advantages", [])),
                "technologies": ";".join(analysis.get("technologies_used", [])),
                "certifications": ";".join(analysis.get("certifications_awards", [])),
                "pain_points": ";".join(analysis.get("pain_points_addressed", [])),
                "confidence_score": analysis.get("confidence_score", 0)
            })
        else:
            # Add empty fields
            row.update({
                "business_type": "",
                "naics_code": "",
                "target_market": "",
                "products_services": "",
                "value_propositions": "",
                "competitive_advantages": "",
                "technologies": "",
                "certifications": "",
                "pain_points": "",
                "confidence_score": 0
            })
        
        return row
    
    def _prepare_lead_row(self, lead: dict, enriched_data: dict) -> dict:
        """Prepare a lead row for CSV export."""
        row = {
            "email": lead.get("email", ""),
            "first_name": lead.get("first_name", ""),
            "last_name": lead.get("last_name", ""),
            "company_domain": enriched_data.get("domain", ""),
            "enriched_at": enriched_data.get("enriched_at", ""),
            "error": lead.get("error", "")
        }
        
        # Add analysis fields if available
        if lead.get("analysis"):
            analysis = lead["analysis"]
            row.update({
                "buyer_persona": analysis.get("buyer_persona", ""),
                "lead_score_adjustment": analysis.get("lead_score_adjustment", 0)
            })
        else:
            row.update({
                "buyer_persona": "",
                "lead_score_adjustment": 0
            })
        
        return row
        
    def process_single_domain(
        self,
        domain: str,
        output_dir: str = "output"
    ) -> None:
        """
        Process a single domain.
        
        Args:
            domain: Domain to process
            output_dir: Directory for CSV outputs
        """
        self.process_domains_from_file(
            input_file=None,
            output_dir=output_dir,
            use_celery=self.use_celery
        )


def run_pipeline(
    input_file: str,
    output_dir: str = "output",
    use_celery: bool = True,
    hubspot_token: Optional[str] = None,
    import_to_hubspot: bool = True
) -> None:
    """
    Main entry point for running the pipeline.
    
    Args:
        input_file: Path to file containing domains
        output_dir: Directory for CSV outputs
        use_celery: Whether to use Celery for distributed processing
        hubspot_token: Optional HubSpot API token for bulk import
        import_to_hubspot: Whether to import results to HubSpot after CSV export (default: True)
    """
    pipeline = DomainPipeline(use_celery=use_celery, hubspot_token=hubspot_token)
    pipeline.process_domains_from_file(input_file, output_dir, use_celery, import_to_hubspot)


def run_single_domain_pipeline(
    domain: str,
    output_dir: str = "output",
    use_celery: bool = True,
    hubspot_token: Optional[str] = None,
    import_to_hubspot: bool = True
) -> None:
    """
    Main entry point for running the pipeline on a single domain.
    
    Args:
        domain: Single domain to process
        output_dir: Directory for CSV outputs
        use_celery: Whether to use Celery for distributed processing
        hubspot_token: Optional HubSpot API token for bulk import
        import_to_hubspot: Whether to import results to HubSpot after CSV export (default: True)
    """
    logger.info(f"Processing single domain: {domain}")
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate output file names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    company_csv = output_path / f"companies_{timestamp}.csv"
    leads_csv = output_path / f"leads_{timestamp}.csv"
    
    pipeline = DomainPipeline(use_celery=use_celery, hubspot_token=hubspot_token)
    
    if use_celery and pipeline.use_celery:
        pipeline._process_with_celery([domain], str(company_csv), str(leads_csv))
    else:
        pipeline._process_with_luigi([domain], str(company_csv), str(leads_csv))
    
    # Import to HubSpot if requested
    if import_to_hubspot and hubspot_token:
        pipeline._import_to_hubspot(str(company_csv), str(leads_csv))