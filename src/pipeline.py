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
        logger.info(f"Processing {len(domains)} domains with Celery")
        
        # Submit all domain pipelines to Celery
        results = []
        for domain in domains:
            result = process_domain_pipeline.delay(domain, company_csv, leads_csv)
            results.append((domain, result))
            logger.info(f"Submitted pipeline for {domain} (task_id: {result.id})")
        
        # Monitor results
        logger.info("All domains submitted. Monitor Celery workers for progress.")
        logger.info(f"Company CSV will be written to: {company_csv}")
        logger.info(f"Leads CSV will be written to: {leads_csv}")
    
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
    import_to_hubspot: bool = False
) -> None:
    """
    Main entry point for running the pipeline.
    
    Args:
        input_file: Path to file containing domains
        output_dir: Directory for CSV outputs
        use_celery: Whether to use Celery for distributed processing
        hubspot_token: Optional HubSpot API token for bulk import
        import_to_hubspot: Whether to import results to HubSpot after CSV export
    """
    pipeline = DomainPipeline(use_celery=use_celery, hubspot_token=hubspot_token)
    pipeline.process_domains_from_file(input_file, output_dir, use_celery, import_to_hubspot)


def run_single_domain_pipeline(
    domain: str,
    output_dir: str = "output",
    use_celery: bool = True,
    hubspot_token: Optional[str] = None,
    import_to_hubspot: bool = False
) -> None:
    """
    Main entry point for running the pipeline on a single domain.
    
    Args:
        domain: Single domain to process
        output_dir: Directory for CSV outputs
        use_celery: Whether to use Celery for distributed processing
        hubspot_token: Optional HubSpot API token for bulk import
        import_to_hubspot: Whether to import results to HubSpot after CSV export
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