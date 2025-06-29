"""Celery tasks for distributed processing."""

import luigi
import logging
from celery import Task
from typing import Dict, Any

from celery_app import app
from src.tasks.scrape import ScrapeWebsiteTask
from src.tasks.enrich import EnrichCompanyTask, EnrichLeadsTask
from src.tasks.export import ExportCompanyCSVTask, ExportLeadsCSVTask

logger = logging.getLogger(__name__)


class LuigiCeleryTask(Task):
    """Base Celery task that runs Luigi tasks."""
    
    def run_luigi_task(self, task):
        """Run a Luigi task and return success status."""
        try:
            # Run the task
            result = luigi.build([task], local_scheduler=True, log_level='WARNING')
            return result
        except Exception as e:
            logger.error(f"Failed to run Luigi task: {str(e)}")
            raise


@app.task(base=LuigiCeleryTask, bind=True)
def scrape_domain(self, domain: str) -> Dict[str, Any]:
    """Celery task to scrape a domain."""
    logger.info(f"Starting scrape task for domain: {domain}")
    
    try:
        task = ScrapeWebsiteTask(domain=domain)
        success = self.run_luigi_task(task)
        
        return {
            "domain": domain,
            "task": "scrape",
            "success": success,
            "output_path": task.output().path if success else None
        }
    except Exception as e:
        logger.error(f"Scrape task failed for {domain}: {str(e)}")
        raise


@app.task(base=LuigiCeleryTask, bind=True)
def enrich_company(self, domain: str) -> Dict[str, Any]:
    """Celery task to enrich company data."""
    logger.info(f"Starting company enrichment task for domain: {domain}")
    
    try:
        task = EnrichCompanyTask(domain=domain)
        success = self.run_luigi_task(task)
        
        return {
            "domain": domain,
            "task": "enrich_company",
            "success": success,
            "output_path": task.output().path if success else None
        }
    except Exception as e:
        logger.error(f"Company enrichment task failed for {domain}: {str(e)}")
        raise


@app.task(base=LuigiCeleryTask, bind=True)
def enrich_leads(self, domain: str) -> Dict[str, Any]:
    """Celery task to enrich lead data."""
    logger.info(f"Starting lead enrichment task for domain: {domain}")
    
    try:
        task = EnrichLeadsTask(domain=domain)
        success = self.run_luigi_task(task)
        
        return {
            "domain": domain,
            "task": "enrich_leads",
            "success": success,
            "output_path": task.output().path if success else None
        }
    except Exception as e:
        logger.error(f"Lead enrichment task failed for {domain}: {str(e)}")
        raise


@app.task(base=LuigiCeleryTask, bind=True)
def export_company_csv(self, domain: str, output_file: str) -> Dict[str, Any]:
    """Celery task to export company data to CSV."""
    logger.info(f"Starting company CSV export for domain: {domain}")
    
    try:
        task = ExportCompanyCSVTask(domain=domain, output_file=output_file)
        success = self.run_luigi_task(task)
        
        return {
            "domain": domain,
            "task": "export_company_csv",
            "success": success,
            "output_path": output_file if success else None
        }
    except Exception as e:
        logger.error(f"Company CSV export failed for {domain}: {str(e)}")
        raise


@app.task(base=LuigiCeleryTask, bind=True)
def export_leads_csv(self, domain: str, output_file: str) -> Dict[str, Any]:
    """Celery task to export lead data to CSV."""
    logger.info(f"Starting leads CSV export for domain: {domain}")
    
    try:
        task = ExportLeadsCSVTask(domain=domain, output_file=output_file)
        success = self.run_luigi_task(task)
        
        return {
            "domain": domain,
            "task": "export_leads_csv",
            "success": success,
            "output_path": output_file if success else None
        }
    except Exception as e:
        logger.error(f"Leads CSV export failed for {domain}: {str(e)}")
        raise


@app.task
def process_domain_pipeline(domain: str, company_csv: str, leads_csv: str) -> Dict[str, Any]:
    """Process a complete domain pipeline with all steps."""
    logger.info(f"Starting complete pipeline for domain: {domain}")
    
    try:
        # Step 1: Scrape the domain
        scrape_result = scrape_domain.apply(args=[domain])
        if not scrape_result.successful():
            return {
                "domain": domain,
                "status": "failed",
                "step": "scrape",
                "error": str(scrape_result.result)
            }
        
        # Step 2: Enrich company data
        company_result = enrich_company.apply(args=[domain])
        company_success = company_result.successful()
        
        # Step 3: Enrich leads data  
        leads_result = enrich_leads.apply(args=[domain])
        leads_success = leads_result.successful()
        
        return {
            "domain": domain,
            "status": "completed",
            "scrape_success": True,
            "company_enrichment_success": company_success,
            "leads_enrichment_success": leads_success
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed for {domain}: {str(e)}")
        return {
            "domain": domain,
            "status": "failed",
            "error": str(e)
        }