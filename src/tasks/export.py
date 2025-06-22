"""Luigi task for CSV export."""

import json
import csv
import luigi
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from src.tasks.base import BaseTask
from src.tasks.enrich import EnrichCompanyTask, EnrichLeadsTask

logger = logging.getLogger(__name__)


class ExportCompanyCSVTask(BaseTask):
    """Task to export enriched company data to CSV."""
    
    output_file = luigi.Parameter()
    
    def requires(self):
        """This task requires enriched company data."""
        return EnrichCompanyTask(domain=self.domain)
    
    def output(self):
        """Define output target."""
        return luigi.LocalTarget(str(self.output_file))
    
    def run(self):
        """Execute the CSV export task."""
        logger.info(f"Starting company CSV export for domain: {self.domain}")
        
        try:
            # Read enriched data
            with open(self.input().path, 'r') as f:
                enriched_data = json.load(f)
            
            # Prepare CSV row
            row = self._prepare_company_row(enriched_data)
            
            # Write to CSV (append mode)
            output_path = Path(self.output().path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if file exists to write header
            file_exists = output_path.exists()
            
            with open(output_path, 'a', newline='', encoding='utf-8') as f:
                fieldnames = self._get_company_fieldnames()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # Write header if file is new
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(row)
            
            logger.info(f"Successfully exported company data for {self.domain} to CSV")
            
        except Exception as e:
            logger.error(f"Failed to export company CSV for {self.domain}: {str(e)}")
            raise
    
    def _prepare_company_row(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a row for CSV export."""
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
    
    def _get_company_fieldnames(self) -> List[str]:
        """Get fieldnames for company CSV."""
        return [
            "domain", "enriched_at", "success", "error", "scraped_url",
            "emails_found", "business_type", "naics_code", "target_market",
            "products_services", "value_propositions", "competitive_advantages",
            "technologies", "certifications", "pain_points", "confidence_score"
        ]


class ExportLeadsCSVTask(BaseTask):
    """Task to export enriched lead data to CSV."""
    
    output_file = luigi.Parameter()
    
    def requires(self):
        """This task requires enriched lead data."""
        return EnrichLeadsTask(domain=self.domain)
    
    def output(self):
        """Define output target."""
        return luigi.LocalTarget(str(self.output_file))
    
    def run(self):
        """Execute the CSV export task."""
        logger.info(f"Starting leads CSV export for domain: {self.domain}")
        
        try:
            # Read enriched data
            with open(self.input().path, 'r') as f:
                enriched_data = json.load(f)
            
            # Skip if no leads
            leads = enriched_data.get("leads", [])
            if not leads:
                logger.info(f"No leads to export for {self.domain}")
                # Create empty file to mark as complete
                Path(self.output().path).touch()
                return
            
            # Prepare CSV rows
            rows = [self._prepare_lead_row(lead, enriched_data) for lead in leads]
            
            # Write to CSV (append mode)
            output_path = Path(self.output().path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if file exists to write header
            file_exists = output_path.exists()
            
            with open(output_path, 'a', newline='', encoding='utf-8') as f:
                fieldnames = self._get_lead_fieldnames()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # Write header if file is new
                if not file_exists:
                    writer.writeheader()
                
                writer.writerows(rows)
            
            logger.info(f"Successfully exported {len(rows)} leads for {self.domain} to CSV")
            
        except Exception as e:
            logger.error(f"Failed to export leads CSV for {self.domain}: {str(e)}")
            raise
    
    def _prepare_lead_row(self, lead: Dict[str, Any], enriched_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a row for CSV export."""
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
    
    def _get_lead_fieldnames(self) -> List[str]:
        """Get fieldnames for lead CSV."""
        return [
            "email", "first_name", "last_name", "company_domain",
            "enriched_at", "error", "buyer_persona", "lead_score_adjustment"
        ]


class ExportAllCSVTask(luigi.Task):
    """Task to export both company and lead data to CSV."""
    
    domain = luigi.Parameter()
    company_csv = luigi.Parameter()
    leads_csv = luigi.Parameter()
    
    def requires(self):
        """This task requires both CSV exports."""
        return [
            ExportCompanyCSVTask(domain=self.domain, output_file=self.company_csv),
            ExportLeadsCSVTask(domain=self.domain, output_file=self.leads_csv)
        ]
    
    def output(self):
        """Mark as complete when both CSVs are done."""
        return [
            luigi.LocalTarget(str(self.company_csv)),
            luigi.LocalTarget(str(self.leads_csv))
        ]
    
    def run(self):
        """Nothing to do - just marks completion."""
        logger.info(f"Completed CSV exports for {self.domain}")