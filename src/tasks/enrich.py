"""Luigi task for AI enrichment."""

import json
import luigi
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from src.tasks.base import BaseTask
from src.tasks.scrape import ScrapeWebsiteTask
from src.services.analyzer import AIAnalyzer
from src.models.enrichment import ScrapedContent

logger = logging.getLogger(__name__)


class EnrichCompanyTask(BaseTask):
    """Task to enrich company data using AI analysis."""
    
    def requires(self):
        """This task requires scraped content."""
        return ScrapeWebsiteTask(domain=self.domain)
    
    def output(self):
        """Define output target."""
        output_path = self.get_output_path("enriched_companies", "json")
        return luigi.LocalTarget(str(output_path))
    
    def run(self):
        """Execute the enrichment task."""
        logger.info(f"Starting company enrichment task for domain: {self.domain}")
        
        try:
            # Read scraped content
            with open(self.input().path, 'r') as f:
                scraped_data = json.load(f)
            
            # Check if scraping was successful
            if not scraped_data.get('success', False):
                logger.warning(f"Skipping enrichment for {self.domain} - scraping failed")
                # Create minimal output
                output_data = {
                    "domain": self.domain,
                    "enriched_at": datetime.now().isoformat(),
                    "success": False,
                    "error": f"Scraping failed: {scraped_data.get('error', 'Unknown error')}",
                    "analysis": None
                }
            else:
                # Initialize analyzer
                analyzer = AIAnalyzer()
                
                # Create scraped content object
                scraped_content = ScrapedContent(
                    url=scraped_data['url'],
                    content=scraped_data['content'],
                    success=True,
                    emails=scraped_data.get('emails', [])
                )
                
                # Analyze company with domain and emails context
                analysis = analyzer.analyze_company(
                    scraped_content.content, 
                    domain=self.domain, 
                    emails=scraped_content.emails
                )
                
                # Prepare output data
                output_data = {
                    "domain": self.domain,
                    "enriched_at": datetime.now().isoformat(),
                    "success": True,
                    "scraped_url": scraped_data['url'],
                    "emails_found": scraped_data.get('emails', []),
                    "analysis": analysis.to_dict() if analysis else None
                }
            
            # Ensure output directory exists
            output_path = Path(self.output().path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to file
            with open(output_path, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            logger.info(f"Successfully enriched company data for {self.domain}")
            
        except Exception as e:
            logger.error(f"Failed to enrich {self.domain}: {str(e)}")
            # Still create output file to mark task as complete
            output_data = {
                "domain": self.domain,
                "enriched_at": datetime.now().isoformat(),
                "success": False,
                "error": str(e),
                "analysis": None
            }
            
            output_path = Path(self.output().path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(output_data, f, indent=2)


class EnrichLeadsTask(BaseTask):
    """Task to enrich lead data from scraped emails."""
    
    def requires(self):
        """This task requires scraped content."""
        return ScrapeWebsiteTask(domain=self.domain)
    
    def output(self):
        """Define output target."""
        output_path = self.get_output_path("enriched_leads", "json")
        return luigi.LocalTarget(str(output_path))
    
    def run(self):
        """Execute the lead enrichment task."""
        logger.info(f"Starting lead enrichment task for domain: {self.domain}")
        
        try:
            # Read scraped content
            with open(self.input().path, 'r') as f:
                scraped_data = json.load(f)
            
            # Check if we have emails
            emails = scraped_data.get('emails', [])
            if not emails:
                logger.info(f"No emails found for {self.domain}, creating empty lead file")
                output_data = {
                    "domain": self.domain,
                    "enriched_at": datetime.now().isoformat(),
                    "success": True,
                    "leads": []
                }
            else:
                # Initialize analyzer
                analyzer = AIAnalyzer()
                
                # Create scraped content object
                scraped_content = ScrapedContent(
                    url=scraped_data['url'],
                    content=scraped_data['content'],
                    success=True,
                    emails=emails
                )
                
                # Analyze leads
                leads = []
                for email in emails:
                    try:
                        lead_data = self._parse_email(email)
                        
                        # Analyze lead if we have content
                        if scraped_data.get('success', False) and scraped_data.get('content'):
                            lead_info = {
                                "firstname": lead_data.get('first_name', ''),
                                "lastname": lead_data.get('last_name', ''),
                                "email": email,
                                "company": self.domain
                            }
                            analysis = analyzer.analyze_lead(scraped_content.content, lead_info)
                            lead_data['analysis'] = analysis.to_dict() if analysis else None
                        else:
                            lead_data['analysis'] = None
                        
                        leads.append(lead_data)
                    except Exception as e:
                        logger.error(f"Failed to process lead {email}: {str(e)}")
                        leads.append({
                            "email": email,
                            "first_name": "",
                            "last_name": "",
                            "error": str(e),
                            "analysis": None
                        })
                
                output_data = {
                    "domain": self.domain,
                    "enriched_at": datetime.now().isoformat(),
                    "success": True,
                    "leads": leads
                }
            
            # Ensure output directory exists
            output_path = Path(self.output().path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to file
            with open(output_path, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            logger.info(f"Successfully enriched {len(output_data['leads'])} leads for {self.domain}")
            
        except Exception as e:
            logger.error(f"Failed to enrich leads for {self.domain}: {str(e)}")
            # Still create output file to mark task as complete
            output_data = {
                "domain": self.domain,
                "enriched_at": datetime.now().isoformat(),
                "success": False,
                "error": str(e),
                "leads": []
            }
            
            output_path = Path(self.output().path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(output_data, f, indent=2)
    
    def _parse_email(self, email: str) -> Dict[str, Any]:
        """Parse email to extract first and last name."""
        local_part = email.split('@')[0]
        
        # Try to split by common separators
        for separator in ['.', '_', '-']:
            if separator in local_part:
                parts = local_part.split(separator)
                if len(parts) >= 2:
                    return {
                        "email": email,
                        "first_name": parts[0].title(),
                        "last_name": parts[-1].title()
                    }
        
        # Default: use local part as first name
        return {
            "email": email,
            "first_name": local_part.title(),
            "last_name": ""
        }