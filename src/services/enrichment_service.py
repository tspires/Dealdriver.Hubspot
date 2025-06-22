"""Main enrichment orchestration service."""

import logging
import re
from datetime import datetime
from typing import Optional, Set

from src.models.hubspot import Company, Lead
from src.services.analyzer import AIAnalyzer
from src.services.hubspot_service import HubSpotService
from src.services.scraper import WebScraper
from src.utils.domain import extract_domain


logger = logging.getLogger(__name__)


class EnrichmentService:
    """Service for orchestrating the enrichment process."""
    
    def __init__(self, hubspot_service: HubSpotService):
        """Initialize enrichment service."""
        logger.info("Initializing EnrichmentService")
        self.hubspot = hubspot_service
        logger.debug("Creating WebScraper instance")
        self.scraper = WebScraper()
        logger.debug("Creating AIAnalyzer instance")
        self.analyzer = AIAnalyzer()
        self.processed_domains: Set[str] = set()
        logger.info("EnrichmentService initialized successfully")
    
    def enrich_lead(self, lead: Lead) -> bool:
        """Enrich a single lead."""
        logger.info("Starting enrichment for lead %s (%s)", lead.id, lead.email)
        logger.debug("Lead details: %s %s, Company: %s", 
                    lead.firstname, lead.lastname, lead.company)
        try:
            # Skip check since we don't have custom properties
            # if lead.enrichment_status == "completed":
            #     logger.info(f"Lead {lead.id} already enriched, skipping")
            #     return True
            
            domain = self._get_lead_domain(lead)
            if not domain:
                logger.warning("No domain found for lead %s (email: %s, website: %s)", 
                             lead.id, lead.email, lead.website)
                self._mark_lead_failed(lead.id, "No domain found")
                return False
            
            logger.debug("Extracted domain: %s", domain)
            
            if domain in self.processed_domains:
                logger.info("Domain %s already processed, using cached content", domain)
                logger.debug("Processed domains cache size: %d", len(self.processed_domains))
                return True
            
            scraped = self.scraper.scrape_domain(domain)
            if not scraped.success:
                logger.error(f"Failed to scrape {domain}: {scraped.error}")
                self._mark_lead_failed(lead.id, f"Scraping failed: {scraped.error}")
                return False
            
            self.processed_domains.add(domain)
            
            lead_info = {
                "firstname": lead.firstname,
                "lastname": lead.lastname,
                "company": lead.company,
                "email": lead.email
            }
            
            analysis = self.analyzer.analyze_lead(scraped.content, lead_info)
            if not analysis:
                logger.error(f"Failed to analyze content for lead {lead.id}")
                self._mark_lead_failed(lead.id, "Analysis failed")
                return False
            
            # Map lead score to lead status
            if analysis.lead_score_adjustment >= 7:
                lead_status = "OPEN_DEAL"
            elif analysis.lead_score_adjustment >= 5:
                lead_status = "CONNECTED"
            elif analysis.lead_score_adjustment >= 0:
                lead_status = "OPEN"
            else:
                lead_status = "UNQUALIFIED"
            
            # Standard properties we can reliably update
            properties = {
                "hs_lead_status": lead_status,
                "lifecyclestage": "lead" if analysis.lead_score_adjustment < 5 else "marketingqualifiedlead"
            }
            
            # Update the lead
            self.hubspot.update_lead(lead.id, properties)
            
            # Create a note with the enrichment details
            note_content = f"""Website Enrichment Analysis - {datetime.now().strftime('%Y-%m-%d %H:%M')}

Lead: {lead.firstname} {lead.lastname} ({lead.email})
Domain: {domain}

Analysis Results:
- Buyer Persona: {analysis.buyer_persona}
- Lead Score Adjustment: {analysis.lead_score_adjustment}
- Confidence: {analysis.confidence:.1%}
- Reasoning: {analysis.reasoning}

Website Content Summary:
{scraped.content[:2000]}...

---
Enrichment performed by HubSpot Enrichment Tool
"""
            
            # Create the note engagement
            try:
                self.hubspot.create_note(lead.id, note_content)
                logger.info(f"Created note for lead {lead.id} with enrichment details")
            except Exception as e:
                logger.warning(f"Could not create note: {e}")
            
            # Log full analysis results
            logger.info(f"""
Lead Enrichment Complete for {lead.id} ({lead.email}):
- Domain: {domain}
- Buyer Persona: {analysis.buyer_persona}
- Lead Score Adjustment: {analysis.lead_score_adjustment}
- Confidence: {analysis.confidence}
- Reasoning: {analysis.reasoning}
""")
            
            # The update has already happened above - this was causing a duplicate update
            logger.info(f"Successfully enriched lead {lead.id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to enrich lead {lead.id}: {e}", exc_info=True)
            self._mark_lead_failed(lead.id, str(e))
            return False
    
    def enrich_company(self, company: Company) -> bool:
        """Enrich a single company."""
        try:
            # Skip check since we don't have custom properties
            # if company.enrichment_status == "completed":
            #     logger.info(f"Company {company.id} already enriched, skipping")
            #     return True
            
            domain = company.domain or self._extract_domain_from_website(company.website)
            if not domain:
                logger.warning(f"No domain found for company {company.id} ({company.name})")
                self._mark_company_failed(company.id, "No domain found")
                return False
            
            # Skip if domain looks invalid
            from src.constants import MIN_DOMAIN_LENGTH
            if len(domain) < MIN_DOMAIN_LENGTH or '.' not in domain:
                logger.warning(f"Invalid domain '{domain}' for company {company.id} ({company.name})")
                self._mark_company_failed(company.id, f"Invalid domain: {domain}")
                return False
            
            if domain in self.processed_domains:
                logger.info("Domain %s already processed, using cached content", domain)
                logger.debug("Processed domains cache size: %d", len(self.processed_domains))
                return True
            
            scraped = self.scraper.scrape_domain(domain)
            if not scraped.success:
                logger.error(f"Failed to scrape {domain}: {scraped.error}")
                self._mark_company_failed(company.id, f"Scraping failed: {scraped.error}")
                return False
            
            self.processed_domains.add(domain)
            
            analysis = self.analyzer.analyze_company(scraped.content, domain=domain, emails=scraped.emails)
            if not analysis:
                logger.error(f"Failed to analyze content for company {company.id}")
                self._mark_company_failed(company.id, "Analysis failed")
                return False
            
            # Map to standard HubSpot fields
            analysis_dict = analysis.to_dict()
            
            # Prepare properties - handle field validations
            # Extract numeric annual revenue if possible
            annual_revenue_str = analysis_dict.get('annual_revenue', '')
            annual_revenue = None
            if annual_revenue_str:
                # Try to extract a number from strings like "$5M-$20M"
                import re
                revenue_match = re.search(r'(\d+)', annual_revenue_str)
                if revenue_match:
                    # Convert to full number (assuming millions)
                    annual_revenue = int(revenue_match.group(1)) * 1000000
            
            # Map industry to HubSpot's enum values
            industry_mapping = {
                "Financial Services": "FINANCIAL_SERVICES",
                "Management Consulting": "MANAGEMENT_CONSULTING",
                "Business Services": "CONSUMER_SERVICES",
                "Professional Services": "PROFESSIONAL_TRAINING_COACHING",
                "Technology": "INFORMATION_TECHNOLOGY_AND_SERVICES",
                "Healthcare": "HOSPITAL_HEALTH_CARE",
                "Retail": "RETAIL",
                "Manufacturing": "MACHINERY",
                "Real Estate": "REAL_ESTATE",
                "Education": "EDUCATION_MANAGEMENT"
            }
            industry_value = industry_mapping.get(analysis_dict.get('industry', ''), '')
            
            # Convert datetime to Unix timestamp (milliseconds)
            enrichment_timestamp = int(datetime.now().timestamp() * 1000)
            
            properties = {
                # Standard HubSpot fields
                "description": analysis_dict.get('company_summary', ''),
                "city": analysis_dict.get('city', ''),
                "state": analysis_dict.get('state_region', ''),
                "zip": analysis_dict.get('postal_code', ''),
                "country": analysis_dict.get('country', ''),
                "timezone": analysis_dict.get('timezone', ''),
                # Store scraped content in site_content (will create if needed)
                "site_content": scraped.content[:65536],  # Truncate to field limit
                "enrichment_status": "completed",
                "enrichment_date": enrichment_timestamp
            }
            
            # Only add fields if they have valid values
            if industry_value:
                properties["industry"] = industry_value
            if annual_revenue:
                properties["annualrevenue"] = annual_revenue
            
            # Handle number of employees - extract just the number
            employees_str = analysis_dict.get('number_of_employees', '')
            if employees_str:
                emp_match = re.search(r'(\d+)', employees_str)
                if emp_match:
                    properties["numberofemployees"] = int(emp_match.group(1))
            
            # Remove None values
            properties = {k: v for k, v in properties.items() if v is not None and v != ''}
            
            self.hubspot.update_company(company.id, properties)
            
            # Create a detailed note with all enrichment results
            note_content = f"""Website Enrichment Analysis - {datetime.now().strftime('%Y-%m-%d %H:%M')}

Company: {company.name}
Domain: {domain}

EXTRACTED INFORMATION:
=====================
Industry: {analysis_dict.get('industry', 'N/A')}
NAICS Code: {analysis_dict.get('naics_code', 'N/A')}
Owner/CEO: {analysis_dict.get('company_owner', 'N/A')}
Location: {analysis_dict.get('city', 'N/A')}, {analysis_dict.get('state_region', 'N/A')} {analysis_dict.get('postal_code', 'N/A')}, {analysis_dict.get('country', 'N/A')}
Timezone: {analysis_dict.get('timezone', 'N/A')}
Employees: {analysis_dict.get('number_of_employees', 'N/A')}
Annual Revenue: {analysis_dict.get('annual_revenue', 'N/A')}

BUSINESS ANALYSIS:
==================
Summary: {analysis_dict.get('company_summary', 'N/A')}

Business Type: {analysis_dict.get('business_type_description', 'N/A')}

Target Market: {analysis_dict.get('target_market', 'N/A')}

Products/Services: {analysis_dict.get('primary_products_services', 'N/A')}

Value Propositions: {analysis_dict.get('value_propositions', 'N/A')}

Competitive Advantages: {analysis_dict.get('competitive_advantages', 'N/A')}

Technologies Used: {analysis_dict.get('technologies_used', 'N/A')}

Certifications & Awards: {analysis_dict.get('certifications_awards', 'N/A')}

Pain Points Addressed: {analysis_dict.get('pain_points_addressed', 'N/A')}

Confidence Score: {analysis_dict.get('confidence_score', 0):.1%}

SOURCE CONTENT PREVIEW:
======================
{scraped.content[:1000]}...

---
Enrichment performed by HubSpot Enrichment Tool
"""
            
            # Create the note
            try:
                self.hubspot.create_note_for_company(company.id, note_content)
                logger.info(f"Created note for company {company.id} with enrichment details")
            except Exception as e:
                logger.warning(f"Could not create note: {e}")
            
            # Log full analysis
            logger.info(f"""
Company Enrichment Complete for {company.id} ({company.name}):
- Domain: {domain}
- Business Type: {analysis_dict.get('business_type_description')}
- Target Market: {analysis_dict.get('target_market')}
- Confidence Score: {analysis_dict.get('confidence_score')}
""")
            
            logger.info(f"Successfully enriched company {company.id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to enrich company {company.id}: {e}", exc_info=True)
            self._mark_company_failed(company.id, str(e))
            return False
    
    def _get_lead_domain(self, lead: Lead) -> Optional[str]:
        """Extract domain from lead data."""
        if lead.website:
            return self._extract_domain_from_website(lead.website)
        if lead.email:
            return extract_domain(lead.email)
        return None
    
    def _extract_domain_from_website(self, website: Optional[str]) -> Optional[str]:
        """Extract domain from website URL."""
        if not website:
            return None
        return extract_domain(website)
    
    def _mark_lead_failed(self, lead_id: str, reason: str) -> None:
        """Mark lead enrichment as failed."""
        # Log failure since we don't have custom properties
        logger.warning(f"Lead {lead_id} enrichment failed: {reason}")
    
    def _mark_company_failed(self, company_id: str, reason: str) -> None:
        """Mark company enrichment as failed."""
        # Log failure since we don't have custom properties
        logger.warning(f"Company {company_id} enrichment failed: {reason}")