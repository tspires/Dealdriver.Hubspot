"""HubSpot API service."""

import logging
from datetime import datetime
from typing import Dict, Iterator, List, Optional

from src.models.hubspot import Company, Lead


logger = logging.getLogger(__name__)


class HubSpotService:
    """Service for interacting with HubSpot API."""
    
    def __init__(self, token: str):
        """Initialize HubSpot service."""
        try:
            import sys
            from pathlib import Path
            # Add common library to path
            common_path = Path(__file__).parent.parent.parent.parent / "common"
            sys.path.insert(0, str(common_path))
            
            from clients.hubspot import HubSpotClient
            self.client = HubSpotClient(access_token=token)
        except Exception as e:
            logger.error(f"Failed to import HubspotClient: {e}")
            raise
    
    def create_contact_properties(self) -> None:
        """Create custom contact properties."""
        properties = [
            {
                "name": "site_content",
                "label": "Site Content",
                "type": "string",
                "fieldType": "textarea",
                "groupName": "contactinformation"
            },
            {
                "name": "enrichment_status",
                "label": "Enrichment Status",
                "type": "string",
                "fieldType": "text",
                "groupName": "contactinformation"
            },
            {
                "name": "enrichment_date",
                "label": "Enrichment Date",
                "type": "datetime",
                "fieldType": "date",
                "groupName": "contactinformation"
            },
            {
                "name": "buyer_persona",
                "label": "Buyer Persona",
                "type": "string",
                "fieldType": "text",
                "groupName": "contactinformation"
            },
            {
                "name": "lead_score_adjustment",
                "label": "Lead Score Adjustment",
                "type": "number",
                "fieldType": "number",
                "groupName": "contactinformation"
            }
        ]
        
        for prop in properties:
            try:
                self.client.create_property(
                    object_type="contacts",
                    name=prop["name"],
                    label=prop["label"],
                    property_type=prop["type"],
                    field_type=prop["fieldType"],
                    group_name=prop["groupName"]
                )
                logger.info(f"Created contact property: {prop['name']}")
            except Exception as e:
                logger.warning(f"Failed to create contact property {prop['name']}: {e}")
    
    def create_company_properties(self) -> None:
        """Create custom company properties."""
        properties = [
            {
                "name": "site_content",
                "label": "Site Content",
                "type": "string",
                "fieldType": "textarea",
                "groupName": "companyinformation",
                "description": "Scraped website content used for enrichment"
            },
            {
                "name": "enrichment_status",
                "label": "Enrichment Status",
                "type": "string",
                "fieldType": "text",
                "groupName": "companyinformation"
            },
            {
                "name": "enrichment_date",
                "label": "Enrichment Date",
                "type": "datetime",
                "fieldType": "date",
                "groupName": "companyinformation"
            },
            {
                "name": "business_type_description",
                "label": "Business Type Description",
                "type": "string",
                "fieldType": "textarea",
                "groupName": "companyinformation"
            },
            {
                "name": "naics_code",
                "label": "NAICS Code",
                "type": "string",
                "fieldType": "text",
                "groupName": "companyinformation"
            },
            {
                "name": "target_market",
                "label": "Target Market",
                "type": "string",
                "fieldType": "text",
                "groupName": "companyinformation"
            },
            {
                "name": "primary_products_services",
                "label": "Primary Products/Services",
                "type": "string",
                "fieldType": "textarea",
                "groupName": "companyinformation"
            },
            {
                "name": "value_propositions",
                "label": "Value Propositions",
                "type": "string",
                "fieldType": "textarea",
                "groupName": "companyinformation"
            },
            {
                "name": "competitive_advantages",
                "label": "Competitive Advantages",
                "type": "string",
                "fieldType": "textarea",
                "groupName": "companyinformation"
            },
            {
                "name": "technologies_used",
                "label": "Technologies Used",
                "type": "string",
                "fieldType": "textarea",
                "groupName": "companyinformation"
            },
            {
                "name": "certifications_awards",
                "label": "Certifications & Awards",
                "type": "string",
                "fieldType": "textarea",
                "groupName": "companyinformation"
            },
            {
                "name": "pain_points_addressed",
                "label": "Pain Points Addressed",
                "type": "string",
                "fieldType": "textarea",
                "groupName": "companyinformation"
            },
            {
                "name": "confidence_score",
                "label": "Confidence Score",
                "type": "number",
                "fieldType": "number",
                "groupName": "companyinformation"
            }
        ]
        
        for prop in properties:
            try:
                self.client.create_property(
                    object_type="companies",
                    name=prop["name"],
                    label=prop["label"],
                    property_type=prop["type"],
                    field_type=prop["fieldType"],
                    group_name=prop["groupName"]
                )
                logger.info(f"Created company property: {prop['name']}")
            except Exception as e:
                logger.warning(f"Failed to create company property {prop['name']}: {e}")
    
    def get_leads(self, limit: Optional[int] = None) -> Iterator[Lead]:
        """Get leads from HubSpot."""
        properties = [
            "email", "firstname", "lastname", "company", "website",
            "site_content", "enrichment_status", "enrichment_date",
            "buyer_persona", "lead_score_adjustment"
        ]
        
        count = 0
        for contact in self.client.iter_contacts(properties=properties):
            if limit and count >= limit:
                break
            yield Lead.from_hubspot(contact)
            count += 1
    
    def get_companies(self, limit: Optional[int] = None) -> Iterator[Company]:
        """Get companies from HubSpot."""
        properties = [
            "name", "domain", "website", "site_content", "enrichment_status",
            "enrichment_date", "business_type_description", "naics_code",
            "target_market", "primary_products_services", "value_propositions",
            "competitive_advantages", "technologies_used", "certifications_awards",
            "pain_points_addressed", "confidence_score"
        ]
        
        count = 0
        for company in self.client.iter_companies(properties=properties):
            if limit and count >= limit:
                break
            yield Company.from_hubspot(company)
            count += 1
    
    def update_lead(self, lead_id: str, properties: Dict[str, any]) -> None:
        """Update lead properties."""
        try:
            self.client.update_contact(lead_id, properties)
            logger.info(f"Updated lead {lead_id}")
        except Exception as e:
            logger.error(f"Failed to update lead {lead_id}: {e}")
            raise
    
    def update_company(self, company_id: str, properties: Dict[str, any]) -> None:
        """Update company properties."""
        try:
            self.client.update_company(company_id, properties)
            logger.info(f"Updated company {company_id}")
        except Exception as e:
            logger.error(f"Failed to update company {company_id}: {e}")
            raise
    
    def get_lead_by_id(self, lead_id: str) -> Optional[Lead]:
        """Get a single lead by ID."""
        properties = [
            "email", "firstname", "lastname", "company", "website",
            "site_content", "enrichment_status", "enrichment_date",
            "buyer_persona", "lead_score_adjustment"
        ]
        
        try:
            contact = self.client.get_contact(lead_id, properties=properties)
            return Lead.from_hubspot(contact)
        except Exception as e:
            logger.error(f"Failed to get lead {lead_id}: {e}")
            return None
    
    def get_lead_by_email(self, email: str) -> Optional[Lead]:
        """Get a single lead by email."""
        properties = [
            "email", "firstname", "lastname", "company", "website",
            "site_content", "enrichment_status", "enrichment_date",
            "buyer_persona", "lead_score_adjustment"
        ]
        
        try:
            # Use regular iteration and filter manually
            for contact in self.client.iter_contacts(properties=properties):
                contact_email = contact.get("properties", {}).get("email")
                if contact_email and contact_email.lower() == email.lower():
                    return Lead.from_hubspot(contact)
            
            return None
        except Exception as e:
            logger.error(f"Failed to get lead with email {email}: {e}")
            return None
    
    def get_company_by_id(self, company_id: str) -> Optional[Company]:
        """Get a single company by ID."""
        properties = [
            "name", "domain", "website", "site_content", "enrichment_status",
            "enrichment_date", "business_type_description", "naics_code",
            "target_market", "primary_products_services", "value_propositions",
            "competitive_advantages", "technologies_used", "certifications_awards",
            "pain_points_addressed", "confidence_score"
        ]
        
        try:
            company = self.client.get_company(company_id, properties=properties)
            return Company.from_hubspot(company)
        except Exception as e:
            logger.error(f"Failed to get company {company_id}: {e}")
            return None
    
    def create_note(self, contact_id: str, note_content: str) -> None:
        """Create a note engagement for a contact."""
        try:
            self.client.create_note(
                body=note_content[:65536],  # Truncate to field limit
                associations={
                    "contacts": [contact_id]
                }
            )
            logger.info(f"Created note for contact {contact_id}")
        except Exception as e:
            logger.error(f"Failed to create note for contact {contact_id}: {e}")
            raise
    
    def create_note_for_company(self, company_id: str, note_content: str) -> None:
        """Create a note engagement for a company."""
        try:
            self.client.create_note(
                body=note_content[:65536],  # Truncate to field limit
                associations={
                    "companies": [company_id]
                }
            )
            logger.info(f"Created note for company {company_id}")
        except Exception as e:
            logger.error(f"Failed to create note for company {company_id}: {e}")
            raise
    
    def get_company_by_domain(self, domain: str) -> Optional[Company]:
        """Get a single company by domain using HubSpot search API."""
        properties = [
            "name", "domain", "website", "site_content", "enrichment_status",
            "enrichment_date", "business_type_description", "naics_code",
            "target_market", "primary_products_services", "value_propositions",
            "competitive_advantages", "technologies_used", "certifications_awards",
            "pain_points_addressed", "confidence_score"
        ]
        
        try:
            logger.info(f"Searching for company with domain: {domain}")
            
            # Normalize search domain
            search_domain = domain.lower().replace('http://', '').replace('https://', '').strip('/')
            
            # Try different search strategies
            search_strategies = [
                # Strategy 1: Search by domain field
                {
                    "filters": [{
                        "propertyName": "domain",
                        "operator": "CONTAINS_TOKEN",
                        "value": search_domain
                    }]
                },
                # Strategy 2: Search by website field
                {
                    "filters": [{
                        "propertyName": "website",
                        "operator": "CONTAINS_TOKEN", 
                        "value": search_domain
                    }]
                },
                # Strategy 3: Text search
                {
                    "query": search_domain
                }
            ]
            
            for strategy in search_strategies:
                logger.debug(f"Trying search strategy: {strategy}")
                
                if "filters" in strategy:
                    results = self.client.search(
                        object_type="companies",
                        filter_groups=[{"filters": strategy["filters"]}],
                        properties=properties,
                        limit=10
                    )
                else:
                    results = self.client.search(
                        object_type="companies",
                        query=strategy["query"],
                        properties=properties,
                        limit=10
                    )
                
                if results.get("results"):
                    company = results["results"][0]
                    logger.info(f"Found company: {company.get('properties', {}).get('name', 'Unknown')}")
                    return Company.from_hubspot(company)
            
            logger.warning(f"No company found with domain {domain}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to search for company with domain {domain}: {e}")
            return None