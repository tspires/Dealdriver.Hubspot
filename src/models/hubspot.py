"""HubSpot data models."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class Lead:
    """HubSpot lead/contact model."""
    id: str
    email: Optional[str] = None
    firstname: Optional[str] = None
    lastname: Optional[str] = None
    company: Optional[str] = None
    website: Optional[str] = None
    site_content: Optional[str] = None
    enrichment_status: Optional[str] = None
    enrichment_date: Optional[datetime] = None
    buyer_persona: Optional[str] = None
    lead_score_adjustment: Optional[int] = None
    
    @classmethod
    def from_hubspot(cls, data: Dict[str, Any]) -> "Lead":
        """Create Lead from HubSpot API response."""
        properties = data.get("properties", {})
        return cls(
            id=data["id"],
            email=properties.get("email"),
            firstname=properties.get("firstname"),
            lastname=properties.get("lastname"),
            company=properties.get("company"),
            website=properties.get("website"),
            site_content=properties.get("site_content"),
            enrichment_status=properties.get("enrichment_status"),
            enrichment_date=properties.get("enrichment_date"),
            buyer_persona=properties.get("buyer_persona"),
            lead_score_adjustment=properties.get("lead_score_adjustment")
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Lead to dictionary."""
        return {
            'id': self.id,
            'email': self.email,
            'firstname': self.firstname,
            'lastname': self.lastname,
            'company': self.company,
            'website': self.website,
            'site_content': self.site_content,
            'enrichment_status': self.enrichment_status,
            'enrichment_date': self.enrichment_date,
            'buyer_persona': self.buyer_persona,
            'lead_score_adjustment': self.lead_score_adjustment
        }


@dataclass
class Company:
    """HubSpot company model."""
    id: str
    name: Optional[str] = None
    domain: Optional[str] = None
    website: Optional[str] = None
    site_content: Optional[str] = None
    enrichment_status: Optional[str] = None
    enrichment_date: Optional[datetime] = None
    business_type_description: Optional[str] = None
    naics_code: Optional[str] = None
    target_market: Optional[str] = None
    primary_products_services: Optional[str] = None
    value_propositions: Optional[str] = None
    competitive_advantages: Optional[str] = None
    technologies_used: Optional[str] = None
    certifications_awards: Optional[str] = None
    pain_points_addressed: Optional[str] = None
    confidence_score: Optional[float] = None
    
    @classmethod
    def from_hubspot(cls, data: Dict[str, Any]) -> "Company":
        """Create Company from HubSpot API response."""
        properties = data.get("properties", {})
        return cls(
            id=data["id"],
            name=properties.get("name"),
            domain=properties.get("domain"),
            website=properties.get("website"),
            site_content=properties.get("site_content"),
            enrichment_status=properties.get("enrichment_status"),
            enrichment_date=properties.get("enrichment_date"),
            business_type_description=properties.get("business_type_description"),
            naics_code=properties.get("naics_code"),
            target_market=properties.get("target_market"),
            primary_products_services=properties.get("primary_products_services"),
            value_propositions=properties.get("value_propositions"),
            competitive_advantages=properties.get("competitive_advantages"),
            technologies_used=properties.get("technologies_used"),
            certifications_awards=properties.get("certifications_awards"),
            pain_points_addressed=properties.get("pain_points_addressed"),
            confidence_score=properties.get("confidence_score")
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Company to dictionary."""
        return {
            'id': self.id,
            'name': self.name,
            'domain': self.domain,
            'website': self.website,
            'site_content': self.site_content,
            'enrichment_status': self.enrichment_status,
            'enrichment_date': self.enrichment_date,
            'business_type_description': self.business_type_description,
            'naics_code': self.naics_code,
            'target_market': self.target_market,
            'primary_products_services': self.primary_products_services,
            'value_propositions': self.value_propositions,
            'competitive_advantages': self.competitive_advantages,
            'technologies_used': self.technologies_used,
            'certifications_awards': self.certifications_awards,
            'pain_points_addressed': self.pain_points_addressed,
            'confidence_score': self.confidence_score
        }