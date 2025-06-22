"""Enrichment data models."""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class ScrapedContent:
    """Scraped website content."""
    url: str
    content: str
    success: bool
    error: Optional[str] = None
    emails: List[str] = None


@dataclass
class LeadAnalysis:
    """AI analysis results for a lead."""
    buyer_persona: str
    lead_score_adjustment: int
    confidence: float
    reasoning: str
    
    def to_dict(self) -> Dict[str, any]:
        """Convert to dictionary for HubSpot update."""
        return {
            "buyer_persona": self.buyer_persona,
            "lead_score_adjustment": self.lead_score_adjustment,
            "confidence": self.confidence,
            "reasoning": self.reasoning
        }


@dataclass
class CompanyAnalysis:
    """AI analysis results for a company."""
    business_type_description: str
    company_summary: str
    industry: str
    naics_code: str
    company_owner: Optional[str]
    city: Optional[str]
    state_region: Optional[str]
    postal_code: Optional[str]
    country: Optional[str]
    number_of_employees: Optional[str]
    annual_revenue: Optional[str]
    timezone: Optional[str]
    target_market: str
    primary_products_services: List[str]
    value_propositions: List[str]
    competitive_advantages: List[str]
    technologies_used: List[str]
    certifications_awards: List[str]
    pain_points_addressed: List[str]
    confidence_score: float
    
    def to_dict(self) -> Dict[str, any]:
        """Convert to dictionary for HubSpot update."""
        return {
            "business_type_description": self.business_type_description,
            "company_summary": self.company_summary,
            "industry": self.industry,
            "naics_code": self.naics_code,
            "company_owner": self.company_owner,
            "city": self.city,
            "state_region": self.state_region,
            "postal_code": self.postal_code,
            "country": self.country,
            "number_of_employees": self.number_of_employees,
            "annual_revenue": self.annual_revenue,
            "timezone": self.timezone,
            "target_market": self.target_market,
            "primary_products_services": ", ".join(self.primary_products_services) if self.primary_products_services else "",
            "value_propositions": ", ".join(self.value_propositions) if self.value_propositions else "",
            "competitive_advantages": ", ".join(self.competitive_advantages) if self.competitive_advantages else "",
            "technologies_used": ", ".join(self.technologies_used) if self.technologies_used else "",
            "certifications_awards": ", ".join(self.certifications_awards) if self.certifications_awards else "",
            "pain_points_addressed": ", ".join(self.pain_points_addressed) if self.pain_points_addressed else "",
            "confidence_score": self.confidence_score
        }