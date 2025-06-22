"""Unit tests for data models."""

import pytest
from datetime import datetime

from src.models.hubspot import Lead, Company
from src.models.enrichment import ScrapedContent, LeadAnalysis, CompanyAnalysis


class TestHubSpotModels:
    """Test HubSpot data models."""
    
    def test_lead_creation(self):
        """Test Lead model creation."""
        lead = Lead(
            id="123",
            email="john.doe@example.com",
            firstname="John",
            lastname="Doe",
            company="Example Corp"
        )
        
        assert lead.id == "123"
        assert lead.email == "john.doe@example.com"
        assert lead.firstname == "John"
        assert lead.lastname == "Doe"
        assert lead.company == "Example Corp"
    
    def test_lead_optional_fields(self):
        """Test Lead model with optional fields."""
        lead = Lead(
            id="123",
            email="test@example.com",
            site_content="Website content",
            enrichment_status="completed",
            enrichment_date="2024-01-01",
            buyer_persona="Decision Maker",
            lead_score_adjustment=25
        )
        
        assert lead.site_content == "Website content"
        assert lead.enrichment_status == "completed"
        assert lead.buyer_persona == "Decision Maker"
        assert lead.lead_score_adjustment == 25
    
    def test_company_creation(self):
        """Test Company model creation."""
        company = Company(
            id="456",
            name="Example Corp",
            domain="example.com"
        )
        
        assert company.id == "456"
        assert company.name == "Example Corp"
        assert company.domain == "example.com"
    
    def test_company_with_enrichment(self):
        """Test Company model with enrichment fields."""
        company = Company(
            id="456",
            name="Example Corp",
            domain="example.com",
            site_content="Company website content",
            enrichment_status="completed",
            business_type_description="Software company",
            naics_code="541511",
            target_market="SMB",
            confidence_score=0.95
        )
        
        assert company.business_type_description == "Software company"
        assert company.naics_code == "541511"
        assert company.target_market == "SMB"
        assert company.confidence_score == 0.95


class TestEnrichmentModels:
    """Test enrichment data models."""
    
    def test_scraped_content_success(self):
        """Test ScrapedContent model for successful scrape."""
        content = ScrapedContent(
            url="https://example.com",
            content="Website content here",
            success=True,
            emails=["info@example.com"]
        )
        
        assert content.url == "https://example.com"
        assert content.content == "Website content here"
        assert content.success is True
        assert content.emails == ["info@example.com"]
        assert content.error is None
    
    def test_scraped_content_failure(self):
        """Test ScrapedContent model for failed scrape."""
        content = ScrapedContent(
            url="https://example.com",
            content="",
            success=False,
            error="Connection timeout",
            emails=[]
        )
        
        assert content.success is False
        assert content.error == "Connection timeout"
        assert content.content == ""
        assert content.emails == []
    
    def test_lead_analysis(self):
        """Test LeadAnalysis model."""
        analysis = LeadAnalysis(
            buyer_persona="Technical Buyer",
            lead_score_adjustment=30,
            confidence=0.85,
            reasoning="Strong technical background based on website content"
        )
        
        assert analysis.buyer_persona == "Technical Buyer"
        assert analysis.lead_score_adjustment == 30
        assert analysis.confidence == 0.85
        assert "technical background" in analysis.reasoning
    
    def test_company_analysis(self):
        """Test CompanyAnalysis model."""
        analysis = CompanyAnalysis(
            business_type_description="SaaS company",
            company_summary="Leading SaaS provider",
            industry="Software",
            naics_code="518210",
            company_owner=None,
            city="San Francisco",
            state_region="CA",
            postal_code="94105",
            country="USA",
            number_of_employees="100-500",
            annual_revenue="$10M-$50M",
            timezone="PST",
            target_market="Enterprise",
            primary_products_services=["CRM", "Analytics"],
            value_propositions=["Easy to use", "Scalable"],
            competitive_advantages=["Market leader"],
            technologies_used=["Python", "AWS"],
            certifications_awards=["SOC2"],
            pain_points_addressed=["Data silos"],
            confidence_score=0.88
        )
        
        assert analysis.business_type_description == "SaaS company"
        assert analysis.naics_code == "518210"
        assert analysis.target_market == "Enterprise"
        assert len(analysis.primary_products_services) == 2
        assert "CRM" in analysis.primary_products_services
        assert analysis.confidence_score == 0.88
        assert analysis.city == "San Francisco"
        assert analysis.number_of_employees == "100-500"
    
    def test_company_analysis_to_dict(self):
        """Test CompanyAnalysis to_dict method."""
        analysis = CompanyAnalysis(
            business_type_description="Tech startup",
            company_summary="Innovative tech solutions",
            industry="Technology",
            naics_code="541511",
            company_owner="John Doe",
            city="Austin",
            state_region="TX",
            postal_code="78701",
            country="USA",
            number_of_employees="10-50",
            annual_revenue="$1M-$10M",
            timezone="CST",
            target_market="SMB",
            primary_products_services=["API", "SDK"],
            value_propositions=["Fast", "Reliable"],
            competitive_advantages=["First to market"],
            technologies_used=["Node.js", "React"],
            certifications_awards=[],
            pain_points_addressed=["Integration complexity"],
            confidence_score=0.92
        )
        
        result = analysis.to_dict()
        
        assert result["business_type_description"] == "Tech startup"
        assert result["primary_products_services"] == "API, SDK"
        assert result["value_propositions"] == "Fast, Reliable"
        assert result["certifications_awards"] == ""
        assert result["confidence_score"] == 0.92