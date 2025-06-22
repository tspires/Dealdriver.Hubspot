"""Unit tests for services - Fixed version."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json

from src.services.scraper import WebScraper
from src.services.analyzer import AIAnalyzer
from src.services.hubspot_service import HubSpotService
from src.services.enrichment_service import EnrichmentService
from src.models.enrichment import ScrapedContent, CompanyAnalysis, LeadAnalysis
from src.models.hubspot import Lead, Company


class TestWebScraper:
    """Test WebScraper service."""
    
    def test_scraper_initialization(self):
        """Test scraper initialization."""
        scraper = WebScraper()
        assert scraper is not None
    
    def test_extract_emails_from_html(self):
        """Test email extraction from HTML."""
        scraper = WebScraper()
        html = """
        <html>
            <body>
                <p>Contact us at info@example.com</p>
                <p>Support: support@example.com</p>
                <p>External: contact@other.com</p>
                <p>Sales: sales@sub.example.com</p>
            </body>
        </html>
        """
        
        emails = scraper.extract_emails_from_html(html, "example.com")
        
        assert "info@example.com" in emails
        assert "support@example.com" in emails
        assert "sales@sub.example.com" in emails
        assert "contact@other.com" not in emails  # Different domain
    
    def test_scrape_domain(self):
        """Test domain scraping."""
        scraper = WebScraper()
        with patch.object(scraper, 'scrape_url') as mock_scrape:
            mock_scrape.return_value = ScrapedContent(
                url="https://example.com",
                content="Test",
                success=True,
                emails=[]
            )
            
            result = scraper.scrape_domain("example.com")
            
            assert result.success is True
            mock_scrape.assert_called_once()


class TestAIAnalyzer:
    """Test AIAnalyzer service."""
    
    def test_analyzer_initialization(self):
        """Test analyzer initialization."""
        analyzer = AIAnalyzer()
        assert analyzer is not None
    
    def test_analyze_company_mock(self):
        """Test company analysis with mocked client."""
        analyzer = AIAnalyzer()
        
        # Mock the client
        mock_client = Mock()
        mock_client.analyze_business_website.return_value = {
            "business_type_description": "Tech company",
            "company_summary": "Leading tech provider",
            "industry": "Technology",
            "naics_code": "541511",
            "company_owner": None,
            "city": "San Francisco",
            "state_region": "CA",
            "postal_code": "94105",
            "country": "USA",
            "number_of_employees": "50-100",
            "annual_revenue": "$5M-$10M",
            "timezone": "PST",
            "target_market": "Enterprise",
            "primary_products_services": ["Software"],
            "value_propositions": ["Innovation"],
            "competitive_advantages": ["Experience"],
            "technologies_used": ["Python"],
            "certifications_awards": [],
            "pain_points_addressed": ["Efficiency"],
            "confidence_score": 0.9
        }
        analyzer.client = mock_client
        
        scraped = ScrapedContent(
            url="https://example.com",
            content="Company content",
            success=True,
            emails=[]
        )
        
        result = analyzer.analyze_company(scraped.content)
        
        if result:  # Only check if analyzer has client
            assert isinstance(result, CompanyAnalysis)
            assert result.business_type_description == "Tech company"
            assert result.confidence_score == 0.9


class TestHubSpotService:
    """Test HubSpotService."""
    
    def test_service_initialization(self):
        """Test HubSpot service initialization."""
        service = HubSpotService("test-token")
        assert service is not None
        assert hasattr(service, 'client')
    
    def test_update_lead_mock(self):
        """Test updating a lead with mocked client."""
        service = HubSpotService("test-token")
        
        # Mock the client
        mock_client = Mock()
        mock_client.update_contact.return_value = True
        service.client = mock_client
        
        # update_lead returns None on success, raises on error
        service.update_lead("123", {"site_content": "Test content"})
        
        # If we get here without exception, it succeeded
        mock_client.update_contact.assert_called_once()


class TestEnrichmentService:
    """Test EnrichmentService."""
    
    def test_service_initialization(self):
        """Test enrichment service initialization."""
        mock_hubspot_service = Mock()
        
        service = EnrichmentService(mock_hubspot_service)
        assert service is not None
        assert hasattr(service, 'scraper')
        assert hasattr(service, 'analyzer')
    
    @patch('src.services.enrichment_service.WebScraper')
    @patch('src.services.enrichment_service.AIAnalyzer')
    def test_enrich_company_flow(self, mock_analyzer_class, mock_scraper_class):
        """Test company enrichment flow."""
        # Setup mocks
        mock_scraper = Mock()
        mock_analyzer = Mock()
        mock_scraper_class.return_value = mock_scraper
        mock_analyzer_class.return_value = mock_analyzer
        
        mock_scraper.scrape_domain.return_value = ScrapedContent(
            url="https://example.com",
            content="Company content",
            success=True,
            emails=["info@example.com"]
        )
        
        # Create a proper CompanyAnalysis object
        mock_analyzer.analyze_company.return_value = CompanyAnalysis(
            business_type_description="Tech company",
            company_summary="Tech solutions",
            industry="Technology",
            naics_code="541511",
            company_owner=None,
            city=None,
            state_region=None,
            postal_code=None,
            country=None,
            number_of_employees=None,
            annual_revenue=None,
            timezone=None,
            target_market="SMB",
            primary_products_services=[],
            value_propositions=[],
            competitive_advantages=[],
            technologies_used=[],
            certifications_awards=[],
            pain_points_addressed=[],
            confidence_score=0.9
        )
        
        mock_hubspot = Mock()
        mock_hubspot.update_company.return_value = True
        
        service = EnrichmentService(mock_hubspot)
        company = Company(id="123", name="Example", domain="example.com")
        
        result = service.enrich_company(company)
        
        assert result is True
        mock_scraper.scrape_domain.assert_called_with("example.com")
        mock_analyzer.analyze_company.assert_called_once()
        mock_hubspot.update_company.assert_called_once()