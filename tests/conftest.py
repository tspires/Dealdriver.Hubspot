"""Pytest configuration and fixtures."""

import pytest
import sys
import os
from pathlib import Path
from unittest.mock import Mock, MagicMock
import tempfile
import shutil

# Add project to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

# Add common library to path
common_path = project_root.parent / "common"
sys.path.insert(0, str(common_path))


@pytest.fixture
def mock_hubspot_client():
    """Mock HubSpot client."""
    mock_client = Mock()
    mock_client.get_contacts.return_value = []
    mock_client.get_companies.return_value = []
    mock_client.update_contact.return_value = True
    mock_client.update_company.return_value = True
    mock_client.create_contact_property.return_value = True
    mock_client.create_company_property.return_value = True
    return mock_client


@pytest.fixture
def mock_deepseek_client():
    """Mock DeepSeek client."""
    mock_client = Mock()
    mock_client.analyze_business_website.return_value = {
        "business_type_description": "Software development company",
        "naics_code": "541511",
        "target_market": "Small to medium businesses",
        "primary_products_services": ["Web development", "Mobile apps"],
        "value_propositions": ["Fast delivery", "Quality code"],
        "competitive_advantages": ["Experienced team"],
        "technologies_used": ["Python", "React"],
        "certifications_awards": [],
        "pain_points_addressed": ["Digital transformation"],
        "confidence_score": 0.85
    }
    return mock_client


@pytest.fixture
def mock_selenium_scraper():
    """Mock Selenium scraper."""
    mock_scraper = Mock()
    mock_scraper.fetch_page.return_value = {
        "url": "https://example.com",
        "title": "Example Company",
        "content": "We are a software company providing innovative solutions.",
        "success": True,
        "error": None
    }
    return mock_scraper


@pytest.fixture
def temp_data_dir():
    """Create temporary data directory."""
    temp_dir = tempfile.mkdtemp()
    data_dirs = [
        "data/site_content/raw",
        "data/enriched_companies/raw",
        "data/enriched_leads/raw",
        "output"
    ]
    for dir_path in data_dirs:
        os.makedirs(os.path.join(temp_dir, dir_path), exist_ok=True)
    
    yield temp_dir
    
    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_scraped_content():
    """Sample scraped content."""
    return {
        "domain": "example.com",
        "url": "https://example.com",
        "success": True,
        "scraped_at": "2024-01-01T12:00:00",
        "content": "Example Company - We provide software solutions. Contact: info@example.com",
        "emails": ["info@example.com", "support@example.com"],
        "error": None
    }


@pytest.fixture
def sample_company_analysis():
    """Sample company analysis."""
    return {
        "business_type_description": "Software development and consulting",
        "naics_code": "541511",
        "target_market": "SMB and Enterprise",
        "primary_products_services": ["Custom software", "Consulting", "Support"],
        "value_propositions": ["Innovation", "Reliability", "Support"],
        "competitive_advantages": ["20 years experience", "Industry expertise"],
        "technologies_used": ["Python", "JavaScript", "Cloud"],
        "certifications_awards": ["ISO 9001"],
        "pain_points_addressed": ["Digital transformation", "Process automation"],
        "confidence_score": 0.9
    }


@pytest.fixture
def sample_lead_analysis():
    """Sample lead analysis."""
    return {
        "buyer_persona": "Technical Decision Maker",
        "lead_score_adjustment": 20
    }


@pytest.fixture
def test_domains():
    """List of test domains for e2e testing."""
    return [
        "python.org",
        "github.com",
        "stackoverflow.com",
        "aws.amazon.com",
        "nodejs.org",
        "reactjs.org",
        "docker.com",
        "kubernetes.io",
        "postgresql.org",
        "redis.io"
    ]