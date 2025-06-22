#!/usr/bin/env python3
"""Test script to demonstrate logging functionality."""

import logging
import sys
sys.path.insert(0, '/home/tspires/Development/Dealdriver/Dealdriver.Hubspot')

from src.utils.logging import setup_logging
from src.services.scraper import WebScraper
from src.services.analyzer import AIAnalyzer
from src.services.enrichment_service import EnrichmentService
from src.models.hubspot import Lead
from unittest.mock import Mock

# Set up logging at DEBUG level to see detailed logs
setup_logging(level="DEBUG")
logger = logging.getLogger(__name__)

def test_logging_demo():
    """Demonstrate the enhanced logging."""
    logger.info("=== Starting Logging Demonstration ===")
    
    # Test scraper initialization
    logger.info("\n1. Testing WebScraper initialization:")
    scraper = WebScraper(use_browser_pool=False)
    
    # Test analyzer initialization  
    logger.info("\n2. Testing AIAnalyzer initialization:")
    analyzer = AIAnalyzer()
    
    # Test enrichment service
    logger.info("\n3. Testing EnrichmentService:")
    mock_hubspot = Mock()
    enrichment = EnrichmentService(mock_hubspot)
    
    # Test lead enrichment logging
    logger.info("\n4. Testing lead enrichment logging:")
    test_lead = Lead(
        id="test-123",
        email="john.doe@example.com",
        firstname="John",
        lastname="Doe",
        company="Example Corp"
    )
    
    # Mock the scraper to return a failed result
    enrichment.scraper.scrape_domain = Mock(return_value=Mock(
        success=False,
        error="Connection timeout",
        url="https://example.com",
        content="",
        emails=[]
    ))
    
    enrichment.enrich_lead(test_lead)
    
    logger.info("\n=== Logging Demonstration Complete ===")

if __name__ == "__main__":
    test_logging_demo()