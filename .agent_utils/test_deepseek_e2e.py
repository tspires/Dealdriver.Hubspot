#!/usr/bin/env python3
"""End-to-end test for DeepSeek prompt functionality."""

import sys
import os
import json
import logging
from pathlib import Path

# Add project to path
sys.path.insert(0, '/home/tspires/Development/Dealdriver/Dealdriver.Hubspot')

from src.services.analyzer import AIAnalyzer
from src.services.scraper import WebScraper
from src.models.enrichment import ScrapedContent
from src.utils.logging import setup_logging

# Setup logging
setup_logging(level="INFO")
logger = logging.getLogger(__name__)


def test_deepseek_prompts():
    """Test DeepSeek prompts with real examples."""
    
    # Test cases with expected outputs
    test_cases = [
        {
            "domain": "plumber.example",
            "content": """
                Joe's Plumbing - Your Local Plumbing Experts
                
                We provide 24/7 emergency plumbing services to homeowners and businesses 
                in the Chicago area. Licensed and insured since 1985.
                
                Services:
                - Emergency repairs
                - Water heater installation
                - Drain cleaning
                - Pipe repair and replacement
                
                Contact us at: info@joesplumbing.com or call (312) 555-0123
            """,
            "emails": ["info@joesplumbing.com", "joe@joesplumbing.com"],
            "expected": {
                "business_type": "Plumbing Contractor",
                "naics_prefix": "2382",  # Plumbing contractors
                "target_market": "homeowners and businesses",
                "min_confidence": 0.7
            }
        },
        {
            "domain": "saas.example",
            "content": """
                TechFlow Analytics - Modern Business Intelligence Platform
                
                Transform your data into insights with our cloud-based analytics platform.
                Trusted by over 500 enterprises worldwide.
                
                Features:
                - Real-time dashboards
                - AI-powered insights
                - Custom reporting
                - API integrations
                
                Start your 14-day free trial. No credit card required.
                Contact: sales@techflow.io, support@techflow.io
            """,
            "emails": ["sales@techflow.io", "support@techflow.io", "ceo@techflow.io"],
            "expected": {
                "business_type": "SaaS",
                "naics_prefix": "5415",  # Computer systems design
                "target_market": "enterprises",
                "min_confidence": 0.8
            }
        },
        {
            "domain": "restaurant.example",
            "content": """
                Luigi's Italian Kitchen - Authentic Italian Cuisine Since 1972
                
                Located in the heart of Boston's North End, we serve traditional 
                Italian dishes made from family recipes passed down through generations.
                
                Hours: Mon-Sun 11am-10pm
                Reservations: (617) 555-LUIGI
                
                Private dining available for parties up to 50 guests.
                Email: reservations@luigiskitchen.com
            """,
            "emails": ["reservations@luigiskitchen.com"],
            "expected": {
                "business_type": "Restaurant",
                "naics_prefix": "7225",  # Restaurants
                "target_market": "local diners",
                "min_confidence": 0.9
            }
        }
    ]
    
    # Initialize analyzer
    logger.info("=== Starting DeepSeek Prompt E2E Test ===")
    analyzer = AIAnalyzer()
    
    if not analyzer.client:
        logger.error("DeepSeek client not available - check API key")
        return
    
    results = []
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"\n--- Test Case {i}: {test_case['domain']} ---")
        
        try:
            # Test company analysis
            logger.info("Testing company analysis...")
            company_analysis = analyzer.analyze_company(
                test_case["content"], 
                domain=test_case["domain"],
                emails=test_case["emails"]
            )
            
            if company_analysis:
                logger.info("Company Analysis Results:")
                logger.info(f"  Business Type: {company_analysis.business_type_description}")
                logger.info(f"  NAICS Code: {company_analysis.naics_code}")
                logger.info(f"  Industry: {company_analysis.industry}")
                logger.info(f"  Target Market: {company_analysis.target_market}")
                logger.info(f"  Confidence: {company_analysis.confidence_score:.2f}")
                
                # Check expectations
                passed = True
                if test_case["expected"]["naics_prefix"]:
                    # Extract just the numeric part of NAICS code
                    naics_numeric = ''.join(c for c in company_analysis.naics_code if c.isdigit())[:6]
                    if not naics_numeric.startswith(test_case["expected"]["naics_prefix"]):
                        logger.warning(f"  ❌ NAICS mismatch: expected {test_case['expected']['naics_prefix']}*, got {company_analysis.naics_code}")
                        passed = False
                    else:
                        logger.info(f"  ✅ NAICS correct: {company_analysis.naics_code}")
                
                if company_analysis.confidence_score < test_case["expected"]["min_confidence"]:
                    logger.warning(f"  ❌ Low confidence: {company_analysis.confidence_score} < {test_case['expected']['min_confidence']}")
                    passed = False
                else:
                    logger.info(f"  ✅ Confidence acceptable: {company_analysis.confidence_score}")
                
                # Test lead analysis for each email
                logger.info("\nTesting lead analysis...")
                for email in test_case["emails"]:
                    lead_info = {
                        "email": email,
                        "firstname": email.split("@")[0].split(".")[0].title(),
                        "lastname": "",
                        "company": test_case["domain"]
                    }
                    
                    lead_analysis = analyzer.analyze_lead(test_case["content"], lead_info)
                    
                    if lead_analysis:
                        logger.info(f"  Email: {email}")
                        logger.info(f"    Persona: {lead_analysis.buyer_persona}")
                        logger.info(f"    Score: {lead_analysis.lead_score_adjustment:+d}")
                        logger.info(f"    Confidence: {lead_analysis.confidence:.2f}")
                        logger.info(f"    Reasoning: {lead_analysis.reasoning[:100]}...")
                
                results.append({
                    "domain": test_case["domain"],
                    "passed": passed,
                    "company_analysis": company_analysis.to_dict() if company_analysis else None
                })
                
            else:
                logger.error("  ❌ Company analysis failed")
                results.append({
                    "domain": test_case["domain"],
                    "passed": False,
                    "error": "Analysis returned None"
                })
                
        except Exception as e:
            logger.error(f"  ❌ Test failed with error: {e}")
            results.append({
                "domain": test_case["domain"],
                "passed": False,
                "error": str(e)
            })
    
    # Summary
    logger.info("\n=== Test Summary ===")
    passed = sum(1 for r in results if r.get("passed", False))
    total = len(results)
    logger.info(f"Passed: {passed}/{total} ({passed/total*100:.0f}%)")
    
    # Save results
    output_file = Path(".agent_utils/deepseek_test_results.json")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"Results saved to: {output_file}")
    
    return results


def test_with_real_website():
    """Test with a real website scrape."""
    logger.info("\n=== Testing with Real Website ===")
    
    scraper = WebScraper(use_browser_pool=False)
    analyzer = AIAnalyzer()
    
    # Test with a simple, well-known website
    test_domain = "python.org"
    logger.info(f"Scraping {test_domain}...")
    
    scraped = scraper.scrape_domain(test_domain)
    
    if scraped.success:
        logger.info(f"Scraped successfully - Content length: {len(scraped.content)}")
        logger.info(f"Emails found: {scraped.emails}")
        
        # Analyze
        logger.info("Analyzing content...")
        analysis = analyzer.analyze_company(scraped.content)
        
        if analysis:
            logger.info("\nAnalysis Results:")
            logger.info(f"  Business Type: {analysis.business_type_description}")
            logger.info(f"  NAICS Code: {analysis.naics_code}")
            logger.info(f"  Industry: {analysis.industry}")
            logger.info(f"  Summary: {analysis.company_summary}")
            logger.info(f"  Products/Services: {', '.join(analysis.primary_products_services[:3])}")
            logger.info(f"  Technologies: {', '.join(analysis.technologies_used[:3])}")
            logger.info(f"  Confidence: {analysis.confidence_score:.2f}")
        else:
            logger.error("Analysis failed")
    else:
        logger.error(f"Scraping failed: {scraped.error}")


if __name__ == "__main__":
    # Run tests
    test_deepseek_prompts()
    
    # Optionally test with real website
    # Uncomment to test with actual scraping:
    # test_with_real_website()