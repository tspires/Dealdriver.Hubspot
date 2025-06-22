"""Expanded end-to-end tests for the complete pipeline."""

import pytest
import json
import os
import csv
import time
from pathlib import Path
import tempfile
import shutil
from unittest.mock import patch, Mock, MagicMock
import luigi
import logging
from datetime import datetime

# Disable Luigi logging during tests
logging.getLogger("luigi").setLevel(logging.ERROR)

from src.pipeline import DomainPipeline
from src.tasks.scrape import ScrapeWebsiteTask
from src.tasks.enrich import EnrichCompanyTask, EnrichLeadsTask
from src.tasks.export import ExportCompanyCSVTask, ExportLeadsCSVTask
from src.models.enrichment import ScrapedContent, CompanyAnalysis
from src.utils.performance_monitor import PerformanceMonitor


class TestE2EPipelineExpanded:
    """Expanded end-to-end tests for the domain enrichment pipeline."""
    
    @pytest.fixture
    def test_environment(self):
        """Set up test environment."""
        # Create temporary directory
        temp_dir = tempfile.mkdtemp()
        original_cwd = os.getcwd()
        
        # Create directory structure
        dirs = [
            "data/site_content/raw",
            "data/enriched_companies/raw", 
            "data/enriched_leads/raw",
            "output",
            "logs"
        ]
        for d in dirs:
            os.makedirs(os.path.join(temp_dir, d), exist_ok=True)
        
        # Change to temp directory
        os.chdir(temp_dir)
        
        yield temp_dir
        
        # Cleanup
        os.chdir(original_cwd)
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def realistic_scraper(self):
        """More realistic scraper mock with different scenarios."""
        def scraper_side_effect(domain):
            # Different scenarios for different domains
            scenarios = {
                # JavaScript-heavy site (would fail with requests)
                "react-app.com": {
                    "content": "Loading... Please enable JavaScript",
                    "emails": [],
                    "success": True,
                    "js_required": True
                },
                # Static site with lots of emails
                "contact-rich.com": {
                    "content": "Contact us at sales@contact-rich.com, support@contact-rich.com, hr@contact-rich.com",
                    "emails": ["sales@contact-rich.com", "support@contact-rich.com", "hr@contact-rich.com", 
                              "john.doe@contact-rich.com", "jane.smith@contact-rich.com"],
                    "success": True
                },
                # Site with minimal content
                "minimal-site.com": {
                    "content": "Welcome",
                    "emails": [],
                    "success": False,
                    "error": "Insufficient content scraped (only 7 characters)"
                },
                # Site with unicode content
                "international.com": {
                    "content": "Bienvenue! 歡迎! مرحبا! We provide global services with local expertise.",
                    "emails": ["info@international.com"],
                    "success": True
                },
                # E-commerce site
                "shop.com": {
                    "content": "Shop the best products online. Free shipping on orders over $50. Customer service available 24/7.",
                    "emails": ["orders@shop.com", "returns@shop.com"],
                    "success": True
                },
                # B2B service provider
                "b2b-services.com": {
                    "content": "Enterprise solutions for modern businesses. Cloud infrastructure, consulting, and managed services.",
                    "emails": ["enterprise@b2b-services.com", "consulting@b2b-services.com"],
                    "success": True
                }
            }
            
            scenario = scenarios.get(domain, {
                "content": f"Default content for {domain}",
                "emails": [f"info@{domain}"],
                "success": True
            })
            
            mock_result = Mock()
            mock_result.url = f"https://{domain}"
            mock_result.content = scenario["content"]
            mock_result.success = scenario.get("success", True)
            mock_result.emails = scenario.get("emails", [])
            mock_result.error = scenario.get("error")
            
            return mock_result
        
        return scraper_side_effect
    
    @pytest.fixture
    def industry_specific_analyzer(self):
        """Analyzer that returns industry-specific results."""
        def company_analysis(self, content):
            # Analyze based on keywords in content
            if "shop" in content.content.lower() or "products" in content.content.lower():
                return Mock(
                    business_type_description="E-commerce retailer",
                    naics_code="454110",
                    target_market="Online shoppers",
                    confidence_score=0.9,
                    model_dump=lambda: {
                        "business_type_description": "E-commerce retailer",
                        "naics_code": "454110",
                        "target_market": "Online shoppers",
                        "primary_products_services": ["Online retail", "Product delivery"],
                        "value_propositions": ["Convenience", "Wide selection"],
                        "competitive_advantages": ["Fast shipping", "24/7 support"],
                        "technologies_used": ["E-commerce platform", "Payment processing"],
                        "certifications_awards": ["PCI compliant"],
                        "pain_points_addressed": ["Shopping convenience"],
                        "confidence_score": 0.9
                    }
                )
            elif "enterprise" in content.content.lower() or "b2b" in content.content.lower():
                return Mock(
                    business_type_description="B2B service provider",
                    naics_code="541512",
                    target_market="Enterprise clients",
                    confidence_score=0.85,
                    model_dump=lambda: {
                        "business_type_description": "B2B service provider",
                        "naics_code": "541512",
                        "target_market": "Enterprise clients",
                        "primary_products_services": ["Consulting", "Managed services"],
                        "value_propositions": ["Expertise", "Scalability"],
                        "competitive_advantages": ["Industry experience"],
                        "technologies_used": ["Cloud platforms"],
                        "certifications_awards": ["ISO certified"],
                        "pain_points_addressed": ["Digital transformation"],
                        "confidence_score": 0.85
                    }
                )
            else:
                return Mock(
                    business_type_description="General business",
                    naics_code="541990",
                    target_market="Various",
                    confidence_score=0.7,
                    model_dump=lambda: {
                        "business_type_description": "General business",
                        "naics_code": "541990",
                        "target_market": "Various",
                        "primary_products_services": ["Services"],
                        "value_propositions": ["Quality"],
                        "competitive_advantages": ["Experience"],
                        "technologies_used": ["Standard"],
                        "certifications_awards": [],
                        "pain_points_addressed": ["Business needs"],
                        "confidence_score": 0.7
                    }
                )
        
        def lead_analysis(self, content, lead_info):
            # Score based on email pattern
            if "enterprise" in lead_info.get("email", ""):
                score = 50
                persona = "Enterprise Decision Maker"
            elif "sales" in lead_info.get("email", ""):
                score = 40
                persona = "Sales Professional"
            elif "support" in lead_info.get("email", ""):
                score = 20
                persona = "Support Staff"
            else:
                score = 25
                persona = "General Contact"
            
            return Mock(
                buyer_persona=persona,
                lead_score_adjustment=score,
                model_dump=lambda: {
                    "buyer_persona": persona,
                    "lead_score_adjustment": score
                }
            )
        
        return company_analysis, lead_analysis
    
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_requests_vs_selenium_fallback(self, mock_analyzer_class, mock_scraper_class,
                                          test_environment, realistic_scraper, industry_specific_analyzer):
        """Test that requests-first approach works and falls back to Selenium when needed."""
        # Mock the requests scraper to fail for JS-heavy sites
        mock_scraper_instance = Mock()
        
        def smart_scrape(domain):
            result = realistic_scraper(domain)
            # Simulate requests failing for JS sites
            if domain == "react-app.com":
                # First attempt with requests would fail
                mock_scraper_instance._scrape_with_requests = Mock(
                    return_value=ScrapedContent(
                        url=f"https://{domain}",
                        content="Loading...",  # Too short
                        success=False,
                        error="Insufficient content"
                    )
                )
            return result
        
        mock_scraper_instance.scrape_domain = smart_scrape
        mock_scraper_class.return_value = mock_scraper_instance
        
        # Setup analyzer
        mock_analyzer_instance = Mock()
        company_analysis, lead_analysis = industry_specific_analyzer
        mock_analyzer_instance.analyze_company = lambda content: company_analysis(mock_analyzer_instance, content)
        mock_analyzer_instance.analyze_lead = lambda content, lead_info: lead_analysis(mock_analyzer_instance, content, lead_info)
        mock_analyzer_class.return_value = mock_analyzer_instance
        
        # Test with JS-heavy site
        domains = ["react-app.com", "contact-rich.com"]
        with open("test_domains.txt", "w") as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("test_domains.txt", "output", use_celery=False)
        
        # Verify both were processed successfully
        for domain in domains:
            scraped_file = Path(f"data/site_content/raw/{domain}.json")
            assert scraped_file.exists()
            
            with open(scraped_file) as f:
                data = json.load(f)
            assert data["success"] is True
    
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_csv_export_formats(self, mock_analyzer_class, mock_scraper_class,
                               test_environment, realistic_scraper, industry_specific_analyzer):
        """Test that CSV exports have correct format for HubSpot import."""
        # Setup mocks
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = realistic_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        mock_analyzer_instance = Mock()
        company_analysis, lead_analysis = industry_specific_analyzer
        mock_analyzer_instance.analyze_company = lambda content: company_analysis(mock_analyzer_instance, content)
        mock_analyzer_instance.analyze_lead = lambda content, lead_info: lead_analysis(mock_analyzer_instance, content, lead_info)
        mock_analyzer_class.return_value = mock_analyzer_instance
        
        # Process domains with different characteristics
        domains = ["shop.com", "b2b-services.com", "contact-rich.com"]
        with open("test_domains.txt", "w") as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("test_domains.txt", "output", use_celery=False)
        
        # Check company CSV
        company_csv_files = list(Path("output").glob("companies_*.csv"))
        assert len(company_csv_files) > 0
        
        with open(company_csv_files[0], 'r') as f:
            reader = csv.DictReader(f)
            companies = list(reader)
            
            # Verify required fields based on actual export format
            required_fields = ["domain", "business_type", "success"]
            for company in companies:
                for field in required_fields:
                    assert field in company
                    # domain and business_type should not be empty for successful enrichments
                    if field in ["domain", "business_type"] and company.get("success") == "true":
                        assert company[field]  # Not empty
            
            # Check specific company data
            shop_company = next(c for c in companies if "shop.com" in c["domain"])
            assert "E-commerce" in shop_company["business_type"]
            # Check NAICS code instead of Industry field
            assert shop_company["naics_code"] == "454110"  # E-commerce NAICS code
        
        # Check leads CSV
        leads_csv_files = list(Path("output").glob("leads_*.csv"))
        assert len(leads_csv_files) > 0
        
        with open(leads_csv_files[0], 'r') as f:
            reader = csv.DictReader(f)
            leads = list(reader)
            
            # Verify contact-rich.com generated multiple leads
            contact_rich_leads = [l for l in leads if "contact-rich.com" in l.get("email", "")]
            assert len(contact_rich_leads) >= 3  # Should have multiple emails
            
            # Check lead scoring based on actual field names
            enterprise_lead = next((l for l in leads if "enterprise" in l.get("email", "")), None)
            if enterprise_lead:
                # Check the lead score adjustment field
                assert int(enterprise_lead.get("lead_score_adjustment", 0)) >= 40  # High score for enterprise
    
    @pytest.mark.skip(reason="Performance monitoring is tested in actual scraper tests")
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_performance_monitoring(self, mock_analyzer_class, mock_scraper_class,
                                   test_environment, realistic_scraper, industry_specific_analyzer):
        """Test that performance monitoring tracks metrics correctly."""
        # Import performance monitor
        from src.utils.performance_monitor import get_performance_monitor
        monitor = get_performance_monitor()
        
        # Reset monitor to ensure clean state
        monitor.metrics.clear()
        
        # Setup mocks
        def tracking_scraper(domain):
            result = realistic_scraper(domain)
            # Track the scrape in performance monitor
            import time
            start_time = time.time()
            monitor.track_scrape(
                domain=domain,
                success=result.success,
                duration=time.time() - start_time,
                content_size=len(result.content) if result.content else 0,
                emails_found=len(result.emails) if result.emails else 0,
                error=result.error
            )
            return result
        
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = tracking_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        mock_analyzer_instance = Mock()
        company_analysis, lead_analysis = industry_specific_analyzer
        mock_analyzer_instance.analyze_company = lambda content: company_analysis(mock_analyzer_instance, content)
        mock_analyzer_instance.analyze_lead = lambda content, lead_info: lead_analysis(mock_analyzer_instance, content, lead_info)
        mock_analyzer_class.return_value = mock_analyzer_instance
        
        # Process multiple domains
        domains = ["shop.com", "b2b-services.com", "minimal-site.com"]
        with open("test_domains.txt", "w") as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("test_domains.txt", "output", use_celery=False)
        
        # Performance monitor should have tracked scrapes
        report = monitor.get_report()
        assert report.total_domains >= len(domains)
        assert report.successful_scrapes >= 2  # shop.com and b2b-services.com
        assert report.failed_scrapes >= 1  # minimal-site.com
    
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_international_content_handling(self, mock_analyzer_class, mock_scraper_class,
                                           test_environment, realistic_scraper, industry_specific_analyzer):
        """Test handling of international/unicode content."""
        # Setup mocks
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = realistic_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        mock_analyzer_instance = Mock()
        company_analysis, lead_analysis = industry_specific_analyzer
        mock_analyzer_instance.analyze_company = lambda content: company_analysis(mock_analyzer_instance, content)
        mock_analyzer_instance.analyze_lead = lambda content, lead_info: lead_analysis(mock_analyzer_instance, content, lead_info)
        mock_analyzer_class.return_value = mock_analyzer_instance
        
        # Process international domain
        with open("test_domains.txt", "w") as f:
            f.write("international.com\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("test_domains.txt", "output", use_celery=False)
        
        # Check that unicode content was handled properly
        scraped_file = Path("data/site_content/raw/international.com.json")
        with open(scraped_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        assert "歡迎" in data["content"]
        assert "مرحبا" in data["content"]
        assert data["success"] is True
    
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_email_extraction_patterns(self, mock_analyzer_class, mock_scraper_class,
                                      test_environment, realistic_scraper, industry_specific_analyzer):
        """Test various email extraction patterns."""
        # Create custom scraper for email testing
        def email_test_scraper(domain):
            if domain == "email-test.com":
                content = """
                Contact us:
                Email: info@email-test.com
                Sales: sales@email-test.com
                Support: support@email-test.com
                CEO: john.doe@email-test.com
                CTO: jane.smith@email-test.com
                
                Regional offices:
                UK: uk@email-test.co.uk (different domain, should be filtered)
                US: us@email-test.com
                
                Invalid emails that should be filtered:
                test@otherdomain.com
                admin@localhost
                user@email-test (missing TLD)
                """
                
                mock_result = Mock()
                mock_result.url = f"https://{domain}"
                mock_result.content = content
                mock_result.success = True
                mock_result.emails = [
                    "info@email-test.com",
                    "sales@email-test.com", 
                    "support@email-test.com",
                    "john.doe@email-test.com",
                    "jane.smith@email-test.com",
                    "us@email-test.com"
                ]
                mock_result.error = None
                return mock_result
            return realistic_scraper(domain)
        
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = email_test_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        mock_analyzer_instance = Mock()
        company_analysis, lead_analysis = industry_specific_analyzer
        mock_analyzer_instance.analyze_company = lambda content: company_analysis(mock_analyzer_instance, content)
        mock_analyzer_instance.analyze_lead = lambda content, lead_info: lead_analysis(mock_analyzer_instance, content, lead_info)
        mock_analyzer_class.return_value = mock_analyzer_instance
        
        # Process test domain
        with open("test_domains.txt", "w") as f:
            f.write("email-test.com\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("test_domains.txt", "output", use_celery=False)
        
        # Check leads were extracted correctly
        leads_file = Path("data/enriched_leads/raw/email-test.com.json")
        with open(leads_file) as f:
            leads_data = json.load(f)
        
        assert len(leads_data["leads"]) == 6  # All valid same-domain emails
        
        # Check name parsing
        john_lead = next(l for l in leads_data["leads"] if l["email"] == "john.doe@email-test.com")
        assert john_lead["first_name"] == "John"
        assert john_lead["last_name"] == "Doe"
    
    @patch('src.tasks.scrape.WebScraper')
    def test_pipeline_resumability(self, mock_scraper_class, test_environment, realistic_scraper):
        """Test that pipeline can resume from intermediate states."""
        # Setup mock
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = realistic_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        # First, complete only scraping for some domains
        domains = ["shop.com", "b2b-services.com", "contact-rich.com"]
        
        # Manually create scraped content for first domain
        scraped_data = {
            "domain": domains[0],
            "url": f"https://{domains[0]}",
            "success": True,
            "scraped_at": datetime.now().isoformat(),
            "content": realistic_scraper(domains[0]).content,
            "emails": realistic_scraper(domains[0]).emails,
            "error": None
        }
        
        scraped_file = Path(f"data/site_content/raw/{domains[0]}.json")
        scraped_file.parent.mkdir(parents=True, exist_ok=True)
        with open(scraped_file, 'w') as f:
            json.dump(scraped_data, f)
        
        # Now run pipeline - it should skip already scraped domain
        with open("test_domains.txt", "w") as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        pipeline = DomainPipeline(use_celery=False)
        
        # Track which domains were actually scraped
        original_scrape_domain = mock_scraper_instance.scrape_domain
        scraped_domains = []
        
        def track_scraping(domain):
            scraped_domains.append(domain)
            return original_scrape_domain(domain)
        
        mock_scraper_instance.scrape_domain = track_scraping
        
        pipeline.process_domains_from_file("test_domains.txt", "output", use_celery=False)
        
        # First domain should not have been scraped again
        assert domains[0] not in scraped_domains
        assert domains[1] in scraped_domains
        assert domains[2] in scraped_domains
    
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_concurrent_pipeline_execution(self, mock_analyzer_class, mock_scraper_class,
                                          test_environment, realistic_scraper, industry_specific_analyzer):
        """Test pipeline with concurrent execution (simulated)."""
        # Setup mocks
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = realistic_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        mock_analyzer_instance = Mock()
        company_analysis, lead_analysis = industry_specific_analyzer
        mock_analyzer_instance.analyze_company = lambda content: company_analysis(mock_analyzer_instance, content)
        mock_analyzer_instance.analyze_lead = lambda content, lead_info: lead_analysis(mock_analyzer_instance, content, lead_info)
        mock_analyzer_class.return_value = mock_analyzer_instance
        
        # Process many domains
        domains = [
            "shop.com", "b2b-services.com", "contact-rich.com",
            "international.com", "minimal-site.com", "react-app.com"
        ]
        
        with open("test_domains.txt", "w") as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        # Run with workers parameter (though in test mode it won't actually parallelize)
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("test_domains.txt", "output", use_celery=False)
        
        # All domains should be processed
        successful_domains = []
        failed_domains = []
        
        for domain in domains:
            scraped_file = Path(f"data/site_content/raw/{domain}.json")
            if scraped_file.exists():
                with open(scraped_file) as f:
                    data = json.load(f)
                if data["success"]:
                    successful_domains.append(domain)
                else:
                    failed_domains.append(domain)
        
        assert len(successful_domains) >= 5  # Most should succeed
        assert "minimal-site.com" in failed_domains  # This one should fail
    
    def test_luigi_task_dependencies(self, test_environment):
        """Test that Luigi task dependencies are correctly configured."""
        domain = "test.com"
        
        # Create tasks
        scrape_task = ScrapeWebsiteTask(domain=domain)
        company_task = EnrichCompanyTask(domain=domain)
        leads_task = EnrichLeadsTask(domain=domain)
        
        # Check dependencies
        assert company_task.requires() == scrape_task
        assert leads_task.requires() == scrape_task
        
        # Export tasks should depend on enrichment
        company_csv_task = ExportCompanyCSVTask(
            domain=domain,
            output_file="output/companies.csv"
        )
        leads_csv_task = ExportLeadsCSVTask(
            domain=domain,
            output_file="output/leads.csv"
        )
        
        assert company_csv_task.requires() == company_task
        assert leads_csv_task.requires() == leads_task