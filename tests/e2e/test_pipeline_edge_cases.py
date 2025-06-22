"""Edge case tests for the pipeline."""

import pytest
import json
import os
import csv
from pathlib import Path
import tempfile
import shutil
from unittest.mock import patch, Mock
import luigi
import logging

# Disable Luigi logging during tests
logging.getLogger("luigi").setLevel(logging.ERROR)

from src.pipeline import DomainPipeline
from src.models.enrichment import ScrapedContent


class TestE2EPipelineEdgeCases:
    """Edge case tests for the domain enrichment pipeline."""
    
    @pytest.fixture
    def test_environment(self):
        """Set up test environment."""
        temp_dir = tempfile.mkdtemp()
        original_cwd = os.getcwd()
        
        dirs = [
            "data/site_content/raw",
            "data/enriched_companies/raw", 
            "data/enriched_leads/raw",
            "output",
            "logs"
        ]
        for d in dirs:
            os.makedirs(os.path.join(temp_dir, d), exist_ok=True)
        
        os.chdir(temp_dir)
        yield temp_dir
        
        os.chdir(original_cwd)
        shutil.rmtree(temp_dir)
    
    @patch('src.tasks.scrape.WebScraper')
    def test_empty_domain_file(self, mock_scraper_class, test_environment):
        """Test handling of empty domain file."""
        # Create empty file
        with open("empty_domains.txt", "w") as f:
            f.write("")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("empty_domains.txt", "output", use_celery=False)
        
        # Should complete without errors
        # Pipeline doesn't create CSV files when no domains are processed
        csv_files = list(Path("output").glob("*.csv"))
        assert len(csv_files) == 0  # No CSV files created for empty input
    
    @patch('src.tasks.scrape.WebScraper')
    def test_malformed_domains(self, mock_scraper_class, test_environment):
        """Test handling of malformed domain names."""
        # Mock scraper to handle various malformed inputs
        def malformed_scraper(domain):
            # Simulate different error scenarios
            if not domain or len(domain) < 4:
                raise ValueError(f"Invalid domain: {domain}")
            
            if domain.startswith("http"):
                # URL instead of domain
                from urllib.parse import urlparse
                parsed = urlparse(domain)
                actual_domain = parsed.hostname or domain
            else:
                actual_domain = domain
            
            mock_result = Mock()
            mock_result.url = f"https://{actual_domain}"
            mock_result.content = f"Content for {actual_domain}"
            mock_result.success = True
            mock_result.emails = []
            mock_result.error = None
            return mock_result
        
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = malformed_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        # Test various malformed domains
        domains = [
            "https://example.com",  # Full URL
            "http://test.com/page",  # URL with path
            "www.site.com",  # With www
            "test",  # Too short
            "",  # Empty
            "domain.com",  # Valid
            "sub.domain.com",  # Subdomain
            "domain.co.uk",  # Country TLD
            "192.168.1.1",  # IP address
            "domain..com",  # Double dot
            "-domain.com",  # Starts with dash
            "domain-.com",  # Ends with dash
        ]
        
        with open("malformed_domains.txt", "w") as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("malformed_domains.txt", "output", use_celery=False)
        
        # Check that valid domains were processed
        valid_domains = ["domain.com", "sub.domain.com", "domain.co.uk"]
        for domain in valid_domains:
            scraped_file = Path(f"data/site_content/raw/{domain}.json")
            assert scraped_file.exists()
    
    @patch('src.tasks.scrape.WebScraper')
    def test_duplicate_domains(self, mock_scraper_class, test_environment):
        """Test handling of duplicate domains in input file."""
        scrape_count = {}
        
        def counting_scraper(domain):
            scrape_count[domain] = scrape_count.get(domain, 0) + 1
            
            mock_result = Mock()
            mock_result.url = f"https://{domain}"
            mock_result.content = f"Content for {domain}"
            mock_result.success = True
            mock_result.emails = [f"info@{domain}"]
            mock_result.error = None
            return mock_result
        
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = counting_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        # Create file with duplicates
        with open("duplicate_domains.txt", "w") as f:
            f.write("example.com\n")
            f.write("test.com\n")
            f.write("example.com\n")  # Duplicate
            f.write("demo.com\n")
            f.write("test.com\n")  # Duplicate
            f.write("example.com\n")  # Another duplicate
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("duplicate_domains.txt", "output", use_celery=False)
        
        # Each domain should only be scraped once
        assert scrape_count.get("example.com", 0) == 1
        assert scrape_count.get("test.com", 0) == 1
        assert scrape_count.get("demo.com", 0) == 1
    
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_very_large_content(self, mock_analyzer_class, mock_scraper_class, test_environment):
        """Test handling of very large scraped content."""
        # Create scraper that returns very large content
        def large_content_scraper(domain):
            if domain == "large-content.com":
                # Generate 10MB of content
                large_text = "This is a very long text. " * 500000
                
                mock_result = Mock()
                mock_result.url = f"https://{domain}"
                mock_result.content = large_text
                mock_result.success = True
                mock_result.emails = ["contact@large-content.com"]
                mock_result.error = None
                return mock_result
            
            return Mock(
                url=f"https://{domain}",
                content="Normal content",
                success=True,
                emails=[],
                error=None
            )
        
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = large_content_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        # Mock analyzer to handle large content
        mock_analyzer_instance = Mock()
        mock_analyzer_instance.analyze_company = Mock(
            return_value=Mock(
                business_type_description="Large content site",
                naics_code="519130",
                model_dump=lambda: {
                    "business_type_description": "Large content site",
                    "naics_code": "519130",
                    "confidence_score": 0.8
                }
            )
        )
        mock_analyzer_instance.analyze_lead = Mock(
            return_value=Mock(
                buyer_persona="Contact",
                lead_score_adjustment=25,
                model_dump=lambda: {
                    "buyer_persona": "Contact",
                    "lead_score_adjustment": 25
                }
            )
        )
        mock_analyzer_class.return_value = mock_analyzer_instance
        
        with open("large_content_domains.txt", "w") as f:
            f.write("large-content.com\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("large_content_domains.txt", "output", use_celery=False)
        
        # Check that large content was handled
        scraped_file = Path("data/site_content/raw/large-content.com.json")
        assert scraped_file.exists()
        
        # Content should be truncated in some fields
        with open(scraped_file) as f:
            data = json.load(f)
        
        # JSON file should still be reasonable size
        assert scraped_file.stat().st_size < 100 * 1024 * 1024  # Less than 100MB
    
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_special_characters_in_content(self, mock_analyzer_class, mock_scraper_class, test_environment):
        """Test handling of special characters and encodings."""
        def special_char_scraper(domain):
            special_contents = {
                "special-chars.com": "Special chars: © ® ™ € £ ¥ • — – … ‚ „ " " ' '",
                "emoji-site.com": "Welcome! 🎉 Check our products 🛍️ Contact us 📧 👋",
                "mixed-encoding.com": "Café • Résumé • Naïve • 中文 • العربية • Русский",
                "html-entities.com": "HTML entities: &lt;div&gt; &amp; &quot; &#39; &nbsp;",
            }
            
            content = special_contents.get(domain, "Normal content")
            
            mock_result = Mock()
            mock_result.url = f"https://{domain}"
            mock_result.content = content
            mock_result.success = True
            mock_result.emails = [f"info@{domain}"]
            mock_result.error = None
            return mock_result
        
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = special_char_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        # Simple analyzer mock
        mock_analyzer_instance = Mock()
        mock_analyzer_instance.analyze_company = Mock(
            return_value=Mock(
                business_type_description="International business",
                model_dump=lambda: {"business_type_description": "International business", "confidence_score": 0.8}
            )
        )
        mock_analyzer_instance.analyze_lead = Mock(
            return_value=Mock(
                buyer_persona="Contact",
                lead_score_adjustment=25,
                model_dump=lambda: {"buyer_persona": "Contact", "lead_score_adjustment": 25}
            )
        )
        mock_analyzer_class.return_value = mock_analyzer_instance
        
        domains = ["special-chars.com", "emoji-site.com", "mixed-encoding.com", "html-entities.com"]
        with open("special_char_domains.txt", "w") as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("special_char_domains.txt", "output", use_celery=False)
        
        # All domains should be processed successfully
        for domain in domains:
            scraped_file = Path(f"data/site_content/raw/{domain}.json")
            assert scraped_file.exists()
            
            # Content should be properly encoded
            with open(scraped_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                assert data["success"] is True
                
                # Special characters should be preserved
                if domain == "emoji-site.com":
                    assert "🎉" in data["content"]
                elif domain == "mixed-encoding.com":
                    assert "中文" in data["content"]
    
    @patch('src.tasks.scrape.WebScraper')
    def test_timeout_handling(self, mock_scraper_class, test_environment):
        """Test handling of timeouts during scraping."""
        import time
        
        def timeout_scraper(domain):
            if domain == "slow-site.com":
                # Simulate timeout
                raise TimeoutError("Request timed out after 30 seconds")
            elif domain == "very-slow-site.com":
                # Simulate very slow response
                time.sleep(0.1)  # In real scenario this would be longer
                raise Exception("Connection timeout")
            
            # Normal response for other domains
            return Mock(
                url=f"https://{domain}",
                content="Normal content",
                success=True,
                emails=[],
                error=None
            )
        
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = timeout_scraper
        mock_scraper_class.return_value = mock_scraper_instance
        
        domains = ["normal-site.com", "slow-site.com", "very-slow-site.com", "another-normal.com"]
        with open("timeout_domains.txt", "w") as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("timeout_domains.txt", "output", use_celery=False)
        
        # Normal sites should succeed
        for domain in ["normal-site.com", "another-normal.com"]:
            scraped_file = Path(f"data/site_content/raw/{domain}.json")
            assert scraped_file.exists()
            with open(scraped_file) as f:
                data = json.load(f)
            assert data["success"] is True
        
        # Timeout sites should have error files
        for domain in ["slow-site.com", "very-slow-site.com"]:
            scraped_file = Path(f"data/site_content/raw/{domain}.json")
            assert scraped_file.exists()
            with open(scraped_file) as f:
                data = json.load(f)
            assert data["success"] is False
            # The error message contains "timed out" which includes "timeout"
            assert "timed out" in data["error"].lower() or "timeout" in data["error"].lower()
    
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_partial_pipeline_failure(self, mock_analyzer_class, mock_scraper_class, test_environment):
        """Test pipeline behavior when some stages fail."""
        # Scraper succeeds
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = Mock(
            return_value=Mock(
                url="https://test.com",
                content="Test content",
                success=True,
                emails=["info@test.com"],
                error=None
            )
        )
        mock_scraper_class.return_value = mock_scraper_instance
        
        # Analyzer fails for company but succeeds for lead
        mock_analyzer_instance = Mock()
        mock_analyzer_instance.analyze_company = Mock(
            side_effect=Exception("AI service unavailable")
        )
        mock_analyzer_instance.analyze_lead = Mock(
            return_value=Mock(
                buyer_persona="Contact",
                lead_score_adjustment=25,
                model_dump=lambda: {"buyer_persona": "Contact", "lead_score_adjustment": 25}
            )
        )
        mock_analyzer_class.return_value = mock_analyzer_instance
        
        with open("partial_failure_domains.txt", "w") as f:
            f.write("test.com\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("partial_failure_domains.txt", "output", use_celery=False)
        
        # Scraping should succeed
        scraped_file = Path("data/site_content/raw/test.com.json")
        assert scraped_file.exists()
        with open(scraped_file) as f:
            data = json.load(f)
        assert data["success"] is True
        
        # Company enrichment should fail
        company_file = Path("data/enriched_companies/raw/test.com.json")
        assert company_file.exists()
        with open(company_file) as f:
            data = json.load(f)
        assert data["success"] is False
        
        # Lead enrichment should succeed
        leads_file = Path("data/enriched_leads/raw/test.com.json")
        assert leads_file.exists()
        with open(leads_file) as f:
            data = json.load(f)
        assert data["success"] is True
        assert len(data["leads"]) > 0
    
    @patch('src.tasks.scrape.WebScraper')
    def test_domain_with_no_emails(self, mock_scraper_class, test_environment):
        """Test processing domains that have no email addresses."""
        mock_scraper_instance = Mock()
        mock_scraper_instance.scrape_domain = Mock(
            return_value=Mock(
                url="https://no-emails.com",
                content="This is a website with no contact information whatsoever.",
                success=True,
                emails=[],  # No emails found
                error=None
            )
        )
        mock_scraper_class.return_value = mock_scraper_instance
        
        with open("no_email_domains.txt", "w") as f:
            f.write("no-emails.com\n")
        
        pipeline = DomainPipeline(use_celery=False)
        pipeline.process_domains_from_file("no_email_domains.txt", "output", use_celery=False)
        
        # Check that leads file is created but empty
        leads_file = Path("data/enriched_leads/raw/no-emails.com.json")
        assert leads_file.exists()
        
        with open(leads_file) as f:
            data = json.load(f)
        assert data["success"] is True
        assert data["leads"] == []
        
        # CSV should still be created
        leads_csv_files = list(Path("output").glob("leads_*.csv"))
        assert len(leads_csv_files) > 0