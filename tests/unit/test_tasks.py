"""Unit tests for Luigi tasks."""

import pytest
import json
import luigi
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os

from src.tasks.scrape import ScrapeWebsiteTask
from src.tasks.enrich import EnrichCompanyTask, EnrichLeadsTask
from src.tasks.export import ExportCompanyCSVTask, ExportLeadsCSVTask


class TestScrapeTask:
    """Test ScrapeWebsiteTask."""
    
    def test_task_initialization(self):
        """Test task initialization."""
        task = ScrapeWebsiteTask(domain="example.com")
        assert task.domain == "example.com"
        assert task.domain_safe == "example.com"
    
    def test_output_path(self):
        """Test output path generation."""
        task = ScrapeWebsiteTask(domain="example.com")
        output = task.output()
        assert "site_content/raw/example.com.json" in output.path
    
    @patch('src.tasks.scrape.WebScraper')
    def test_run_success(self, mock_scraper_class, temp_data_dir):
        """Test successful scraping."""
        # Setup mock
        mock_scraper = Mock()
        mock_scraper_class.return_value = mock_scraper
        mock_scraper.scrape_domain.return_value = Mock(
            url="https://example.com",
            content="Test content",
            success=True,
            emails=["test@example.com"],
            error=None
        )
        
        # Change to temp directory
        original_cwd = os.getcwd()
        os.chdir(temp_data_dir)
        
        try:
            # Use scraping_depth=0 to use single-page scraping
            task = ScrapeWebsiteTask(domain="example.com", scraping_depth=0)
            task.run()
            
            # Check output file was created
            output_path = Path(task.output().path)
            assert output_path.exists()
            
            # Check content
            with open(output_path, 'r') as f:
                data = json.load(f)
            
            assert data['domain'] == "example.com"
            assert data['success'] is True
            assert data['content'] == "Test content"
            assert "test@example.com" in data['emails']
        finally:
            os.chdir(original_cwd)
    
    @patch('src.tasks.scrape.WebScraper')
    def test_run_failure(self, mock_scraper_class, temp_data_dir):
        """Test failed scraping."""
        # Setup mock to raise exception
        mock_scraper = Mock()
        mock_scraper_class.return_value = mock_scraper
        mock_scraper.scrape_domain.side_effect = Exception("Network error")
        
        # Change to temp directory
        original_cwd = os.getcwd()
        os.chdir(temp_data_dir)
        
        try:
            # Use scraping_depth=0 to use single-page scraping
            task = ScrapeWebsiteTask(domain="example.com", scraping_depth=0)
            task.run()
            
            # Should still create output file
            output_path = Path(task.output().path)
            assert output_path.exists()
            
            # Check content
            with open(output_path, 'r') as f:
                data = json.load(f)
            
            assert data['success'] is False
            assert "Network error" in data['error']
        finally:
            os.chdir(original_cwd)


class TestEnrichTasks:
    """Test enrichment tasks."""
    
    def test_company_task_requires(self):
        """Test EnrichCompanyTask requirements."""
        task = EnrichCompanyTask(domain="example.com")
        requires = task.requires()
        assert isinstance(requires, ScrapeWebsiteTask)
        assert requires.domain == "example.com"
    
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_company_enrichment(self, mock_analyzer_class, temp_data_dir):
        """Test company enrichment."""
        # Setup
        original_cwd = os.getcwd()
        os.chdir(temp_data_dir)
        
        # Create input file
        input_data = {
            "domain": "example.com",
            "url": "https://example.com",
            "success": True,
            "content": "Company content",
            "emails": ["info@example.com"],
            "scraped_at": "2024-01-01T12:00:00",
            "error": None
        }
        input_path = Path("data/site_content/raw/example.com.json")
        input_path.parent.mkdir(parents=True, exist_ok=True)
        with open(input_path, 'w') as f:
            json.dump(input_data, f)
        
        # Setup mock analyzer
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer
        mock_analyzer.analyze_company.return_value = Mock(
            business_type_description="Tech company",
            naics_code="541511",
            confidence_score=0.9,
            to_dict=lambda: {
                "business_type_description": "Tech company",
                "naics_code": "541511",
                "confidence_score": 0.9
            }
        )
        
        try:
            task = EnrichCompanyTask(domain="example.com")
            # Mock the input
            task.input = lambda: Mock(path=str(input_path))
            task.run()
            
            # Check output
            output_path = Path(task.output().path)
            assert output_path.exists()
            
            with open(output_path, 'r') as f:
                data = json.load(f)
            
            assert data['success'] is True
            assert data['analysis']['business_type_description'] == "Tech company"
        finally:
            os.chdir(original_cwd)
    
    def test_lead_task_requires(self):
        """Test EnrichLeadsTask requirements."""
        task = EnrichLeadsTask(domain="example.com")
        requires = task.requires()
        assert isinstance(requires, ScrapeWebsiteTask)
        assert requires.domain == "example.com"
    
    def test_parse_email(self):
        """Test email parsing."""
        task = EnrichLeadsTask(domain="example.com")
        
        # Test with dot separator
        result = task._parse_email("john.doe@example.com")
        assert result['first_name'] == "John"
        assert result['last_name'] == "Doe"
        
        # Test with underscore
        result = task._parse_email("jane_smith@example.com")
        assert result['first_name'] == "Jane"
        assert result['last_name'] == "Smith"
        
        # Test with no separator
        result = task._parse_email("admin@example.com")
        assert result['first_name'] == "Admin"
        assert result['last_name'] == ""


class TestExportTasks:
    """Test export tasks."""
    
    def test_company_csv_task_requires(self):
        """Test ExportCompanyCSVTask requirements."""
        task = ExportCompanyCSVTask(domain="example.com", output_file="out.csv")
        requires = task.requires()
        assert isinstance(requires, EnrichCompanyTask)
        assert requires.domain == "example.com"
    
    def test_company_csv_export(self, temp_data_dir):
        """Test company CSV export."""
        # Setup
        original_cwd = os.getcwd()
        os.chdir(temp_data_dir)
        
        # Create input file
        input_data = {
            "domain": "example.com",
            "success": True,
            "analysis": {
                "business_type_description": "Tech company",
                "naics_code": "541511",
                "confidence_score": 0.9
            }
        }
        input_path = Path("data/enriched_companies/raw/example.com.json")
        input_path.parent.mkdir(parents=True, exist_ok=True)
        with open(input_path, 'w') as f:
            json.dump(input_data, f)
        
        try:
            output_file = "output/companies.csv"
            task = ExportCompanyCSVTask(domain="example.com", output_file=output_file)
            # Mock the input
            task.input = lambda: Mock(path=str(input_path))
            task.run()
            
            # Check output
            assert Path(output_file).exists()
            
            # Read CSV
            import csv
            with open(output_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            assert len(rows) == 1
            assert rows[0]['domain'] == "example.com"
            assert rows[0]['business_type'] == "Tech company"
        finally:
            os.chdir(original_cwd)
    
    def test_lead_csv_export_no_leads(self, temp_data_dir):
        """Test lead CSV export with no leads."""
        # Setup
        original_cwd = os.getcwd()
        os.chdir(temp_data_dir)
        
        # Create input file with no leads
        input_data = {
            "domain": "example.com",
            "success": True,
            "leads": []
        }
        input_path = Path("data/enriched_leads/raw/example.com.json")
        input_path.parent.mkdir(parents=True, exist_ok=True)
        with open(input_path, 'w') as f:
            json.dump(input_data, f)
        
        try:
            output_file = "output/leads.csv"
            task = ExportLeadsCSVTask(domain="example.com", output_file=output_file)
            # Mock the input
            task.input = lambda: Mock(path=str(input_path))
            task.run()
            
            # Should create empty file
            assert Path(output_file).exists()
        finally:
            os.chdir(original_cwd)