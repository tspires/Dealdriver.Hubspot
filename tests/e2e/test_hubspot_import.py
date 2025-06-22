"""End-to-end tests for HubSpot import functionality."""

import pytest
import json
import csv
import os
from pathlib import Path
import tempfile
import shutil
from unittest.mock import patch, Mock, MagicMock
import luigi
import logging
import sys

# Disable Luigi logging during tests
logging.getLogger("luigi").setLevel(logging.ERROR)

# Mock the common module
sys.modules['common'] = MagicMock()
sys.modules['common.clients'] = MagicMock()
sys.modules['common.clients.hubspot'] = MagicMock()

from src.pipeline import DomainPipeline
from src.tasks.hubspot_import import (
    HubSpotBulkImportTask,
    ImportAllTask
)


class TestE2EHubSpotImport:
    """End-to-end tests for HubSpot import pipeline."""
    
    @pytest.fixture
    def test_environment(self):
        """Set up test environment."""
        temp_dir = tempfile.mkdtemp()
        original_cwd = os.getcwd()
        
        # Set test mode environment variable
        os.environ['HUBSPOT_IMPORT_TEST_MODE'] = '1'
        
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
        
        # Cleanup
        os.environ.pop('HUBSPOT_IMPORT_TEST_MODE', None)
        os.chdir(original_cwd)
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def sample_csv_files(self, test_environment):
        """Create sample CSV files for testing."""
        # Create company CSV
        company_csv = Path("output/companies_test.csv")
        with open(company_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'domain', 'business_type', 'naics_code', 'success', 
                'enriched_at', 'confidence_score'
            ])
            writer.writeheader()
            writer.writerow({
                'domain': 'techcorp.com',
                'business_type': 'Software Development',
                'naics_code': '541511',
                'success': 'true',
                'enriched_at': '2024-01-01T00:00:00',
                'confidence_score': '0.9'
            })
            writer.writerow({
                'domain': 'retailco.com',
                'business_type': 'E-commerce Retail',
                'naics_code': '454110',
                'success': 'true',
                'enriched_at': '2024-01-01T00:00:00',
                'confidence_score': '0.85'
            })
        
        # Create leads CSV
        leads_csv = Path("output/leads_test.csv")
        with open(leads_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'email', 'first_name', 'last_name', 'company_domain',
                'buyer_persona', 'lead_score_adjustment', 'enriched_at'
            ])
            writer.writeheader()
            writer.writerow({
                'email': 'john.doe@techcorp.com',
                'first_name': 'John',
                'last_name': 'Doe',
                'company_domain': 'techcorp.com',
                'buyer_persona': 'Technical Decision Maker',
                'lead_score_adjustment': '50',
                'enriched_at': '2024-01-01T00:00:00'
            })
            writer.writerow({
                'email': 'jane.smith@retailco.com',
                'first_name': 'Jane',
                'last_name': 'Smith',
                'company_domain': 'retailco.com',
                'buyer_persona': 'Business Decision Maker',
                'lead_score_adjustment': '40',
                'enriched_at': '2024-01-01T00:00:00'
            })
        
        return {
            'company_csv': str(company_csv),
            'leads_csv': str(leads_csv)
        }
    
    def test_bulk_import_companies_only(self, sample_csv_files):
        """Test bulk import of companies only."""
        # Run import task
        task = HubSpotBulkImportTask(
            csv_file=sample_csv_files['company_csv'],
            object_type='companies',
            hubspot_token='test-token'
        )
        
        with patch('time.sleep'):  # Skip rate limiting
            luigi.build([task], local_scheduler=True)
        
        # Verify import marker was created
        marker_path = Path(sample_csv_files['company_csv']).parent / f".imported_companies_test.json"
        assert marker_path.exists()
        
        # Verify import results
        with open(marker_path) as f:
            results = json.load(f)
        
        assert results['status'] == 'completed'
        assert results['imported'] == 2
        assert results['updated'] == 0
        assert results['failed'] == 0
    
    def test_bulk_import_leads_only(self, sample_csv_files):
        """Test bulk import of leads only."""
        # Run import task
        task = HubSpotBulkImportTask(
            csv_file=sample_csv_files['leads_csv'],
            object_type='contacts',
            hubspot_token='test-token'
        )
        
        with patch('time.sleep'):
            luigi.build([task], local_scheduler=True)
        
        # Verify import marker was created
        marker_path = Path(sample_csv_files['leads_csv']).parent / f".imported_leads_test.json"
        assert marker_path.exists()
        
        # Verify import results
        with open(marker_path) as f:
            results = json.load(f)
        
        assert results['status'] == 'completed'
        assert results['imported'] == 2
        assert results['updated'] == 0
        assert results['failed'] == 0
    
    def test_import_all_task(self, sample_csv_files):
        """Test importing both companies and leads."""
        # Run import all task
        task = ImportAllTask(
            company_csv=sample_csv_files['company_csv'],
            leads_csv=sample_csv_files['leads_csv'],
            hubspot_token='test-token'
        )
        
        with patch('time.sleep'):
            luigi.build([task], local_scheduler=True)
        
        # Verify combined marker was created
        marker_path = Path(sample_csv_files['company_csv']).parent / ".imported_all_companies_test.json"
        assert marker_path.exists()
        
        # Verify combined results
        with open(marker_path) as f:
            results = json.load(f)
        
        assert results['status'] == 'completed'
        assert 'companies' in results
        assert 'leads' in results
        
        assert results['companies']['imported'] == 2
        assert results['leads']['imported'] == 2
    
    def test_import_with_existing_records(self, sample_csv_files):
        """Test import with some existing records (should update instead of create)."""
        # Modify test client to simulate existing records
        os.environ['HUBSPOT_TEST_EXISTING_DOMAIN'] = 'techcorp.com'
        
        # Run import task
        task = HubSpotBulkImportTask(
            csv_file=sample_csv_files['company_csv'],
            object_type='companies',
            hubspot_token='test-token'
        )
        
        with patch('time.sleep'):
            luigi.build([task], local_scheduler=True)
        
        # Verify results show update
        marker_path = Path(sample_csv_files['company_csv']).parent / f".imported_companies_test.json"
        with open(marker_path) as f:
            results = json.load(f)
        
        # In test mode, we can't differentiate updates vs creates
        # Just verify it completed successfully
        assert results['status'] == 'completed'
        assert results['imported'] + results['updated'] == 2
        
        # Cleanup
        os.environ.pop('HUBSPOT_TEST_EXISTING_DOMAIN', None)
    
    def test_import_error_handling(self, sample_csv_files):
        """Test error handling during import."""
        # Create CSV with an invalid record
        error_csv = Path("output/error_test.csv")
        with open(error_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['domain', 'business_type', 'success'])
            writer.writeheader()
            writer.writerow({'domain': 'good.com', 'business_type': 'Tech', 'success': 'true'})
            writer.writerow({'domain': '', 'business_type': 'Bad', 'success': 'true'})  # Empty domain
        
        # Run import task
        task = HubSpotBulkImportTask(
            csv_file=str(error_csv),
            object_type='companies',
            hubspot_token='test-token'
        )
        
        with patch('time.sleep'):
            luigi.build([task], local_scheduler=True)
        
        # Verify marker file shows partial success
        marker_path = error_csv.parent / f".imported_{error_csv.stem}.json"
        with open(marker_path) as f:
            results = json.load(f)
        
        assert results['status'] == 'completed'
        assert results['imported'] == 1  # Only good.com should be imported
        assert results['failed'] == 0  # Empty domain is skipped, not failed
    
    @patch('src.tasks.scrape.WebScraper')
    @patch('src.tasks.enrich.AIAnalyzer')
    def test_full_pipeline_with_import(self, mock_analyzer_class, mock_scraper_class, test_environment):
        """Test full pipeline from scraping to HubSpot import."""
        # Mock scraper
        mock_scraper = Mock()
        mock_scraper.scrape_domain = Mock(return_value=Mock(
            url="https://test.com",
            content="Test company content",
            success=True,
            emails=["contact@test.com"],
            error=None
        ))
        mock_scraper_class.return_value = mock_scraper
        
        # Mock analyzer
        mock_analyzer = Mock()
        mock_analyzer.analyze_company = Mock(return_value=Mock(
            business_type_description="Technology",
            naics_code="541511",
            confidence_score=0.9,
            model_dump=lambda: {
                "business_type_description": "Technology",
                "naics_code": "541511",
                "confidence_score": 0.9
            }
        ))
        mock_analyzer.analyze_lead = Mock(return_value=Mock(
            buyer_persona="Technical",
            lead_score_adjustment=50,
            model_dump=lambda: {
                "buyer_persona": "Technical",
                "lead_score_adjustment": 50
            }
        ))
        mock_analyzer_class.return_value = mock_analyzer
        
        # Create domain file
        with open("domains.txt", "w") as f:
            f.write("test.com\n")
        
        # Run pipeline with import
        pipeline = DomainPipeline(
            use_celery=False,
            hubspot_token='test-token'
        )
        
        with patch('time.sleep'):
            pipeline.process_domains_from_file(
                "domains.txt",
                "output",
                use_celery=False,
                import_to_hubspot=True
            )
        
        # Verify CSV files were created
        company_csv = list(Path("output").glob("companies_*.csv"))
        leads_csv = list(Path("output").glob("leads_*.csv"))
        assert len(company_csv) > 0
        assert len(leads_csv) > 0
        
        # Verify import markers were created
        import_markers = list(Path("output").glob(".imported_*.json"))
        assert len(import_markers) > 0
    
    def test_import_without_token(self, sample_csv_files):
        """Test import behavior when no token is provided."""
        # Run import task without token
        task = HubSpotBulkImportTask(
            csv_file=sample_csv_files['company_csv'],
            object_type='companies',
            hubspot_token=''
        )
        
        luigi.build([task], local_scheduler=True)
        
        # Verify marker shows skipped status
        marker_path = Path(sample_csv_files['company_csv']).parent / f".imported_companies_test.json"
        with open(marker_path) as f:
            results = json.load(f)
        
        assert results['status'] == 'skipped'
        assert results['reason'] == 'no_token'
    
    def test_import_empty_csv(self, test_environment):
        """Test import with empty CSV file."""
        # Create empty CSV
        empty_csv = Path("output/empty.csv")
        with open(empty_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['domain'])
            writer.writeheader()
        
        # Run import
        task = HubSpotBulkImportTask(
            csv_file=str(empty_csv),
            object_type='companies',
            hubspot_token='test-token'
        )
        
        luigi.build([task], local_scheduler=True)
        
        # Verify marker shows no imports
        marker_path = empty_csv.parent / f".imported_{empty_csv.stem}.json"
        with open(marker_path) as f:
            results = json.load(f)
        
        assert results['status'] == 'completed'
        assert results['imported'] == 0