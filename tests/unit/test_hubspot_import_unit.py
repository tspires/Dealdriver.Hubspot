"""Unit tests for HubSpot import tasks."""

import pytest
import json
import csv
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
import tempfile
import os
import sys

# Mock the common module
sys.modules['common'] = MagicMock()
sys.modules['common.clients'] = MagicMock()
sys.modules['common.clients.hubspot'] = MagicMock()

from src.tasks.hubspot_import import (
    HubSpotBulkImportTask,
    ImportCompaniesTask,
    ImportLeadsTask,
    ImportAllTask
)


class TestHubSpotBulkImportTask:
    """Test HubSpot bulk import task."""
    
    @pytest.fixture
    def temp_csv(self):
        """Create temporary CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=['domain', 'business_type', 'success'])
            writer.writeheader()
            writer.writerow({'domain': 'test.com', 'business_type': 'Tech', 'success': 'true'})
            writer.writerow({'domain': 'example.com', 'business_type': 'Retail', 'success': 'true'})
            temp_path = f.name
        
        yield temp_path
        
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)
    
    def test_init(self):
        """Test task initialization."""
        task = HubSpotBulkImportTask(
            csv_file='test.csv',
            object_type='companies',
            hubspot_token='test-token'
        )
        
        assert task.csv_file == 'test.csv'
        assert task.object_type == 'companies'
        assert task.hubspot_token == 'test-token'
    
    def test_output_marker_path(self, temp_csv):
        """Test output marker file path generation."""
        task = HubSpotBulkImportTask(
            csv_file=temp_csv,
            object_type='companies'
        )
        
        expected_marker = Path(temp_csv).parent / f".imported_{Path(temp_csv).stem}.json"
        assert task.output().path == str(expected_marker)
    
    @patch('src.tasks.hubspot_import.HubSpotBulkImportTask._read_csv_file')
    def test_run_no_token(self, mock_read_csv):
        """Test run without HubSpot token."""
        task = HubSpotBulkImportTask(
            csv_file='test.csv',
            object_type='companies',
            hubspot_token=''
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            marker_path = Path(temp_dir) / '.imported_test.json'
            with patch.object(task, 'output') as mock_output:
                mock_output.return_value.path = str(marker_path)
                
                task.run()
                
                # Should create marker file with skipped status
                assert marker_path.exists()
                with open(marker_path) as f:
                    data = json.load(f)
                assert data['status'] == 'skipped'
                assert data['reason'] == 'no_token'
    
    def test_read_csv_file(self, temp_csv):
        """Test reading CSV file."""
        task = HubSpotBulkImportTask(
            csv_file=temp_csv,
            object_type='companies'
        )
        
        records = task._read_csv_file()
        
        assert len(records) == 2
        assert records[0]['domain'] == 'test.com'
        assert records[0]['business_type'] == 'Tech'
        assert records[1]['domain'] == 'example.com'
    
    def test_read_csv_file_not_found(self):
        """Test reading non-existent CSV file."""
        task = HubSpotBulkImportTask(
            csv_file='nonexistent.csv',
            object_type='companies'
        )
        
        records = task._read_csv_file()
        assert records == []
    
    def test_prepare_company_properties(self):
        """Test preparing company properties for HubSpot API."""
        task = HubSpotBulkImportTask(
            csv_file='test.csv',
            object_type='companies'
        )
        
        record = {
            'domain': 'test.com',
            'business_type': 'Technology Company',
            'naics_code': '541512',
            'target_market': 'Enterprise',
            'confidence_score': '0.85',
            'products_services': 'Software;Consulting',
            'enriched_at': '2024-01-01T00:00:00'
        }
        
        properties = task._prepare_company_properties(record)
        
        assert properties['domain'] == 'test.com'
        assert properties['name'] == 'test.com'
        assert properties['business_type_description'] == 'Technology Company'
        assert properties['naics_code'] == '541512'
        assert properties['target_market'] == 'Enterprise'
        assert properties['confidence_score'] == '0.85'
        assert properties['primary_products_services'] == 'Software;Consulting'
        assert properties['enrichment_status'] == 'completed'
    
    def test_prepare_contact_properties(self):
        """Test preparing contact properties for HubSpot API."""
        task = HubSpotBulkImportTask(
            csv_file='test.csv',
            object_type='contacts'
        )
        
        record = {
            'email': 'john.doe@test.com',
            'first_name': 'John',
            'last_name': 'Doe',
            'company_domain': 'test.com',
            'buyer_persona': 'Technical Decision Maker',
            'lead_score_adjustment': '50',
            'enriched_at': '2024-01-01T00:00:00'
        }
        
        properties = task._prepare_contact_properties(record)
        
        assert properties['email'] == 'john.doe@test.com'
        assert properties['firstname'] == 'John'
        assert properties['lastname'] == 'Doe'
        assert properties['company'] == 'test.com'
        assert properties['buyer_persona'] == 'Technical Decision Maker'
        assert properties['lead_score_adjustment'] == '50'
        assert properties['enrichment_status'] == 'completed'
    
    @patch('common.clients.hubspot.HubSpotClient')
    def test_import_companies_success(self, mock_client_class, temp_csv):
        """Test successful company import."""
        # Mock HubSpot client
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock search - no existing companies
        mock_client.search_companies.return_value = {'results': []}
        
        # Mock create company
        mock_client.create_company.return_value = {'id': '12345'}
        
        task = HubSpotBulkImportTask(
            csv_file=temp_csv,
            object_type='companies',
            hubspot_token='test-token'
        )
        
        records = [
            {'domain': 'test.com', 'business_type': 'Tech', 'success': 'true'},
            {'domain': 'example.com', 'business_type': 'Retail', 'success': 'true'}
        ]
        
        with patch('time.sleep'):  # Skip rate limiting in tests
            results = task._import_companies(mock_client, records)
        
        assert results['status'] == 'completed'
        assert results['imported'] == 2
        assert results['updated'] == 0
        assert results['failed'] == 0
        assert mock_client.create_company.call_count == 2
    
    @patch('common.clients.hubspot.HubSpotClient')
    def test_import_companies_with_existing(self, mock_client_class):
        """Test company import with existing companies."""
        # Mock HubSpot client
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock search - find existing company
        mock_client.search_companies.side_effect = [
            {'results': [{'id': '12345', 'properties': {'domain': 'test.com'}}]},
            {'results': []}
        ]
        
        # Mock update company
        mock_client.update_company.return_value = {'id': '12345'}
        mock_client.create_company.return_value = {'id': '67890'}
        
        task = HubSpotBulkImportTask(
            csv_file='test.csv',
            object_type='companies',
            hubspot_token='test-token'
        )
        
        records = [
            {'domain': 'test.com', 'business_type': 'Tech', 'success': 'true'},
            {'domain': 'example.com', 'business_type': 'Retail', 'success': 'true'}
        ]
        
        with patch('time.sleep'):
            results = task._import_companies(mock_client, records)
        
        assert results['imported'] == 1
        assert results['updated'] == 1
        assert mock_client.update_company.call_count == 1
        assert mock_client.create_company.call_count == 1
    
    @patch('common.clients.hubspot.HubSpotClient')
    def test_import_contacts_success(self, mock_client_class):
        """Test successful contact import."""
        # Mock HubSpot client
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock search - no existing contacts
        mock_client.search_contacts.return_value = {'results': []}
        
        # Mock create contact
        mock_client.create_contact.return_value = {'id': '12345'}
        
        task = HubSpotBulkImportTask(
            csv_file='test.csv',
            object_type='contacts',
            hubspot_token='test-token'
        )
        
        records = [
            {
                'email': 'john@test.com',
                'first_name': 'John',
                'last_name': 'Doe',
                'company_domain': 'test.com'
            },
            {
                'email': 'jane@example.com',
                'first_name': 'Jane',
                'last_name': 'Smith',
                'company_domain': 'example.com'
            }
        ]
        
        with patch('time.sleep'):
            results = task._import_contacts(mock_client, records)
        
        assert results['status'] == 'completed'
        assert results['imported'] == 2
        assert results['updated'] == 0
        assert results['failed'] == 0
        assert mock_client.create_contact.call_count == 2
    
    @patch('common.clients.hubspot.HubSpotClient')
    def test_import_companies_with_errors(self, mock_client_class):
        """Test company import with errors."""
        # Mock HubSpot client
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock search
        mock_client.search_companies.return_value = {'results': []}
        
        # Mock create company - raise error for second company
        mock_client.create_company.side_effect = [
            {'id': '12345'},
            Exception("API Error")
        ]
        
        task = HubSpotBulkImportTask(
            csv_file='test.csv',
            object_type='companies',
            hubspot_token='test-token'
        )
        
        records = [
            {'domain': 'test.com', 'business_type': 'Tech', 'success': 'true'},
            {'domain': 'example.com', 'business_type': 'Retail', 'success': 'true'}
        ]
        
        with patch('time.sleep'):
            results = task._import_companies(mock_client, records)
        
        assert results['imported'] == 1
        assert results['failed'] == 1
        assert len(results['errors']) == 1
        assert 'API Error' in results['errors'][0]
    
    def test_find_company_by_domain(self):
        """Test finding company by domain."""
        mock_client = Mock()
        mock_client.search_companies.return_value = {
            'results': [{'id': '12345', 'properties': {'domain': 'test.com'}}]
        }
        
        task = HubSpotBulkImportTask(
            csv_file='test.csv',
            object_type='companies'
        )
        
        result = task._find_company_by_domain(mock_client, 'test.com')
        
        assert result is not None
        assert result['id'] == '12345'
        
        # Test not found
        mock_client.search_companies.return_value = {'results': []}
        result = task._find_company_by_domain(mock_client, 'notfound.com')
        assert result is None
    
    def test_find_contact_by_email(self):
        """Test finding contact by email."""
        mock_client = Mock()
        mock_client.search_contacts.return_value = {
            'results': [{'id': '12345', 'properties': {'email': 'john@test.com'}}]
        }
        
        task = HubSpotBulkImportTask(
            csv_file='test.csv',
            object_type='contacts'
        )
        
        result = task._find_contact_by_email(mock_client, 'john@test.com')
        
        assert result is not None
        assert result['id'] == '12345'
        
        # Test not found
        mock_client.search_contacts.return_value = {'results': []}
        result = task._find_contact_by_email(mock_client, 'notfound@test.com')
        assert result is None


class TestImportWrapperTasks:
    """Test import wrapper tasks."""
    
    def test_import_companies_task(self):
        """Test ImportCompaniesTask."""
        task = ImportCompaniesTask(
            company_csv='companies.csv',
            hubspot_token='test-token'
        )
        
        assert task.company_csv == 'companies.csv'
        assert task.hubspot_token == 'test-token'
        
        # Test output delegates to bulk import task
        bulk_task = HubSpotBulkImportTask(
            csv_file='companies.csv',
            object_type='companies',
            hubspot_token='test-token'
        )
        assert task.output().path == bulk_task.output().path
    
    def test_import_leads_task(self):
        """Test ImportLeadsTask."""
        task = ImportLeadsTask(
            leads_csv='leads.csv',
            hubspot_token='test-token'
        )
        
        assert task.leads_csv == 'leads.csv'
        assert task.hubspot_token == 'test-token'
        
        # Test output delegates to bulk import task
        bulk_task = HubSpotBulkImportTask(
            csv_file='leads.csv',
            object_type='contacts',
            hubspot_token='test-token'
        )
        assert task.output().path == bulk_task.output().path
    
    def test_import_all_task_requirements(self):
        """Test ImportAllTask requirements."""
        task = ImportAllTask(
            company_csv='companies.csv',
            leads_csv='leads.csv',
            hubspot_token='test-token'
        )
        
        requirements = task.requires()
        assert len(requirements) == 2
        
        # Check company import task
        assert isinstance(requirements[0], ImportCompaniesTask)
        assert requirements[0].company_csv == 'companies.csv'
        
        # Check leads import task
        assert isinstance(requirements[1], ImportLeadsTask)
        assert requirements[1].leads_csv == 'leads.csv'
    
    def test_import_all_task_output(self):
        """Test ImportAllTask output marker."""
        task = ImportAllTask(
            company_csv='/path/to/companies_20240101.csv',
            leads_csv='/path/to/leads_20240101.csv',
            hubspot_token='test-token'
        )
        
        expected_marker = Path('/path/to/.imported_all_companies_20240101.json')
        assert task.output().path == str(expected_marker)