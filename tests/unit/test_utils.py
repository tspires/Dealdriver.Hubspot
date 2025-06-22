"""Unit tests for utility functions."""

import pytest
from pathlib import Path
import tempfile
import os

from src.utils.domain import extract_domain, normalize_url
from src.utils.file_processor import DomainFileProcessor
from src.utils.csv_exporter import CSVExporter
from src.utils.lead_csv_exporter import LeadCSVExporter


class TestDomainUtils:
    """Test domain utility functions."""
    
    def test_extract_domain_from_email(self):
        """Test domain extraction from email."""
        assert extract_domain("user@example.com") == "example.com"
        assert extract_domain("admin@sub.example.com") == "sub.example.com"
        assert extract_domain("test@EXAMPLE.COM") == "example.com"
    
    def test_extract_domain_from_url(self):
        """Test domain extraction from URL."""
        assert extract_domain("https://example.com") == "example.com"
        assert extract_domain("http://www.example.com") == "example.com"
        assert extract_domain("https://sub.example.com/page") == "sub.example.com"
        assert extract_domain("example.com/page") == "example.com"
    
    def test_extract_domain_invalid(self):
        """Test domain extraction with invalid input."""
        assert extract_domain("") is None
        assert extract_domain("not-a-domain") is None
        assert extract_domain("@") is None
    
    def test_normalize_url(self):
        """Test URL normalization."""
        assert normalize_url("example.com") == "https://example.com"
        assert normalize_url("http://example.com") == "http://example.com"
        assert normalize_url("https://example.com") == "https://example.com"
        assert normalize_url("www.example.com") == "https://www.example.com"
        assert normalize_url("example.com/page") == "https://example.com/page"


class TestFileProcessor:
    """Test DomainFileProcessor."""
    
    def test_read_domains_from_file(self):
        """Test reading domains from file."""
        # Create temp file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("example.com\n")
            f.write("# This is a comment\n")
            f.write("\n")  # Empty line
            f.write("https://test.com/page\n")
            f.write("duplicate.com\n")
            f.write("duplicate.com\n")  # Duplicate
            temp_file = f.name
        
        try:
            processor = DomainFileProcessor()
            domains, errors = processor.read_domains_from_file(temp_file)
            
            assert len(domains) == 3  # Duplicates removed
            assert "example.com" in domains
            assert "test.com" in domains
            assert "duplicate.com" in domains
            assert len(errors) == 1  # One duplicate error
        finally:
            os.unlink(temp_file)
    
    def test_validate_input_file(self):
        """Test input file validation."""
        processor = DomainFileProcessor()
        
        # Test non-existent file - should return empty domains and error
        domains, errors = processor.read_domains_from_file(Path("non_existent.txt"))
        assert len(domains) == 0
        assert len(errors) == 1
        assert "No such file or directory" in errors[0]
        
        # Test valid file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("example.com\n")
            f.flush()
            temp_file = f.name
        
        try:
            domains, errors = processor.read_domains_from_file(Path(temp_file))
            assert len(domains) == 1  # Should read one domain
            assert domains[0] == "example.com"
        finally:
            os.unlink(temp_file)


class TestCSVExporter:
    """Test CSVExporter."""
    
    def test_add_company(self):
        """Test adding company to exporter."""
        exporter = CSVExporter("test.csv")
        
        company_data = {
            "name": "Example Corp",
            "business_type": "Software",
            "confidence_score": 0.9
        }
        
        exporter.add_company("example.com", company_data)
        assert len(exporter.rows) == 1
    
    def test_write_csv(self):
        """Test writing CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            temp_file = f.name
        
        exporter = CSVExporter(temp_file)
        
        # Add test data
        exporter.add_company("example1.com", {
            "name": "Example 1",
            "business_type": "Software"
        })
        exporter.add_company("example2.com", {
            "name": "Example 2",
            "business_type": "Hardware"
        })
        
        
        try:
            exporter.write()
            
            # Verify file contents
            import csv
            with open(temp_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            assert len(rows) == 2
            # Check that we have data
            assert len(rows) == 2
            # Domain field might be lowercase
            assert any('example1.com' in str(row.values()) for row in rows)
            assert any('example2.com' in str(row.values()) for row in rows)
        finally:
            os.unlink(temp_file)


class TestLeadCSVExporter:
    """Test LeadCSVExporter."""
    
    def test_add_lead(self):
        """Test adding lead to exporter."""
        exporter = LeadCSVExporter("test_leads.csv")
        
        lead_data = {
            "firstname": "John",
            "lastname": "Doe"
        }
        
        exporter.add_lead("john@example.com", "example.com", lead_data)
        assert len(exporter.rows) == 1
    
    def test_add_leads_from_scraped_emails(self):
        """Test adding leads from scraped emails."""
        exporter = LeadCSVExporter("test_leads.csv")
        
        emails = ["john.doe@example.com", "jane_smith@example.com"]
        company_data = {
            "name": "Example Corp",
            "industry": "Technology"
        }
        
        exporter.add_leads_from_scraped_emails("example.com", emails, company_data)
        
        assert len(exporter.rows) == 2
    
    def test_write_incremental(self):
        """Test incremental writing."""
        # Create temp file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            temp_file = f.name
        
        try:
            exporter = LeadCSVExporter(temp_file)
            
            # Open file for writing
            exporter.open_for_writing()
            
            # Write first lead
            lead1_data = {
                "firstname": "John",
                "lastname": "Doe"
            }
            exporter.write_lead_incremental("john@example.com", "example.com", lead1_data)
            
            # Write second lead
            lead2_data = {
                "firstname": "Jane",
                "lastname": "Smith"
            }
            exporter.write_lead_incremental("jane@example.com", "example.com", lead2_data)
            
            # Close file handle
            if exporter._file_handle:
                exporter._file_handle.close()
            
            # Verify file contents
            import csv
            with open(temp_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            assert len(rows) == 2
            assert rows[0]['Email'] == "john@example.com"
            assert rows[1]['Email'] == "jane@example.com"
        finally:
            os.unlink(temp_file)