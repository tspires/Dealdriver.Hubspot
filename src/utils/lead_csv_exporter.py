"""CSV export utilities for leads data in HubSpot format."""

import csv
import logging
import os
import threading
from typing import Dict, List, Any, Optional
from datetime import datetime

from src.constants import (
    HUBSPOT_NAME_LIMIT,
    HUBSPOT_DESCRIPTION_LIMIT,
    HUBSPOT_STANDARD_FIELD_LIMIT,
    ENRICHMENT_STATUS_COMPLETED
)

logger = logging.getLogger(__name__)


class LeadCSVExporter:
    """Export lead data to CSV format for HubSpot import."""
    
    def __init__(self, filename: str) -> None:
        """Initialize CSV exporter with output filename."""
        self.filename = filename
        self.rows: List[Dict[str, str]] = []
        self._file_handle = None
        self._csv_writer = None
        self._headers_written = False
        self._write_lock = threading.Lock()
    
    def add_lead(self, email: str, domain: str, lead_data: Optional[Dict[str, Any]] = None) -> None:
        """Add lead data to export."""
        email = email.strip().lower()
        row = self._build_csv_row(email, domain, lead_data or {})
        self.rows.append(row)
        logger.debug(f"Added lead {email} to export queue")
    
    def add_leads_from_scraped_emails(self, domain: str, emails: List[str], company_data: Optional[Dict[str, Any]] = None) -> None:
        """Add multiple leads from scraped emails."""
        for email in emails:
            lead_data = {
                "company": company_data.get("name", domain) if company_data else domain,
                "company_domain": domain,
                "lead_source": "Website Scraping",
                "enrichment_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            if company_data:
                # Add relevant company info to lead
                lead_data.update({
                    "company_industry": company_data.get("industry", ""),
                    "company_city": company_data.get("city", ""),
                    "company_state": company_data.get("state_region", ""),
                    "company_country": company_data.get("country", ""),
                    "company_employees": company_data.get("number_of_employees", ""),
                    "company_revenue": company_data.get("annual_revenue", "")
                })
            self.add_lead(email, domain, lead_data)
    
    def write(self) -> None:
        """Write all collected data to CSV file."""
        if not self.rows:
            logger.warning("No lead data to export")
            return
        
        fieldnames = self._get_ordered_fieldnames()
        
        try:
            self._write_csv_file(fieldnames)
            logger.info(f"Exported {len(self.rows)} leads to {self.filename}")
        except Exception as e:
            logger.error(f"Failed to write lead CSV file: {e}")
            raise
    
    def _build_csv_row(self, email: str, domain: str, lead_data: Dict[str, Any]) -> Dict[str, str]:
        """Build a CSV row from lead data."""
        # Extract name parts from email if not provided
        email_prefix = email.split('@')[0]
        firstname = lead_data.get("firstname", "")
        lastname = lead_data.get("lastname", "")
        
        # If no name provided, try to guess from email
        if not firstname and not lastname:
            name_parts = email_prefix.replace('.', ' ').replace('_', ' ').replace('-', ' ').split()
            if len(name_parts) >= 2:
                firstname = name_parts[0].title()
                lastname = ' '.join(name_parts[1:]).title()
            elif len(name_parts) == 1:
                firstname = name_parts[0].title()
        
        row = {
            # Required fields for HubSpot contact import
            "Email": email,
            
            # Standard contact fields
            "First Name": self._truncate_field(firstname, HUBSPOT_NAME_LIMIT),
            "Last Name": self._truncate_field(lastname, HUBSPOT_NAME_LIMIT),
            "Company Name": self._truncate_field(
                lead_data.get("company", domain), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            
            # Additional fields
            "Website URL": f"https://{domain}",
            "Lead Status": "New",
            "Lifecycle Stage": "Lead",
            
            # Custom fields from enrichment
            "Lead Source": self._truncate_field(
                lead_data.get("lead_source", "Website Scraping"), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Buyer Persona": self._truncate_field(
                lead_data.get("buyer_persona", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Lead Score Adjustment": str(lead_data.get("lead_score_adjustment", "0")),
            
            # Company information
            "Company Domain": domain,
            "Company Industry": self._truncate_field(
                lead_data.get("company_industry", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Company City": self._truncate_field(
                lead_data.get("company_city", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Company State/Region": self._truncate_field(
                lead_data.get("company_state", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Company Country": self._truncate_field(
                lead_data.get("company_country", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Company Employee Count": self._truncate_field(
                lead_data.get("company_employees", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Company Annual Revenue": self._truncate_field(
                lead_data.get("company_revenue", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            
            # Metadata
            "Enrichment Status": lead_data.get(
                "enrichment_status", 
                ENRICHMENT_STATUS_COMPLETED
            ),
            "Enrichment Date": lead_data.get("enrichment_date", ""),
        }
        
        # Remove empty values but keep required fields
        return {k: v for k, v in row.items() if v or k in ["Email", "Enrichment Status"]}
    
    def _truncate_field(self, value: Any, limit: int) -> str:
        """Truncate field value to specified limit and strip newlines."""
        if not value:
            return ""
        # Convert to string, strip newlines, and truncate
        cleaned = str(value).replace('\n', ' ').replace('\r', ' ')
        return cleaned[:limit]
    
    def _get_ordered_fieldnames(self) -> List[str]:
        """Get ordered list of fieldnames for CSV."""
        # Get all unique fieldnames
        fieldnames = set()
        for row in self.rows:
            fieldnames.update(row.keys())
        
        # Sort with required fields first
        ordered = sorted(list(fieldnames))
        
        # Move required fields to front
        priority_fields = ["Email", "First Name", "Last Name", "Company Name"]
        for field in reversed(priority_fields):
            if field in ordered:
                ordered.remove(field)
                ordered.insert(0, field)
        
        return ordered
    
    def _write_csv_file(self, fieldnames: List[str]) -> None:
        """Write CSV file with given fieldnames."""
        with open(self.filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, restval='')
            writer.writeheader()
            writer.writerows(self.rows)
    
    def open_for_writing(self, append: bool = False) -> None:
        """Open file for incremental writing."""
        mode = 'a' if append and os.path.exists(self.filename) else 'w'
        self._file_handle = open(self.filename, mode, newline='', encoding='utf-8')
        self._headers_written = append and os.path.exists(self.filename) and os.path.getsize(self.filename) > 0
        
        if mode == 'a':
            logger.info(f"Opened {self.filename} for appending (resume mode)")
        else:
            logger.info(f"Opened {self.filename} for writing (new file)")
    
    def write_lead_incremental(self, email: str, domain: str, lead_data: Dict[str, Any]) -> None:
        """Write a single lead to CSV immediately (thread-safe)."""
        with self._write_lock:
            email = email.strip().lower()
            row = self._build_csv_row(email, domain, lead_data)
            
            if not self._csv_writer:
                # Initialize writer (for both new files and append mode)
                fieldnames = self._get_fieldnames_from_row(row)
                self._csv_writer = csv.DictWriter(
                    self._file_handle, 
                    fieldnames=fieldnames, 
                    restval=''
                )
                
                if not self._headers_written:
                    self._csv_writer.writeheader()
                    self._headers_written = True
            
            self._csv_writer.writerow(row)
            self._file_handle.flush()  # Ensure data is written to disk
            logger.debug(f"Wrote lead {email} to CSV")
    
    def close(self) -> None:
        """Close the file handle."""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None
            logger.info(f"Closed {self.filename}")
    
    def _get_fieldnames_from_row(self, row: Dict[str, str]) -> List[str]:
        """Get ordered fieldnames from a single row."""
        # Start with all possible fields that could be in any row
        all_possible_fields = [
            # Required fields
            "Email",
            "First Name", 
            "Last Name",
            "Company Name",
            
            # Other standard fields
            "Company Domain Name",
            "Job Title",
            "Phone Number",
            "Lead Source",
            
            # Company info fields
            "Company Industry",
            "Company City",
            "Company State/Region",
            "Company Country",
            "Company Employee Count",
            "Company Annual Revenue",
            
            # Metadata fields
            "Enrichment Status",
            "Enrichment Date"
        ]
        
        # Add any additional fields from the row that aren't in our list
        for field in row.keys():
            if field not in all_possible_fields:
                all_possible_fields.append(field)
        
        return all_possible_fields