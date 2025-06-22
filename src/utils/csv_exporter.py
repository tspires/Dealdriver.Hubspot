"""CSV export utilities for enriched company data."""

import csv
import logging
import os
import threading
from typing import Dict, List, Any, Set

from src.constants import (
    HUBSPOT_NAME_LIMIT,
    HUBSPOT_DESCRIPTION_LIMIT,
    HUBSPOT_STANDARD_FIELD_LIMIT,
    HUBSPOT_POSTAL_CODE_LIMIT,
    HUBSPOT_NAICS_CODE_LIMIT,
    ENRICHMENT_STATUS_COMPLETED
)

logger = logging.getLogger(__name__)


class CSVExporter:
    """Export enriched company data to CSV format for HubSpot import."""
    
    def __init__(self, filename: str) -> None:
        """Initialize CSV exporter with output filename."""
        self.filename = filename
        self.rows: List[Dict[str, str]] = []
        self._file_handle = None
        self._csv_writer = None
        self._headers_written = False
        self._processed_domains: set = set()
        self._write_lock = threading.Lock()
    
    def add_company(self, domain: str, company_data: Dict[str, Any]) -> None:
        """Add enriched company data to export."""
        domain = self._normalize_domain(domain)
        row = self._build_csv_row(domain, company_data)
        self.rows.append(row)
        logger.debug(f"Added company {domain} to export queue")
    
    def write(self) -> None:
        """Write all collected data to CSV file."""
        if not self.rows:
            logger.warning("No data to export")
            return
        
        fieldnames = self._get_ordered_fieldnames()
        
        try:
            self._write_csv_file(fieldnames)
            logger.info(f"Exported {len(self.rows)} companies to {self.filename}")
        except Exception as e:
            logger.error(f"Failed to write CSV file: {e}")
            raise
    
    def _normalize_domain(self, domain: str) -> str:
        """Normalize domain for consistency."""
        return domain.strip().lower()
    
    def _build_csv_row(self, domain: str, company_data: Dict[str, Any]) -> Dict[str, str]:
        """Build a CSV row from company data."""
        row = {
            # Required field
            "Company Domain Name": domain,
            
            # Standard HubSpot fields
            **self._build_standard_fields(domain, company_data),
            
            # Custom fields
            **self._build_custom_fields(company_data),
            
            # Metadata
            **self._build_metadata_fields(company_data)
        }
        
        # Add error field if present
        if "enrichment_error" in company_data:
            row["Enrichment Error"] = self._truncate_field(
                company_data["enrichment_error"], 
                HUBSPOT_STANDARD_FIELD_LIMIT
            )
        
        # Remove empty values but keep enrichment status
        return {k: v for k, v in row.items() if v or k == "Enrichment Status"}
    
    def _build_standard_fields(
        self, 
        domain: str, 
        company_data: Dict[str, Any]
    ) -> Dict[str, str]:
        """Build standard HubSpot fields."""
        return {
            "Name": self._truncate_field(
                company_data.get("name", domain), 
                HUBSPOT_NAME_LIMIT
            ),
            "Company Description": self._truncate_field(
                company_data.get("company_summary", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            ),
            "Industry": self._truncate_field(
                company_data.get("industry", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "City": self._truncate_field(
                company_data.get("city", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "State/Region": self._truncate_field(
                company_data.get("state_region", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Postal Code": self._truncate_field(
                company_data.get("postal_code", ""), 
                HUBSPOT_POSTAL_CODE_LIMIT
            ),
            "Country": self._truncate_field(
                company_data.get("country", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Timezone": self._truncate_field(
                company_data.get("timezone", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Number of Employees": self._truncate_field(
                company_data.get("number_of_employees", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
            "Annual Revenue": self._truncate_field(
                company_data.get("annual_revenue", ""), 
                HUBSPOT_STANDARD_FIELD_LIMIT
            ),
        }
    
    def _build_custom_fields(self, company_data: Dict[str, Any]) -> Dict[str, str]:
        """Build custom HubSpot fields."""
        return {
            "Business Type Description": self._truncate_field(
                company_data.get("business_type_description", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            ),
            "NAICS Code": self._truncate_field(
                company_data.get("naics_code", ""), 
                HUBSPOT_NAICS_CODE_LIMIT
            ),
            "Target Market": self._truncate_field(
                company_data.get("target_market", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            ),
            "Primary Products Services": self._truncate_field(
                company_data.get("primary_products_services", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            ),
            "Value Propositions": self._truncate_field(
                company_data.get("value_propositions", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            ),
            "Competitive Advantages": self._truncate_field(
                company_data.get("competitive_advantages", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            ),
            "Technologies Used": self._truncate_field(
                company_data.get("technologies_used", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            ),
            "Certifications Awards": self._truncate_field(
                company_data.get("certifications_awards", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            ),
            "Pain Points Addressed": self._truncate_field(
                company_data.get("pain_points_addressed", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            ),
            "Confidence Score": self._format_confidence_score(
                company_data.get("confidence_score")
            ),
        }
    
    def _build_metadata_fields(self, company_data: Dict[str, Any]) -> Dict[str, str]:
        """Build enrichment metadata fields."""
        return {
            "Enrichment Status": company_data.get(
                "enrichment_status", 
                ENRICHMENT_STATUS_COMPLETED
            ),
            "Site Content": self._truncate_field(
                company_data.get("site_content", ""), 
                HUBSPOT_DESCRIPTION_LIMIT
            )
        }
    
    def _truncate_field(self, value: Any, limit: int) -> str:
        """Truncate field value to specified limit and strip newlines."""
        if not value:
            return ""
        # Convert to string, strip newlines, and truncate
        cleaned = str(value).replace('\n', ' ').replace('\r', ' ')
        return cleaned[:limit]
    
    def _format_confidence_score(self, score: Any) -> str:
        """Format confidence score value."""
        if not score:
            return ""
        try:
            return str(round(float(score), 2))
        except (ValueError, TypeError):
            return ""
    
    def _get_ordered_fieldnames(self) -> List[str]:
        """Get ordered list of fieldnames for CSV."""
        # Get all unique fieldnames
        fieldnames = set()
        for row in self.rows:
            fieldnames.update(row.keys())
        
        # Sort with required fields first
        ordered = sorted(list(fieldnames))
        
        # Move required fields to front
        priority_fields = ["Company Domain Name", "Name"]
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
    
    def write_company_incremental(self, domain: str, company_data: Dict[str, Any]) -> None:
        """Write a single company to CSV immediately (thread-safe)."""
        with self._write_lock:
            domain = self._normalize_domain(domain)
            row = self._build_csv_row(domain, company_data)
            
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
            self.mark_domain_processed(domain)
            logger.debug(f"Wrote company {domain} to CSV")
    
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
            "Company Domain Name",
            "Name",
            
            # Standard HubSpot fields
            "Company Description",
            "Industry",
            "City",
            "State/Region",
            "Postal Code",
            "Country",
            "Timezone",
            "Number of Employees",
            "Annual Revenue",
            
            # Custom fields
            "Business Type Description",
            "NAICS Code",
            "Target Market",
            "Primary Products Services",
            "Value Propositions",
            "Competitive Advantages",
            "Technologies Used",
            "Certifications Awards",
            "Pain Points Addressed",
            "Confidence Score",
            
            # Metadata fields
            "Enrichment Status",
            "Site Content",
            "Enrichment Error"
        ]
        
        # Add any additional fields from the row that aren't in our list
        for field in row.keys():
            if field not in all_possible_fields:
                all_possible_fields.append(field)
        
        return all_possible_fields
    
    def load_existing_domains(self) -> Set[str]:
        """Load domains from existing CSV file if it exists."""
        if not os.path.exists(self.filename):
            logger.info(f"No existing CSV file found at {self.filename}")
            return set()
        
        domains = set()
        try:
            with open(self.filename, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if 'Company Domain Name' in row and row['Company Domain Name']:
                        domain = self._normalize_domain(row['Company Domain Name'])
                        domains.add(domain)
                        self._processed_domains.add(domain)
            
            logger.info(f"Loaded {len(domains)} existing domains from {self.filename}")
            return domains
        except Exception as e:
            logger.error(f"Failed to load existing CSV: {e}")
            return set()
    
    def is_domain_processed(self, domain: str) -> bool:
        """Check if a domain has already been processed (thread-safe)."""
        normalized = self._normalize_domain(domain)
        with self._write_lock:
            return normalized in self._processed_domains
    
    def mark_domain_processed(self, domain: str) -> None:
        """Mark a domain as processed (thread-safe)."""
        normalized = self._normalize_domain(domain)
        with self._write_lock:
            self._processed_domains.add(normalized)