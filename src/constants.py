"""Application constants organized by category."""

from enum import Enum


# HubSpot field length limits
class HubSpotLimits:
    """HubSpot API field length constraints."""
    NAME = 255
    DESCRIPTION = 65536
    STANDARD_FIELD = 255
    POSTAL_CODE = 40
    NAICS_CODE = 50


# Processing configuration
class ProcessingConfig:
    """Processing and timing configuration."""
    DOMAIN_DELAY_SECONDS = 2
    MIN_DOMAIN_LENGTH = 4
    DEFAULT_BATCH_SIZE = 10
    DEFAULT_SCRAPING_DEPTH = 2


# Enrichment status values
class EnrichmentStatus(str, Enum):
    """Enumeration of enrichment status values."""
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"
    IN_PROGRESS = "in_progress"


# Export configuration
class ExportConfig:
    """CSV export configuration."""
    DEFAULT_COMPANY_FILENAME = "enriched_companies.csv"
    DEFAULT_LEADS_FILENAME = "enriched_leads.csv"
    COMPANY_NAME_FORMAT = "{domain} ({owner})"


# Legacy constants for backward compatibility
HUBSPOT_NAME_LIMIT = HubSpotLimits.NAME
HUBSPOT_DESCRIPTION_LIMIT = HubSpotLimits.DESCRIPTION
HUBSPOT_STANDARD_FIELD_LIMIT = HubSpotLimits.STANDARD_FIELD
HUBSPOT_POSTAL_CODE_LIMIT = HubSpotLimits.POSTAL_CODE
HUBSPOT_NAICS_CODE_LIMIT = HubSpotLimits.NAICS_CODE

DOMAIN_PROCESSING_DELAY_SECONDS = ProcessingConfig.DOMAIN_DELAY_SECONDS
MIN_DOMAIN_LENGTH = ProcessingConfig.MIN_DOMAIN_LENGTH

ENRICHMENT_STATUS_COMPLETED = EnrichmentStatus.COMPLETED.value
ENRICHMENT_STATUS_FAILED = EnrichmentStatus.FAILED.value

DEFAULT_OUTPUT_FILENAME = ExportConfig.DEFAULT_COMPANY_FILENAME
COMPANY_NAME_WITH_OWNER_FORMAT = ExportConfig.COMPANY_NAME_FORMAT