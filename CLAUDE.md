# Dealdriver.Hubspot Project Documentation

## Overview
This project provides integration with HubSpot CRM for lead and company enrichment using web scraping and AI analysis. It also supports bulk domain enrichment from files with CSV export for HubSpot import.

## Application Structure

```
dealdriver-hubspot/
├── config/                      # Configuration files
│   ├── __init__.py
│   ├── celery_config.py        # Celery configuration
│   └── settings.ini.template   # Settings template
├── data/                       # Pipeline data storage
│   ├── site_content/raw/       # Scraped content (JSON)
│   ├── enriched_companies/raw/ # Company enrichment (JSON)
│   └── enriched_leads/raw/     # Lead enrichment (JSON)
├── logs/                       # Application logs
├── output/                     # CSV export directory
├── scripts/                    # Utility scripts
│   ├── __init__.py
│   ├── start_celery_workers.py # Celery worker manager
│   ├── check_enrichment_status.py
│   ├── enrichment_summary.py
│   ├── run_batch_enrichment.py
│   └── test_imports.py
├── src/                        # Source code
│   ├── __init__.py
│   ├── main.py                # CLI entry point
│   ├── pipeline.py            # Pipeline orchestration
│   ├── constants.py           # Application constants
│   ├── cli/                   # CLI commands
│   ├── config/                # Configuration classes
│   ├── models/                # Data models
│   ├── services/              # Business logic
│   ├── tasks/                 # Luigi/Celery tasks
│   └── utils/                 # Utility functions
├── tests/                     # Test suite
│   └── __init__.py
├── celery_app.py             # Celery application
├── requirements.txt          # Python dependencies
├── setup.py                  # Package setup
├── MANIFEST.in              # Package manifest
├── README.md                # Project documentation
├── run.py                   # Main entry point
└── CLAUDE.md               # This file
```

### Main Entry Points
- `run.py`: Main executable script
- `src/main.py`: Application entry point with CLI argument parsing

### Models (`src/models/`)
- `hubspot.py`: 
  - `Lead`: HubSpot contact data model
  - `Company`: HubSpot company data model
- `enrichment.py`:
  - `ScrapedContent`: Web scraping result model
  - `LeadAnalysis`: AI analysis result for leads
  - `CompanyAnalysis`: AI analysis result for companies

### Services (`src/services/`)
- `hubspot_service.py`:
  - `HubSpotService`: HubSpot API interactions
    - `create_contact_properties()`: Creates custom contact fields
    - `create_company_properties()`: Creates custom company fields
    - `get_leads()`: Retrieves leads from HubSpot
    - `get_companies()`: Retrieves companies from HubSpot
    - `update_lead()`: Updates lead properties
    - `update_company()`: Updates company properties
- `scraper.py`:
  - `WebScraper`: Website content scraping service with requests-first approach
    - `__init__(use_browser_pool)`: Initialize with optional browser pool support
    - `scrape_url()`: Scrapes content from URL - tries requests first, falls back to Selenium
    - `scrape_domain()`: Scrapes content from domain
    - `extract_emails_from_html()`: Extracts emails matching domain from raw HTML
    - `_scrape_with_requests()`: Fast scraping using requests library (no JavaScript)
    - `_scrape_with_browser_pool()`: Uses persistent browser sessions from pool
    - `_scrape_with_new_browser()`: Fallback to create new browser per scrape
- `analyzer.py`:
  - `AIAnalyzer`: AI-powered content analysis using DeepSeek common library
    - `analyze_lead()`: Analyzes content for lead scoring
    - `analyze_company()`: Enhanced to use sophisticated `analyze_business_website()` method from common library
      - Now accepts domain and emails parameters for better context
      - Uses structured JSON output with HubSpot field alignment
      - Improved NAICS code accuracy from 33% to 67% in E2E tests
- `enrichment_service.py`:
  - `EnrichmentService`: Main orchestration service
    - `__init__(hubspot_service)`: Initialize service
    - `enrich_lead()`: Enriches a single lead
    - `enrich_company()`: Enriches a single company
- `concurrent_enrichment_service.py`:
  - `ConcurrentEnrichmentService`: Concurrent enrichment with multiprocessing
    - `__init__(num_workers)`: Initialize with worker count
    - `enrich_domains()`: Enriches multiple domains in parallel
    - `enrich_companies()`: Enriches multiple companies in parallel
    - `enrich_leads()`: Enriches multiple leads in parallel

### CLI (`src/cli/`)
- `commands.py`:
  - `EnrichmentCommand`: CLI command handler
    - `__init__()`: Initialize command handler
    - `create_custom_properties()`: Creates HubSpot custom fields
    - `process_leads()`: Processes and enriches leads
    - `process_companies()`: Processes and enriches companies
    - `process_file_domains()`: Processes domains from file and exports to CSV

### Utils (`src/utils/`)
- `domain.py`: Domain extraction utilities
  - `extract_domain()`: Extracts domain from email or URL
  - `normalize_url()`: Normalizes URL format
- `logging.py`: Logging configuration
  - `setup_logging()`: Configures application logging
- `csv_exporter.py`: CSV export functionality
  - `CSVExporter`: Exports enriched company data to HubSpot-compatible CSV
    - `add_company()`: Adds company data to export queue
    - `write()`: Writes all data to CSV file
- `lead_csv_exporter.py`: Lead CSV export functionality
  - `LeadCSVExporter`: Exports lead data to HubSpot-compatible CSV
    - `add_lead()`: Adds single lead to export queue
    - `add_leads_from_scraped_emails()`: Adds multiple leads from scraped emails
    - `write()`: Writes all data to CSV file
    - `write_lead_incremental()`: Writes lead immediately to CSV
- `rate_limiter.py`: Thread-safe rate limiting for APIs
  - `ThreadSafeRateLimiter`: Token bucket rate limiter
  - `APIRateLimitManager`: Manages rate limits for multiple APIs
- `multiprocessing_manager.py`: Multiprocessing pool management
  - `EnrichmentWorkerPool`: Manages worker pool for parallel processing
  - `batch_process_with_progress()`: Batch process items with progress tracking

### Config (`src/config/`)
- `settings.py`:
  - `Settings`: Application configuration dataclass

### Luigi Tasks (`src/tasks/`)
- `base.py`: Base task configuration
- `scrape.py`:
  - `ScrapeWebsiteTask`: Scrapes website and saves to JSON
- `enrich.py`:
  - `EnrichCompanyTask`: Enriches company data using AI
  - `EnrichLeadsTask`: Enriches lead data from scraped emails
- `export.py`:
  - `ExportCompanyCSVTask`: Exports company data to CSV
  - `ExportLeadsCSVTask`: Exports lead data to CSV
  - `ExportAllCSVTask`: Orchestrates both CSV exports
- `hubspot_import.py`:
  - `HubSpotBulkImportTask`: Imports CSV data to HubSpot
  - `ImportCompaniesTask`: Wrapper for company imports
  - `ImportLeadsTask`: Wrapper for lead imports
  - `ImportAllTask`: Orchestrates both imports
- `celery_tasks.py`:
  - Celery wrappers for Luigi tasks
  - `scrape_domain`: Celery task for scraping
  - `enrich_company`: Celery task for company enrichment
  - `enrich_leads`: Celery task for lead enrichment
  - `export_company_csv`: Celery task for company CSV export
  - `export_leads_csv`: Celery task for lead CSV export
  - `process_domain_pipeline`: Complete pipeline for a domain

### Pipeline (`src/pipeline.py`)
- `DomainPipeline`: Main pipeline orchestrator
  - `__init__(use_celery, hubspot_token)`: Initialize with optional HubSpot token
  - `process_domains_from_file()`: Process domains with Luigi/Celery
  - `_process_with_celery()`: Distributed processing with Celery
  - `_process_with_luigi()`: Local processing with Luigi
  - `_import_to_hubspot()`: Import CSV files to HubSpot after processing
  - Fixed: Properly unpacks tuple from file processor (domains, errors)

### Celery Configuration
- `celery_app.py`: Main Celery application
- `config/celery_config.py`: Celery configuration settings
- Queue configuration (scraping, enrichment, export)
- Redis broker and backend

**Key Features:**
- **Luigi task orchestration**: DAG-based task dependencies
- **Celery distributed processing**: Parallel execution across workers
- **File-based intermediate storage**: JSON files for each processing stage
- Domain extraction from emails and URLs
- **Requests-first scraping**: Attempts fast requests-based scraping before falling back to Selenium
- Website content scraping using Selenium with JavaScript support (when needed)
- Email extraction from scraped pages (same-domain filtering)
- AI analysis using DeepSeek client
- Custom field creation in HubSpot
- Comprehensive logging for debugging and progress tracking
- Domain caching to avoid duplicate scraping
- Dual CSV export: companies and leads from scraped emails
- **Thread-safe rate limiting**: Respects API rate limits across workers
- **Configurable worker count**: Adjust concurrency with --workers flag
- **HubSpot bulk import**: Automatically import CSV results to HubSpot

## Modified Components

### Refactored Architecture
- **Removed browser manager**: Simplified to create new browser instances per scrape
- **Added Luigi tasks**: Three-stage pipeline (scrape → enrich → export)
- **Added Celery integration**: Distributed task execution across workers
- **File-based intermediate storage**: JSON files between pipeline stages

### HubSpot Client (`/home/tspires/Development/common/clients/hubspot.py`)
Added `iter_companies()` method to support iterating through all company records.

### Web Scraper (`src/services/scraper.py`)
- Now uses enhanced `SeleniumScraper` from common library
- Added domain validation before scraping attempts
- Improved error handling with safe cleanup
- Simplified to always create new browser instances

### Multiprocessing Manager (`src/utils/multiprocessing_manager.py`)
- Added graceful shutdown handling for KeyboardInterrupt
- Cancels pending futures on interrupt for faster exit

### Common Library Enhancements (`/home/tspires/Development/common/scrape/`)
- `selenium_scraper.py`: Enhanced with DNS validation and better error handling
  - `validate_domain()`: Pre-validates domains before scraping
  - `handle_alert_safely()`: Dismisses JavaScript alerts gracefully
  - `safe_cleanup()`: Ensures driver cleanup even during interruption
  - Enhanced `fetch_page()`: Better error handling with specific error messages
- `domain_validator.py`: Domain validation utilities (kept separate for reusability)
  - `DomainValidator`: Validates domains with DNS lookups
  - `is_valid_domain_format()`: Checks domain format validity
  - `check_dns_resolution()`: Verifies domain exists via DNS
  - `batch_validate_domains()`: Validates multiple domains in parallel

## Configuration Files

- `config/celery_config.py`: Celery broker, queue, and worker settings
- `config/settings.ini.template`: Template for application settings
- `setup.py`: Python package configuration
- `MANIFEST.in`: Package file inclusion rules
- `.gitignore`: Git ignore patterns
- `README.md`: User-facing project documentation

## New Components Added

### Multi-Page Scraping (`src/services/`)
- `html_aware_scraper.py`: Preserves HTML content for link extraction
  - `HTMLAwareScraper`: Enhanced scraper that returns both text and HTML
  - `ScrapedPage`: Data model with HTML content preservation
  - `extract_links_from_html()`: Extracts internal links from HTML
- `multi_page_scraper.py`: Implements depth-based web crawling
  - `MultiPageScraper`: Crawls websites up to specified depth
  - `scrape_multi_page()`: Queue-based crawling with depth tracking
  - `create_combined_content()`: Aggregates content from multiple pages
  - Respects robots.txt and adds delays between requests
- `multi_page_enrichment_service.py`: Enrichment with multi-page support
  - `MultiPageEnrichmentService`: Uses multi-page scraping for enrichment
  - Automatically combines content from multiple pages
  - Tracks pages scraped in enrichment results
- `multi_page_domain_enrichment_service.py`: Domain enrichment with multi-page support
  - `MultiPageDomainEnrichmentService`: Multi-page domain enrichment
  - Configurable scraping depth
  - Enhanced content analysis from multiple pages

### Constants (`src/constants.py`)
- Application-wide constants for field limits, statuses, and defaults

### File Processing (`src/utils/file_processor.py`)
- `DomainFileProcessor`: Handles reading and validating domains from files
  - `read_domains_from_file()`: Reads domains with deduplication
  - `validate_input_file()`: Validates file existence and readability

### Domain Enrichment Service (`src/services/domain_enrichment_service.py`)
- `DomainEnrichmentService`: Service for enriching individual domains
  - `enrich_domain()`: Enriches a single domain
  - `_build_enrichment_result()`: Formats enrichment results
  - `add_processing_delay()`: Manages rate limiting

## Dependencies
- HubSpot API access token
- DeepSeek API client
- Selenium-based web scraper
- Python packages from common library
- Redis server (for Celery distributed processing)
- Luigi (for task orchestration)
- Celery (for distributed task execution)

## Performance Optimizations

### Scraping Performance
- **Browser Pool** (`src/services/browser_pool.py`): Reuses browser instances
  - Maintains pool of up to 5 browser sessions
  - Eliminates ~2-3 seconds startup time per request
  - Automatic session recycling after 50 requests or 30 minutes
  - Thread-safe with queue management
- **Concurrent Scraping** (`src/tasks/concurrent_scrape.py`): Batch processing with Celery
  - Process multiple domains in parallel
  - Intelligent batch sizing based on domain count
  - 3-5x throughput improvement with proper workers
- **Performance Monitoring** (`src/utils/performance_monitor.py`): Tracks metrics
  - Per-domain scraping time and success rate
  - Throughput calculation (domains/minute)
  - Error categorization and reporting
  - Automatic logging of performance summary
- **Expected Performance Gains**:
  - Without optimizations: ~5-10 seconds per domain
  - With browser pool: ~2-4 seconds per domain
  - With concurrent processing: 30-60 domains/minute throughput
- See `docs/SCRAPING_PERFORMANCE.md` for detailed optimization guide
- Test performance with: `python scripts/test_performance.py`

## Custom HubSpot Properties Created

### Contact Properties
- `site_content`: Scraped website content
- `enrichment_status`: Status of enrichment process
- `enrichment_date`: Date when enriched
- `buyer_persona`: Identified buyer persona
- `lead_score_adjustment`: Suggested lead score adjustment

### Company Properties
- `site_content`: Scraped website content
- `enrichment_status`: Status of enrichment process
- `enrichment_date`: Date when enriched
- `business_type_description`: Business type description
- `naics_code`: Industry classification code
- `target_market`: Identified target market
- `primary_products_services`: Main products/services
- `value_propositions`: Key value propositions
- `competitive_advantages`: Competitive advantages
- `technologies_used`: Technologies used
- `certifications_awards`: Certifications and awards
- `pain_points_addressed`: Customer pain points addressed
- `confidence_score`: Analysis confidence score

## Configuration

### Scraping Depth
The application supports configurable scraping depth (default: 2):
- **Depth 0**: Scrape only the single specified page
- **Depth 1**: Scrape the main page and follow links one level deep
- **Depth 2**: Scrape up to two levels deep from the starting page

Configure via:
- Command line: `--scraping-depth 2`
- Environment variable: `SCRAPING_DEPTH=2`
- Config file: `scraping.depth: 2`

Multi-page crawling is now fully implemented:
- Automatically follows internal links up to the specified depth
- Combines content from all scraped pages for AI analysis
- Respects robots.txt and adds delays between page requests
- Limited to 10 pages per domain to prevent excessive crawling

## Usage

### Starting Celery Workers
```bash
# Start all Celery workers (in a separate terminal)
python scripts/start_celery_workers.py

# Or start workers manually:
celery -A celery_app worker --loglevel=info --queues=scraping --concurrency=2
celery -A celery_app worker --loglevel=info --queues=enrichment --concurrency=4
celery -A celery_app worker --loglevel=info --queues=export --concurrency=4
```

### Running the Pipeline
```bash
# Create custom properties
python run.py --token YOUR_TOKEN --create-properties

# Process domains from file with Celery (default)
python run.py --token YOUR_TOKEN --file domains.txt

# Process domains from file with Luigi local scheduler
python run.py --token YOUR_TOKEN --file domains.txt --no-celery

# Specify output directory (default: output)
python run.py --token YOUR_TOKEN --file domains.txt --output results

# Process single lead by email
python run.py --token YOUR_TOKEN --lead-email john.doe@example.com

# Process single lead by HubSpot ID
python run.py --token YOUR_TOKEN --lead-id 12345

# Process single company by domain
python run.py --token YOUR_TOKEN --company-domain example.com

# Process single company by HubSpot ID
python run.py --token YOUR_TOKEN --company-id 67890

# With debug logging
python run.py --token YOUR_TOKEN --log-level DEBUG

# Process and automatically import to HubSpot
python run.py --token YOUR_TOKEN --file domains.txt --import-to-hubspot
```

### File Processing
The `--file` option processes domains through a multi-stage pipeline:

1. **Scraping Stage**: Downloads website content and saves to `data/site_content/raw/`
2. **Enrichment Stage**: Analyzes content with AI and saves to `data/enriched_companies/raw/` and `data/enriched_leads/raw/`
3. **Export Stage**: Converts JSON to CSV in the output directory
4. **Import Stage** (optional): Imports CSV data to HubSpot when `--import-to-hubspot` is used

Output files (timestamped):
- **companies_YYYYMMDD_HHMMSS.csv**: Enriched company data
- **leads_YYYYMMDD_HHMMSS.csv**: Extracted leads with enrichment
- **.imported_*.json**: Import status markers (when using --import-to-hubspot)

The file format supports:
- One domain per line
- Comments (lines starting with #)
- Empty lines (ignored)
- Full URLs (domain will be extracted)

Email extraction features:
- Extracts email addresses from raw HTML using regex
- Filters to only include emails matching the scraped domain
- Automatically creates lead records with company context
- Attempts to parse first/last names from email addresses

### Pipeline Architecture
- **Luigi**: Manages task dependencies and ensures idempotency
- **Celery**: Distributes tasks across multiple workers for parallel processing
- **Redis**: Message broker for Celery (required for distributed mode)
- **File Storage**: Intermediate JSON files allow for resumable pipelines

### HubSpot Import
The `--import-to-hubspot` flag enables automatic bulk import of enriched data:
- Creates new companies or updates existing ones (matched by domain)
- Creates new contacts or updates existing ones (matched by email)
- Handles rate limiting and error recovery
- Generates import status reports (.imported_*.json files)
- Supports test mode for development (HUBSPOT_IMPORT_TEST_MODE environment variable)