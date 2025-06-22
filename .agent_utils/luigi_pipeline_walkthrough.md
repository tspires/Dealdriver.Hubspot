# Luigi Task Pipeline Walkthrough

## Overview

The Luigi pipeline implements a 4-stage data processing workflow for domain enrichment:

```
Domain → Scrape → Enrich → Export → (Optional) Import to HubSpot
```

## Task Dependency Graph

```
ExportAllCSVTask (Final Task)
    ├── ExportCompanyCSVTask
    │       └── EnrichCompanyTask
    │               └── ScrapeWebsiteTask
    └── ExportLeadsCSVTask
            └── EnrichLeadsTask
                    └── ScrapeWebsiteTask (same as above)
```

## Detailed Task Steps

### 1. **ScrapeWebsiteTask** (Stage 1: Web Scraping)
**File**: `src/tasks/scrape.py`

**Purpose**: Scrapes website content from a domain and saves raw HTML/text.

**Process**:
1. Takes a `domain` parameter (e.g., "example.com")
2. Uses `WebScraper` service to fetch website content
3. Extracts:
   - Raw text content
   - Email addresses found on the page
   - Success/failure status
4. Saves output to: `data/site_content/raw/{domain}.json`

**Output Structure**:
```json
{
  "domain": "example.com",
  "url": "https://example.com",
  "success": true,
  "scraped_at": "2024-01-01T12:00:00",
  "content": "Website text content...",
  "emails": ["contact@example.com", "info@example.com"],
  "error": null
}
```

### 2. **EnrichCompanyTask** (Stage 2A: Company AI Analysis)
**File**: `src/tasks/enrich.py`

**Purpose**: Uses AI to analyze scraped content and extract company information.

**Dependencies**: Requires `ScrapeWebsiteTask` to complete first.

**Process**:
1. Reads scraped content from Stage 1
2. If scraping was successful:
   - Sends content to AI analyzer (DeepSeek)
   - Extracts business information:
     - Industry, NAICS code
     - Products/services
     - Value propositions
     - Technologies used
     - Target market
     - Competitive advantages
3. Saves enriched data to: `data/enriched_companies/raw/{domain}.json`

**Output Structure**:
```json
{
  "domain": "example.com",
  "enriched_at": "2024-01-01T12:05:00",
  "success": true,
  "analysis": {
    "business_type_description": "Software Development",
    "naics_code": "541511",
    "target_market": "Small businesses",
    "primary_products_services": ["CRM Software", "Analytics"],
    "confidence_score": 0.85
    // ... more fields
  }
}
```

### 3. **EnrichLeadsTask** (Stage 2B: Lead AI Analysis)
**File**: `src/tasks/enrich.py`

**Purpose**: Analyzes scraped emails to create lead profiles.

**Dependencies**: Requires `ScrapeWebsiteTask` to complete first.

**Process**:
1. Reads email addresses from Stage 1
2. For each email found:
   - Parses email to extract first/last name
   - Uses AI to determine:
     - Buyer persona (Technical, Executive, etc.)
     - Lead score adjustment (-10 to +10)
     - Confidence level
3. Saves lead data to: `data/enriched_leads/raw/{domain}.json`

**Output Structure**:
```json
{
  "domain": "example.com",
  "enriched_at": "2024-01-01T12:05:00",
  "success": true,
  "leads": [
    {
      "email": "john.doe@example.com",
      "first_name": "John",
      "last_name": "Doe",
      "buyer_persona": "Technical Decision Maker",
      "lead_score_adjustment": 7
    }
  ]
}
```

### 4. **ExportCompanyCSVTask** (Stage 3A: Company CSV Export)
**File**: `src/tasks/export.py`

**Purpose**: Converts enriched company data to CSV format.

**Dependencies**: Requires `EnrichCompanyTask` to complete first.

**Process**:
1. Reads enriched company data from Stage 2A
2. Flattens nested JSON into CSV row
3. Appends to company CSV file (creates header if new file)
4. Handles multi-value fields with semicolon separation

**CSV Fields**:
- Basic: domain, success, error, enriched_at
- Business: business_type, naics_code, target_market
- Lists: products_services, value_propositions, technologies (semicolon-separated)

### 5. **ExportLeadsCSVTask** (Stage 3B: Leads CSV Export)
**File**: `src/tasks/export.py`

**Purpose**: Converts enriched lead data to CSV format.

**Dependencies**: Requires `EnrichLeadsTask` to complete first.

**Process**:
1. Reads enriched lead data from Stage 2B
2. Creates one CSV row per email/lead found
3. Appends to leads CSV file

**CSV Fields**:
- email, first_name, last_name, company_domain
- buyer_persona, lead_score_adjustment
- enriched_at, error

### 6. **ExportAllCSVTask** (Stage 3 Coordinator)
**File**: `src/tasks/export.py`

**Purpose**: Ensures both CSV exports complete before marking pipeline done.

**Dependencies**: 
- `ExportCompanyCSVTask`
- `ExportLeadsCSVTask`

**Process**:
1. Simply waits for both export tasks to complete
2. Logs completion
3. Acts as a synchronization point

### 7. **ImportAllTask** (Stage 4: Optional HubSpot Import)
**File**: `src/tasks/hubspot_import.py`

**Purpose**: Bulk imports CSV data into HubSpot CRM.

**Dependencies**: CSVs must exist (runs after pipeline completion)

**Process**:
1. Reads company CSV and imports to HubSpot companies
2. Reads leads CSV and imports to HubSpot contacts
3. Handles:
   - Duplicate detection (search by domain/email)
   - Update vs create logic
   - Rate limiting
   - Error tracking

## Pipeline Execution Flow

### Using Luigi Local Scheduler:
```python
# From pipeline.py
tasks = []
for domain in domains:
    task = ExportAllCSVTask(
        domain=domain,
        company_csv="output/companies.csv",
        leads_csv="output/leads.csv"
    )
    tasks.append(task)

luigi.build(tasks, local_scheduler=True)
```

### Using Celery (Distributed):
```python
# Each domain is processed as a separate Celery task
for domain in domains:
    result = process_domain_pipeline.delay(
        domain, company_csv, leads_csv
    )
```

## Key Features

1. **Dependency Management**: Luigi automatically handles task dependencies. If scraping fails, enrichment won't run.

2. **Idempotency**: Tasks check for existing output files. If a task output exists, Luigi skips re-running it.

3. **Failure Handling**: Each task saves an output file even on failure (with error info) to prevent infinite retries.

4. **Parallel Processing**: 
   - Multiple domains can be processed in parallel
   - Company and lead enrichment run independently after scraping

5. **Incremental Processing**: CSVs are appended to, allowing batch processing of large domain lists.

## File Structure After Processing

```
data/
├── site_content/raw/
│   ├── example.com.json
│   └── another.com.json
├── enriched_companies/raw/
│   ├── example.com.json
│   └── another.com.json
└── enriched_leads/raw/
    ├── example.com.json
    └── another.com.json

output/
├── companies_20240622_120000.csv
├── leads_20240622_120000.csv
└── .imported_all_companies_20240622_120000.json  # Import marker
```

## Example Command

```bash
# Process domains from file with Luigi
python run.py --token YOUR_TOKEN --file domains.txt --no-celery

# Process with HubSpot import
python run.py --token YOUR_TOKEN --file domains.txt --import-to-hubspot
```

## Monitoring Progress

Luigi provides:
- Console output showing task progress
- Task completion status
- Error messages for failed tasks
- Dependency resolution information

Example output:
```
INFO: [pid 12345] Running ScrapeWebsiteTask(domain=example.com)
INFO: [pid 12345] Done ScrapeWebsiteTask(domain=example.com)
INFO: [pid 12345] Running EnrichCompanyTask(domain=example.com)
INFO: [pid 12345] Running EnrichLeadsTask(domain=example.com)
...
===== Luigi Execution Summary =====
Scheduled 6 tasks of which:
* 6 ran successfully
```