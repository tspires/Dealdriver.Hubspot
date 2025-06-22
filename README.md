# Dealdriver HubSpot Integration

A Python application for enriching HubSpot CRM data through web scraping and AI analysis.

## Features

- **Web Scraping**: Automated website content extraction with JavaScript support
- **AI Analysis**: Company and lead enrichment using DeepSeek AI
- **HubSpot Integration**: Direct CRM updates with custom properties
- **Distributed Processing**: Luigi + Celery for scalable pipeline execution
- **Email Extraction**: Automatic lead discovery from scraped websites
- **CSV Export**: HubSpot-compatible export formats

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-org/dealdriver-hubspot.git
cd dealdriver-hubspot
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install Redis (for Celery):
```bash
# Ubuntu/Debian
sudo apt-get install redis-server

# macOS
brew install redis
```

4. Configure settings:
```bash
cp config/settings.ini.template config/settings.ini
# Edit config/settings.ini with your API tokens
```

## Usage

### Start Celery Workers

```bash
# Start all workers
python scripts/start_celery_workers.py

# Or start individually:
celery -A celery_app worker --loglevel=info --queues=scraping --concurrency=2
celery -A celery_app worker --loglevel=info --queues=enrichment --concurrency=4
celery -A celery_app worker --loglevel=info --queues=export --concurrency=4
```

### Process Domains

```bash
# Create HubSpot custom properties
python run.py --token YOUR_TOKEN --create-properties

# Process domains from file (with Celery)
python run.py --token YOUR_TOKEN --file domains.txt

# Process domains locally (without Celery)
python run.py --token YOUR_TOKEN --file domains.txt --no-celery

# Process with custom output directory
python run.py --token YOUR_TOKEN --file domains.txt --output results/
```

### Process Individual Records

```bash
# Process single lead
python run.py --token YOUR_TOKEN --lead-email john.doe@example.com

# Process single company
python run.py --token YOUR_TOKEN --company-domain example.com

# With custom scraping depth (default: 2)
python run.py --token YOUR_TOKEN --file domains.txt --scraping-depth 1
```

## Project Structure

```
dealdriver-hubspot/
├── config/                 # Configuration files
│   ├── __init__.py
│   ├── celery_config.py   # Celery configuration
│   └── settings.ini.template
├── data/                  # Pipeline data storage
│   ├── site_content/raw/  # Scraped content (JSON)
│   ├── enriched_companies/raw/  # Company enrichment (JSON)
│   └── enriched_leads/raw/      # Lead enrichment (JSON)
├── logs/                  # Application logs
├── output/                # CSV export directory
├── scripts/               # Utility scripts
│   ├── __init__.py
│   └── start_celery_workers.py
├── src/                   # Source code
│   ├── __init__.py
│   ├── main.py           # CLI entry point
│   ├── pipeline.py       # Pipeline orchestration
│   ├── constants.py      # Application constants
│   ├── cli/              # CLI commands
│   ├── config/           # Configuration classes
│   ├── models/           # Data models
│   ├── services/         # Business logic
│   ├── tasks/            # Luigi/Celery tasks
│   └── utils/            # Utility functions
├── tests/                # Test suite
│   └── __init__.py
├── celery_app.py         # Celery application
├── requirements.txt      # Python dependencies
├── run.py               # Main entry point
└── README.md            # This file
```

## Pipeline Architecture

The application uses a three-stage pipeline:

1. **Scraping Stage**: Downloads website content
2. **Enrichment Stage**: AI analysis of scraped content
3. **Export Stage**: CSV generation for HubSpot import

Each stage saves intermediate results as JSON files, enabling:
- Pipeline resumption on failure
- Debugging and inspection
- Reprocessing without re-scraping

## Development

### Running Tests

```bash
pytest tests/
```

### Code Style

This project follows [PEP 8](https://www.python.org/dev/peps/pep-0008/) and uses:
- Black for code formatting
- Flake8 for linting
- MyPy for type checking

```bash
black src/
flake8 src/
mypy src/
```

## License

[Your License Here]

## Contributing

[Your Contributing Guidelines Here]