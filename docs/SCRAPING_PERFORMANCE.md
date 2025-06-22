# Scraping Performance Optimization Guide

## Current Performance Bottlenecks

1. **New Browser Instance Per Request**: Each scrape creates a new Chrome browser instance
2. **Fixed Wait Times**: Hard-coded 2-second wait for JavaScript
3. **Serial Processing**: Domains processed one at a time in Luigi tasks
4. **Full Page Loads**: Loading all resources including images/CSS
5. **No Caching**: Re-scraping same domains multiple times

## Optimization Strategies

### 1. Browser Session Pool (High Impact)
```python
# Reuse browser instances across requests
# Benefits: 
# - Eliminate browser startup time (~2-3 seconds per request)
# - Reduce memory usage
# - Faster overall throughput

# Implementation:
# - Pool of 5-10 browser instances
# - Recycle after 50 requests or 30 minutes
# - Thread-safe queue management
# - Automatic session cleanup on errors
```

### 2. Concurrent Scraping (High Impact)
```python
# Use asyncio or threading for parallel scrapes
from concurrent.futures import ThreadPoolExecutor

# Scrape multiple domains simultaneously
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(scrape_domain, domain) for domain in domains]
    results = [f.result() for f in futures]
```

### 3. Smart Wait Strategies (Medium Impact)
```python
# Replace fixed waits with dynamic conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Wait for specific elements instead of fixed time
wait = WebDriverWait(driver, 10)
wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
```

### 4. Optimize Browser Settings (Medium Impact)
```python
chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--disable-web-security')
chrome_options.add_argument('--disable-features=VizDisplayCompositor')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')

# Disable images for faster loads
prefs = {"profile.managed_default_content_settings.images": 2}
chrome_options.add_experimental_option("prefs", prefs)

# Use Chrome page load strategy
chrome_options.page_load_strategy = 'eager'  # Don't wait for all resources
```

### 5. Implement Caching (Medium Impact)
```python
# Cache scraped content with TTL
from functools import lru_cache
from datetime import datetime, timedelta

class ScrapedContentCache:
    def __init__(self, ttl_hours=24):
        self.cache = {}
        self.ttl = timedelta(hours=ttl_hours)
    
    def get(self, domain):
        if domain in self.cache:
            content, timestamp = self.cache[domain]
            if datetime.now() - timestamp < self.ttl:
                return content
        return None
    
    def set(self, domain, content):
        self.cache[domain] = (content, datetime.now())
```

### 6. Request-Level Optimizations (Low Impact)
```python
# Use HEAD requests to check if site is accessible
import requests

def pre_check_domain(domain):
    try:
        response = requests.head(f"https://{domain}", timeout=5)
        return response.status_code < 400
    except:
        return False

# Only scrape if domain is accessible
if pre_check_domain(domain):
    scrape_result = scraper.scrape_domain(domain)
```

### 7. Celery Task Optimization
```python
# Configure Celery for scraping workload
CELERY_TASK_ROUTES = {
    'scraping.*': {'queue': 'scraping', 'routing_key': 'scraping'},
}

# Increase concurrency for scraping workers
# celery -A celery_app worker --queues=scraping --concurrency=10

# Use prefetch multiplier to process more tasks
CELERY_WORKER_PREFETCH_MULTIPLIER = 4
```

### 8. Network-Level Optimizations
```python
# Use DNS caching
import socket
socket.setdefaulttimeout(10)

# Configure connection pooling in requests
from requests.adapters import HTTPAdapter
session = requests.Session()
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('http://', adapter)
session.mount('https://', adapter)
```

## Implementation Priority

1. **Browser Pool** (est. 50-70% improvement)
   - Biggest impact on performance
   - Reduces startup overhead
   
2. **Concurrent Scraping** (est. 3-5x speedup with 5 workers)
   - Parallel processing of domains
   - Better resource utilization
   
3. **Smart Waits** (est. 20-30% improvement)
   - Reduce unnecessary waiting
   - Faster page processing
   
4. **Browser Settings** (est. 10-20% improvement)
   - Disable unnecessary features
   - Faster page loads
   
5. **Caching** (depends on repeat rate)
   - Avoid re-scraping same domains
   - Immediate response for cached content

## Monitoring Performance

```python
import time
from contextlib import contextmanager

@contextmanager
def timer(name):
    start = time.time()
    yield
    logger.info(f"{name} took {time.time() - start:.2f} seconds")

# Usage
with timer("Scraping example.com"):
    result = scraper.scrape_domain("example.com")
```

## Configuration Example

```python
# scraper_config.py
SCRAPER_CONFIG = {
    'browser_pool': {
        'enabled': True,
        'max_sessions': 5,
        'max_requests_per_session': 50,
        'session_timeout_minutes': 30
    },
    'concurrency': {
        'max_workers': 5,
        'timeout_seconds': 30
    },
    'optimization': {
        'disable_images': True,
        'page_load_strategy': 'eager',
        'wait_strategy': 'dynamic'
    },
    'caching': {
        'enabled': True,
        'ttl_hours': 24
    }
}
```

## Expected Performance Gains

With all optimizations:
- **Current**: ~5-10 seconds per domain
- **Optimized**: ~1-2 seconds per domain
- **Throughput**: From 6-12 domains/minute to 30-60 domains/minute
- **Resource Usage**: 50% less memory with browser pooling
- **Reliability**: Better error handling and recovery

## Testing Performance

```bash
# Create performance test script
python scripts/test_scraping_performance.py --domains 100 --workers 5

# Monitor with htop/top during execution
# Track metrics: CPU, Memory, Network I/O
# Log analysis for bottlenecks
```