#!/usr/bin/env python3
"""Test script to measure performance improvements."""

import time
import sys
import os
import tempfile
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.scraper import WebScraper
from src.utils.performance_monitor import get_performance_monitor

# Test domains
TEST_DOMAINS = [
    "example.com",
    "python.org",
    "github.com",
    "stackoverflow.com",
    "docker.com"
]


def test_scraping_performance():
    """Test scraping performance with and without browser pool."""
    
    print("\n" + "="*60)
    print("SCRAPING PERFORMANCE TEST")
    print("="*60)
    
    # Test 1: Without browser pool
    print("\n1. Testing WITHOUT browser pool (new browser for each domain)...")
    monitor1 = get_performance_monitor()
    scraper1 = WebScraper(use_browser_pool=False)
    
    start_time = time.time()
    for domain in TEST_DOMAINS:
        print(f"   Scraping {domain}...", end="", flush=True)
        result = scraper1.scrape_domain(domain)
        print(f" {'✓' if result.success else '✗'}")
    
    no_pool_duration = time.time() - start_time
    report1 = monitor1.get_report()
    
    # Test 2: With browser pool
    print("\n2. Testing WITH browser pool (reusing browser sessions)...")
    # Reset performance monitor
    from src.utils.performance_monitor import _performance_monitor
    _performance_monitor = None
    
    monitor2 = get_performance_monitor()
    scraper2 = WebScraper(use_browser_pool=True)
    
    start_time = time.time()
    for domain in TEST_DOMAINS:
        print(f"   Scraping {domain}...", end="", flush=True)
        result = scraper2.scrape_domain(domain)
        print(f" {'✓' if result.success else '✗'}")
    
    pool_duration = time.time() - start_time
    report2 = monitor2.get_report()
    
    # Print results
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    
    print("\nWithout Browser Pool:")
    print(f"  Total Time: {no_pool_duration:.2f}s")
    print(f"  Average per Domain: {report1.average_duration:.2f}s")
    print(f"  Throughput: {report1.domains_per_minute:.1f} domains/minute")
    
    print("\nWith Browser Pool:")
    print(f"  Total Time: {pool_duration:.2f}s")
    print(f"  Average per Domain: {report2.average_duration:.2f}s")
    print(f"  Throughput: {report2.domains_per_minute:.1f} domains/minute")
    
    improvement = ((no_pool_duration - pool_duration) / no_pool_duration) * 100
    speedup = no_pool_duration / pool_duration if pool_duration > 0 else 0
    
    print(f"\nPerformance Improvement:")
    print(f"  Time Saved: {no_pool_duration - pool_duration:.2f}s ({improvement:.1f}%)")
    print(f"  Speedup: {speedup:.2f}x faster")
    
    # Print browser pool stats
    if scraper2.browser_pool:
        stats = scraper2.browser_pool.get_stats()
        print(f"\nBrowser Pool Statistics:")
        print(f"  Sessions Created: {stats['sessions_created']}")
        print(f"  Sessions Recycled: {stats['sessions_recycled']}")
        print(f"  Pool Hit Rate: {stats['hit_rate']:.1%}")
        
        # Cleanup
        scraper2.browser_pool.close_all()
    
    # Print detailed performance reports
    print("\n" + "-"*60)
    print("DETAILED PERFORMANCE REPORT (Without Pool):")
    report1.print_summary()
    
    print("\n" + "-"*60)
    print("DETAILED PERFORMANCE REPORT (With Pool):")
    report2.print_summary()


def test_concurrent_vs_sequential():
    """Test concurrent scraping vs sequential."""
    from src.pipeline import DomainPipeline
    import tempfile
    
    print("\n" + "="*60)
    print("CONCURRENT VS SEQUENTIAL PIPELINE TEST")
    print("="*60)
    
    # Create test file with domains
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        for domain in TEST_DOMAINS:
            f.write(f"{domain}\n")
        test_file = f.name
    
    try:
        # Test sequential (Luigi only)
        print("\n1. Testing SEQUENTIAL processing (Luigi only)...")
        pipeline1 = DomainPipeline(use_celery=False)
        
        start_time = time.time()
        with tempfile.TemporaryDirectory() as output_dir:
            pipeline1.process_domains_from_file(test_file, output_dir, use_celery=False)
        sequential_duration = time.time() - start_time
        
        print(f"Sequential processing took: {sequential_duration:.2f}s")
        
        # Note: Concurrent Celery test requires running workers
        print("\n2. Concurrent processing requires Celery workers to be running.")
        print("   To test concurrent processing:")
        print("   1. Start Redis: redis-server")
        print("   2. Start Celery workers: python scripts/start_celery_workers.py")
        print("   3. Run: python run.py --token YOUR_TOKEN --file domains.txt")
        
    finally:
        os.unlink(test_file)


if __name__ == "__main__":
    print("Starting performance tests...")
    
    # Test 1: Browser pool performance
    test_scraping_performance()
    
    # Test 2: Concurrent vs sequential
    test_concurrent_vs_sequential()
    
    print("\nPerformance tests complete!")