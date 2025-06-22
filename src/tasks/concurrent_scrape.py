"""Concurrent scraping implementation using Celery batch processing."""

import logging
from typing import List, Dict, Any
from celery import group
from celery_app import app
from src.tasks.celery_tasks import scrape_domain

logger = logging.getLogger(__name__)


@app.task(bind=True, name='tasks.batch_scrape_domains')
def batch_scrape_domains(self, domains: List[str], batch_size: int = 10) -> Dict[str, Any]:
    """
    Scrape multiple domains concurrently using Celery.
    
    Args:
        domains: List of domains to scrape
        batch_size: Number of domains to process concurrently
        
    Returns:
        Dictionary mapping domains to their scrape results
    """
    logger.info(f"Starting batch scrape of {len(domains)} domains in batches of {batch_size}")
    
    results = {}
    
    # Process domains in batches
    for i in range(0, len(domains), batch_size):
        batch = domains[i:i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} domains")
        
        # Create a group of scraping tasks
        job = group(scrape_domain.s(domain) for domain in batch)
        
        # Execute the group and wait for results
        batch_results = job.apply_async().get(timeout=300)  # 5 minute timeout
        
        # Map results back to domains
        for domain, result in zip(batch, batch_results):
            results[domain] = result
            logger.info(f"Scraped {domain}: {'success' if result.get('success') else 'failed'}")
    
    logger.info(f"Batch scraping complete. Success rate: {sum(1 for r in results.values() if r.get('success'))}/{len(domains)}")
    return results


class ConcurrentScrapingStrategy:
    """Strategy for optimizing scraping performance."""
    
    @staticmethod
    def estimate_optimal_batch_size(num_domains: int, available_workers: int = 5) -> int:
        """
        Estimate optimal batch size based on domains and workers.
        
        Args:
            num_domains: Total number of domains to scrape
            available_workers: Number of Celery workers available
            
        Returns:
            Optimal batch size
        """
        # Base batch size on available workers
        base_batch = available_workers * 2  # Allow some queueing
        
        # Adjust based on total domains
        if num_domains < 10:
            return num_domains  # Small batches for small sets
        elif num_domains < 50:
            return min(10, base_batch)
        elif num_domains < 200:
            return min(20, base_batch)
        else:
            return min(50, base_batch)
    
    @staticmethod
    def should_use_concurrent(num_domains: int) -> bool:
        """
        Determine if concurrent scraping should be used.
        
        Args:
            num_domains: Number of domains to scrape
            
        Returns:
            True if concurrent scraping is beneficial
        """
        # Use concurrent for more than 3 domains
        return num_domains > 3


# Example usage in pipeline
def scrape_domains_optimized(domains: List[str]) -> Dict[str, Any]:
    """
    Scrape domains using the most optimal strategy.
    
    Args:
        domains: List of domains to scrape
        
    Returns:
        Scraping results by domain
    """
    strategy = ConcurrentScrapingStrategy()
    
    if strategy.should_use_concurrent(len(domains)):
        # Use concurrent scraping
        batch_size = strategy.estimate_optimal_batch_size(len(domains))
        logger.info(f"Using concurrent scraping with batch size {batch_size}")
        return batch_scrape_domains.delay(domains, batch_size).get()
    else:
        # Use sequential scraping for small sets
        logger.info("Using sequential scraping for small domain set")
        results = {}
        for domain in domains:
            results[domain] = scrape_domain.delay(domain).get()
        return results