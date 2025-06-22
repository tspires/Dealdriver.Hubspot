import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import queue
import threading
import time
from functools import partial

from src.utils.rate_limiter import rate_limit_manager


logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """Configuration for worker processes"""
    num_workers: int = 4
    use_processes: bool = False  # Use threads by default for better shared state
    timeout: int = 300  # 5 minutes timeout per task
    batch_size: int = 10


class EnrichmentWorkerPool:
    """Manages a pool of workers for parallel enrichment processing"""
    
    def __init__(self, config: WorkerConfig):
        self.config = config
        self.executor = None
        self.futures = []
        self.results = []
        self.errors = []
        self.progress_lock = threading.Lock()
        self.processed_count = 0
        self.total_count = 0
        
    def __enter__(self):
        if self.config.use_processes:
            self.executor = ProcessPoolExecutor(max_workers=self.config.num_workers)
        else:
            self.executor = ThreadPoolExecutor(max_workers=self.config.num_workers)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.executor:
            # Handle graceful shutdown
            if exc_type == KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down gracefully...")
                # Cancel pending futures
                for future in self.futures:
                    if not future.done():
                        future.cancel()
                
                # Shutdown with shorter wait to exit faster
                self.executor.shutdown(wait=True, cancel_futures=True)
            else:
                self.executor.shutdown(wait=True)
    
    def submit_task(self, func: Callable, *args, **kwargs) -> None:
        """Submit a task to the worker pool"""
        if not self.executor:
            raise RuntimeError("Worker pool not initialized. Use with statement.")
        
        future = self.executor.submit(func, *args, **kwargs)
        self.futures.append(future)
    
    def process_batch(self, items: List[Any], process_func: Callable, 
                     callback: Optional[Callable] = None) -> Tuple[List[Any], List[Dict]]:
        """
        Process a batch of items in parallel.
        
        Args:
            items: List of items to process
            process_func: Function to process each item
            callback: Optional callback for progress updates
            
        Returns:
            Tuple of (results, errors)
        """
        self.total_count = len(items)
        self.processed_count = 0
        results = []
        errors = []
        
        # Submit all tasks
        for item in items:
            self.submit_task(self._process_with_error_handling, item, process_func)
        
        # Collect results as they complete
        try:
            for future in as_completed(self.futures, timeout=self.config.timeout):
                try:
                    result, error = future.result()
                    
                    with self.progress_lock:
                        self.processed_count += 1
                        
                        if error:
                            errors.append(error)
                            logger.error(f"Error processing item: {error}")
                        else:
                            results.append(result)
                        
                        if callback:
                            callback(self.processed_count, self.total_count, result, error)
                            
                except Exception as e:
                    logger.error(f"Future execution failed: {str(e)}")
                    errors.append({
                        'error': str(e),
                        'type': 'future_execution_error'
                    })
        except KeyboardInterrupt:
            logger.info("Batch processing interrupted by user")
            # Cancel remaining futures
            for future in self.futures:
                if not future.done():
                    future.cancel()
            raise
        
        return results, errors
    
    def _process_with_error_handling(self, item: Any, process_func: Callable) -> Tuple[Optional[Any], Optional[Dict]]:
        """Process an item with error handling"""
        try:
            result = process_func(item)
            return result, None
        except Exception as e:
            error_info = {
                'item': str(item),
                'error': str(e),
                'type': type(e).__name__
            }
            return None, error_info


class RateLimitedWorker:
    """Worker that respects rate limits for various APIs"""
    
    @staticmethod
    def process_with_rate_limit(item: Any, process_func: Callable, 
                               rate_limit_apis: List[str]) -> Any:
        """
        Process an item while respecting rate limits.
        
        Args:
            item: Item to process
            process_func: Function to process the item
            rate_limit_apis: List of API names to rate limit
        """
        # Acquire rate limit tokens before processing
        for api in rate_limit_apis:
            wait_time = rate_limit_manager.acquire(api)
            if wait_time > 0:
                logger.debug(f"Rate limit wait for {api}: {wait_time:.2f}s")
        
        # Process the item
        return process_func(item)


def create_enrichment_worker_pool(num_workers: Optional[int] = None, 
                                 use_processes: bool = False) -> EnrichmentWorkerPool:
    """Create a configured worker pool for enrichment tasks"""
    if num_workers is None:
        # Default to CPU count but cap at 4 for API rate limiting
        num_workers = min(mp.cpu_count(), 4)
    
    config = WorkerConfig(
        num_workers=num_workers,
        use_processes=use_processes,
        timeout=300
    )
    
    return EnrichmentWorkerPool(config)


def batch_process_with_progress(items: List[Any], 
                               process_func: Callable,
                               num_workers: int = 4,
                               progress_callback: Optional[Callable] = None,
                               rate_limit_apis: Optional[List[str]] = None) -> Tuple[List[Any], List[Dict]]:
    """
    Convenience function to batch process items with progress tracking.
    
    Args:
        items: Items to process
        process_func: Function to process each item
        num_workers: Number of parallel workers
        progress_callback: Optional callback for progress updates
        rate_limit_apis: Optional list of APIs to rate limit
        
    Returns:
        Tuple of (results, errors)
    """
    with create_enrichment_worker_pool(num_workers) as pool:
        if rate_limit_apis:
            # Wrap the process function with rate limiting
            wrapped_func = partial(
                RateLimitedWorker.process_with_rate_limit,
                process_func=process_func,
                rate_limit_apis=rate_limit_apis
            )
        else:
            wrapped_func = process_func
        
        return pool.process_batch(items, wrapped_func, progress_callback)