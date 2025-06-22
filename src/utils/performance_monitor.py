"""Performance monitoring utilities for scraping operations."""

import time
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime
import statistics

logger = logging.getLogger(__name__)


@dataclass
class ScrapingMetrics:
    """Metrics for a scraping operation."""
    domain: str
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    success: bool = False
    error: Optional[str] = None
    content_size: int = 0
    emails_found: int = 0
    
    def complete(self, success: bool, content_size: int = 0, emails_found: int = 0, error: Optional[str] = None):
        """Mark the operation as complete."""
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        self.success = success
        self.content_size = content_size
        self.emails_found = emails_found
        self.error = error


@dataclass
class PerformanceReport:
    """Aggregated performance report."""
    total_domains: int = 0
    successful_scrapes: int = 0
    failed_scrapes: int = 0
    total_duration: float = 0
    average_duration: float = 0
    min_duration: float = 0
    max_duration: float = 0
    domains_per_minute: float = 0
    total_content_size: int = 0
    total_emails_found: int = 0
    error_summary: Dict[str, int] = field(default_factory=dict)
    
    @classmethod
    def from_metrics(cls, metrics: List[ScrapingMetrics]) -> 'PerformanceReport':
        """Generate report from metrics."""
        if not metrics:
            return cls()
        
        report = cls()
        report.total_domains = len(metrics)
        
        durations = []
        for metric in metrics:
            if metric.duration:
                durations.append(metric.duration)
                report.total_duration += metric.duration
            
            if metric.success:
                report.successful_scrapes += 1
                report.total_content_size += metric.content_size
                report.total_emails_found += metric.emails_found
            else:
                report.failed_scrapes += 1
                error_type = metric.error or "Unknown"
                report.error_summary[error_type] = report.error_summary.get(error_type, 0) + 1
        
        if durations:
            report.average_duration = statistics.mean(durations)
            report.min_duration = min(durations)
            report.max_duration = max(durations)
            
            # Calculate throughput
            if report.total_duration > 0:
                report.domains_per_minute = (report.total_domains / report.total_duration) * 60
        
        return report
    
    # Report formatting constants
    REPORT_WIDTH = 60
    REPORT_SEPARATOR = "=" * REPORT_WIDTH
    REPORT_DIVIDER = "-" * REPORT_WIDTH
    
    def print_summary(self):
        """Print a formatted summary of the report."""
        print(f"\n{self.REPORT_SEPARATOR}")
        print("SCRAPING PERFORMANCE REPORT")
        print(self.REPORT_SEPARATOR)
        print(f"Total Domains: {self.total_domains}")
        if self.total_domains > 0:
            print(f"Successful: {self.successful_scrapes} ({self.successful_scrapes/self.total_domains*100:.1f}%)")
            print(f"Failed: {self.failed_scrapes} ({self.failed_scrapes/self.total_domains*100:.1f}%)")
        else:
            print(f"Successful: {self.successful_scrapes} (0.0%)")
            print(f"Failed: {self.failed_scrapes} (0.0%)")
        print(f"\nTiming:")
        print(f"  Total Duration: {self.total_duration:.2f}s")
        print(f"  Average per Domain: {self.average_duration:.2f}s")
        print(f"  Min Duration: {self.min_duration:.2f}s")
        print(f"  Max Duration: {self.max_duration:.2f}s")
        print(f"  Throughput: {self.domains_per_minute:.1f} domains/minute")
        print(f"\nContent:")
        print(f"  Total Content: {self.total_content_size:,} bytes")
        print(f"  Total Emails: {self.total_emails_found}")
        
        if self.error_summary:
            print(f"\nErrors:")
            for error_type, count in self.error_summary.items():
                print(f"  {error_type}: {count}")
        print(self.REPORT_SEPARATOR + "\n")


class PerformanceMonitor:
    """Monitor and track scraping performance."""
    
    def __init__(self):
        self.metrics: List[ScrapingMetrics] = []
        self.start_time = time.time()
    
    def start_scrape(self, domain: str) -> ScrapingMetrics:
        """Start tracking a scrape operation."""
        metric = ScrapingMetrics(
            domain=domain,
            start_time=time.time()
        )
        self.metrics.append(metric)
        return metric
    
    @contextmanager
    def track_scrape(self, domain: str):
        """Context manager for tracking a scrape operation."""
        metric = self.start_scrape(domain)
        try:
            yield metric
            if metric.end_time is None:
                metric.complete(success=True)
        except Exception as e:
            metric.complete(success=False, error=str(e))
            raise
    
    def get_report(self) -> PerformanceReport:
        """Generate a performance report."""
        return PerformanceReport.from_metrics(self.metrics)
    
    def log_summary(self):
        """Log a summary of performance metrics."""
        report = self.get_report()
        
        logger.info(f"Scraping Performance Summary:")
        logger.info(f"  Total: {report.total_domains} domains in {report.total_duration:.2f}s")
        success_rate = (report.successful_scrapes/report.total_domains*100) if report.total_domains > 0 else 0
        logger.info(f"  Success Rate: {report.successful_scrapes}/{report.total_domains} ({success_rate:.1f}%)")
        logger.info(f"  Throughput: {report.domains_per_minute:.1f} domains/minute")
        logger.info(f"  Avg Duration: {report.average_duration:.2f}s per domain")
        
        if report.error_summary:
            logger.warning(f"  Errors: {report.error_summary}")


# Global performance monitor instance
_performance_monitor = None


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance."""
    global _performance_monitor
    if _performance_monitor is None:
        _performance_monitor = PerformanceMonitor()
    return _performance_monitor


# Example usage with scraper
def scrape_with_monitoring(scraper, domain: str):
    """Scrape a domain with performance monitoring."""
    monitor = get_performance_monitor()
    
    with monitor.track_scrape(domain) as metric:
        result = scraper.scrape_domain(domain)
        
        metric.complete(
            success=result.success,
            content_size=len(result.content) if result.content else 0,
            emails_found=len(result.emails) if result.emails else 0,
            error=result.error
        )
        
        return result