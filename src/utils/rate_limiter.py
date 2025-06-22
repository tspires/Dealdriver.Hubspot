import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting"""
    calls_per_second: float = 1.0
    burst_size: int = 1
    min_interval: float = 0.1


class ThreadSafeRateLimiter:
    """Thread-safe rate limiter using token bucket algorithm"""
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.tokens = config.burst_size
        self.max_tokens = config.burst_size
        self.refill_rate = config.calls_per_second
        self.last_refill = time.time()
        self.lock = threading.Lock()
        self.min_interval = config.min_interval
        
    def acquire(self, tokens: int = 1) -> float:
        """
        Acquire tokens from the bucket, blocking if necessary.
        Returns the time waited.
        """
        start_time = time.time()
        
        with self.lock:
            while True:
                self._refill()
                
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    wait_time = time.time() - start_time
                    return wait_time
                
                # Calculate time to wait for enough tokens
                tokens_needed = tokens - self.tokens
                wait_time = max(tokens_needed / self.refill_rate, self.min_interval)
                
                # Release lock while waiting
                self.lock.release()
                time.sleep(wait_time)
                self.lock.acquire()
    
    def _refill(self):
        """Refill tokens based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_refill
        
        # Add tokens based on elapsed time
        new_tokens = elapsed * self.refill_rate
        self.tokens = min(self.tokens + new_tokens, self.max_tokens)
        self.last_refill = now


class APIRateLimitManager:
    """Manages rate limits for multiple APIs"""
    
    def __init__(self):
        self.limiters: Dict[str, ThreadSafeRateLimiter] = {}
        self.lock = threading.Lock()
        
        # Default configurations
        self.default_configs = {
            'deepseek': RateLimitConfig(calls_per_second=2.0, burst_size=5, min_interval=0.2),
            'hubspot': RateLimitConfig(calls_per_second=10.0, burst_size=20, min_interval=0.05),
            'selenium': RateLimitConfig(calls_per_second=3.0, burst_size=5, min_interval=0.1),
        }
    
    def get_limiter(self, api_name: str, config: Optional[RateLimitConfig] = None) -> ThreadSafeRateLimiter:
        """Get or create a rate limiter for an API"""
        with self.lock:
            if api_name not in self.limiters:
                if config is None:
                    config = self.default_configs.get(api_name, RateLimitConfig())
                self.limiters[api_name] = ThreadSafeRateLimiter(config)
            return self.limiters[api_name]
    
    def acquire(self, api_name: str, tokens: int = 1) -> float:
        """Acquire tokens for an API call"""
        limiter = self.get_limiter(api_name)
        return limiter.acquire(tokens)


# Global instance
rate_limit_manager = APIRateLimitManager()