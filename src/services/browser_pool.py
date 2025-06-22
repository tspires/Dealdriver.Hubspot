"""Browser session pool for reusing browser instances."""

import logging
import threading
import time
from queue import Queue, Empty
from typing import Optional, Dict, Any
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class BrowserSession:
    """Represents a browser session in the pool."""
    browser: Any
    created_at: datetime
    last_used: datetime
    request_count: int = 0
    
    def is_expired(self, max_age_minutes: int = 30) -> bool:
        """Check if session is too old."""
        return datetime.now() - self.created_at > timedelta(minutes=max_age_minutes)
    
    def is_stale(self, max_idle_minutes: int = 10) -> bool:
        """Check if session has been idle too long."""
        return datetime.now() - self.last_used > timedelta(minutes=max_idle_minutes)


class BrowserPool:
    """Manages a pool of browser sessions for reuse."""
    
    # Default configuration constants
    DEFAULT_MAX_SESSIONS = 5
    DEFAULT_MAX_REQUESTS_PER_SESSION = 50
    DEFAULT_MAX_AGE_MINUTES = 30
    DEFAULT_MAX_IDLE_MINUTES = 10
    
    def __init__(
        self,
        max_sessions: int = DEFAULT_MAX_SESSIONS,
        max_requests_per_session: int = DEFAULT_MAX_REQUESTS_PER_SESSION,
        max_age_minutes: int = DEFAULT_MAX_AGE_MINUTES,
        max_idle_minutes: int = DEFAULT_MAX_IDLE_MINUTES
    ):
        """
        Initialize browser pool.
        
        Args:
            max_sessions: Maximum number of concurrent browser sessions
            max_requests_per_session: Max requests before recycling a session
            max_age_minutes: Max age of a session before recycling
            max_idle_minutes: Max idle time before recycling
        """
        self.max_sessions = max_sessions
        self.max_requests_per_session = max_requests_per_session
        self.max_age_minutes = max_age_minutes
        self.max_idle_minutes = max_idle_minutes
        
        self._pool = Queue(maxsize=max_sessions)
        self._active_sessions = 0
        self._lock = threading.Lock()
        self._stats = {
            'sessions_created': 0,
            'sessions_recycled': 0,
            'total_requests': 0,
            'pool_hits': 0,
            'pool_misses': 0
        }
    
    @contextmanager
    def get_browser(self):
        """Get a browser from the pool."""
        session = None
        try:
            # Try to get existing session
            session = self._get_or_create_session()
            if session:
                yield session.browser
                session.request_count += 1
                session.last_used = datetime.now()
                self._stats['total_requests'] += 1
                
                # Check if session should be recycled
                if self._should_recycle_session(session):
                    self._close_session(session)
                    session = None
                else:
                    # Return to pool
                    self._pool.put(session)
            else:
                yield None
                
        except Exception as e:
            logger.error(f"Error in browser pool: {e}")
            if session:
                self._close_session(session)
            raise
    
    def _get_or_create_session(self) -> Optional[BrowserSession]:
        """Get existing session or create new one."""
        # Try to get from pool
        try:
            session = self._pool.get_nowait()
            self._stats['pool_hits'] += 1
            
            # Check if session is still valid
            if session.is_expired(self.max_age_minutes) or session.is_stale(self.max_idle_minutes):
                self._close_session(session)
                return self._create_new_session()
            
            return session
            
        except Empty:
            self._stats['pool_misses'] += 1
            return self._create_new_session()
    
    def _create_new_session(self) -> Optional[BrowserSession]:
        """Create a new browser session."""
        with self._lock:
            if self._active_sessions >= self.max_sessions:
                logger.warning(f"Browser pool at capacity ({self.max_sessions})")
                return None
            
            try:
                import sys
                # Add the common directory to Python path
                sys.path.insert(0, '/home/tspires/Development/common')
                
                from scrape.selenium_scraper import SeleniumScraper
                from scrape.base_scraper import ScraperConfig
                
                # Create config for pooled browsers
                config = ScraperConfig(
                    timeout=10,
                    max_pages=10,
                    enable_javascript=True,
                    load_images=False,  # Faster without images
                    load_css=True,
                    delay_between_requests=1.0,
                    max_retries=1
                )
                
                scraper = SeleniumScraper(config=config, headless=True)
                scraper.initialize()  # Initialize the driver
                
                session = BrowserSession(
                    browser=scraper,
                    created_at=datetime.now(),
                    last_used=datetime.now()
                )
                
                self._active_sessions += 1
                self._stats['sessions_created'] += 1
                logger.info(f"Created new browser session (total: {self._active_sessions})")
                
                return session
                
            except Exception as e:
                logger.error(f"Failed to create browser session: {e}")
                return None
    
    def _should_recycle_session(self, session: BrowserSession) -> bool:
        """Check if session should be recycled."""
        return (
            session.request_count >= self.max_requests_per_session or
            session.is_expired(self.max_age_minutes) or
            session.is_stale(self.max_idle_minutes)
        )
    
    def _close_session(self, session: BrowserSession):
        """Close a browser session."""
        try:
            session.browser.close()
            with self._lock:
                self._active_sessions -= 1
            self._stats['sessions_recycled'] += 1
            logger.info(f"Closed browser session (remaining: {self._active_sessions})")
        except Exception as e:
            logger.error(f"Error closing browser session: {e}")
    
    def close_all(self):
        """Close all browser sessions."""
        logger.info("Closing all browser sessions")
        
        # Close pooled sessions
        while not self._pool.empty():
            try:
                session = self._pool.get_nowait()
                self._close_session(session)
            except Empty:
                break
        
        self._active_sessions = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics."""
        return {
            **self._stats,
            'active_sessions': self._active_sessions,
            'pooled_sessions': self._pool.qsize(),
            'hit_rate': (
                self._stats['pool_hits'] / 
                (self._stats['pool_hits'] + self._stats['pool_misses'])
                if (self._stats['pool_hits'] + self._stats['pool_misses']) > 0
                else 0
            )
        }


# Global browser pool instance
_browser_pool = None


def get_browser_pool() -> BrowserPool:
    """Get the global browser pool instance."""
    global _browser_pool
    if _browser_pool is None:
        _browser_pool = BrowserPool()
    return _browser_pool


def cleanup_browser_pool():
    """Clean up the global browser pool."""
    global _browser_pool
    if _browser_pool:
        _browser_pool.close_all()
        _browser_pool = None