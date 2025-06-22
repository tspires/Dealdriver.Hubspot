"""Domain utility functions."""

import re
from typing import Optional
from urllib.parse import urlparse

from src.constants import MIN_DOMAIN_LENGTH


def extract_domain(text: str) -> Optional[str]:
    """Extract domain from email or URL."""
    if not text:
        return None
    
    text = text.strip().lower()
    
    # Handle email addresses
    if "@" in text:
        parts = text.split("@")
        if len(parts) == 2:
            domain = parts[1].strip()
            # Validate the email domain
            if domain and "." in domain:
                return domain
    
    # Handle URLs
    if not text.startswith(("http://", "https://", "ftp://", "ftps://")):
        # Assume https if no protocol
        text = f"https://{text}"
    
    try:
        parsed = urlparse(text)
        domain = parsed.netloc or parsed.path
        
        # Remove www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        
        # Remove port if present
        if ":" in domain:
            domain = domain.split(":")[0]
        
        # Remove path if no netloc (e.g., "example.com/path" without protocol)
        if not parsed.netloc and "/" in domain:
            domain = domain.split("/")[0]
        
        # Validate domain format
        domain_pattern = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]*\.([a-zA-Z]{2,}|xn--[a-zA-Z0-9]+)([\.][a-zA-Z]{2,}|[\.](xn--[a-zA-Z0-9]+))*$')
        if domain and domain_pattern.match(domain):
            return domain
        
        # Try simpler validation for edge cases
        if domain and "." in domain and len(domain) >= MIN_DOMAIN_LENGTH:
            return domain
            
    except Exception:
        pass
    
    return None


def normalize_url(url: str) -> str:
    """Normalize URL for consistency."""
    if not url:
        return ""
    
    url = url.strip()
    
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    
    return url