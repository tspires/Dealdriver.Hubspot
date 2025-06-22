"""File processing utilities for domain lists."""

import logging
from pathlib import Path
from typing import List, Set, Tuple

from src.utils.domain import extract_domain

logger = logging.getLogger(__name__)


class DomainFileProcessor:
    """Process domain files with validation and deduplication."""
    
    @staticmethod
    def read_domains_from_file(file_path: Path) -> Tuple[List[str], List[str]]:
        """
        Read and validate domains from a file.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            Tuple of (valid_domains, errors)
        """
        domains = []
        errors = []
        seen_domains: Set[str] = set()
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    if not line or line.startswith('#'):
                        continue
                    
                    domain = extract_domain(line)
                    if not domain:
                        error_msg = f"Line {line_num}: Invalid domain/URL '{line}'"
                        errors.append(error_msg)
                        logger.warning(error_msg)
                        continue
                    
                    if domain in seen_domains:
                        error_msg = f"Line {line_num}: Duplicate domain '{domain}'"
                        errors.append(error_msg)
                        logger.warning(error_msg)
                        continue
                    
                    domains.append(domain)
                    seen_domains.add(domain)
                    
        except Exception as e:
            error_msg = f"Error reading file: {e}"
            errors.append(error_msg)
            logger.error(error_msg)
            
        return domains, errors
    
    @staticmethod
    def validate_input_file(file_path: str) -> Tuple[bool, str]:
        """
        Validate that input file exists and is readable.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        path = Path(file_path)
        
        if not path.exists():
            return False, f"Input file {file_path} does not exist"
        
        if not path.is_file():
            return False, f"{file_path} is not a file"
        
        if not path.stat().st_size:
            return False, f"Input file {file_path} is empty"
        
        return True, ""