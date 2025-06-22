#!/usr/bin/env python3
"""Fix common pylint issues in Python files."""

import os
import re
import sys


def fix_trailing_whitespace(content):
    """Remove trailing whitespace from lines."""
    lines = content.split('\n')
    fixed_lines = [line.rstrip() for line in lines]
    return '\n'.join(fixed_lines)


def fix_line_too_long(content, max_length=80):
    """Attempt to fix lines that are too long."""
    lines = content.split('\n')
    fixed_lines = []
    
    for line in lines:
        if len(line) <= max_length:
            fixed_lines.append(line)
            continue
            
        # Don't break lines with URLs or long strings
        if 'http' in line or '"""' in line or "'''" in line:
            fixed_lines.append(line)
            continue
            
        # Try to break at commas, parentheses, or operators
        if len(line) > max_length and ',' in line:
            # Find a good break point
            indent = len(line) - len(line.lstrip())
            parts = line.split(',')
            current_line = parts[0]
            
            for i, part in enumerate(parts[1:], 1):
                if len(current_line + ',' + part.strip()) <= max_length:
                    current_line += ',' + part.strip()
                else:
                    fixed_lines.append(current_line + ',')
                    current_line = ' ' * (indent + 4) + part.strip()
                    
            fixed_lines.append(current_line)
        else:
            # For now, just keep the line as is if we can't easily break it
            fixed_lines.append(line)
            
    return '\n'.join(fixed_lines)


def fix_file(filepath):
    """Fix pylint issues in a single file."""
    print(f"Fixing {filepath}...")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Apply fixes
    content = fix_trailing_whitespace(content)
    # Skip line length fixes for now - they're complex and can break code
    # content = fix_line_too_long(content)
    
    # Write back
    with open(filepath, 'w') as f:
        f.write(content)
    
    print(f"Fixed {filepath}")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python fix_pylint_issues.py <file_or_directory>")
        sys.exit(1)
    
    target = sys.argv[1]
    
    if os.path.isfile(target):
        fix_file(target)
    elif os.path.isdir(target):
        for root, _, files in os.walk(target):
            for file in files:
                if file.endswith('.py'):
                    fix_file(os.path.join(root, file))
    else:
        print(f"Error: {target} is not a valid file or directory")
        sys.exit(1)


if __name__ == '__main__':
    main()