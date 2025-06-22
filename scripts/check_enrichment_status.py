#!/usr/bin/env python3
"""Check enrichment status of companies in HubSpot."""

import sys
sys.path.insert(0, '/home/tspires/Development/common')

import argparse
import importlib.util
from datetime import datetime

# Parse arguments
parser = argparse.ArgumentParser(description="Check enrichment status")
parser.add_argument("--token", required=True, help="HubSpot API token")
args = parser.parse_args()

# Import HubSpot client
spec = importlib.util.spec_from_file_location(
    "hubspot_client", 
    "/home/tspires/Development/common/clients/hubspot.py"
)
hubspot_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hubspot_module)

# Initialize client
client = hubspot_module.HubSpotClient(access_token=args.token)

# Get all companies with enrichment data
properties = [
    "name", "domain", "website", "description", 
    "city", "state", "country", "timezone",
    "site_content", "enrichment_status", "enrichment_date",
    "numberofemployees", "annualrevenue", "industry"
]

total = 0
enriched = 0
has_site_content = 0
has_location = 0
has_description = 0
failed = 0

print("\nChecking enrichment status...\n")

# Iterate through companies
for company in client.iter_companies(properties=properties):
    total += 1
    props = company.get("properties", {})
    
    # Check enrichment status
    if props.get("enrichment_status") == "completed":
        enriched += 1
        
        # Print enriched company details
        print(f"✓ {props.get('name', 'Unknown')} ({props.get('domain', 'no domain')})")
        
        if props.get("description"):
            has_description += 1
            print(f"  Description: {props.get('description', '')[:100]}...")
        
        if props.get("city") or props.get("state") or props.get("country"):
            has_location += 1
            location = f"{props.get('city', '')}, {props.get('state', '')} {props.get('country', '')}"
            print(f"  Location: {location.strip(', ')}")
        
        if props.get("numberofemployees"):
            print(f"  Employees: {props.get('numberofemployees')}")
        
        if props.get("annualrevenue"):
            print(f"  Revenue: ${props.get('annualrevenue'):,}")
        
        if props.get("industry"):
            print(f"  Industry: {props.get('industry')}")
        
        if props.get("site_content"):
            has_site_content += 1
            print(f"  Site content: {len(props.get('site_content', ''))} chars")
        
        print()
    elif props.get("site_content"):
        # Some may have content but no status
        has_site_content += 1

print("\n" + "="*50)
print("ENRICHMENT SUMMARY")
print("="*50)
print(f"Total companies: {total}")
print(f"Enriched: {enriched} ({enriched/total*100:.1f}%)")
print(f"With descriptions: {has_description}")
print(f"With location data: {has_location}")
print(f"With site content: {has_site_content}")
print(f"\nRemaining to enrich: {total - enriched}")