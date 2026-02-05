# SEC 10-D ABS Scraper
# Extracts Tables 2-5 from Ford Credit Auto Owner Trust 10-D filings

import requests
import pandas as pd
from io import StringIO
import re

import os

# Configuration
OUTPUT_DIR = "output"
TABLES = [
    ("table_2_available_funds", "available funds"),
    ("table_3_distributions", "distributions"),
    ("table_4_noteholder", "noteholder"),
    ("table_5_note_balance", "note factor"),  # This table contains note factors
]

def extract_metadata(raw_content):
    """Extract filing metadata from SEC filing header."""
    accession = re.search(r"ACCESSION NUMBER:\s+(.+)", raw_content).group(1).strip()
    company = re.search(r"COMPANY CONFORMED NAME:\s+(.+)", raw_content).group(1).strip()
    period = re.search(r"CONFORMED PERIOD OF REPORT:\s+(\d+)", raw_content).group(1).strip()
    filed_date = re.search(r"FILED AS OF DATE:\s+(\d+)", raw_content).group(1).strip()
    return accession, company, period, filed_date

def extract_table(raw_content, anchor_text):
    """Extract a table from the filing using an anchor text to locate it."""
    full_text_lower = raw_content.lower()
    
    # Find anchor position
    anchor_pos = full_text_lower.find(anchor_text)
    if anchor_pos == -1:
        print(f"  Warning: Anchor '{anchor_text}' not found")
        return None
    
    # Find enclosing <table> tags
    table_start = full_text_lower.rfind("<table", 0, anchor_pos)
    table_end = full_text_lower.find("</table>", anchor_pos)
    
    if table_start == -1 or table_end == -1:
        print(f"  Warning: Could not find table for anchor '{anchor_text}'")
        return None
    
    table_html = raw_content[table_start:table_end + 8]
    
    # Parse HTML table to DataFrame
    df = pd.read_html(StringIO(table_html))[0]
    df = df.dropna(how='all').dropna(axis=1, how='all')
    
    return df

def scrape_filing(url):
    """Scrape a single SEC 10-D filing and extract all four tables."""
    headers = {'User-Agent': 'Teerth Patel (tmp00725@uga.edu)'}
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Fetching: {url}")
    response = requests.get(url, headers=headers)
    raw_content = response.text
    
    # Extract metadata
    accession, company, period, filed_date = extract_metadata(raw_content)
    print(f"Processing: {company}")
    print(f"  Accession: {accession}")
    print(f"  Period: {period}, Filed: {filed_date}")
    
    # Extract each table
    for table_name, anchor in TABLES:
        print(f"  Extracting: {table_name}...")
        df = extract_table(raw_content, anchor)
        
        if df is not None:
            # Add metadata columns
            df['accession_number'] = accession
            df['company_name'] = company
            df['report_period'] = period
            df['filed_date'] = filed_date
            
            # Save to CSV in output directory
            filename = os.path.join(OUTPUT_DIR, f"{table_name}_{accession}.csv")
            df.to_csv(filename, index=False)
            print(f"    Saved: {filename} ({len(df)} rows)")
    
    print(f"Done!\n")

# Main execution
if __name__ == "__main__":
    # 2025 filing (most recent)
    url_2025 = "https://www.sec.gov/Archives/edgar/data/1843634/000184363425000014/0001843634-25-000014.txt"
    scrape_filing(url_2025)
    
    # 2021 filing (older - for testing)
    url_2021 = "https://www.sec.gov/Archives/edgar/data/1843634/000184363421000042/0001843634-21-000042.txt"
    scrape_filing(url_2021)
