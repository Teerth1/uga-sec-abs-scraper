# SEC 10-D ABS Scraper
# Extracts Tables 2-5 from Ford Credit Auto Owner Trust 10-D filings

import requests
import pandas as pd
from io import StringIO
import re
import os
import sys
import time
# Configuration
OUTPUT_DIR = "output"

# Updated configuration with flexible anchors
TABLES = [
    ("table_2_available_funds", ["available funds", "reserve account", "cash flows"]),
    ("table_3_distributions", ["distributions", "payment date", "collection period"]),
    ("table_4_noteholder", ["noteholder", "class a-1 notes", "interest distributable"]),
    ("table_5_note_balance", ["note factor", "note balance", "principal balance"]),
]

def extract_metadata(raw_content):
    """Extract filing metadata from SEC filing header with robust regex."""
    # Use re.IGNORECASE and allow for flexible whitespace
    patterns = {
        'accession': r"ACCESSION NUMBER:\s+(.+)",
        'cik': r"CENTRAL INDEX KEY:\s+(.+)",
        'company': r"COMPANY CONFORMED NAME:\s+(.+)",
        'period': r"CONFORMED PERIOD OF REPORT:\s+(\d+)",
        'filed_date': r"FILED AS OF DATE:\s+(\d+)"
    }
    
    metadata = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, raw_content, re.IGNORECASE)
        if match:
             metadata[key] = match.group(1).strip()
        else:
             print(f"Warning: Could not find metadata for {key}")
             metadata[key] = "UNKNOWN"
             
    return metadata['accession'], metadata['company'], metadata['period'], metadata['filed_date'], metadata['cik']

def extract_exhibit_99(raw_content):
    """Isolate the Exhibit 99 section from the full text."""
    # Look for document start/end tags or specific exhibit type tags
    # This is a simplified approach; robust parsing would handle the full SGML structure
    
    # Try to find EX-99 start
    ex99_start = re.search(r"<TYPE>EX-99", raw_content, re.IGNORECASE)
    if not ex99_start:
        print("Warning: Could not find <TYPE>EX-99 tag. Using full content.")
        return raw_content
        
    start_pos = ex99_start.start()
    
    # Find the end of this document (next <DOCUMENT> or end of file)
    next_doc = re.search(r"<DOCUMENT>", raw_content[start_pos + 1:], re.IGNORECASE)
    
    if next_doc:
        end_pos = start_pos + 1 + next_doc.start()
        return raw_content[start_pos:end_pos]
    
    return raw_content[start_pos:]

def load_filing_urls(filepath):
    urls = []
    with open(filepath, 'r') as f:
        next(f)  # skip header line
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('|')
            url = "https://www.sec.gov/Archives/" + parts[2]
            urls.append(url)
    return urls

def extract_table(raw_content, anchor_texts):
    """Extract a table using a list of potential anchors."""
    full_text_lower = raw_content.lower()
    
    anchor_pos = -1
    used_anchor = None
    
    # Try each anchor until one works
    for anchor in anchor_texts:
        pos = full_text_lower.find(anchor)
        if pos != -1:
            anchor_pos = pos
            used_anchor = anchor
            break
            
    if anchor_pos == -1:
        print(f"  Warning: No anchors found from {anchor_texts}")
        return None
    
    # Find enclosing <table> tags
    table_start = full_text_lower.rfind("<table", 0, anchor_pos)
    table_end = full_text_lower.find("</table>", anchor_pos)
    
    if table_start == -1 or table_end == -1:
        print(f"  Warning: Could not find table for anchor '{used_anchor}'")
        return None
    
    table_html = raw_content[table_start:table_end + 8]
    
    try:
        # Parse HTML table to DataFrame
        dfs = pd.read_html(StringIO(table_html))
        if not dfs:
            return None
        df = dfs[0]
        
        # Data Cleaning: Drop empty rows/cols
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Attempt to clean default headers (0, 1, 2...) if they exist
        # This is a heuristic: if columns are Int64Index(0, 1, ...), try to set first row as header
        # if isinstance(df.columns, pd.RangeIndex) and len(df) > 1:
             # Check if first row looks like strings
             # new_header = df.iloc[0]
             # df = df[1:]
             # df.columns = new_header
             
        return df
    except Exception as e:
        print(f"  Error parsing table for anchor '{used_anchor}': {e}")
        return None

def scrape_filing(url):
    """Scrape a single SEC 10-D filing and extract tables."""
    headers = {'User-Agent': 'Teerth Patel (tmp00725@uga.edu)'}
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Fetching: {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return

    raw_content = response.text
    
    # Extract metadata
    accession, company, period, filed_date, cik = extract_metadata(raw_content)
    print(f"Processing: {company}")
    print(f"  Accession: {accession}")
    print(f"  Period: {period}, Filed: {filed_date}")
    print(f"  CIK: {cik}")
    # Isolate Exhibit 99 to prevent false positives
    exhibit_content = extract_exhibit_99(raw_content)
    
    # Extract each table
    for table_name, anchors in TABLES:
        print(f"  Extracting: {table_name}...")
        df = extract_table(exhibit_content, anchors)
        
        if df is not None:
            # Add metadata columns
            df['accession_number'] = accession
            df['company_name'] = company
            df['report_period'] = period
            df['filed_date'] = filed_date
            df['cik'] = cik
            
            # Save to CSV in output directory
            filename = os.path.join(OUTPUT_DIR, f"{table_name}_{accession}.csv")
            df.to_csv(filename, index=False)
            print(f"    Saved: {filename} ({len(df)} rows)")
    
    print(f"Done!\n")

# Main execution
if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "ford_ABS.txt"
    urls = load_filing_urls(filepath)
    
    # optional: parse --limit from sys.argv
    
    for i, url in enumerate(urls):
        print(f"\n[{i+1}/{len(urls)}] ", end="")
        scrape_filing(url)
        time.sleep(0.15)