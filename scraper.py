# SEC 10-D ABS Scraper
# Extracts Tables 2-5 from 10-D filings, consolidates into per-table CSVs,
# and writes a separate metadata.csv for file-size efficiency.

import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import os
import sys
import time

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR = "output"

TABLES = [
    ("table_2_available_funds",  ["available funds", "reserve account", "cash flows", "collections"]),
    ("table_3_distributions",    ["distributions", "determination date", "payment date", "collection period", "additional information"]),
    ("table_4_noteholder",       ["noteholder", "class a-1 notes", "interest distributable"]),
    ("table_5_note_balance",     ["note factor", "note balance", "principal balance"]),
]

COLUMN_ALIASES = {
    "label":                      ["label", "description", "item", "item description"],
    "dollar_amount":              ["dollar amount", "amount", "value", "available funds", "available collections"],
}

_ALIAS_LOOKUP = {}
for canonical, aliases in COLUMN_ALIASES.items():
    for a in aliases:
        _ALIAS_LOOKUP[a.lower().replace(" ", "")] = canonical
    _ALIAS_LOOKUP[canonical.lower().replace(" ", "")] = canonical

def _norm(s):
    return str(s).lower().strip().replace(" ", "").replace("_", "").replace("-", "").replace(":", "").replace(".", "")

# ---------------------------------------------------------------------------
# Core parsing logic
# ---------------------------------------------------------------------------

def cell_text(cell):
    t = cell.get_text(separator=" ", strip=True)
    t = t.replace("\xa0", " ").replace("\n", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def parse_html_table(table_html):
    soup = BeautifulSoup(table_html, "html.parser")
    rows = soup.find_all("tr")
    if not rows:
        return None

    max_cols = 0
    for row in rows:
        cells = row.find_all(["td", "th"])
        w = sum(int(c.get("colspan", 1)) for c in cells)
        if w > max_cols:
            max_cols = w
    if max_cols == 0: return None

    layout_row = rows[0]
    layout_cells = layout_row.find_all(["td", "th"])
    slot_widths = []
    for cell in layout_cells:
        span = int(cell.get("colspan", 1))
        style = cell.get("style", "")
        match = re.search(r"width:([\d.]+)%", style)
        w = float(match.group(1)) if match else 0.0
        slot_widths.extend([w] * span)
    while len(slot_widths) < max_cols: slot_widths.append(0.0)

    # Classification constants
    SPACER_MAX = 0.5
    DOLLAR_MAX = 2.0  # Increased to catch '$' columns more reliably
    
    slot_type = []
    for w in slot_widths:
        if w <= SPACER_MAX:
            slot_type.append("SPACER")
        elif w <= DOLLAR_MAX:
            slot_type.append("DOLLAR")
        else:
            slot_type.append("VALUE")

    # The widest VALUE is definitively the LABEL
    max_label_w = -1
    label_idx = -1
    for i, (w, t) in enumerate(zip(slot_widths, slot_type)):
        if t == "VALUE" and w > max_label_w:
            max_label_w = w
            label_idx = i
    if label_idx != -1: slot_type[label_idx] = "LABEL"

    keep_slots = [i for i, t in enumerate(slot_type) if t in ("LABEL", "VALUE")]
    if not keep_slots: return None

    col_labels = [""] * max_cols
    header_row_indices = set()
    for ri, row in enumerate(rows):
        cells = row.find_all(["td", "th"])
        non_empty = [c for c in cells if cell_text(c)]
        if not non_empty: continue
        all_texts = [cell_text(c) for c in cells if cell_text(c)]
        
        # Stop at first numeric-looking data row
        if any(re.search(r"\d[\d,]*\.\d", t) for t in all_texts): break

        bold_count = sum(1 for c in non_empty if any(x in c.get('style', '') for x in ['700', 'bold']) or c.find('b'))
        if bold_count / len(non_empty) >= 0.5:
            header_row_indices.add(ri)
            slot = 0
            last_label = ""
            for cell in cells:
                span = int(cell.get("colspan", 1))
                text = cell_text(cell)
                if not text: text = last_label
                elif text != "$": last_label = text
                for s in range(slot, min(slot + span, max_cols)):
                    if text and text != "$":
                        col_labels[s] = (col_labels[s] + " | " + text).strip(" | ")
                slot += span

    # Default names
    for i, t in enumerate(slot_type):
        if t == "LABEL" and not col_labels[i]: col_labels[i] = "label"

    final_col_names = []
    seen_names = {}
    for s in keep_slots:
        name = col_labels[s].strip() if col_labels[s].strip() else f"col_{s}"
        if name not in seen_names:
            seen_names[name] = 0
            final_col_names.append(name)
        else:
            seen_names[name] += 1
            final_col_names.append(f"{name}_{seen_names[name]}")

    emit_slots = [s for s, n in zip(keep_slots, final_col_names)]
    emit_names = final_col_names

    records = []
    for ri, row in enumerate(rows):
        if ri in header_row_indices: continue
        cells = row.find_all(["td", "th"])
        slot_data = [""] * max_cols
        slot = 0
        for cell in cells:
            span = int(cell.get("colspan", 1))
            text = cell_text(cell)
            for s in range(slot, min(slot + span, max_cols)):
                slot_data[s] = text
            slot += span
        record = [slot_data[s] for s in emit_slots]
        if any(v.strip() and v != "$" for v in record):
            records.append(record)

    if not records: return None

    df = pd.DataFrame(records, columns=emit_names)
    df = _harmonize_columns(df)
    
    # Heuristic for Table 2 style tables: 
    # If we have only 1-3 columns and no 'label' or 'dollar_amount', 
    # assign based on column content (text vs numeric).
    if 'label' not in df.columns or 'dollar_amount' not in df.columns:
        text_cols = []
        numeric_cols = []
        for c in df.columns:
            if c == 'accession_number': continue
            vals = df[c].dropna().astype(str).str.replace(r'[$,\s()]', '', regex=True)
            try:
                pd.to_numeric(vals[vals != ''], errors='raise')
                numeric_cols.append(c)
            except:
                text_cols.append(c)
        
        if 'label' not in df.columns and text_cols:
            df = df.rename(columns={text_cols[0]: 'label'})
        if 'dollar_amount' not in df.columns and numeric_cols:
            df = df.rename(columns={numeric_cols[-1]: 'dollar_amount'})

    return df

def _harmonize_columns(df):
    rename_map = {}
    for col in df.columns:
        key = _norm(col)
        if key in _ALIAS_LOOKUP: rename_map[col] = _ALIAS_LOOKUP[key]
    return df.rename(columns=rename_map)

def extract_metadata(raw_content):
    patterns = {
        "accession": r"ACCESSION NUMBER:\s+(.+)",
        "cik":       r"CENTRAL INDEX KEY:\s+(.+)",
        "company":   r"COMPANY CONFORMED NAME:\s+(.+)",
        "period":    r"CONFORMED PERIOD OF REPORT:\s+(\d+)",
        "filed_date": r"FILED AS OF DATE:\s+(\d+)",
    }
    results = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, raw_content)
        results[key] = match.group(1).strip() if match else "Unknown"
    return results["accession"], results["company"], results["period"], results["filed_date"], results["cik"]

def extract_exhibit_99(raw_content):
    match = re.search(r"<TYPE>EX-99.*?</TYPE>.*?<TEXT>(.*?)</TEXT>", raw_content, re.DOTALL | re.IGNORECASE)
    if match: return match.group(1)
    return raw_content

def load_filing_urls(filepath):
    urls = []
    with open(filepath, "r") as f:
        f.readline()
        for line in f:
            parts = line.strip().split("|")
            if len(parts) >= 3: urls.append("https://www.sec.gov/Archives/" + parts[2])
    return urls

def extract_table(raw_content, anchor_texts):
    full_text_lower = raw_content.lower()
    anchor_pos = -1
    for anchor in anchor_texts:
        if anchor in full_text_lower:
            anchor_pos = full_text_lower.find(anchor)
            break
    if anchor_pos == -1: return None
    table_start = full_text_lower.rfind("<table", 0, anchor_pos)
    table_end   = full_text_lower.find("</table>", anchor_pos)
    if table_start == -1 or table_end == -1: return None
    return parse_html_table(raw_content[table_start: table_end + 8])

def scrape_filing(url, accumulators, metadata_rows):
    headers = {"User-Agent": "Teerth Patel (tmp00725@uga.edu)"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        raw_content = response.text
        accession, company, period, filed_date, cik = extract_metadata(raw_content)
        print(f"  {company} | {accession}")
        metadata_rows.append({"accession_number": accession, "company_name": company, "report_period": period, "filed_date": filed_date, "cik": cik})
        exhibit_content = extract_exhibit_99(raw_content)
        for name, anchors in TABLES:
            df = extract_table(exhibit_content, anchors)
            if df is not None:
                df.insert(0, "accession_number", accession)
                accumulators[name].append(df)
                print(f"    {name}: OK")
    except Exception as e: print(f"  Error: {e}")

def save_outputs(accumulators, metadata_rows):
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    pd.DataFrame(metadata_rows).to_csv(os.path.join(OUTPUT_DIR, "metadata.csv"), index=False)
    for name, frames in accumulators.items():
        if frames: pd.concat(frames, ignore_index=True).to_csv(os.path.join(OUTPUT_DIR, f"{name}.csv"), index=False)

if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "ford_ABS.txt"
    urls = load_filing_urls(filepath)
    accumulators = {name: [] for name, _ in TABLES}
    metadata_rows = []
    for i, url in enumerate(urls):
        print(f"[{i+1}/{len(urls)}]", end="")
        scrape_filing(url, accumulators, metadata_rows)
        time.sleep(0.1)
    save_outputs(accumulators, metadata_rows)