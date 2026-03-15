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

# ---------------------------------------------------------------------------
# Column harmonization — canonical name → list of known aliases
# ---------------------------------------------------------------------------
COLUMN_ALIASES = {
    "label":                      ["label", "description", "item"],
    "dollar_amount":              ["dollar amount"],
    "num_receivables":            ["number of receivables"],
    "wtd_avg_remaining_term":     ["weighted average remaining term", "avg term"],
}

# Pre-compute a lookup for faster matching
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
    """Clean text from a BeautifulSoup cell."""
    t = cell.get_text(separator=" ", strip=True)
    t = t.replace("\xa0", " ").replace("\n", " ")
    # Replace multiple spaces with one
    t = re.sub(r"\s+", " ", t).strip()
    return t

def parse_html_table(table_html):
    """
    Parses a single SEC HTML table. 
    Implements a 'slot-based' layout to handle complex colspans accurately.
    """
    soup = BeautifulSoup(table_html, "html.parser")
    rows = soup.find_all("tr")
    if not rows:
        return None

    # ----------------------------------------------------------------
    # Step 1: determine max slots
    # ----------------------------------------------------------------
    max_cols = 0
    for row in rows:
        cells = row.find_all(["td", "th"])
        w = sum(int(c.get("colspan", 1)) for c in cells)
        if w > max_cols:
            max_cols = w

    if max_cols == 0:
        return None

    # ----------------------------------------------------------------
    # Step 2: read layout row (row 0) to get per-slot widths
    # ----------------------------------------------------------------
    layout_row = rows[0]
    layout_cells = layout_row.find_all(["td", "th"])
    slot_widths = []
    for cell in layout_cells:
        span = int(cell.get("colspan", 1))
        style = cell.get("style", "")
        match = re.search(r"width:([\d.]+)%", style)
        w = float(match.group(1)) if match else 0.0
        slot_widths.extend([w] * span)
    while len(slot_widths) < max_cols:
        slot_widths.append(0.0)

    # ----------------------------------------------------------------
    # Step 3: classify each slot
    #   SPACER  : width <= 0.15%   (the tiny padding columns)
    #   DOLLAR  : 0.15% < width <= 1.6%  and not one of the widest cols
    #             (the '$' prefix and the small sep cols ≈1% and 1.5%)
    #   VALUE   : 1.6% < width < 30%   (actual data value columns)
    #   LABEL   : width >= 30%          (the row description column)
    # ----------------------------------------------------------------
    SPACER_MAX = 0.15
    DOLLAR_MAX = 1.6
    VALUE_MAX  = 30.0

    slot_type = []  # 'SPACER', 'DOLLAR', 'VALUE', 'LABEL'
    for w in slot_widths:
        if w <= SPACER_MAX:
            slot_type.append("SPACER")
        elif w <= DOLLAR_MAX:
            slot_type.append("DOLLAR")
        elif w < VALUE_MAX:
            slot_type.append("VALUE")
        else:
            slot_type.append("LABEL")

    keep_slots = [i for i, t in enumerate(slot_type) if t in ("LABEL", "VALUE")]

    if not keep_slots:
        return None

    # ----------------------------------------------------------------
    # Step 4: identify header rows (rows where bold text appears)
    # and assign column labels to the VALUE (and LABEL) slots.
    # ----------------------------------------------------------------
    def is_bold_cell(cell):
        style = cell.get("style", "")
        if "font-weight:700" in style or "font-weight: 700" in style:
            return True
        if cell.find("b"):
            return True
        for font in cell.find_all("font"):
            fs = font.get("style", "")
            if "700" in fs or "bold" in fs.lower():
                return True
        return False

    col_labels = [""] * max_cols  # one label per raw slot

    header_row_indices = set()
    for ri, row in enumerate(rows):
        cells = row.find_all(["td", "th"])
        non_empty = [c for c in cells if cell_text(c)]
        if not non_empty:
            continue

        # Stop considering header rows as soon as we hit a row with numeric data
        # (e.g. "1,234.56" pattern). This prevents data values from bleeding into
        # column label strings when bold styling carries over to summary rows.
        all_texts = [cell_text(c) for c in cells if cell_text(c)]
        has_numeric = any(re.search(r"\d[\d,]*\.\d", t) for t in all_texts)
        if has_numeric:
            break  # everything from here on is a data row

        bold_count = sum(1 for c in non_empty if is_bold_cell(c))
        if bold_count / len(non_empty) >= 0.5:
            header_row_indices.add(ri)
            # Assign header labels to slots
            slot = 0
            last_label = ""
            for cell in cells:
                span = int(cell.get("colspan", 1))
                text = cell_text(cell)
                if not text:
                    text = last_label
                elif text != "$":
                    last_label = text
                for s in range(slot, min(slot + span, max_cols)):
                    if text and text != "$":
                        if col_labels[s]:
                            col_labels[s] = col_labels[s] + " | " + text
                        else:
                            col_labels[s] = text

                slot += span

    # For LABEL slots with no header text, assign "label"
    for i, t in enumerate(slot_type):
        if t == "LABEL" and not col_labels[i]:
            col_labels[i] = "label"

    # ----------------------------------------------------------------
    # Step 5: build final column name list for kept slots only.
    # For VALUE slots: use the header label for that slot.
    # Deduplicate names that repeat (same header spanning multiple VALUE slots).
    # ----------------------------------------------------------------
    final_col_names = []
    seen_names: dict = {}
    for s in keep_slots:
        name = col_labels[s].strip() if col_labels[s].strip() else f"col_{s}"
        if name not in seen_names:
            seen_names[name] = 0
            final_col_names.append(name)
        else:
            # Already emitted this column name — this is a duplicate VALUE slot
            # (e.g. three slots all labeled "Beginning of Period | Balance").
            # Skip it (we'll deduplicate at the slot-keep level).
            final_col_names.append(None)  # mark for removal

    # Identify which keep_slots to actually emit (drop duplicates)
    emit_slots = [s for s, n in zip(keep_slots, final_col_names) if n is not None]
    emit_names = [n for n in final_col_names if n is not None]

    if not emit_slots:
        return None

    # ----------------------------------------------------------------
    # Step 6: extract data rows
    # ----------------------------------------------------------------
    records = []
    for ri, row in enumerate(rows):
        if ri in header_row_indices:
            continue
        cells = row.find_all(["td", "th"])
        # Expand colspans into flat slot array
        slot_data = [""] * max_cols
        slot = 0
        for cell in cells:
            span = int(cell.get("colspan", 1))
            text = cell_text(cell)
            for s in range(slot, min(slot + span, max_cols)):
                slot_data[s] = text
            slot += span

        record = [slot_data[s] for s in emit_slots]
        if all(v == "" for v in record):
            continue
        records.append(record)

    if not records:
        return None

    df = pd.DataFrame(records, columns=emit_names)

    # Drop any remaining all-empty or all-"$" columns
    def useless(col):
        vals = df[col].replace("", pd.NA).dropna().astype(str)
        if len(vals) == 0:
            return True
        return (vals == "$").all()

    df = df[[c for c in df.columns if not useless(c)]]
    df = df.dropna(how="all").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Column harmonization
# ---------------------------------------------------------------------------

def _harmonize_columns(df):
    rename_map = {}
    for col in df.columns:
        key = _norm(col)
        if key in _ALIAS_LOOKUP:
            rename_map[col] = _ALIAS_LOOKUP[key]
    return df.rename(columns=rename_map)


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

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
    """Fina 99.1 or equivalent exhibit (Monthly Servicer's Certificate)."""
    # Simple strategy: look for Exhibit 99 and take everything after it
    # until the next exhibit delimiter <DOCUMENT>.
    match = re.search(r"<TYPE>EX-99.*?</TYPE>.*?<TEXT>(.*?)</TEXT>", raw_content, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1)
    return raw_content

def load_filing_urls(filepath):
    urls = []
    with open(filepath, "r") as f:
        # Skip header
        f.readline()
        for line in f:
            parts = line.strip().split("|")
            if len(parts) < 3: continue
            urls.append("https://www.sec.gov/Archives/" + parts[2])
    return urls


# ---------------------------------------------------------------------------
# Table extraction — find HTML fragment and parse
# ---------------------------------------------------------------------------

def extract_table(raw_content, anchor_texts):
    full_text_lower = raw_content.lower()

    anchor_pos = -1
    used_anchor = None
    for anchor in anchor_texts:
        pos = full_text_lower.find(anchor)
        if pos != -1:
            anchor_pos = pos
            used_anchor = anchor
            break

    if anchor_pos == -1:
        # print(f"    Warning: No anchors found from {anchor_texts}")
        return None

    table_start = full_text_lower.rfind("<table", 0, anchor_pos)
    table_end   = full_text_lower.find("</table>", anchor_pos)

    if table_start == -1 or table_end == -1:
        # print(f"    Warning: Could not find table tags for anchor '{used_anchor}'")
        return None

    table_html = raw_content[table_start: table_end + 8]
    df = parse_html_table(table_html)

    if df is None:
        return None

    df = _harmonize_columns(df)
    return df


# ---------------------------------------------------------------------------
# Single filing scraper
# ---------------------------------------------------------------------------

def scrape_filing(url, accumulators, metadata_rows):
    headers = {"User-Agent": "Teerth Patel (tmp00725@uga.edu)"}

    print(f"Fetching: {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching URL: {e}")
        return

    raw_content = response.text
    accession, company, period, filed_date, cik = extract_metadata(raw_content)

    print(f"  {company}  |  {accession}  |  period: {period}")

    metadata_rows.append({
        "accession_number": accession,
        "company_name":     company,
        "report_period":    period,
        "filed_date":       filed_date,
        "cik":              cik,
    })

    exhibit_content = extract_exhibit_99(raw_content)

    for table_name, anchors in TABLES:
        print(f"  Extracting {table_name}...", end=" ")
        df = extract_table(exhibit_content, anchors)

        if df is not None:
            df.insert(0, "accession_number", accession)
            accumulators[table_name].append(df)
            print(f"OK  ({len(df)} rows, {len(df.columns)} cols)")
        else:
            print("--  (not found)")

    print()


# ---------------------------------------------------------------------------
# Save consolidated output
# ---------------------------------------------------------------------------

def _normalize_table3(frames):
    """
    Normalize all Table 3 frames to a common long schema:
      accession_number | label | value

    Issuers like Ford produce a single-value column (long format already).
    Issuers like CarMax produce one value column per row — single period per
    filing — but the column name encodes the period dates.
    Both reduce to the same 3-column schema.
    """
    out = []
    for df in frames:
        acc_col = "accession_number"
        data_cols = [c for c in df.columns if c != acc_col]

        if len(data_cols) == 0:
            continue

        label_col = data_cols[0]   # always the first non-accession col
        value_cols = data_cols[1:]

        if len(value_cols) == 0:
            # Maybe it's already long format or has only one data column.
            # Convert to label/value.
            temp = df.copy()
            temp.columns = [acc_col, "label", "value"] if len(temp.columns) == 3 else temp.columns
            out.append(temp)
            continue

        # Melt wide into long
        melted = df.melt(id_vars=[acc_col, label_col], value_vars=value_cols)
        # Rename back to canonical (we lose the date in the col header but keep the label)
        melted = melted[[acc_col, label_col, "value"]].rename(columns={label_col: "label"})
        out.append(melted)

    return pd.concat(out) if out else pd.DataFrame()

def save_outputs(accumulators, metadata_rows):
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    pd.DataFrame(metadata_rows).to_csv(os.path.join(OUTPUT_DIR, "metadata.csv"), index=False)

    for name, frames in accumulators.items():
        if not frames: continue

        if name == "table_3_distributions":
            final_df = _normalize_table3(frames)
        else:
            final_df = pd.concat(frames, ignore_index=True)
            
        final_df.to_csv(os.path.join(OUTPUT_DIR, f"{name}.csv"), index=False)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else "ford_ABS.txt"
    urls = load_filing_urls(filepath)

    accumulators = {name: [] for name, _ in TABLES}
    metadata_rows = []

    for i, url in enumerate(urls):
        print(f"\n[{i+1}/{len(urls)}] ", end="")
        scrape_filing(url, accumulators, metadata_rows)
        time.sleep(0.15)

    print("\n--- Saving consolidated output ---")
    save_outputs(accumulators, metadata_rows)
    print("\nDone!")