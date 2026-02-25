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
    ("table_2_available_funds",  ["available funds", "reserve account", "cash flows"]),
    ("table_3_distributions",    ["distributions", "payment date", "collection period"]),
    ("table_4_noteholder",       ["noteholder", "class a-1 notes", "interest distributable"]),
    ("table_5_note_balance",     ["note factor", "note balance", "principal balance"]),
]

# ---------------------------------------------------------------------------
# Column harmonization — canonical name → list of known aliases
# ---------------------------------------------------------------------------
COLUMN_ALIASES = {
    "label":                      ["label", "description", "item"],
    "dollar_amount":              ["dollar amount"],
    "num_receivables":            ["# of receivables", "number of receivables"],
    "wtd_avg_remaining_term":     ["weighted avg remaining term at cutoff",
                                   "weighted average remaining term"],
    "note_interest_rate":         ["note interest rate"],
    "final_scheduled_payment":    ["final scheduled payment date"],
    "collection_period":          ["collection period"],
    "payment_date":               ["payment date"],
    "transaction_month":          ["transaction month"],
    "calculated_amount":          ["calculated amount"],
    "amount_paid":                ["amount paid"],
    "shortfall":                  ["shortfall"],
    "carryover_shortfall":        ["carryover shortfall"],
    "remaining_available_funds":  ["remaining available funds"],
    "note_class":                 ["note class", "class", "notes"],
    "bop_balance":                ["beginning of period balance", "bop balance",
                                   "beginning balance"],
    "bop_note_factor":            ["beginning of period note factor", "bop note factor"],
    "eop_balance":                ["end of period balance", "eop balance", "ending balance"],
    "eop_note_factor":            ["end of period note factor", "eop note factor"],
}

_ALIAS_LOOKUP = {}
for _canonical, _aliases in COLUMN_ALIASES.items():
    for _alias in _aliases:
        _ALIAS_LOOKUP[re.sub(r"\s+", " ", _alias.lower().strip())] = _canonical


def _norm(text):
    return re.sub(r"\s+", " ", str(text)).lower().strip()


# ---------------------------------------------------------------------------
# HTML table parser — width-aware, colspan-aware
# ---------------------------------------------------------------------------

def parse_html_table(table_html):
    """
    Parse an SEC 10-D HTML table into a clean DataFrame.

    Structure of these tables (verified against raw HTML):
      Row 0  : Layout row — individual <td> cells each have a 'width:XX%' style.
               This tells us exactly which slots are spacers (0.1%), $ markers (1%),
               and value/label columns (wider).
      Row 1+ : Header row(s) — cells use colspan=3 to span each logical group.
               Bold text = column header.
      Data rows : cells use colspan=3 for the row-label cell and colspan=1 for
                 $ / value / spacer within each column slot.

    Algorithm:
      1. Read the layout row to get per-slot widths.
      2. Classify each slot: SPACER (<=0.15%), DOLLAR (~1%), VALUE (>1%), LABEL (wide).
         We keep LABEL and VALUE slots; discard DOLLAR and SPACER.
      3. Read the header row(s) — expand colspan cells into slot indices.
         Assign the header text to the VALUE slot(s) of each group.
         Assign 'label' to the first LABEL slot.
      4. For each data row expand colspans and keep only the kept slots.
      5. Build DataFrame.
    """
    soup = BeautifulSoup(table_html, "html.parser")
    rows = soup.find_all("tr")
    if not rows:
        return None

    def cell_text(cell):
        return re.sub(r"\s+", " ", cell.get_text(separator=" ")).strip()

    # ----------------------------------------------------------------
    # Step 1: compute max_cols from the widest row (accounting for colspan)
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
        "filed_date":r"FILED AS OF DATE:\s+(\d+)",
    }
    metadata = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, raw_content, re.IGNORECASE)
        metadata[key] = match.group(1).strip() if match else "UNKNOWN"
    return (metadata["accession"], metadata["company"],
            metadata["period"],    metadata["filed_date"], metadata["cik"])


# ---------------------------------------------------------------------------
# Exhibit isolation
# ---------------------------------------------------------------------------

def extract_exhibit_99(raw_content):
    ex99_start = re.search(r"<TYPE>EX-99", raw_content, re.IGNORECASE)
    if not ex99_start:
        print("  Warning: Could not find <TYPE>EX-99. Using full content.")
        return raw_content
    start_pos = ex99_start.start()
    next_doc = re.search(r"<DOCUMENT>", raw_content[start_pos + 1:], re.IGNORECASE)
    if next_doc:
        return raw_content[start_pos: start_pos + 1 + next_doc.start()]
    return raw_content[start_pos:]


# ---------------------------------------------------------------------------
# URL loader
# ---------------------------------------------------------------------------

def load_filing_urls(filepath):
    urls = []
    with open(filepath, "r") as f:
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
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
        print(f"    Warning: No anchors found from {anchor_texts}")
        return None

    table_start = full_text_lower.rfind("<table", 0, anchor_pos)
    table_end   = full_text_lower.find("</table>", anchor_pos)

    if table_start == -1 or table_end == -1:
        print(f"    Warning: Could not find table tags for anchor '{used_anchor}'")
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

def save_outputs(accumulators, metadata_rows):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    meta_path = os.path.join(OUTPUT_DIR, "metadata.csv")
    pd.DataFrame(metadata_rows).to_csv(meta_path, index=False)
    print(f"Saved: {meta_path}  ({len(metadata_rows)} filings)")

    for table_name, frames in accumulators.items():
        if not frames:
            print(f"  No data for {table_name}, skipping.")
            continue
        combined = pd.concat(frames, ignore_index=True, sort=False)
        out_path = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
        combined.to_csv(out_path, index=False)
        print(f"Saved: {out_path}  ({len(combined)} rows, {len(combined.columns)} cols)")


# ---------------------------------------------------------------------------
# Main
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