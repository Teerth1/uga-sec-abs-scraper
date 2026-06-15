"""
abs_scatter_honkanen.py
=======================
Reproduces Dr. Honkanen's reference scatter plot:
  X-axis : Scraped total collections from SEC 10-D filings (raw USD)
  Y-axis : Provided total collections from summary CSV   (raw USD)
  Style  : White background, brand+year labels, 1:1 match line

Labels grouped as "BRAND YYYY" (e.g. "ALLY 2017"), not per-series.
Magnitude-matching used for Honda & Volkswagen where label-pattern
extraction found all-NaN columns.

Author : [Your Name] — University of Georgia
Date   : May 2026
"""

import pandas as pd
import numpy as np
import os
import re
import csv
import itertools
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

OUT = 'output/analysis_v7'
os.makedirs(OUT, exist_ok=True)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def clean_money(x) -> float:
    if pd.isna(x): return np.nan
    s = (str(x).replace('$','').replace(',','')
                .replace('(', '-').replace(')','')
                .replace(' ','').replace('%','').strip())
    if s == '-': return 0.0
    try:    return float(s)
    except: return np.nan


def get_brand(name: str) -> str:
    if pd.isna(name): return 'OTHER'
    s = str(name).upper()
    for kw, label in [
        ('CARMAX','CARMAX'), ('FORD','FORD'), ('BMW','BMW'),
        ('TOYOTA','TOYOTA'), ('HONDA','HONDA'), ('NISSAN','NISSAN'),
        ('HYUNDAI','HYUNDAI'), ('MERCEDES','MERCEDES'),
        ('VOLKSWAGEN','VW'),
        ('ALLY','ALLY'), ('CAPITAL ONE','CAPITAL'),
        ('FIFTH THIRD','FIFTH'), ('HARLEY','HARLEY'),
        ('WORLD OMNI','WORLD'), ('WORLD','WORLD'),
        ('SANTANDER','SANTANDER'),
        ('AMERICREDIT','GM/AMERI'), ('GM FINANCIAL','GM/AMERI'),
        ('DRIVE','DRIVE'), ('CARVANA','CARVANA'), ('USAA','USAA'),
        ('EXETER','EXETER'), ('CALIFORNIA','CALIFORNIA'),
    ]:
        if kw in s: return label
    return 'OTHER'


def brand_year_label(name: str) -> str:
    """'ALLY AUTO RECEIVABLES TRUST 2017-1' -> 'ALLY 2017'"""
    brand = get_brand(name)
    year  = re.search(r'(20\d{2})', str(name))
    if brand != 'OTHER' and year:
        return f"{brand} {year.group(1)}"
    return str(name).upper()[:20]


def normalize_pool(n: str) -> str:
    """Normalize for matching — keep brand + vintage + series, strip legal boilerplate."""
    if pd.isna(n): return ''
    s = str(n).upper()
    # Strip multi-word phrases first
    for w in ['OWNER TRUST','AUTO OWNER','AUTO RECEIVABLES','AUTO LOAN TRUST',
              'RECEIVABLES TRUST','FUNDING LLC','ASSET-BACKED',
              'MOTORCYCLE TRUST','PASS-THROUGH','FLOORPLAN','ENHANCED TRUST',
              'AUTO LOAN ENHANCED','AUTOMOBILE LEASE SECURITIZATION',
              'AUTO LEASE TRUST']:
        s = s.replace(w, ' ')
    # Strip single words (including LEASE so FORD CREDIT LEASE → FORD CREDIT)
    s = re.sub(r'\b(TRUST|AUTO|OWNER|RECEIVABLES|LLC|INC|CORP|FUNDING|ASSET|'
               r'LOAN|LEASE|SELECT|ENHANCED)\b', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# ---------------------------------------------------------------------------
# Load metadata
# ---------------------------------------------------------------------------
print("Loading metadata...")
meta = pd.read_csv('output/metadata.csv')
meta['acc']       = meta['accession_number'].astype(str)
meta['dt']        = pd.to_datetime(meta['report_period'].astype(str),
                                    format='%Y%m%d', errors='coerce')
meta['month_key'] = meta['dt'].dt.strftime('%Y-%m')
meta['norm_name'] = meta['company_name'].apply(normalize_pool)
meta['label']     = meta['company_name'].apply(brand_year_label)
meta_dedup        = meta.drop_duplicates(subset='acc', keep='first')
acc_to_meta       = meta_dedup.set_index('acc')[
    ['company_name','month_key','norm_name','label']
].to_dict('index')


# ---------------------------------------------------------------------------
# Extract scraped total collections via label-pattern matching
# ---------------------------------------------------------------------------
SCRAPE_CACHE = f'{OUT}/cache_scraped_collections_v2.csv'

if os.path.exists(SCRAPE_CACHE):
    print("Loading pattern-scraped collections from cache...")
    coll_raw = pd.read_csv(SCRAPE_CACHE)
else:
    print("Extracting scraped total collections from SEC tables...")
    patterns = [
        r'Total Collections',
        r'Total Available Funds',
        r'Total Available Amount',
        r'Total Available Collections',
        r'Total Distribution Amount',
        r'Total Cash Available',
        r'Total Available for Distribution',
        r'Amount Available for Distribution',
        r'Aggregate Available Funds',
        r'Determination of Available Funds',
        r'Available Funds', # Added for CarMax
        r'Available Collections', # Added for CarMax
        r'Collections',      # Added for CarMax
    ]
    regex = re.compile('|'.join(patterns), re.IGNORECASE)
    rows  = []
    for tbl in ['table_2_available_funds','table_3_distributions',
                'table_4_noteholder','table_5_note_balance']:
        path = f'output/{tbl}.csv'
        if not os.path.exists(path): continue
        print(f"  scanning {tbl}...")
        
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            try: next(reader) # skip header
            except StopIteration: continue
            
            for row in reader:
                if not row: continue
                acc = row[0]
                # Label is typically the second column
                label = row[1] if len(row) > 1 else ''
                
                if regex.search(label):
                    # Only take the FIRST valid numeric value in the row.
                    # This prevents picking Cumulative or Prior values.
                    for i in range(2, len(row)):
                        val = clean_money(row[i])
                        if not np.isnan(val) and val > 1000:
                            rows.append({'acc': acc, 'scraped_total': val})
                            break 

    if not rows:
        raise RuntimeError("No scraped data found.")
    
    coll_raw = pd.DataFrame(rows)
    # Dedup: keep the first (highest in table) match for each accession
    coll_raw = coll_raw.drop_duplicates(subset='acc', keep='first')
    coll_raw.to_csv(SCRAPE_CACHE, index=False)
    print(f"  {len(coll_raw):,} accessions with pattern-scraped data.")

# ── Load ALL scrape caches and attach metadata ─────────────────────────────
def load_scrape_cache(path, scrape_col='scraped_total'):
    """Load a scrape cache CSV, join metadata if needed, compute norm_name."""
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    if 'val' in df.columns and scrape_col not in df.columns:
        df = df.rename(columns={'val': scrape_col})
    if 'acc' not in df.columns:
        df = df.rename(columns={df.columns[0]: 'acc'})
    df['acc'] = df['acc'].astype(str)
    # Only merge metadata if company_name or month_key is missing
    needs_meta = 'company_name' not in df.columns or 'month_key' not in df.columns
    if needs_meta:
        df = df.merge(meta_dedup[['acc','company_name','month_key']], on='acc', how='left')
    df = df[df['company_name'].notna() & (df['company_name'] != '')]
    df['norm_name'] = df['company_name'].apply(normalize_pool)
    df['label']     = df['company_name'].apply(brand_year_label)
    if scrape_col not in df.columns:
        df[scrape_col] = np.nan
    df = df.rename(columns={scrape_col: 'scraped_total'})
    return df[['acc','company_name','month_key','norm_name','label','scraped_total']]

all_scrape_parts = []

# 1. Pattern-scraped (Ally, Ford, World, Santander, etc.)
part1 = load_scrape_cache(SCRAPE_CACHE)
if not part1.empty:
    all_scrape_parts.append(('pattern', part1))
    print(f"Pattern-scraped: {len(part1):,} obs, {part1['company_name'].nunique()} pools")
    print("  Brands:", part1['company_name'].str.upper()
          .str.extract(r'(FORD|WORLD|ALLY|FIFTH|SANTANDER|CALIFORNIA|CAPITAL)')[0]
          .value_counts().to_dict())

# 2. Magnitude-matched (Honda, VW)
part2 = load_scrape_cache(f'{OUT}/cache_magnitude_matched.csv')
if not part2.empty:
    all_scrape_parts.append(('magnitude', part2))
    print(f"Magnitude-matched: {len(part2):,} obs, {part2['company_name'].nunique()} pools")

# 3. EDGAR rescrape (Toyota, BMW, Nissan, Hyundai, Mercedes, CarMax, Harley)
EDGAR_CACHE = f'{OUT}/cache_edgar_rescrape.csv'
part3 = load_scrape_cache(EDGAR_CACHE)
if not part3.empty:
    all_scrape_parts.append(('edgar', part3))
    print(f"EDGAR-rescrape: {len(part3):,} obs, {part3['company_name'].nunique()} pools")

# Combine — later caches fill gaps only (don't overwrite existing data)
if not all_scrape_parts:
    raise RuntimeError("No scrape data loaded.")
combined = all_scrape_parts[0][1].copy()
covered_norms = set(combined['norm_name'].unique())
for _tag, part in all_scrape_parts[1:]:
    new = part[~part['norm_name'].isin(covered_norms)]
    combined = pd.concat([combined, new], ignore_index=True)
    covered_norms |= set(new['norm_name'].unique())

combined = (combined[combined['scraped_total'] > 0]
            .sort_values('scraped_total', ascending=False)
            .drop_duplicates(subset=['norm_name','month_key'], keep='first'))

print(f"\nTotal scrape data: {len(combined):,} obs from "
      f"{combined['company_name'].nunique()} pools")

# ── Load provided summary ────────────────────────────────────────────────────
print("Loading provided summary...")
fs = pd.read_csv('output/final_abs_summary_dr_honkanen.csv', low_memory=False)
if 'poolname' in fs.columns:
    fs = fs.rename(columns={'poolname':'company_name'})

fs['date_dt']   = pd.to_datetime(fs['date'], errors='coerce')
fs['month_key'] = fs['date_dt'].dt.strftime('%Y-%m')
fs['norm_name'] = fs['company_name'].apply(normalize_pool)
fs['label']     = fs['company_name'].apply(brand_year_label)

# Build provided total using all available columns
fs['provided_total'] = (
    pd.to_numeric(fs['totalInterest'],  errors='coerce').fillna(0) +
    pd.to_numeric(fs['totalPrincipal'], errors='coerce').fillna(0)
)
for mask_col, col_list in [
    (fs['provided_total'] <= 0,
     ['totalInterest','principalCollections','prepaymentsInFullCollected',
      'recoveries','liquidationProceeds']),
    (fs['provided_total'] <= 0, ['totalInterest']),
]:
    mask = mask_col
    if mask.any():
        fs.loc[mask, 'provided_total'] = sum(
            pd.to_numeric(fs.loc[mask, c], errors='coerce').fillna(0)
            for c in col_list if c in fs.columns
        )

fs_monthly = fs[fs['provided_total'] > 0][
    ['company_name','norm_name','label','month_key','provided_total']
].copy()

# ── Match scraped ↔ provided on (norm_name, month_key) ───────────────────────
print("Matching scraped to provided...")
matched = fs_monthly.merge(
    combined[['norm_name','month_key','scraped_total', 'acc']],
    on=['norm_name','month_key'],
    how='inner'
)

# Unit correction
matched['ratio']       = matched['scraped_total'] / matched['provided_total']
matched['scraped_adj'] = np.where(matched['ratio'].between(500,1500),
                                   matched['scraped_total']/1000,
                                   matched['scraped_total'])
matched['ratio2'] = matched['scraped_adj'] / matched['provided_total']
matched = matched[matched['ratio2'].between(0.3, 3.0)]

print(f"Matched: {len(matched):,} monthly obs, "
      f"{matched['label'].nunique()} unique brand-years")
print(matched.groupby(matched['label'].str.split().str[0])['label'].nunique()
             .rename('brand_years_per_issuer'))


# ---------------------------------------------------------------------------
# Colour + marker — one style per brand-year label
# ---------------------------------------------------------------------------
labels_sorted = sorted(matched['label'].unique())
cmap_colors   = list(plt.cm.tab20.colors) + list(plt.cm.tab20b.colors)
markers       = ['o','^','s','D','v','P','*','X','<','>','h','8','p','H']
color_cycle   = itertools.cycle(cmap_colors)
marker_cycle  = itertools.cycle(markers)
label_style   = {lbl: {'color': next(color_cycle), 'marker': next(marker_cycle)}
                 for lbl in labels_sorted}


# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------
plt.style.use('default')
fig, ax = plt.subplots(figsize=(12, 9))

for lbl in labels_sorted:
    sub = matched[matched['label'] == lbl]
    ax.scatter(
        sub['scraped_adj'], sub['provided_total'],
        label=lbl,
        color=label_style[lbl]['color'],
        marker=label_style[lbl]['marker'],
        s=45, alpha=0.75, linewidths=0.4, edgecolors='#444',
        zorder=3
    )

# 1:1 match line
max_val = max(matched['scraped_adj'].max(), matched['provided_total'].max())
ax.plot([0,max_val],[0,max_val],
        linestyle='--', color='#555555', linewidth=1.2,
        alpha=0.7, label='1:1 Match Line')

ax.set_title('Scraped vs Provided Collection Data – All Auto Loan ABS Issuers',
             fontsize=13, fontweight='bold')
ax.set_xlabel('Scraped Collections from 10-D (USD)', fontsize=11)
ax.set_ylabel('Provided Collections (USD)',           fontsize=11)
ax.ticklabel_format(style='sci', axis='both', scilimits=(0,0))
ax.xaxis.set_major_formatter(mticker.ScalarFormatter(useMathText=True))
ax.yaxis.set_major_formatter(mticker.ScalarFormatter(useMathText=True))
ax.legend(loc='upper left', bbox_to_anchor=(1.01,1), borderaxespad=0,
          fontsize=7, ncol=3, frameon=True, edgecolor='#cccccc',
          title='Issuer (Brand Year)', title_fontsize=8)

plt.tight_layout()
out_path = f'{OUT}/01_scatter_by_brand.png'
plt.savefig(out_path, dpi=300, bbox_inches='tight')
plt.close()
print(f"\nSaved: {out_path}")
print(f"Brand-years plotted : {len(labels_sorted)}")
print(f"Observations        : {len(matched):,}")

# Add brand column for summary
matched['brand'] = matched['label'].str.split().str[0]

# --- MATCH QUALITY SUMMARY ---
matched['diff_pct'] = (abs(matched['scraped_adj'] - matched['provided_total']) / matched['provided_total']) * 100
summary = matched.groupby('brand').agg({
    'label': 'nunique',
    'scraped_adj': 'count',
    'diff_pct': 'mean'
}).rename(columns={'label': 'Cohorts', 'scraped_adj': 'Obs', 'diff_pct': 'Avg Diff %'})

print("\n--- Match Quality by Brand ---")
print(summary.sort_values('Obs', ascending=False).to_string())

# Export validation report
report_path = f'{OUT}/validation_report.csv'
matched[['acc','company_name','month_key','brand','label','scraped_adj','provided_total','diff_pct']].to_csv(report_path, index=False)
print(f"\nValidation report saved to: {report_path}")
