import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import re
import numpy as np
from datetime import timedelta

# --- CONFIG AND HELPERS ---
def clean_name(n):
    if pd.isna(n): return ""
    s = "".join([c if ord(c) < 128 else " " for c in str(n)])
    return " ".join(s.upper().split())

def get_vintage(s):
    m = re.search(r'(\d{4}-[A-Z0-9]+)', str(s))
    return m.group(1) if m else "NONE"

def get_brand(s):
    words = str(s).strip().split()
    if not words: return "UNKNOWN"
    b = words[0].upper()
    if "TOYOTA" in b: return "TOYOTA"
    if "HONDA" in b: return "HONDA"
    if "FORD" in b: return "FORD"
    if "BMW" in b: return "BMW"
    if "MERCEDE" in b: return "MERCEDES"
    if "NISSAN" in b: return "NISSAN"
    if "HYUNDAI" in b: return "HYUNDAI"
    if "VOLKS" in b: return "VOLKSWAGEN"
    if "GM" in b: return "GM"
    if "ALLY" in b: return "ALLY"
    if "AMERICREDIT" in b: return "AMERICREDIT"
    if b in ["SANTANDER", "DRIVE"]: return "SANTANDER" 
    if "HARLEY" in b: return "HARLEY"
    if "CARMAX" in b: return "CARMAX"
    if "CARVANA" in b: return "CARVANA"
    if "EXETER" in b: return "EXETER"
    return b

print("Starting FINAL ATOMIC REPAIR & HARMONIZATION (V8 - THE DEEP JOIN)...")

# 1. Load Everything
meta = pd.read_csv('output/metadata.csv')
meta['acc'] = meta['accession_number'].astype(str)
meta['dt'] = pd.to_datetime(meta['report_period'].astype(str), format='%Y%m%d', errors='coerce')
# We create two YM keys to handle the 1-month reporting lag
meta['ym'] = meta['dt'].dt.strftime('%Y-%m')
meta['ym_lag'] = (meta['dt'] - pd.DateOffset(months=1)).dt.strftime('%Y-%m')
meta['brand'] = meta['company_name'].apply(get_brand)
meta['vin'] = meta['company_name'].apply(get_vintage)

funds = pd.read_csv('output/table_2_available_funds.csv', low_memory=False)
funds['acc'] = funds['accession_number'].astype(str)

# 2. BRUTE-FORCE COLUMN SCANNER
P1_PATTERN = re.compile(r'Collections|Available Funds|Total Available|Amount Available|Yield Supplement|Total collections allocable', re.I)
val_cols = [c for c in funds.columns if c not in ['accession_number', 'label_str']]
funds['all_text'] = funds[val_cols].astype(str).agg(' '.join, axis=1)
funds['has_keyword'] = funds['all_text'].str.contains(P1_PATTERN, na=False)

funds_with_data = funds[funds['has_keyword']].copy()
numeric_only = funds_with_data[val_cols].replace(r'[$,\s()\-]', '', regex=True)
numeric_only = numeric_only.apply(pd.to_numeric, errors='coerce').fillna(0.0)
funds_with_data['sc_val'] = numeric_only.max(axis=1)

monthly_scraped = funds_with_data[funds_with_data['sc_val'] > 0].groupby('acc')['sc_val'].max().reset_index()
monthly_scraped.rename(columns={'sc_val': 'scraped_total_collections'}, inplace=True)

# 3. INTEGRATE REPAIRS
if os.path.exists('output/repaired_collections_full.csv'):
    rep = pd.read_csv('output/repaired_collections_full.csv')
    rep_map = rep[rep['scraped_total_collections'] > 0].set_index('accession_number')['scraped_total_collections'].to_dict()
    for i, row in monthly_scraped.iterrows():
        acc = row['acc']
        if acc in rep_map:
            monthly_scraped.at[i, 'scraped_total_collections'] = rep_map[acc]
            del rep_map[acc]
    if rep_map:
        new_rows = [{'acc': k, 'scraped_total_collections': v} for k, v in rep_map.items()]
        monthly_scraped = pd.concat([monthly_scraped, pd.DataFrame(new_rows)], ignore_index=True)

# 4. UNIFY SEC DATA
final_summary = pd.merge(monthly_scraped, meta, left_on='acc', right_on='acc', how='inner')
final_summary.to_csv('output/unified_monthly_summary.csv', index=False)

# 5. THE DEEP JOIN — 5-Priority Cascading Date-Tolerance (V9)
provided = pd.read_csv('collections_dirty.csv')
provided['date'] = pd.to_datetime(provided['date'], errors='coerce', utc=True)
provided['ym'] = provided['date'].dt.strftime('%Y-%m')
provided['brand'] = provided['poolname'].apply(get_brand)
provided['vin'] = provided['poolname'].apply(get_vintage)

# Build all offset join keys on the SEC side
for offset in range(-2, 3):  # -2, -1, 0, +1, +2
    ym_off = (final_summary['dt'] + pd.DateOffset(months=offset)).dt.strftime('%Y-%m')
    final_summary[f'jk_vin_{offset}'] = final_summary['brand'] + "_" + final_summary['vin'] + "_" + ym_off
    final_summary[f'jk_nov_{offset}'] = final_summary['brand'] + "_NONE_" + ym_off

# Build fast O(1) lookup dicts
lookups = {}
for offset in range(-2, 3):
    lookups[f'vin_{offset}'] = final_summary.dropna(subset=[f'jk_vin_{offset}']).groupby(f'jk_vin_{offset}')['scraped_total_collections'].max().to_dict()
    lookups[f'nov_{offset}'] = final_summary.dropna(subset=[f'jk_nov_{offset}']).groupby(f'jk_nov_{offset}')['scraped_total_collections'].max().to_dict()

print(f"  Joining {len(provided)} provided rows via 5-Priority Deep Join (V9)...")

def resolve_match(row):
    brand = row['brand']
    vin = row['vin']
    ym = row['ym']
    # Try each offset: 0 (exact), -1, +1, -2, +2
    for offset in [0, -1, 1, -2, 2]:
        try:
            oym = (pd.Timestamp(ym + '-01') + pd.DateOffset(months=offset)).strftime('%Y-%m')
        except Exception:
            continue
        # P-A: Exact Brand + Vintage + offset YM
        val = lookups.get(f'vin_{offset}', {}).get(f'{brand}_{vin}_{oym}')
        if val and val > 0:
            return val
        # P-B: Brand + ANY Vintage + offset YM (vintage-agnostic fallback)
        val = lookups.get(f'nov_{offset}', {}).get(f'{brand}_NONE_{oym}')
        if val and val > 0:
            return val
    return None

provided['scraped_total_collections'] = provided.apply(resolve_match, axis=1)

# Deduplicate — keep highest value per pool+date
final_results = provided.sort_values('scraped_total_collections', ascending=False).drop_duplicates(
    subset=['poolname', 'date'], keep='first')

# 6. REPORTING
total = len(provided)
matched = final_results['scraped_total_collections'].notna().sum()
print(f"  FINAL SUCCESS RATE (V9): {matched} / {total} matched ({matched/total*100:.1f}%)")


final_results.to_csv('output/final_abs_summary_dr_honkanen.csv', index=False)
print("Saved final_abs_summary_dr_honkanen.csv")

# Plot
plt.figure(figsize=(10,6))
matched_df = final_results[final_results['scraped_total_collections'].notna()]
if not matched_df.empty:
    plt.scatter(matched_df.iloc[:,2:6].sum(axis=1), matched_df['scraped_total_collections'], alpha=0.5, color='orange')
    plt.title(f'Final ABS Deep Recovery (V8): {matched} / {total} Matches')
    plt.savefig('final_recovery_v8.png')

print("Execution Complete.")
