import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import random
import numpy as np
import re

# ── Colormap for many issuers ────────────────────────────────────────────────
MARKERS = ['o', 's', '^', 'D', 'v', 'P', '*', 'X', 'h', '<', '>', 'p', 'H', '+']

def make_color_map(issuers):
    cmap = plt.get_cmap('tab20')
    colors = [cmap(i % 20) for i in range(len(issuers))]
    return {iss: (colors[i], MARKERS[i % len(MARKERS)]) for i, iss in enumerate(sorted(issuers))}

# ── Dollar cleaner ────────────────────────────────────────────────────────────
def clean_dollar(s):
    if pd.isna(s) or s == '': return 0.0
    if isinstance(s, (int, float)): return float(s)
    s = str(s).strip()
    if re.search(r'[a-zA-Z]', s): return 0.0
    is_neg = '(' in s or s.startswith('-')
    s = "".join([c for c in s if c.isdigit() or c == '.'])
    try:
        val = float(s) if s else 0.0
        return -val if is_neg else val
    except ValueError:
        return 0.0

# ── Issuer extraction (first keyword, strip noise words) ─────────────────────
NOISE = {'AUTO', 'OWNER', 'TRUST', 'VEHICLE', 'AUTOMOBILE', 'RECEIVABLES',
         'FUNDING', 'SECURITIES', 'MASTER', 'NOTE', 'NOTES', 'LLC', 'INC',
         'CORP', 'GRANTOR', 'PASS', 'THROUGH', 'LEASE', 'LEASING'}
def extract_issuer(poolname_clean):
    # Strip vintage like 2018-1, 2021-A, etc.
    s = re.sub(r'\d{4}-\d+[A-Z]?|\d{4}-[A-Z]|\d{4}-\d+', '', poolname_clean)
    # Strip common noise words
    s = re.sub(r'\b(RECEIVABLES|TRUST|OWNER|AUTO|LOAN|ENHANCED|ASSETS|LEASING|LEASE|AUTOMOBILE|FUNDING|NOTES?|VEHICLE)\b', ' ', s, flags=re.I)
    words = s.split()
    # Return first 1-2 meaningful words as issuer
    return " ".join(words[:2]) if words else poolname_clean[:10]

def clean_name(n):
    if pd.isna(n): return ""
    s = "".join([c if ord(c) < 128 else " " for c in str(n)])
    return " ".join(s.upper().split())

def analyze():
    print("Loading datasets...")

    # ── 1. Load provided data (original collections_dirty.csv with all issuers) ──
    provided_df = pd.read_csv('collections_dirty.csv', header=0)
    provided_df.columns = provided_df.columns.str.strip()

    # Drop the helper columns Dr. Honkanen added (C+G, C+D+E+F etc.)
    keep_cols = ['poolname', 'date', 'totalInterest', 'prepaymentsInFullCollected',
                 'principalCollections', 'recoveries', 'liquidationProceeds']
    for col in keep_cols:
        if col not in provided_df.columns:
            provided_df[col] = 0.0

    provided_df = provided_df[keep_cols].copy()
    provided_df['date'] = pd.to_datetime(provided_df['date'], errors='coerce', utc=True)
    provided_df = provided_df.dropna(subset=['date', 'poolname'])

    provided_df['year_month']     = provided_df['date'].dt.strftime('%Y-%m')
    provided_df['poolname_clean'] = provided_df['poolname'].apply(clean_name)
    provided_df['issuer']         = provided_df['poolname_clean'].apply(extract_issuer)

    # ── Correct total: EXCLUDE liquidationProceeds (it's cumulative, not monthly) ──
    sum_cols = ['totalInterest', 'prepaymentsInFullCollected', 'principalCollections', 'recoveries']
    for col in sum_cols:
        provided_df[col] = pd.to_numeric(provided_df[col], errors='coerce').fillna(0)
    provided_df['total_collections_provided'] = provided_df[sum_cols].sum(axis=1)
    
    # Filter anomalous negative provided collections (fixes -600M y-axis compression)
    provided_df = provided_df[provided_df['total_collections_provided'] > 0]

    # ── Deduplicate (Ford May 2022 duplicate fix) ────────────────────────────
    provided_df = provided_df.sort_values('date').drop_duplicates(
        subset=['poolname_clean', 'year_month'], keep='first')

    print(f"  Provided data: {len(provided_df)} rows, {provided_df['issuer'].nunique()} issuers")

    # ── 2. Load scraped metadata ─────────────────────────────────────────────
    metadata_df = pd.read_csv('output/metadata.csv')
    metadata_df['company_name_clean'] = metadata_df['company_name'].apply(clean_name)
    metadata_df['year_month'] = pd.to_datetime(
        metadata_df['report_period'].astype(str), format='%Y%m%d', errors='coerce', utc=True
    ).dt.strftime('%Y-%m')

    # ── 3. Aggregate monthly collections from Table 2 ────────────────────────
    print("Aggregating monthly collections (Vectorized)...")
    funds_df = pd.read_csv('output/table_2_available_funds.csv', low_memory=False)

    # Identifty columns to check for numbers (excluding metadata and ALL label columns)
    label_cols = [c for c in funds_df.columns if 'label' in c.lower()]
    cols_to_check = [c for c in funds_df.columns if c not in (['accession_number', 'label_str'] + label_cols)]
    print(f"  Aggregating numeric data from {len(cols_to_check)} columns...")
    
    # Fast vectorized cleaning of ALL numeric columns at once
    print(f"  Cleaning {len(cols_to_check)} columns for {len(funds_df)} rows...")
    val_df = pd.DataFrame(index=funds_df.index)
    for col in cols_to_check:
        # Fast regex-free cleaning for common cases
        s_clean = funds_df[col].astype(str).str.replace(r'[$,\s()\-]', '', regex=True)
        val_df[col] = pd.to_numeric(s_clean, errors='coerce').fillna(0.0)
    
    # Final value is the max across all numeric candidates for that row
    funds_df['val_float'] = val_df.max(axis=1)

    # New Strategy: Check ALL columns that might be labels (handles multi-column layouts like VW)
    label_cols = [c for c in funds_df.columns if 'label' in c.lower()]
    print(f"  Checking {len(label_cols)} label columns: {label_cols}")
    
    funds_df['is_prio_1'] = False
    funds_df['is_prio_2'] = False
    
    p1_regex = '^Collections$|Total Collections|Available Collections|Available Finance Charge Collections|Collections on Receivables|Total Finance Charge and Principal|Total Available Collections|Amount Available for Deposit'
    p2_regex = 'Available Funds|Total Available|Total funds available|Total Available Amount|Available Amounts'

    for l_col in label_cols:
        l_str = funds_df[l_col].astype(str).str.strip()
        funds_df['is_prio_1'] |= l_str.str.contains(p1_regex, case=False, na=False, regex=True)
        funds_df['is_prio_2'] |= l_str.str.contains(p2_regex, case=False, na=False, regex=True)

    # Fast vectorized aggregation with priority
    print("  Grouping by accession (Priority 1)...")
    p1_mask = (funds_df['is_prio_1']) & (funds_df['val_float'] > 0)
    p1_agg = funds_df[p1_mask].groupby('accession_number')['val_float'].max().reset_index()
    p1_agg.rename(columns={'val_float': 'scraped_total_collections'}, inplace=True)

    print("  Grouping by accession (Priority 2 Fallback)...")
    p2_mask = (funds_df['is_prio_2']) & (funds_df['val_float'] > 0)
    p2_agg = funds_df[p2_mask].groupby('accession_number')['val_float'].max().reset_index()
    p2_agg.rename(columns={'val_float': 'scraped_total_collections'}, inplace=True)

    # Merge: Priority 1 wins, Priority 2 fills gaps
    monthly_agg = pd.merge(p1_agg, p2_agg, on='accession_number', how='outer', suffixes=('_p1', '_p2'))
    monthly_agg['scraped_total_collections'] = monthly_agg['scraped_total_collections_p1'].fillna(monthly_agg['scraped_total_collections_p2'])
    monthly_agg = monthly_agg[['accession_number', 'scraped_total_collections']]

    unified_summary = pd.merge(monthly_agg, metadata_df, on='accession_number')
    unified_summary = pd.merge(monthly_agg, metadata_df, on='accession_number')
    
    # Generate complete pool list for Dr. Honkanen
    pool_list = sorted(unified_summary['company_name'].unique())
    with open('output/pool_list.txt', 'w') as f:
        f.write("\n".join(pool_list))
    print(f"  Saved pool list with {len(pool_list)} names to output/pool_list.txt")

    unified_summary.to_csv('output/unified_monthly_summary.csv', index=False)
    print(f"  Scraped data: {len(unified_summary)} filings, {unified_summary['company_name'].nunique()} pools")

    # ── NEW: Plot All Observations (Over Time) ───────────────────────────────
    # Ensure date parsing for the X-axis
    unified_summary['date'] = pd.to_datetime(unified_summary['year_month'])
    unified_summary['parent_issuer'] = unified_summary['company_name_clean'].apply(extract_issuer)
    
    print("Generating comprehensive time-series plot of all pools...")
    fig3, ax3 = plt.subplots(figsize=(14, 8))
    
    parent_issuers = unified_summary['parent_issuer'].unique()
    color_map_iss = make_color_map(parent_issuers)
    
    # We plot each individual pool (178), but color them by their parent issuer (20)
    for issuer in parent_issuers:
        subset = unified_summary[unified_summary['parent_issuer'] == issuer]
        if subset.empty: continue
        color, marker = color_map_iss[issuer]
        ax3.scatter(subset['date'], subset['scraped_total_collections'], 
                    label=issuer, # Now we can safely label since there's only ~20
                    color=color, marker=marker, alpha=0.6, s=40, edgecolors='none')
                    
    ax3.set_xlabel('Date (Year-Month)', fontsize=12)
    ax3.set_ylabel('Scraped Collections (USD)', fontsize=12)
    ax3.set_title('Absolute Tracking: All Recovered ABS Pools Over Time', fontsize=14)
    # Put legend outside
    ax3.legend(bbox_to_anchor=(1.01, 1), loc='upper left', ncol=2, fontsize=8) 
    ax3.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('all_pools_time_series.png', dpi=150, bbox_inches='tight')
    print("Saved 'all_pools_time_series.png'")

    print("Generating comprehensive time-series plot by Issuer...")
    unified_summary['parent_issuer'] = unified_summary['company_name_clean'].apply(extract_issuer)
    
    iss_time_df = unified_summary.groupby(['parent_issuer', 'date']).agg({
        'scraped_total_collections': 'sum'
    }).reset_index()

    fig4, ax4 = plt.subplots(figsize=(14, 8))
    parent_issuers = unified_summary['parent_issuer'].unique()
    color_map_iss = make_color_map(parent_issuers)

    for issuer in parent_issuers:
        subset = iss_time_df[iss_time_df['parent_issuer'] == issuer]
        if subset.empty: continue
        color, marker = color_map_iss[issuer]
        ax4.plot(subset['date'], subset['scraped_total_collections'], 
                 label=issuer, color=color, marker=marker, 
                 alpha=0.8, markersize=8, linewidth=2)
                 
    ax4.set_xlabel('Date (Year-Month)', fontsize=12)
    ax4.set_ylabel('Aggregated Scraped Collections (USD)', fontsize=12)
    ax4.set_title('Absolute Tracking: Aggregated Parent Issuers Over Time', fontsize=14)
    # Put legend outside
    ax4.legend(bbox_to_anchor=(1.01, 1), loc='upper left', ncol=2, fontsize=8) 
    ax4.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('all_issuers_time_series.png', dpi=150, bbox_inches='tight')
    print("Saved 'all_issuers_time_series.png'")

    # ── 4. Join for scatter plot ─────────────────────────────────────────────

    print("Joining datasets...")
    final_df = pd.merge(
        provided_df, unified_summary,
        left_on=['poolname_clean', 'year_month'],
        right_on=['company_name_clean', 'year_month'],
        how='inner'
    )

    if final_df.empty:
        print("No matches! Check joining keys.")
        return

    print(f"Found {len(final_df)} matching records across {final_df['issuer'].nunique()} issuers.")

    # ── 5. Collections Comparison Scatter Plot (multi-issuer) ────────────────
    print("Generating scatter plot...")
    color_map = make_color_map(final_df['issuer'].unique())

    fig, ax = plt.subplots(figsize=(14, 10))
    for issuer, (color, marker) in color_map.items():
        subset = final_df[final_df['issuer'] == issuer]
        if subset.empty: continue
        ax.scatter(subset['scraped_total_collections'],
                   subset['total_collections_provided'],
                   label=issuer, color=color, marker=marker,
                   alpha=0.8, s=60, edgecolors='white', linewidths=0.4)

    all_vals = pd.concat([final_df['scraped_total_collections'],
                           final_df['total_collections_provided']])
    max_val = all_vals.dropna().max() if not all_vals.dropna().empty else 1e8
    ax.plot([0, max_val], [0, max_val], 'k--', alpha=0.3, label='1:1 Match Line')

    ax.set_xlabel('Scraped Collections from 10-D (USD)', fontsize=12)
    ax.set_ylabel('Provided Collections (USD)', fontsize=12)
    ax.set_title('Scraped vs Provided Collection Data – All Auto Loan ABS Issuers', fontsize=14)
    ax.legend(bbox_to_anchor=(1.01, 1), loc='upper left', fontsize=9)
    ax.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout() 
    plt.savefig('collections_comparison.png', dpi=150, bbox_inches='tight')
    print("Saved 'collections_comparison.png'")

    # ── 5b. Issuer-Level Aggregation Plot (Dr. Honkanen's Request) ───────────
    print("Generating issuer-level scatter plot...")
    issuer_df = final_df.groupby(['issuer', 'year_month']).agg({
        'scraped_total_collections': 'sum',
        'total_collections_provided': 'sum'
    }).reset_index()
    
    fig2, ax2 = plt.subplots(figsize=(12, 10))
    # Use color map on the aggregated issuers
    color_map_iss = make_color_map(issuer_df['issuer'].unique())
    
    for issuer, (color, marker) in color_map_iss.items():
        subset = issuer_df[issuer_df['issuer'] == issuer]
        ax2.scatter(subset['scraped_total_collections'],
                   subset['total_collections_provided'],
                   label=issuer, color=color, marker=marker,
                   alpha=0.8, s=80, edgecolors='white', linewidths=0.5)

    max_val_iss = max(issuer_df['scraped_total_collections'].max(), issuer_df['total_collections_provided'].max())
    ax2.plot([0, max_val_iss], [0, max_val_iss], 'k--', alpha=0.3, label='1:1 Match Line')
    
    ax2.set_xlabel('Scraped Collections (USD)', fontsize=12)
    ax2.set_ylabel('Provided Collections (USD)', fontsize=12)
    ax2.set_title('Issuer-Level Collection Data Comparison', fontsize=14)
    ax2.legend(bbox_to_anchor=(1.01, 1), loc='upper left', fontsize=10)
    ax2.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig('issuer_scatter_plot.png', dpi=150, bbox_inches='tight')
    print("Saved 'issuer_scatter_plot.png'")

    # ── 6. Per-issuer discrepancy summary ────────────────────────────────────
    final_df['diff'] = final_df['total_collections_provided'] - final_df['scraped_total_collections']
    final_df['diff_pct'] = final_df['diff'] / final_df['total_collections_provided'] * 100
    summary = final_df.groupby('issuer').agg(
        n=('diff', 'count'),
        mean_diff=('diff', 'mean'),
        mean_diff_pct=('diff_pct', 'mean'),
    ).round(2)
    print("\nPer-issuer discrepancy:")
    print(summary.to_string())
    summary.to_csv('output/issuer_discrepancy_summary.csv')
    print("Saved 'output/issuer_discrepancy_summary.csv'")

    # ── 7. Data Gap Audit (Monthly Continuity) ───────────────────────────────
    print("\nAuditing for Data Gaps...")
    gap_reports = []
    for issuer in final_df['issuer'].unique():
        iss_df = final_df[final_df['issuer'] == issuer].copy()
        iss_df['date'] = pd.to_datetime(iss_df['year_month'])
        iss_df = iss_df.sort_values('date')
        
        if len(iss_df) > 1:
            date_range = pd.date_range(start=iss_df['date'].min(), end=iss_df['date'].max(), freq='MS')
            missing = [d.strftime('%Y-%m') for d in date_range if d not in iss_df['date'].values]
            if missing:
                gap_reports.append(f"{issuer}: Missing {len(missing)} months {missing[:3]}...")
    
    if gap_reports:
        with open('output/data_gaps.txt', 'w') as f:
            f.write("\n".join(gap_reports))
        print(f"  Detected gaps in {len(gap_reports)} issuers. Detailed in output/data_gaps.txt")
    else:
        print("  Excellent: 100% monthly continuity across all matched issuers.")

if __name__ == "__main__":
    analyze()
