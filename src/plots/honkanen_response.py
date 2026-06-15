"""
honkanen_response.py
--------------------
Addresses all requests from Dr. Honkanen's email:
  1. Complete list of ABS pool names (from provided data)
  2. Plot all observations over time (from provided data)
  3. Plot by issuer (strip vintage codes)
  4. Diagnose why some pools are missing from the scraped dataset
  5. Check for data gaps (full monthly series per pool)
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import re
import os
from itertools import cycle

# ─── Color / marker helpers ───────────────────────────────────────────────────
MARKERS = ['o', 's', '^', 'D', 'v', 'P', '*', 'X', 'h', '<', '>', 'p', 'H', '+']

def make_color_map(keys):
    cmap = plt.get_cmap('tab20')
    keys = sorted(set(keys))
    colors = [cmap(i % 20) for i in range(len(keys))]
    return {k: (colors[i], MARKERS[i % len(MARKERS)]) for i, k in enumerate(keys)}

# ─── Name cleaners ────────────────────────────────────────────────────────────
def clean_name(n):
    if pd.isna(n): return ""
    s = "".join([c if ord(c) < 128 else " " for c in str(n)])
    return " ".join(s.upper().split())

def extract_issuer(poolname_clean):
    """Strip vintage codes and noise words; return ~brand name."""
    # Remove vintage patterns: 2018-1, 2021-A, 2018-N3, etc.
    s = re.sub(r'\d{4}-[A-Z0-9]+', '', poolname_clean)
    # Strip common ABS boilerplate words
    noise_words = (
        r'\b(RECEIVABLES|TRUST|OWNER|AUTO|AUTOMOBILE|LOAN|ENHANCED|ASSETS|'
        r'LEASING|LEASE|FUNDING|NOTES?|VEHICLE|SECURITIZATION|GRANTOR|'
        r'SELECT|PRIME|CONSUMER|FINANCIAL|DRIVE|PASS|THROUGH|MASTER|'
        r'LLC|INC|CORP|LTD)\b'
    )
    s = re.sub(noise_words, ' ', s, flags=re.I)
    words = s.split()
    # Return up to 2 meaningful words as the issuer label
    return " ".join(words[:2]) if words else poolname_clean[:10]

# ─── Load and clean the provided dataset ─────────────────────────────────────
print("Loading collections_dirty.csv...")
df = pd.read_csv('collections_dirty.csv', header=0)
df.columns = df.columns.str.strip()

keep_cols = ['poolname', 'date', 'totalInterest', 'prepaymentsInFullCollected',
             'principalCollections', 'recoveries', 'liquidationProceeds']
for col in keep_cols:
    if col not in df.columns:
        df[col] = 0.0

df = df[keep_cols].copy()
df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
df = df.dropna(subset=['date', 'poolname'])
df['year_month'] = df['date'].dt.strftime('%Y-%m')
df['date_m'] = pd.to_datetime(df['year_month'])  # first-of-month for plotting
df['poolname_clean'] = df['poolname'].apply(clean_name)
df['issuer'] = df['poolname_clean'].apply(extract_issuer)

sum_cols = ['totalInterest', 'prepaymentsInFullCollected', 'principalCollections', 'recoveries']
for col in sum_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
df['total_collections'] = df[sum_cols].sum(axis=1)

# Keep only rows with positive collections (mirrors analyze_abs.py logic)
df_pos = df[df['total_collections'] > 0].copy()

# Deduplicate
df_pos = df_pos.sort_values('date').drop_duplicates(
    subset=['poolname_clean', 'year_month'], keep='first')

print(f"  Rows with positive collections: {len(df_pos)}")
print(f"  Unique pool names: {df_pos['poolname_clean'].nunique()}")
print(f"  Unique issuers (extracted): {df_pos['issuer'].nunique()}")

# ─── 1. Complete Pool List ────────────────────────────────────────────────────
all_pools = sorted(df['poolname_clean'].unique())
with open('output/pool_list_provided.txt', 'w') as f:
    f.write("\n".join(all_pools))
print(f"\n[1] Saved {len(all_pools)} pool names to output/pool_list_provided.txt")

# Also save a human-readable summary
pool_summary = (
    df.groupby('poolname_clean')
      .agg(
          n_obs=('total_collections', 'count'),
          first_date=('year_month', 'min'),
          last_date=('year_month', 'max'),
          has_data=('total_collections', lambda x: (x > 0).sum())
      )
      .reset_index()
      .sort_values('poolname_clean')
)
pool_summary.to_csv('output/pool_summary_provided.csv', index=False)
print(f"   Saved pool summary (obs counts, date ranges) to output/pool_summary_provided.csv")

# ─── 2. Diagnose Missing Pools ───────────────────────────────────────────────
print("\n[2] Diagnosing missing pools...")
unified = pd.read_csv('output/unified_monthly_summary.csv')
unified['company_name_clean'] = unified['company_name'].apply(clean_name)

scraped_pools = set(unified['company_name_clean'].unique())
provided_pools = set(df_pos['poolname_clean'].unique())

missing_from_scraped = sorted(provided_pools - scraped_pools)
only_in_scraped = sorted(scraped_pools - provided_pools)

print(f"  Provided pools (with data): {len(provided_pools)}")
print(f"  Scraped pools:              {len(scraped_pools)}")
print(f"  Provided but NOT scraped:   {len(missing_from_scraped)}")
print(f"  Scraped but NOT provided:   {len(only_in_scraped)}")

with open('output/missing_pools_diagnosis.txt', 'w') as f:
    f.write(f"=== Pools in provided data but MISSING from scraped data ({len(missing_from_scraped)}) ===\n")
    f.write("\n".join(missing_from_scraped))
    f.write(f"\n\n=== Pools in scraped data but NOT in provided data ({len(only_in_scraped)}) ===\n")
    f.write("\n".join(only_in_scraped))
print("   Saved diagnosis to output/missing_pools_diagnosis.txt")

# ─── 3. Plot: All Pools Over Time (Provided Data, All Observations) ───────────
print("\n[3] Plotting all provided observations over time...")

all_issuers = sorted(df_pos['issuer'].unique())
color_map = make_color_map(all_issuers)

fig, ax = plt.subplots(figsize=(16, 9))

for issuer in all_issuers:
    subset = df_pos[df_pos['issuer'] == issuer]
    if subset.empty:
        continue
    color, marker = color_map[issuer]
    ax.scatter(
        subset['date_m'], subset['total_collections'],
        label=issuer, color=color, marker=marker,
        alpha=0.55, s=35, edgecolors='none'
    )

ax.set_xlabel('Date', fontsize=12)
ax.set_ylabel('Total Collections (USD)', fontsize=12)
ax.set_title('All Provided ABS Pool Observations Over Time\n(colored by issuer)', fontsize=14)
ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(
    lambda x, _: f'${x/1e6:.0f}M'))
ax.legend(bbox_to_anchor=(1.01, 1), loc='upper left', ncol=2, fontsize=7,
          markerscale=1.5, framealpha=0.8)
ax.grid(True, linestyle='--', alpha=0.4)
plt.tight_layout()
plt.savefig('all_pools_provided_time_series.png', dpi=150, bbox_inches='tight')
plt.close()
print("   Saved 'all_pools_provided_time_series.png'")

# ─── 4. Plot: Aggregated by Issuer Over Time ──────────────────────────────────
print("\n[4] Plotting by issuer (aggregated, vintage codes stripped)...")

iss_df = (
    df_pos.groupby(['issuer', 'date_m'])['total_collections']
    .sum()
    .reset_index()
    .sort_values('date_m')
)

color_map2 = make_color_map(iss_df['issuer'].unique())

fig2, ax2 = plt.subplots(figsize=(16, 9))
for issuer in sorted(iss_df['issuer'].unique()):
    subset = iss_df[iss_df['issuer'] == issuer]
    color, marker = color_map2[issuer]
    ax2.plot(
        subset['date_m'], subset['total_collections'],
        label=issuer, color=color, marker=marker,
        linewidth=1.8, markersize=5, alpha=0.85
    )

ax2.set_xlabel('Date', fontsize=12)
ax2.set_ylabel('Aggregated Total Collections (USD)', fontsize=12)
ax2.set_title('Provided ABS Collections by Issuer Over Time\n(vintage codes stripped; e.g. CARMAX, FORD, CARVANA)', fontsize=14)
ax2.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(
    lambda x, _: f'${x/1e6:.0f}M'))
ax2.legend(bbox_to_anchor=(1.01, 1), loc='upper left', ncol=2, fontsize=7,
           markerscale=1.5, framealpha=0.8)
ax2.grid(True, linestyle='--', alpha=0.4)
plt.tight_layout()
plt.savefig('all_issuers_provided_time_series.png', dpi=150, bbox_inches='tight')
plt.close()
print("   Saved 'all_issuers_provided_time_series.png'")

# ─── 5. Data Gap Analysis (per pool, monthly continuity) ─────────────────────
print("\n[5] Checking for gaps in monthly data per pool...")

gap_records = []
for pool, grp in df_pos.groupby('poolname_clean'):
    grp = grp.sort_values('date_m')
    min_d, max_d = grp['date_m'].min(), grp['date_m'].max()
    expected = pd.date_range(start=min_d, end=max_d, freq='MS')
    actual = set(grp['date_m'].dt.to_period('M').astype(str))
    missing = [d.strftime('%Y-%m') for d in expected if d.strftime('%Y-%m') not in actual]
    gap_records.append({
        'pool': pool,
        'issuer': grp['issuer'].iloc[0],
        'n_observations': len(grp),
        'first_month': min_d.strftime('%Y-%m'),
        'last_month': max_d.strftime('%Y-%m'),
        'expected_months': len(expected),
        'missing_months': len(missing),
        'missing_list': ', '.join(missing[:6]) + ('...' if len(missing) > 6 else '')
    })

gap_df = pd.DataFrame(gap_records).sort_values(['missing_months', 'pool'], ascending=[False, True])
gap_df.to_csv('output/pool_gap_analysis.csv', index=False)

n_with_gaps = (gap_df['missing_months'] > 0).sum()
print(f"   {n_with_gaps} pools have at least one gap.")
print(f"   Saved per-pool gap analysis to output/pool_gap_analysis.csv")

# Print top offenders
if n_with_gaps > 0:
    top = gap_df[gap_df['missing_months'] > 0].head(20)
    print("\n   Top 20 pools with most missing months:")
    print(top[['pool', 'n_observations', 'expected_months', 'missing_months', 'missing_list']].to_string(index=False))

# High-level by issuer
issuer_gaps = (
    gap_df.groupby('issuer')
    .agg(
        total_pools=('pool', 'count'),
        pools_with_gaps=('missing_months', lambda x: (x > 0).sum()),
        total_missing_months=('missing_months', 'sum')
    )
    .reset_index()
    .sort_values('total_missing_months', ascending=False)
)
issuer_gaps.to_csv('output/issuer_gap_summary.csv', index=False)
print("\n   Issuer-level gap summary saved to output/issuer_gap_summary.csv")
print(issuer_gaps[issuer_gaps['pools_with_gaps'] > 0].to_string(index=False))

print("\n=== Done! ===")
print("Output files:")
print("  output/pool_list_provided.txt         - Complete list of ABS pool names")
print("  output/pool_summary_provided.csv      - Pool counts and date ranges")
print("  output/missing_pools_diagnosis.txt    - Which pools are missing from scraped data")
print("  all_pools_provided_time_series.png    - All observations plot")
print("  all_issuers_provided_time_series.png  - Aggregated by issuer plot")
print("  output/pool_gap_analysis.csv          - Per-pool monthly gap analysis")
print("  output/issuer_gap_summary.csv         - Issuer-level gap summary")
