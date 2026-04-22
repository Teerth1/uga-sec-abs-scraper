import pandas as pd
import numpy as np
import os
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates

print("Starting Dr. Honkanen Analytics V6...")
OUT = 'output/analysis_v6'
os.makedirs(OUT, exist_ok=True)

plt.style.use('dark_background')
plt.rcParams.update({
    'font.size': 12, 'axes.labelsize': 13, 'axes.titlesize': 15,
    'xtick.labelsize': 11, 'ytick.labelsize': 11, 'legend.fontsize': 10,
    'axes.grid': True, 'grid.alpha': 0.18, 'axes.edgecolor': '#444444',
    'figure.facecolor': '#0d0d0d', 'axes.facecolor': '#111111',
})

def brand_color(b):
    colors = {
        'CARMAX': '#00ffcc', # Neon Teal
        'FORD': '#ff5533',   # Bright Orange/Red
        'ALLY': '#ffcc00',   # Golden Yellow
        'FIFTH': '#4488ff',  # Royal Blue
        'GM': '#0077ff',     # GM Blue
        'HYUNDAI': '#33cc33',# Green
        'HONDA': '#ff33ff',  # Magenta
        'TOYOTA': '#ffcc00', # Gold
        'NISSAN': '#ff6699', # Pink
        'SANTANDER': '#ff0000', # Red
        'VOLKSWAGEN': '#003366',# Dark Blue
        'BMW': '#0099cc',    # Light Blue
        'VW': '#003366',
        'HARLEY': '#ff6600', # Harley Orange
        'WORLD': '#66ccff',  # World Blue
        'CHASE': '#1111ff',  # Chase Blue
        'CAPITAL': '#009900',# Capital Green
        'GM/AMERI': '#cc0000',# GM Red
    }
    return colors.get(str(b).upper(), '#888888')


def get_brand(name):
    if pd.isna(name): return 'OTHER'
    s = str(name).upper()
    if 'CARMAX' in s: return 'CARMAX'
    if 'FORD' in s:   return 'FORD'
    if 'BMW' in s:    return 'BMW'
    if 'TOYOTA' in s: return 'TOYOTA'
    if 'HONDA' in s:  return 'HONDA'
    if 'NISSAN' in s: return 'NISSAN'
    if 'HYUNDAI' in s: return 'HYUNDAI'
    if 'MERCEDES' in s or 'DAIMLER' in s or 'CHRYSLER' in s: return 'MERCEDES'
    if 'VOLKSWAGEN' in s or 'VW' in s: return 'VW'
    if 'ALLY' in s:   return 'ALLY'
    if 'CAPITAL' in s: return 'CAPITAL'
    if 'FIFTH' in s:  return 'FIFTH'
    if 'CHASE' in s:  return 'CHASE'
    if 'HARLEY' in s: return 'HARLEY'
    if 'WORLD' in s:  return 'WORLD'
    if 'SANTANDER' in s: return 'SANTANDER'
    if 'AMERICREDIT' in s or 'GM FINANCIAL' in s: return 'GM/AMERI'
    return 'OTHER'


def clean_money(x):
    if pd.isna(x): return np.nan
    s = str(x).replace('$','').replace(',','').replace('(', '-').replace(')', '').replace(' ','').strip()
    try:
        v = float(s)
        return v if abs(v) > 0.1 else np.nan
    except:
        return np.nan

def normalize_name(n):
    if pd.isna(n): return ""
    s = str(n).upper().replace(',','').replace('.','').replace('-',' ')
    # Strip everything after 'TRUST' or year patterns
    s = re.sub(r'\s+TRUST.*$', '', s)
    s = re.sub(r'\d{4}.*$', '', s)
    s = re.sub(r'RECEIVABLES|OWNER|ASSET|INC|LLC|AUTO|CREDIT|FINANCE|FINANCIAL', '', s)
    return ' '.join(s.split())



# ── Load metadata (same as v5) ──────────────────────────────────────────────
print("Loading data...")
meta = pd.read_csv('output/metadata.csv')
meta['acc'] = meta['accession_number'].astype(str)
meta['dt']  = pd.to_datetime(meta['report_period'].astype(str), format='%Y%m%d', errors='coerce')
meta_map      = meta.set_index('acc')['company_name'].to_dict()
meta_date_map = meta.set_index('acc')['dt'].to_dict()

# ── extract_metric — exact copy of v5's proven version ──────────────────────
def extract_metric(tables, patterns, col_name, aggregate='max'):
    all_data = []
    regex = re.compile('|'.join(patterns), re.IGNORECASE)
    for table_name in tables:
        path = f'output/{table_name}.csv'
        if not os.path.exists(path): continue
        header = pd.read_csv(path, nrows=0)
        label_cols = [c for c in header.columns if c.startswith('label')]
        val_cols   = [c for c in header.columns if c.startswith('col_')]
        if 'dollar_amount' in header.columns: val_cols.append('dollar_amount')
        df = pd.read_csv(path, usecols=['accession_number'] + label_cols + val_cols, low_memory=False)
        df['full_label'] = df[label_cols].fillna('').agg(' '.join, axis=1)
        matches = df[df['full_label'].str.contains(regex, na=False)].copy()
        if not matches.empty:
            try:
                matches['val'] = matches[val_cols].map(clean_money).max(axis=1)
            except AttributeError:
                matches['val'] = matches[val_cols].applymap(clean_money).max(axis=1)
            matches = matches[matches['val'] > 0]
            if not matches.empty:
                all_data.append(matches[['accession_number', 'val']])
    if not all_data: return pd.DataFrame(columns=['acc', col_name])
    combined = pd.concat(all_data)
    combined['acc'] = combined['accession_number'].astype(str)
    if aggregate == 'max':
        res = combined.groupby('acc')['val'].max().reset_index()
    else:
        res = combined.groupby('acc')['val'].sum().reset_index()
    return res.rename(columns={'val': col_name})

# ── Extract metrics ──────────────────────────────────────────────────────────
print("Extracting Total Collections (for Scatter Plot)...")
coll_df = extract_metric(
    ['table_2_available_funds', 'table_3_distributions'],
    [
        r'^Total Collections$', r'^Total Available Funds$', r'^Total Available Amount$', 
        r'^Total Available Collections$', r'^Total Distribution Amount$', r'^Total Cash Available$',
        r'^Total.*Collections', r'^Total.*Available', r'Total.*Funds.*Available',
        r'Total.*Distribution.*Amount', r'Aggregate.*Available.*Funds'
    ],
    'scraped_total_collections'
)



# Add normalized names and months for better matching
coll_df['company_name'] = coll_df['acc'].map(meta_map)
coll_df['dt']           = coll_df['acc'].map(meta_date_map)
coll_df['norm_name']    = coll_df['company_name'].apply(normalize_name)
coll_df['month_key']    = pd.to_datetime(coll_df['dt']).dt.strftime('%Y-%m')



print("Extracting initial pool sizes...")
initial_df = extract_metric(
    ['table_4_noteholder', 'table_5_note_balance', 'table_2_available_funds', 'table_3_distributions'],
    [r'Original Note Balance',
     r'Initial Receivables Balance',
     r'Original Pool Balance',
     r'Cut-Off Date Pool Balance',
     r'Initial Note Balance',
     r'Original Principal Balance',
     r'Initial Outstanding',
     r'Aggregate Note Balance.*Closing',
     r'Pool Balance.*Closing Date',
     r'Initial Pool Balance'],
    'initial_pool_size'
)
print("Extracting cleanup call amounts...")
call_df = extract_metric(
    ['table_2_available_funds', 'table_3_distributions', 'table_4_noteholder'],
    [r'Optional Purchase Price', r'Optional Purchase Amount',
     r'Clean-up Call', r'Optional Redemption Prepayment', r'Optional Note Redemption'],
    'cleanup_call_amount'
)
print("Extracting remaining pool balances...")
rem_df = extract_metric(
    ['table_4_noteholder', 'table_5_note_balance'],
    [r'End of Period Pool Balance', r'Ending Pool Balance',
     r'Ending Aggregate Principal Balance', r'Aggregate Note Balance at the end'],
    'remaining_pool_balance'
)

# ── Build pool_stats (same structure as v5) ──────────────────────────────────
pool_stats = pd.DataFrame({'acc': meta['acc'].unique()})
pool_stats = pool_stats.merge(initial_df, on='acc', how='left')
pool_stats = pool_stats.merge(call_df,    on='acc', how='left')
pool_stats = pool_stats.merge(rem_df,     on='acc', how='left')
pool_stats['company_name'] = pool_stats['acc'].map(meta_map)
pool_stats['dt']  = pool_stats['acc'].map(meta_date_map)
pool_stats['ym']  = pool_stats['dt'].dt.strftime('%Y-%m')
pool_stats['brand'] = pool_stats['company_name'].apply(get_brand)

# Pin initial pool size to max ever for that company
pool_max = pool_stats.groupby('company_name')['initial_pool_size'].max().reset_index()
pool_stats = pool_stats.drop(columns='initial_pool_size').merge(pool_max, on='company_name', how='left')
valid_stats = pool_stats.dropna(subset=['dt', 'company_name']).sort_values(['company_name', 'dt'])

n_calls = (valid_stats['cleanup_call_amount'] > 0).sum()
print(f"Found {n_calls} filings with cleanup call amounts.")

# ─────────────────────────────────────────────────────────────────────────────
# PLOT 1 — SCATTER: Provided vs Scraped, coloured by BRAND
# ─────────────────────────────────────────────────────────────────────────────
print("Plot 1: Scatter all issuers by brand...")
final_summary = pd.read_csv('output/final_abs_summary_dr_honkanen.csv', low_memory=False)

# Normalise column names early
if 'poolname' in final_summary.columns:
    final_summary = final_summary.rename(columns={'poolname': 'company_name'})

# Re-merge the freshly extracted collections using normalized name and month
if not coll_df.empty:
    # Prepare final_summary for matching
    final_summary['date_dt'] = pd.to_datetime(final_summary['date'], errors='coerce')
    final_summary['month_key'] = final_summary['date_dt'].dt.strftime('%Y-%m')
    final_summary['norm_name'] = final_summary['company_name'].apply(normalize_name)
    
    # Drop corrupted column if it exists
    final_summary = final_summary.drop(columns=['scraped_total_collections'], errors='ignore')
    
    # Deduplicate coll_df
    coll_dedup = coll_df.sort_values('scraped_total_collections', ascending=False).drop_duplicates(['norm_name', 'month_key'])
else:
    coll_dedup = pd.DataFrame(columns=['norm_name', 'month_key', 'scraped_total_collections', 'dt', 'company_name'])
    final_summary['norm_name'] = final_summary['company_name'].apply(normalize_name)
    final_summary['date_dt'] = pd.to_datetime(final_summary['date'], errors='coerce')
    final_summary['month_key'] = final_summary['date_dt'].dt.strftime('%Y-%m')

# Merge on normalized name and month
final_summary = final_summary.merge(
    coll_dedup[['norm_name', 'month_key', 'scraped_total_collections']],
    on=['norm_name', 'month_key'],
    how='left'
)


final_summary['provided_total'] = (
    final_summary['totalInterest'].fillna(0) +
    final_summary['principalCollections'].fillna(0)
)

# Unit fix: Standardize to Millions for better spread
def fix_units(row):
    p = row['provided_total']
    s = row['scraped_total_collections']
    if p > 0 and s > 0:
        ratio = s / p
        # If SEC is 1000x bigger, it's a unit mismatch
        if 500 < ratio < 1500: return s / 1000.0
        if 0.0005 < ratio < 0.0015: return s * 1000.0
        # If it's still way off, it's likely an accumulated total, filter it out
        if ratio > 5 or ratio < 0.2: return np.nan
    return s

matched = final_summary.dropna(subset=['scraped_total_collections', 'provided_total']).copy()
matched['scraped_total_collections'] = matched.apply(fix_units, axis=1)
matched = matched.dropna(subset=['scraped_total_collections'])
matched = matched[matched['provided_total'] > 0].copy()



# Use the 'brand' column already in the CSV; fall back to get_brand() if missing
if 'brand' not in matched.columns:
    matched['brand'] = matched['company_name'].apply(get_brand)
else:
    matched['brand'] = matched['brand'].fillna('OTHER').str.upper()

fig, ax = plt.subplots(figsize=(13, 11))
brands_present = sorted(matched['brand'].unique())
for brand in brands_present:
    sub = matched[matched['brand'] == brand]
    ax.scatter(
        sub['provided_total'] / 1e6,
        sub['scraped_total_collections'] / 1e6,
        alpha=0.65, s=55,
        color=brand_color(brand),
        edgecolors='white', linewidths=0.3,
        label=brand, zorder=3
    )

max_v = max(matched['provided_total'].max(), matched['scraped_total_collections'].max()) / 1e6
ax.plot([0, max_v], [0, max_v], color='white', linestyle='--', linewidth=1.8,
        alpha=0.7, label='1:1 Line')

ax.set_title('All Issuers: Provided vs Extracted Total Collections\n(coloured by brand)',
             fontweight='bold', pad=14)
ax.set_xlabel('Provided Total Collections ($ Millions)', fontweight='bold')
ax.set_ylabel('Scraped SEC Total Collections ($ Millions)', fontweight='bold')
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}M'))
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}M'))
ax.legend(loc='upper left', facecolor='#1a1a1a', edgecolor='#444',
          framealpha=0.9, ncol=2, fontsize=8)

plt.tight_layout()
plt.savefig(f'{OUT}/01_scatter_by_brand.png', dpi=300, bbox_inches='tight')
plt.close()
print("  → saved 01_scatter_by_brand.png")

# ── NEW PLOT: Master Scraped Collections (Everything we found) ───────────────
print("Plot 1b: Master Scraped Collections (All Brands)...")
all_scraped = coll_dedup.copy()
all_scraped['brand'] = all_scraped['company_name'].apply(get_brand)
all_scraped = all_scraped[all_scraped['scraped_total_collections'] > 1e6] # >1M

fig, ax = plt.subplots(figsize=(14, 8))
brands_all = sorted(all_scraped['brand'].unique())
for b in brands_all:
    sub = all_scraped[all_scraped['brand'] == b]
    ax.scatter(pd.to_datetime(sub['dt']), sub['scraped_total_collections']/1e6,
               label=b, color=brand_color(b), alpha=0.7, s=40)

ax.set_title('SEC Master Collections: All Scraped Data Over Time', fontweight='bold')

ax.set_ylabel('Monthly Collections ($ Millions)', fontweight='bold')
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}M'))
ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left', ncol=1, fontsize=8)
plt.tight_layout()
plt.savefig(f'{OUT}/01b_master_scraped_totals.png', dpi=300, bbox_inches='tight')
plt.close()
print("  → saved 01b_master_scraped_totals.png")


# ── PLOTS 2 & 3: Call ratios over time ──────────────────────────────────────
print("Plots 2 & 3: Cleanup call ratios over time...")
calls = valid_stats[valid_stats['cleanup_call_amount'] > 0].copy()
calls['call_to_initial_ratio']   = calls['cleanup_call_amount'] / calls['initial_pool_size'].replace(0, np.nan)
calls['call_to_remaining_ratio'] = calls['cleanup_call_amount'] / calls['remaining_pool_balance'].replace(0, np.nan)
calls_clean = calls[(calls['call_to_initial_ratio'] > 0.001) & (calls['call_to_initial_ratio'] < 1.0)]

ts = calls_clean.groupby('ym').agg(
    avg_init_ratio  = ('call_to_initial_ratio',  'mean'),
    avg_rem_ratio   = ('call_to_remaining_ratio', 'mean'),
    pool_count      = ('acc', 'nunique'),
).reset_index()
ts['date'] = pd.to_datetime(ts['ym'])
ts = ts.sort_values('date')
print(f"  Time-series rows: {len(ts)}")

COLOR_LINE1 = '#00ffcc'; COLOR_LINE2 = '#ffcc00'; COLOR_BAR = '#ff3355'

# Plot 2
fig, ax1 = plt.subplots(figsize=(15, 7))
ax1.plot(ts['date'], ts['avg_init_ratio']*100, color=COLOR_LINE1, marker='o',
         linewidth=2.5, markersize=7, label='Avg Call / Initial Pool (%)', zorder=3)
ax1.set_ylabel('Avg Cleanup Call / Initial Pool Size (%)', color=COLOR_LINE1, fontweight='bold')
ax1.tick_params(axis='y', colors=COLOR_LINE1)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{x:.1f}%'))
ax2 = ax1.twinx()
ax2.bar(ts['date'], ts['pool_count'], width=20, alpha=0.35, color=COLOR_BAR, label='# Pools (RHS)')
ax2.set_ylabel('Number of Pools with Cleanup Call', color=COLOR_BAR, fontweight='bold')
ax2.tick_params(axis='y', colors=COLOR_BAR)
ax2.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
fig.autofmt_xdate()
plt.title('Cleanup Calls: Avg Call as % of Initial Pool Size', fontweight='bold', pad=12)
lines1, lbl1 = ax1.get_legend_handles_labels()
lines2, lbl2 = ax2.get_legend_handles_labels()
ax1.legend(lines1+lines2, lbl1+lbl2, loc='upper left', facecolor='#1a1a1a', edgecolor='#444')
plt.tight_layout()
plt.savefig(f'{OUT}/02_call_to_initial_over_time.png', dpi=300, bbox_inches='tight')
plt.close()
print("  → saved 02_call_to_initial_over_time.png")

# Plot 3: Call Amount vs Remaining Balance (Terminal Event Check)
def calc_rem_ratio(row):
    call = row['cleanup_call_amount']
    rem = row['remaining_pool_balance']
    # If remaining balance is reported as 0 (paid off) or missing, the call WAS the remaining balance.
    if pd.isna(rem) or rem < 1000:
        return 1.0
    return call / rem

calls['call_to_remaining_ratio'] = calls.apply(calc_rem_ratio, axis=1)
calls_rem = calls[(calls['call_to_remaining_ratio'] > 0.01) & (calls['call_to_remaining_ratio'] < 2.0)]
ts2 = calls_rem.groupby('ym').agg(
    avg_rem_ratio = ('call_to_remaining_ratio', 'mean'),
    pool_count    = ('acc', 'nunique'),
).reset_index()
ts2['date'] = pd.to_datetime(ts2['ym'])
ts2 = ts2.sort_values('date')



fig, ax1 = plt.subplots(figsize=(15, 7))
ax1.plot(ts2['date'], ts2['avg_rem_ratio'], color=COLOR_LINE2, marker='o',
         linewidth=2.5, markersize=7, label='Avg Call / Remaining Balance', zorder=3)
ax1.axhline(1.0, color='white', linestyle=':', linewidth=1.8, alpha=0.7, label='Ratio = 1.0')
ax1.set_ylabel('Call Amount / Remaining Pool Balance', color=COLOR_LINE2, fontweight='bold')
ax1.tick_params(axis='y', colors=COLOR_LINE2)
ax2 = ax1.twinx()
ax2.bar(ts2['date'], ts2['pool_count'], width=20, alpha=0.35, color=COLOR_BAR, label='# Pools (RHS)')
ax2.set_ylabel('Number of Pools with Cleanup Call', color=COLOR_BAR, fontweight='bold')
ax2.tick_params(axis='y', colors=COLOR_BAR)
ax2.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
fig.autofmt_xdate()
plt.title('Cleanup Calls: Call Amount / Remaining Pool Balance at Call', fontweight='bold', pad=12)
lines1, lbl1 = ax1.get_legend_handles_labels()
lines2, lbl2 = ax2.get_legend_handles_labels()
ax1.legend(lines1+lines2, lbl1+lbl2, loc='upper left', facecolor='#1a1a1a', edgecolor='#444')
plt.tight_layout()
plt.savefig(f'{OUT}/03_call_to_remaining_over_time.png', dpi=300, bbox_inches='tight')
plt.close()
print("  → saved 03_call_to_remaining_over_time.png")

# ── PLOT 4: Tranche waterfall case studies ───────────────────────────────────
print("Plot 4: Tranche waterfall case studies...")
TRANCHE_REGEX = re.compile(
    r'(Class\s+[A-Z]-?\d*.*?(Balance|Notes|Principal|Outstanding)'
    r'|Balance.*?Class\s+[A-Z]-?\d*'
    r'|[A-Z]-\d+\s+Note.*?Balance'
    r'|Note.*?Balance.*?[A-Z]-\d+'
    r'|^Class\s+[A-Z]-?\d*$)', 
    re.IGNORECASE
)

def load_and_filter_tranche(table_name):
    path = f'output/{table_name}.csv'
    if not os.path.exists(path):
        return pd.DataFrame()
    t = pd.read_csv(path, low_memory=False)
    t['acc']          = t['accession_number'].astype(str)
    t['company_name'] = t['acc'].map(meta_map)
    t['dt']           = t['acc'].map(meta_date_map)
    lcols = ['label'] + [c for c in t.columns if c.startswith('label_')]
    vcols = [c for c in t.columns if c.startswith('col_')]
    t['full_label'] = t[lcols].fillna('').agg(' '.join, axis=1)
    try:
        val_df = t[vcols].map(clean_money)
    except AttributeError:
        val_df = t[vcols].applymap(clean_money)
    t['val'] = val_df.max(axis=1)
    mask = t['full_label'].str.contains(TRANCHE_REGEX, na=False) & (t['val'] > 5000)
    return t[mask][['acc','company_name','dt','full_label','val']].copy()

print("  Loading tranche balance data from Table 4 and Table 5...")
cb_t5 = load_and_filter_tranche('table_5_note_balance')
cb_t4 = load_and_filter_tranche('table_4_noteholder')
cb = pd.concat([cb_t5, cb_t4], ignore_index=True)

def clean_tranche_label(s):
    m = re.search(r'(Class\s+[A-Z0-9-]+)', s, re.IGNORECASE)
    return m.group(1).upper() if m else s[:30]

cb['clean_label'] = cb['full_label'].apply(clean_tranche_label)
cb = cb.drop_duplicates(subset=['acc', 'clean_label'])

print(f"  Tranche balance rows found: {len(cb)} across {cb['company_name'].nunique()} pools")

pool_call_dates = valid_stats[valid_stats['cleanup_call_amount'] > 0].groupby('company_name')['dt'].max().to_dict()
pool_call_amts  = valid_stats[valid_stats['cleanup_call_amount'] > 0].groupby('company_name')['cleanup_call_amount'].max().to_dict()
pool_init_sizes = valid_stats.groupby('company_name')['initial_pool_size'].max().to_dict()
pool_brands     = valid_stats.drop_duplicates('company_name').set_index('company_name')['brand'].to_dict()

print(f"  Pools with detected cleanup calls: {len(pool_call_dates)}")

pool_summary = cb.groupby('company_name').agg(
    dt_max       = ('dt', 'max'),
    dt_min       = ('dt', 'min'),
    num_obs      = ('dt', 'nunique'),
    num_tranches = ('clean_label', 'nunique'),
).reset_index()
pool_summary['brand']     = pool_summary['company_name'].apply(get_brand)
pool_summary['call_date'] = pool_summary['company_name'].map(pool_call_dates)
pool_summary['days_diff'] = (pool_summary['dt_max'] - pool_summary['call_date']).dt.days.abs()

good = pool_summary[
    pool_summary['call_date'].notna() &
    (pool_summary['num_obs'] >= 10) &
    (pool_summary['days_diff'] <= 270)
].copy()
if good.empty:
    good = pool_summary[pool_summary['call_date'].notna() & (pool_summary['num_obs'] > 5)].copy()
if good.empty:
    good = pool_summary[pool_summary['call_date'].notna()].copy()
if good.empty:
    good = pool_summary.sort_values('num_obs', ascending=False).head(40).copy()

TRANCHE_LINE_COLORS = [
    '#00ffcc','#ff5533','#ffcc00','#4488ff','#cc44ff',
    '#ff88aa','#44ffaa','#ffaa44','#aaccff','#ff44ff',
]

def tranche_sort_key(s):
    m = re.search(r'Class\s+([A-Z])(?:-?([0-9]+))?', s, re.IGNORECASE)
    if m:
        letter = m.group(1).upper()
        num    = int(m.group(2)) if m.group(2) else 0
        return f"{letter}{num:02d}"
    return s

chosen = []
used_brands = set()
used_tc     = set()

for tc in [4, 5, 6, 7, 8, 3, 9]:
    cands = good[good['num_tranches'] == tc].copy()
    if cands.empty: continue
    cands['brand_used'] = cands['brand'].apply(lambda b: 1 if b in used_brands else 0)
    cands = cands.sort_values(['brand_used', 'num_obs'], ascending=[True, False])
    best = cands.iloc[0]
    chosen.append(best['company_name'])
    used_brands.add(best['brand'])
    used_tc.add(tc)

if len(chosen) < 4:
    remaining = good[~good['company_name'].isin(chosen)].sort_values('num_obs', ascending=False)
    for _, row in remaining.head(5 - len(chosen)).iterrows():
        chosen.append(row['company_name'])

print(f"  Selected {len(chosen)} pools for case studies: {chosen}")

case_rows = []
for pool_name in chosen:
    pool_data = cb[cb['company_name'] == pool_name].copy()
    if pool_data.empty: continue

    tranches  = sorted(pool_data['full_label'].unique(), key=tranche_sort_key)
    tc        = len(tranches)
    cdate     = pool_call_dates.get(pool_name)
    init_sz   = pool_init_sizes.get(pool_name, np.nan)
    call_am   = pool_call_amts.get(pool_name, np.nan)
    call_pct  = (call_am / init_sz * 100) if (pd.notna(init_sz) and init_sz > 0 and pd.notna(call_am)) else np.nan
    pbrand    = pool_brands.get(pool_name, get_brand(pool_name))

    short = re.sub(r'\d{4}-\w+', '', pool_name)
    short = re.sub(r'Auto Owner Trust|Receivables Trust|Owner Trust|Trust|LLC|Inc', '', short, flags=re.IGNORECASE)
    short = re.sub(r'\s+', ' ', short).strip()

    fig, ax = plt.subplots(figsize=(14, 8))
    for i, tr in enumerate(tranches):
        d = pool_data[pool_data['full_label'] == tr].sort_values('dt')
        m = re.search(r'(Class\s+[A-Z0-9-]+)', tr)
        lbl = m.group(1) if m else tr[:25]
        ax.plot(d['dt'], d['val']/1e6, label=lbl,
                color=TRANCHE_LINE_COLORS[i % len(TRANCHE_LINE_COLORS)],
                linewidth=2.8, marker='o', markersize=3.5)

    if pd.notna(cdate):
        ax.axvline(cdate, color='white', linestyle='--', linewidth=2.0,
                   alpha=0.85, label='Cleanup Call Date')
        ax.text(cdate, ax.get_ylim()[1]*0.95, ' CALL', color='white', fontweight='bold', fontsize=9, alpha=0.8)

    ann = '\n'.join([
        f"Pool:   {short}",
        f"Brand:  {pbrand}",
        f"Init:   ${init_sz/1e6:.1f}M" if pd.notna(init_sz) else "Init:   N/A",
        f"Call:   ${call_am/1e6:.1f}M"  if pd.notna(call_am) else "Call:   N/A",
        f"Ratio:  {call_pct:.1f}%"      if pd.notna(call_pct) else "Ratio:  N/A",
        f"Trch:   {tc}",
    ])
    ax.text(0.02, 0.97, ann, transform=ax.transAxes, fontsize=9, va='top',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='#1a1a1a', edgecolor='#666', alpha=0.9),
            color='white', family='monospace')

    ax.set_title(f'Case Study: Tranche Payment Schedule\n{pool_name}', fontweight='bold', pad=12)
    ax.set_ylabel('Remaining Balance ($ Millions)', fontweight='bold')
    ax.set_xlabel('Date', fontweight='bold')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'${x:,.0f}M'))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    fig.autofmt_xdate()
    ax.legend(bbox_to_anchor=(1.01, 1), loc='upper left', facecolor='#1a1a1a', edgecolor='#444')
    plt.tight_layout()

    fsafe = re.sub(r'\W+', '_', pool_name)[:40]
    out_path = f'{OUT}/04_casestudy_{tc}tr_{fsafe}.png'
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  → saved {out_path}")

    case_rows.append({
        'pool_name': pool_name, 'brand': pbrand, 'num_tranches': tc,
        'initial_pool_size': init_sz, 'cleanup_call_amt': call_am,
        'call_to_initial_pct': call_pct, 'call_date': cdate,
        'first_obs': pool_data['dt'].min(), 'last_obs': pool_data['dt'].max(),
        'num_obs': pool_data['dt'].nunique(),
    })

if case_rows:
    pd.DataFrame(case_rows).to_csv(f'{OUT}/case_study_pools.csv', index=False)
    print(f"  → saved case_study_pools.csv ({len(case_rows)} pools)")

print("\nDone. All outputs in output/analysis_v6/")
