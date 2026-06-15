import pandas as pd
import numpy as np
import os
import re

print("Starting Data Extraction for Clean-Up Calls & Tranche Analysis...")

os.makedirs('output/analysis', exist_ok=True)

# 1. Load Data
print("Loading core datasets (Table 2 & Metadata)...")
funds = pd.read_csv('output/table_2_available_funds.csv', low_memory=False)
meta = pd.read_csv('output/metadata.csv')
final_summary = pd.read_csv('output/final_abs_summary_dr_honkanen.csv')

# Build the complete label string for Table 2
if 'label_str' not in funds.columns:
    funds['label_str'] = funds['label'].fillna('') + ' ' + funds.get('label_1', pd.Series(dtype=str)).fillna('') + ' ' + funds.get('label_2', pd.Series(dtype=str)).fillna('')

final_summary['date'] = pd.to_datetime(final_summary['date'], errors='coerce', utc=True)

# 2. Extract Initial Pool Sizes
print("Extracting Initial Pool Sizes...")
funds_val_cols = [c for c in funds.columns if c.startswith('col_')]
funds['max_val'] = funds[funds_val_cols].replace(r'[$,\s()\-]', '', regex=True).apply(pd.to_numeric, errors='coerce').max(axis=1)

size_matches = funds[funds['label_str'].str.contains('Original|Initial|Offering', case=False, na=False)]
initial_sizes = size_matches.groupby('accession_number')['max_val'].max().reset_index()
initial_sizes.rename(columns={'max_val': 'initial_pool_size'}, inplace=True)

# 3. Extract Clean-up Calls (Optional Redemption / Repurchase)
print("Extracting Clean-up Calls...")
cleanup_matches = funds[funds['label_str'].str.contains('Redemption|Repurchase|Clean-up|Cleanup|Call Option', case=False, na=False)]
cleanup_calls = cleanup_matches.groupby('accession_number')['max_val'].max().reset_index()
cleanup_calls.rename(columns={'max_val': 'cleanup_call_amount'}, inplace=True)

# 4. Extract Remaining Pool Balances (End of Period Pool Balance)
print("Extracting Remaining Pool Balances...")
rem_matches = funds[funds['label_str'].str.contains('End of Period Pool Balance|Ending Pool Balance|Pool Balance at End', case=False, na=False)]
rem_balances = rem_matches.groupby('accession_number')['max_val'].max().reset_index()
rem_balances.rename(columns={'max_val': 'remaining_pool_balance'}, inplace=True)

# Merge into metadata mapping
meta['acc'] = meta['accession_number'].astype(str)
initial_sizes['accession_number'] = initial_sizes['accession_number'].astype(str)
cleanup_calls['accession_number'] = cleanup_calls['accession_number'].astype(str)
rem_balances['accession_number'] = rem_balances['accession_number'].astype(str)

pool_stats = meta[['company_name', 'acc', 'cik', 'report_period', 'filed_date']].copy()
pool_stats = pd.merge(pool_stats, initial_sizes, left_on='acc', right_on='accession_number', how='left').drop(columns=['accession_number'])
pool_stats = pd.merge(pool_stats, cleanup_calls, left_on='acc', right_on='accession_number', how='left').drop(columns=['accession_number'])
pool_stats = pd.merge(pool_stats, rem_balances, left_on='acc', right_on='accession_number', how='left').drop(columns=['accession_number'])

# Determine terminal months for pools
print("Identifying terminal months...")
pool_stats['dt'] = pd.to_datetime(pool_stats['report_period'].astype(str), format='%Y%m%d', errors='coerce')
pool_stats['ym'] = pool_stats['dt'].dt.strftime('%Y-%m')

# Filter to issuers we actually matched
ciks = final_summary['cik'].dropna().unique()
filtered_stats = pool_stats[pool_stats['cik'].isin(ciks)].copy()

filtered_stats = filtered_stats.sort_values(['cik', 'dt'])
# Optional: find max date per *company_name* or *pool* because cik groups across many vintages!
# A single CIK represents Ford. Ford will have many pools. So we group by company_name to find the terminal month of THAT pool.
filtered_stats['is_terminal'] = filtered_stats.groupby('company_name')['dt'].transform('max') == filtered_stats['dt']

filtered_stats.to_csv('output/analysis/pool_stats_augmented.csv', index=False)
print("Saved augmented pool stats.")

# 5. Extract Tranche Data for Case Studies
print("Extracting Tranche Data from Table 1...")
if os.path.exists('output/table_1_payment_schedule.csv'):
    t1 = pd.read_csv('output/table_1_payment_schedule.csv', low_memory=False)
    # Ensure label_str exists
    if 'label_str' not in t1.columns:
        t1['label_str'] = t1['label'].fillna('') + ' ' + t1.get('label_1', pd.Series(dtype=str)).fillna('') + ' ' + t1.get('label_2', pd.Series(dtype=str)).fillna('')
    
    t1_val_cols = [c for c in t1.columns if c.startswith('col_')]
    t1['max_val'] = t1[t1_val_cols].replace(r'[$,\s()\-]', '', regex=True).apply(pd.to_numeric, errors='coerce').max(axis=1)
    
    tranche_matches = t1[t1['label_str'].str.contains('Class A|Class B|Class C', case=False, na=False)]
    
    tranche_data = tranche_matches[['accession_number', 'label_str', 'max_val']].copy()
    tranche_data.to_csv('output/analysis/tranche_balances.csv', index=False)
    print("Saved tranche balances.")
else:
    print("WARNING: table_1_payment_schedule.csv not found.")

print("Extraction completed successfully.")
