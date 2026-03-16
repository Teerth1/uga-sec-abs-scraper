import pandas as pd
import matplotlib.pyplot as plt
import os
import random
import numpy as np

def clean_dollar(s):
    if pd.isna(s) or s == '': return 0.0
    if isinstance(s, (int, float)): return float(s)
    # Be super aggressive: remove anything not a digit, dot, dash or parentheses
    s = str(s).strip()
    if not s: return 0.0
    # Handle ($123,456.78) format
    is_neg = False
    if '(' in s or '-' in s: is_neg = True
    s = "".join([c for c in s if c.isdigit() or c == '.'])
    try:
        val = float(s) if s else 0.0
        return -val if is_neg else val
    except ValueError:
        return 0.0

def analyze():
    print("Loading datasets...")

    def clean_name(n):
        if pd.isna(n): return ""
        s = "".join([c if ord(c) < 128 else " " for c in str(n)])
        return " ".join(s.upper().split())

    # 1. Load provided data
    provided_df = pd.read_csv('collections_dirty.csv')
    provided_dt = pd.to_datetime(provided_df['date'], errors='coerce', utc=True)
    provided_df['year_month'] = provided_dt.dt.strftime('%Y-%m')
    provided_df['poolname_clean'] = provided_df['poolname'].apply(clean_name)

    def extract_issuer(n_clean):
        if "FORD" in n_clean: return "FORD CREDIT"
        if "CARMAX" in n_clean: return "CARMAX"
        return " ".join(n_clean.split()[:2])
    
    provided_df['issuer'] = provided_df['poolname_clean'].apply(extract_issuer)

    cols_to_sum = ['totalInterest', 'prepaymentsInFullCollected', 'principalCollections', 'recoveries', 'liquidationProceeds']
    for col in cols_to_sum:
        if col not in provided_df.columns: provided_df[col] = 0.0
    provided_df['total_collections_provided'] = provided_df[cols_to_sum].fillna(0).sum(axis=1)

    # 2. Load scraped data
    metadata_df = pd.read_csv('output/metadata.csv')
    metadata_dt = pd.to_datetime(metadata_df['report_period'], format='%Y%m%d', errors='coerce', utc=True)
    metadata_df['company_name_clean'] = metadata_df['company_name'].apply(clean_name)
    metadata_df['year_month'] = metadata_dt.dt.strftime('%Y-%m')

    funds_df = pd.read_csv('output/table_2_available_funds.csv')

    # Aggressive mapping: if 'label' is missing or 'dollar_amount' is missing, 
    # and we have 'Available Funds' column, treat it as the value column.
    if 'dollar_amount' not in funds_df.columns:
        if 'Available Funds' in funds_df.columns:
            funds_df = funds_df.rename(columns={'Available Funds': 'dollar_amount'})
    
    # Try to find a label column if others exist
    if 'label' not in funds_df.columns:
        other_cols = [c for c in funds_df.columns if c not in ['accession_number', 'dollar_amount']]
        if other_cols:
            funds_df = funds_df.rename(columns={other_cols[0]: 'label'})

    funds_df['val_float'] = funds_df['dollar_amount'].apply(clean_dollar)

    # 3. Monthly Collections Aggregation
    print("Aggregating monthly collections...")
    p_labels = [
        'Principal Collections', 'Prepayments in Full', 'Liquidation Proceeds', 
        'Recoveries', 'Purchase Amounts Related to Principal', 
        'Collections allocable to Principal', 'a. Collections allocable to Principal'
    ]
    i_labels = [
        'Interest Collections', 'Purchase Amounts Related to Interest', 
        'Collections allocable to Finance Charge', 'a. Collections allocable to Finance Charge'
    ]
    t_labels = [
        'Collections', 'Available Funds - Total', 'Available Funds', 
        'Total Finance Charge and Principal Collections', 
        'Total Finance Charge and Principal Collections (17d + 18d)',
        '25. Available Funds'
    ]
    
    # Also look for row headers in any column
    funds_df['label_str'] = funds_df['label'].astype(str)
    funds_df['is_principal'] = funds_df['label_str'].str.contains('|'.join(p_labels), case=False, na=False)
    funds_df['is_interest'] = funds_df['label_str'].str.contains('|'.join(i_labels), case=False, na=False)
    funds_df['is_total'] = funds_df['label_str'].str.contains('|'.join(t_labels), case=False, na=False)
    
    monthly_agg = funds_df.groupby('accession_number', sort=False).agg(
        scraped_principal=('val_float', lambda x: x[funds_df.loc[x.index, 'is_principal']].sum()),
        scraped_interest=('val_float', lambda x: x[funds_df.loc[x.index, 'is_interest']].sum()),
        scraped_total_collections_raw=('val_float', lambda x: x[funds_df.loc[x.index, 'is_principal'] | funds_df.loc[x.index, 'is_interest']].sum()),
        scraped_available_funds=('val_float', lambda x: x[funds_df.loc[x.index, 'is_total']].sum()),
    ).reset_index()

    monthly_agg['scraped_total_collections'] = monthly_agg.apply(
        lambda row: row['scraped_total_collections_raw'] if row['scraped_total_collections_raw'] > 0 else row['scraped_available_funds'],
        axis=1
    )

    unified_summary = pd.merge(monthly_agg, metadata_df, on='accession_number')
    unified_summary.to_csv('output/unified_monthly_summary.csv', index=False)

    # 4. Join datasets for Collections Comparison Plot
    final_df = pd.merge(
        provided_df,
        unified_summary,
        left_on=['poolname_clean', 'year_month'],
        right_on=['company_name_clean', 'year_month'],
        how='inner'
    )

    if not final_df.empty:
        print(f"Found {len(final_df)} matching records for plotting.")
        plt.figure(figsize=(12, 8))
        issuers = sorted(final_df['issuer'].unique())
        for issuer in issuers:
            subset = final_df[final_df['issuer'] == issuer]
            plt.scatter(subset['scraped_total_collections'], subset['total_collections_provided'], label=issuer, alpha=0.7)

        # Diagonal line
        all_vals = pd.concat([final_df['scraped_total_collections'], final_df['total_collections_provided']])
        max_val = all_vals.max() if not all_vals.empty else 1e7
        plt.plot([0, max_val], [0, max_val], 'k--', alpha=0.3, label='Match Line')
        
        plt.xlabel('Scraped Collections (USD)')
        plt.ylabel('Provided Collections (USD)')
        plt.title('Scraped vs Provided Collection Data (Accumulated Period)')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        plt.savefig('collections_comparison.png')
        print("Saved 'collections_comparison.png'")
    else:
        print("No matches! Check joining keys.")

if __name__ == "__main__":
    analyze()
