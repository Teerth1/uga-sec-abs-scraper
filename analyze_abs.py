import pandas as pd
import matplotlib.pyplot as plt
import os
import random
import numpy as np

def clean_dollar(s):
    if pd.isna(s) or s == '': return 0.0
    if isinstance(s, (int, float)): return float(s)
    s = str(s).replace('$', '').replace(',', '')
    if '(' in s and ')' in s:
        s = '-' + s.replace('(', '').replace(')', '')
    try:
        return float(s)
    except ValueError:
        return 0.0

def analyze():
    print("Loading datasets...")
    # 1. Load provided data
    provided_df = pd.read_csv('collections_dirty.csv')
    provided_dt = pd.to_datetime(provided_df['date'], errors='coerce', utc=True)
    provided_df['date'] = provided_dt.dt.date
    provided_df['year_month'] = provided_dt.dt.strftime('%Y-%m')
    provided_df['poolname'] = provided_df['poolname'].str.upper()

    cols_to_sum = ['totalInterest', 'prepaymentsInFullCollected', 'principalCollections', 'recoveries', 'liquidationProceeds']
    for col in cols_to_sum:
        if col not in provided_df.columns: provided_df[col] = 0.0
    provided_df['total_collections_provided'] = provided_df[cols_to_sum].fillna(0).sum(axis=1)

    def extract_issuer(poolname):
        if pd.isna(poolname): return "Unknown"
        words = str(poolname).split()[:2]
        issuer = " ".join(words)
        for noise in ["AUTO", "VEHICLE", "AUTOMOBILE", "SECURITIZATION", "RECEIVABLES", "OWNER", "TRUST"]:
            issuer = issuer.replace(noise, "").strip()
        return issuer if issuer else "Unknown"
    provided_df['issuer'] = provided_df['poolname'].apply(extract_issuer)

    # 2. Load scraped data
    metadata_df = pd.read_csv('output/metadata.csv')
    metadata_dt = pd.to_datetime(metadata_df['report_period'], format='%Y%m%d', errors='coerce')
    metadata_df['report_period_date'] = metadata_dt.dt.date
    metadata_df['company_name'] = metadata_df['company_name'].str.upper()
    metadata_df['year_month'] = metadata_dt.dt.strftime('%Y-%m')

    funds_df = pd.read_csv('output/table_2_available_funds.csv')
    funds_df['val_float'] = funds_df['dollar_amount'].apply(clean_dollar)
    
    # 3. Monthly Collections Aggregation
    print("Aggregating monthly collections...")
    p_labels = ['Principal Collections', 'Prepayments in Full', 'Liquidation Proceeds', 'Recoveries', 'Purchase Amounts Related to Principal', 'Collections allocable to Principal']
    i_labels = ['Interest Collections', 'Purchase Amounts Related to Interest', 'Collections allocable to Finance Charge']
    t_labels = ['Collections', 'Available Funds - Total', 'Available Funds', 'Total Finance Charge and Principal Collections']
    
    funds_df['is_principal'] = funds_df['label'].isin(p_labels)
    funds_df['is_interest'] = funds_df['label'].isin(i_labels)
    funds_df['is_total'] = funds_df['label'].isin(t_labels)
    
    monthly_agg = funds_df.groupby('accession_number', sort=False).agg(
        scraped_principal=('val_float', lambda x: x[funds_df.loc[x.index, 'is_principal']].sum()),
        scraped_interest=('val_float', lambda x: x[funds_df.loc[x.index, 'is_interest']].sum()),
        scraped_total_collections=('val_float', lambda x: x[funds_df.loc[x.index, 'is_principal'] | funds_df.loc[x.index, 'is_interest']].sum()),
        scraped_available_funds=('val_float', lambda x: x[funds_df.loc[x.index, 'is_total']].sum()),
        cleanup_call_amount=('val_float', lambda x: x[funds_df.loc[x.index, 'label'] == 'Clean-up Call'].sum())
    ).reset_index()

    unified_summary = pd.merge(monthly_agg, metadata_df, on='accession_number')
    
    # If scraped_total_collections is 0 but scraped_available_funds is not, use available funds as fallback for verification
    mask = (unified_summary['scraped_total_collections'] == 0) & (unified_summary['scraped_available_funds'] > 0)
    unified_summary.loc[mask, 'scraped_total_collections'] = unified_summary.loc[mask, 'scraped_available_funds']
    
    unified_summary.to_csv('output/unified_monthly_summary.csv', index=False)
    print("Saved 'output/unified_monthly_summary.csv'")

    # 4. Join datasets for Collections Comparison Plot
    final_df = pd.merge(
        provided_df,
        unified_summary,
        left_on=['poolname', 'year_month'],
        right_on=['company_name', 'year_month'],
        how='inner'
    )

    if not final_df.empty:
        print(f"Found {len(final_df)} matching records for plotting.")
        plt.figure(figsize=(12, 8))
        issuers = final_df['issuer'].unique()
        for issuer in issuers:
            subset = final_df[final_df['issuer'] == issuer]
            plt.scatter(subset['scraped_total_collections'], subset['total_collections_provided'], label=issuer, alpha=0.7)

        all_vals = pd.concat([final_df['scraped_total_collections'], final_df['total_collections_provided']])
        max_val = all_vals.max() if not all_vals.empty else 1.0
        plt.plot([0, max_val], [0, max_val], 'k--', alpha=0.3, label='Match Line')
        
        plt.xlabel('Scraped Collections (USD)')
        plt.ylabel('Provided Collections (USD)')
        plt.title('Scraped vs Provided Collection Data (Accumulated Period)')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        plt.savefig('collections_comparison.png')
        print("Saved 'collections_comparison.png'")

    # 5. Clean-up Call Analysis
    print("Analyzing clean-up calls...")
    cleanup_pools = unified_summary[unified_summary['cleanup_call_amount'] > 0].copy()
    if not cleanup_pools.empty:
        initial_balances = funds_df[funds_df['label'].str.contains('Initial Pool Balance', na=False)][['accession_number', 'val_float']].rename(columns={'val_float': 'initial_pool_balance'})
        cleanup_stats = pd.merge(cleanup_pools, initial_balances, on='accession_number', how='left')
        
        balance_df = pd.read_csv('output/table_5_note_balance.csv')
        balance_df['end_bal'] = balance_df['End of Period | Balance'].apply(clean_dollar)
        total_end_bal = balance_df[balance_df['label'] == 'Total'][['accession_number', 'end_bal']]
        
        cleanup_stats = pd.merge(cleanup_stats, total_end_bal, on='accession_number', how='left')
        cleanup_stats['cleanup_pct_of_initial'] = (cleanup_stats['cleanup_call_amount'] / cleanup_stats['initial_pool_balance']) * 100
        
        cleanup_stats[['company_name', 'report_period', 'cleanup_call_amount', 'end_bal', 'initial_pool_balance', 'cleanup_pct_of_initial']].to_csv('clean_up_stats.csv', index=False)
        print("Saved 'clean_up_stats.csv'")
    else:
        print("No non-zero Clean-up Calls found in sampled data.")

    # 6. Repayment Structure Inference (Random 10 Pools)
    print("Generating randomized repayment plots...")
    balance_df = pd.read_csv('output/table_5_note_balance.csv')
    balance_df['balance_val'] = balance_df['End of Period | Balance'].apply(clean_dollar)
    balance_merged = pd.merge(balance_df, metadata_df, on='accession_number')
    balance_merged['report_period_dt'] = pd.to_datetime(balance_merged['report_period'], format='%Y%m%d')

    unique_pools = balance_merged['company_name'].unique()
    sampled_pools = random.sample(list(unique_pools), min(10, len(unique_pools)))

    for pool in sampled_pools:
        pool_subset = balance_merged[balance_merged['company_name'] == pool].sort_values('report_period_dt')
        plt.figure(figsize=(12, 7))
        note_labels = pool_subset['label'].unique()
        note_classes = [c for c in note_labels if 'Class' in str(c) and ('Note' in str(c) or 'Notes' in str(c))]
        
        if not note_classes: continue

        for nc in note_classes:
            nc_data = pool_subset[pool_subset['label'] == nc]
            if not nc_data.empty:
                plt.plot(nc_data['report_period_dt'], nc_data['balance_val'], marker='o', label=nc)
        
        plt.title(f'Repayment Structure Inference: {pool}')
        plt.xlabel('Report Period')
        plt.ylabel('Note Balance (USD)')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        
        safe_pool_name = "".join([c if c.isalnum() else "_" for c in pool])
        plt.savefig(f'repayment_structure_{safe_pool_name}.png')
        plt.close()
    print(f"Generated repayment plots for {len(sampled_pools)} pools.")

if __name__ == "__main__":
    analyze()
