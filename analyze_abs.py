import pandas as pd
import matplotlib.pyplot as plt
import os
import random
import numpy as np
import re

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

def analyze():
    print("Loading datasets...")

    def clean_name(n):
        if pd.isna(n): return ""
        s = "".join([c if ord(c) < 128 else " " for c in str(n)])
        return " ".join(s.upper().split())

    # ── 1. Load provided data ────────────────────────────────────────────────
    provided_df = pd.read_csv('collections_dirty.csv')
    provided_dt = pd.to_datetime(provided_df['date'], errors='coerce', utc=True)
    provided_df['year_month'] = provided_dt.dt.strftime('%Y-%m')
    provided_df['poolname_clean'] = provided_df['poolname'].apply(clean_name)

    def extract_issuer(n_clean):
        if "FORD" in n_clean:   return "FORD CREDIT"
        if "CARMAX" in n_clean: return "CARMAX"
        return " ".join(n_clean.split()[:2])

    provided_df['issuer'] = provided_df['poolname_clean'].apply(extract_issuer)

    sum_cols = ['totalInterest', 'prepaymentsInFullCollected',
                'principalCollections', 'recoveries', 'liquidationProceeds']
    for col in sum_cols:
        if col not in provided_df.columns: provided_df[col] = 0.0
    provided_df['total_collections_provided'] = provided_df[sum_cols].fillna(0).sum(axis=1)

    # ── 2. Load scraped metadata ─────────────────────────────────────────────
    metadata_df = pd.read_csv('output/metadata.csv')
    metadata_df['company_name_clean'] = metadata_df['company_name'].apply(clean_name)
    metadata_df['year_month'] = pd.to_datetime(
        metadata_df['report_period'], format='%Y%m%d', errors='coerce', utc=True
    ).dt.strftime('%Y-%m')

    # ── 3. Aggregate monthly collections from Table 2 ────────────────────────
    print("Aggregating monthly collections...")
    funds_df = pd.read_csv('output/table_2_available_funds.csv')

    def extract_best_amount(row):
        if 'dollar_amount' in row and pd.notna(row['dollar_amount']):
            v = clean_dollar(row['dollar_amount'])
            if v != 0.0: return v
        for col in row.index[::-1]:
            if col in ['accession_number', 'label', 'label_str']: continue
            v = clean_dollar(row[col])
            if v != 0.0: return v
        return 0.0

    funds_df['val_float'] = funds_df.apply(extract_best_amount, axis=1)
    if 'label' not in funds_df.columns:
        funds_df['label'] = ""
    funds_df['label_str'] = funds_df['label'].astype(str)

    p_labels = [
        'Principal Collections', 'Prepayments in Full', 'Liquidation Proceeds',
        'Recoveries', 'Purchase Amounts Related to Principal',
        'Collections allocable to Principal',
        'a. Collections allocable to Principal',
        'a.  Collections allocable to Principal',
    ]
    i_labels = [
        'Interest Collections', 'Purchase Amounts Related to Interest',
        'Collections allocable to Finance Charge',
        'a. Collections allocable to Finance Charge',
        'a.  Collections allocable to Finance Charge',
    ]
    precise_labels = ["Total Finance Charge and Principal Collections"]
    ford_total     = "Collections"

    funds_df['is_principal']     = funds_df['label_str'].str.contains('|'.join(p_labels), case=False, na=False)
    funds_df['is_interest']      = funds_df['label_str'].str.contains('|'.join(i_labels), case=False, na=False)
    funds_df['is_precise_total'] = (
        funds_df['label_str'].str.contains('|'.join(precise_labels), case=False, na=False) |
        (funds_df['label_str'].str.strip() == ford_total)
    )

    monthly_agg = funds_df.groupby('accession_number', sort=False).agg(
        scraped_principal     = ('val_float', lambda x: x[funds_df.loc[x.index,'is_principal']].max()),
        scraped_interest      = ('val_float', lambda x: x[funds_df.loc[x.index,'is_interest']].max()),
        scraped_sum           = ('val_float', lambda x: (
            x[funds_df.loc[x.index,'is_principal']].max() +
            x[funds_df.loc[x.index,'is_interest']].max()
        )),
        scraped_precise_total = ('val_float', lambda x: x[funds_df.loc[x.index,'is_precise_total']].max()),
    ).reset_index()

    monthly_agg['scraped_total_collections'] = monthly_agg.apply(
        lambda r: r['scraped_precise_total'] if r['scraped_precise_total'] > 0 else r['scraped_sum'],
        axis=1
    )

    unified_summary = pd.merge(monthly_agg, metadata_df, on='accession_number')
    unified_summary.to_csv('output/unified_monthly_summary.csv', index=False)
    print("Saved 'output/unified_monthly_summary.csv'")

    # ── 4. Repayment Structure Plots (10 random pools) ───────────────────────
    print("Generating repayment structure plots...")
    balance_df = pd.read_csv('output/table_5_note_balance.csv')
    balance_df = pd.merge(balance_df,
                          metadata_df[['accession_number', 'company_name', 'year_month', 'report_period']],
                          on='accession_number', how='left')

    # Find numeric value column: first non-label column with numeric data
    label_col = 'label'
    # All non-meta columns are potential value columns
    value_cols = [c for c in balance_df.columns
                  if c not in ['accession_number', 'label', 'company_name', 'year_month', 'report_period']]

    # Parse a "balance" from any numeric column
    def pick_value(row):
        for vc in value_cols:
            v = clean_dollar(row.get(vc, ''))
            if v > 0: return v
        return 0.0

    balance_df['balance_val'] = balance_df.apply(pick_value, axis=1)
    balance_df['report_period_dt'] = pd.to_datetime(
        balance_df['report_period'], format='%Y%m%d', errors='coerce')
    balance_df['label_str'] = balance_df['label'].astype(str)

    # Only keep rows labelled with a note class (both Ford: "Class A-1 Notes" and CarMax: "a. Class A-1 Note Balance")
    note_mask = balance_df['label_str'].str.contains(
        r'Class\s+[A-Z0-9\-]+\s*(Note|Notes|Balance)?', regex=True, case=False, na=False)
    note_balance = balance_df[note_mask].copy()

    all_pools = note_balance['company_name'].dropna().unique().tolist()
    random.seed(42)
    sampled_pools = random.sample(all_pools, min(10, len(all_pools)))

    for pool in sampled_pools:
        pool_data = note_balance[note_balance['company_name'] == pool].copy()
        pool_data = pool_data.sort_values('report_period_dt')
        note_classes = pool_data['label_str'].unique()
        if not len(note_classes): continue

        fig, ax = plt.subplots(figsize=(12, 7))
        for nc in note_classes:
            nc_data = pool_data[pool_data['label_str'] == nc]
            if nc_data['balance_val'].sum() == 0: continue
            ax.plot(nc_data['report_period_dt'], nc_data['balance_val'], marker='o', label=nc)

        ax.set_title(f'Principal Balance Over Time – {pool}')
        ax.set_xlabel('Report Period')
        ax.set_ylabel('Balance (USD)')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        safe_name = "".join([c if c.isalnum() else '_' for c in pool.upper()])
        plt.savefig(f'repayment_structure_{safe_name}.png')
        plt.close()

    print(f"Generated repayment plots for {len(sampled_pools)} pools.")

    # ── 5. Clean-Up Call Analysis ────────────────────────────────────────────
    print("Analyzing clean-up calls...")
    cleanup_rows = []
    for pool_name, group in unified_summary.groupby('company_name'):
        group = group.sort_values('year_month')
        last = group.iloc[-1]
        initial_balance = group['scraped_total_collections'].max()
        last_acc = last['accession_number']

        cleanup_rows_table = funds_df[
            (funds_df['accession_number'] == last_acc) &
            funds_df['label_str'].str.contains('clean', case=False, na=False)
        ]
        cleanup_amount  = cleanup_rows_table['val_float'].max() if not cleanup_rows_table.empty else 0.0
        remaining       = last['scraped_total_collections']
        pct             = (remaining / initial_balance * 100) if initial_balance > 0 else 0.0

        cleanup_rows.append({
            'pool_name':             pool_name,
            'last_filing':           last_acc,
            'cleanup_call_amount':   cleanup_amount,
            'remaining_balance':     remaining,
            'initial_balance_proxy': initial_balance,
            'cleanup_pct_of_initial': pct,
        })

    cleanup_df = pd.DataFrame(cleanup_rows)
    cleanup_df.to_csv('clean_up_stats.csv', index=False)
    print("Saved 'clean_up_stats.csv'")
    print(cleanup_df[['pool_name','cleanup_call_amount','remaining_balance','cleanup_pct_of_initial']].to_string(index=False))

    # ── 6. Collections Comparison Scatter Plot ───────────────────────────────
    print("Generating collections comparison plot...")
    final_df = pd.merge(
        provided_df, unified_summary,
        left_on=['poolname_clean', 'year_month'],
        right_on=['company_name_clean', 'year_month'],
        how='inner'
    )

    if not final_df.empty:
        print(f"Found {len(final_df)} matching records for plotting.")
        fig, ax = plt.subplots(figsize=(12, 8))
        for issuer in sorted(final_df['issuer'].unique()):
            subset = final_df[final_df['issuer'] == issuer]
            ax.scatter(subset['scraped_total_collections'],
                       subset['total_collections_provided'],
                       label=issuer, alpha=0.7)
        all_vals = pd.concat([final_df['scraped_total_collections'],
                               final_df['total_collections_provided']])
        max_val = all_vals.max() if not all_vals.empty else 1e7
        ax.plot([0, max_val], [0, max_val], 'k--', alpha=0.3, label='Match Line')
        ax.set_xlabel('Scraped Collections (USD)')
        ax.set_ylabel('Provided Collections (USD)')
        ax.set_title('Scraped vs Provided Collection Data')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        ax.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        plt.savefig('collections_comparison.png')
        print("Saved 'collections_comparison.png'")
    else:
        print("No matches found!")

if __name__ == "__main__":
    analyze()
