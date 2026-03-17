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
    funds_df['label_str'] = funds_df['label'].astype(str).str.strip()

    # For Ford: "Collections" is the single total line
    # For CarMax: "Available Collections" minus "Reserve Account Draw Amount"
    # For both: "Total Finance Charge and Principal Collections" if it exists
    funds_df['is_ford_total']    = funds_df['label_str'] == 'Collections'
    funds_df['is_avail']         = funds_df['label_str'] == 'Available Collections'
    funds_df['is_reserve_draw']  = funds_df['label_str'] == 'Reserve Account Draw Amount'
    funds_df['is_precise']       = funds_df['label_str'].str.contains('Total Finance Charge and Principal', case=False, na=False)

    def agg_total(grp):
        idx = grp.index
        ford_total   = grp[funds_df.loc[idx, 'is_ford_total']]['val_float'].max()
        precise      = grp[funds_df.loc[idx, 'is_precise']]['val_float'].max()
        avail        = grp[funds_df.loc[idx, 'is_avail']]['val_float'].max()
        reserve_draw = grp[funds_df.loc[idx, 'is_reserve_draw']]['val_float'].max()

        # Priority: precise > ford_total > avail - reserve_draw
        if pd.notna(precise) and precise > 0:
            return precise
        if pd.notna(ford_total) and ford_total > 0:
            return ford_total
        if pd.notna(avail) and avail > 0:
            draw = reserve_draw if pd.notna(reserve_draw) else 0.0
            return avail - draw
        return 0.0

    monthly_agg = funds_df.groupby('accession_number').apply(
        lambda grp: pd.Series({'scraped_total_collections': agg_total(grp)})
    ).reset_index()

    unified_summary = pd.merge(monthly_agg, metadata_df, on='accession_number')
    unified_summary.to_csv('output/unified_monthly_summary.csv', index=False)
    print("Saved 'output/unified_monthly_summary.csv'")

    # Quick check
    for issuer_kw in ['FORD', 'CARMAX']:
        sub = unified_summary[unified_summary['company_name'].str.contains(issuer_kw, case=False, na=False)]
        print(f"  {issuer_kw}: {len(sub)} rows, avg collections = {sub['scraped_total_collections'].mean():,.0f}")

    # ── 4. Repayment Structure Plots (sampled pools) ─────────────────────────
    print("Generating repayment structure plots...")
    balance_df = pd.read_csv('output/table_5_note_balance.csv')
    balance_df = pd.merge(balance_df,
                          metadata_df[['accession_number', 'company_name', 'year_month', 'report_period']],
                          on='accession_number', how='left')

    value_cols = [c for c in balance_df.columns
                  if c not in ['accession_number', 'label', 'company_name', 'year_month', 'report_period']]

    def pick_value(row):
        for vc in value_cols:
            v = clean_dollar(row.get(vc, ''))
            if v > 0: return v
        return 0.0

    balance_df['balance_val']       = balance_df.apply(pick_value, axis=1)
    balance_df['report_period_dt']  = pd.to_datetime(
        balance_df['report_period'], format='%Y%m%d', errors='coerce')
    balance_df['label_str']         = balance_df['label'].astype(str)

    note_mask = balance_df['label_str'].str.contains(
        r'Class\s+[A-Z0-9\-]+', regex=True, case=False, na=False)
    note_balance = balance_df[note_mask].copy()

    all_pools = note_balance['company_name'].dropna().unique().tolist()
    random.seed(42)
    sampled_pools = random.sample(all_pools, min(10, len(all_pools)))

    for pool in sampled_pools:
        pool_data = note_balance[note_balance['company_name'] == pool].sort_values('report_period_dt')
        note_classes = pool_data['label_str'].unique()
        fig, ax = plt.subplots(figsize=(12, 7))
        plotted = 0
        for nc in note_classes:
            nc_data = pool_data[pool_data['label_str'] == nc]
            if nc_data['balance_val'].sum() == 0: continue
            ax.plot(nc_data['report_period_dt'], nc_data['balance_val'], marker='o', label=nc)
            plotted += 1
        if plotted == 0:
            plt.close(); continue
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
        last  = group.iloc[-1]
        initial = group['scraped_total_collections'].max()
        last_acc = last['accession_number']

        cleanup_table = funds_df[
            (funds_df['accession_number'] == last_acc) &
            funds_df['label_str'].str.contains('clean', case=False, na=False)
        ]
        cleanup_amt = cleanup_table['val_float'].max() if not cleanup_table.empty else 0.0
        remaining   = last['scraped_total_collections']
        pct         = (remaining / initial * 100) if initial > 0 else 0.0

        cleanup_rows.append({
            'pool_name':              pool_name,
            'last_filing':            last_acc,
            'cleanup_call_amount':    cleanup_amt,
            'remaining_balance':      remaining,
            'initial_balance_proxy':  initial,
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
        colors = {'FORD CREDIT': 'tab:orange', 'CARMAX': 'tab:blue'}
        for issuer in sorted(final_df['issuer'].unique()):
            subset = final_df[final_df['issuer'] == issuer]
            ax.scatter(subset['scraped_total_collections'],
                       subset['total_collections_provided'],
                       label=issuer,
                       color=colors.get(issuer, None),
                       alpha=0.8, edgecolors='white', linewidths=0.4, s=60)
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
        plt.savefig('collections_comparison.png', dpi=150)
        print("Saved 'collections_comparison.png'")
    else:
        print("No matches found!")

if __name__ == "__main__":
    analyze()
