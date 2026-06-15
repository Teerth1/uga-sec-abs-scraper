import pandas as pd
import os

print("=== FINAL VERIFICATION CHECKLIST ===\n")

# 1. Check final report
final = pd.read_csv('output/final_abs_summary_dr_honkanen.csv')
total = len(final)
matched = final['scraped_total_collections'].notna().sum()
pct = matched / total * 100
status = "PASS" if pct > 95 else "WARN"
print(f"[1] Final Report Match Rate: {matched}/{total} ({pct:.1f}%) [{status}]")

# 2. Check plots
plot_paths = [
    'output/analysis_v2/01_scatter_all_issuers.png',
    'output/analysis_v2/02_cleanup_vs_initial_time_series.png',
    'output/analysis_v2/03_cleanup_vs_remaining_dist.png',
]
for p in plot_paths:
    exists = os.path.exists(p)
    size = os.path.getsize(p) if exists else 0
    ok = "PASS" if exists and size > 10000 else "FAIL"
    print(f"[2] {os.path.basename(p)}: [{ok}] ({size} bytes)")

# 3. Case studies
case_files = [f for f in os.listdir('output/analysis_v2/') if '04_case_study' in f]
print(f"[3] Case Study Plots: {len(case_files)} generated [{('PASS' if len(case_files) >= 2 else 'FAIL')}]")
for c in case_files:
    sz = os.path.getsize(f'output/analysis_v2/{c}')
    print(f"    - {c} ({sz} bytes)")

# 4. Clean-up calls
pool_stats = pd.read_csv('output/analysis/pool_stats_augmented.csv')
calls = pool_stats[pool_stats['cleanup_call_amount'] > 0]
ck4 = "PASS" if len(calls) > 0 else "FAIL"
print(f"[4] Clean-up Calls Extracted: {len(calls)} rows, {calls['company_name'].nunique()} unique pools [{ck4}]")

# 5. Initial pool sizes
sizes = pool_stats[pool_stats['initial_pool_size'] > 0]
ck5 = "PASS" if len(sizes) > 0 else "FAIL"
print(f"[5] Initial Pool Sizes: {len(sizes)} rows, {sizes['company_name'].nunique()} unique pools [{ck5}]")

# 6. Table 5 (Tranche source)
t5_exists = os.path.exists('output/table_5_note_balance.csv')
t5_size = os.path.getsize('output/table_5_note_balance.csv') if t5_exists else 0
ck6 = "PASS" if t5_size > 100000 else "FAIL"
print(f"[6] Table 5 Note Balance (Tranche source): [{ck6}] ({t5_size:,} bytes)")

# 7. Unified monthly summary
summary = pd.read_csv('output/unified_monthly_summary.csv')
ck7 = "PASS" if len(summary) > 1000 else "WARN"
print(f"[7] Unified Monthly Summary: {len(summary):,} filings [{ck7}]")

print("\n=== ALL CHECKS COMPLETE ===")
