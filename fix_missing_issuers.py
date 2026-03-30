import pandas as pd
import numpy as np

print("Loading Master Database...")
master = pd.read_csv('output/table_2_available_funds.csv', on_bad_lines='skip', low_memory=False)

print("Loading Hotfix Database...")
try:
    hotfix = pd.read_csv('output_hotfix/table_2_available_funds.csv', on_bad_lines='skip', low_memory=False)
except FileNotFoundError:
    print("Hotfix CSV not found yet! Scrape still running.")
    exit(1)

# Drop any existing Toyota, Honda, Hyundai, BMW rows from the master database so we don't duplicate
metadata = pd.read_csv('output/metadata.csv', on_bad_lines='skip', low_memory=False)
mask = metadata['company_name'].astype(str).str.upper().str.contains('TOYOTA|HONDA|HYUNDAI|BMW')
target_acc = metadata[mask]['accession_number'].unique()

print(f"Removing {len(target_acc)} target accessions from master...")
master_clean = master[~master['accession_number'].isin(target_acc)]

# Append the hotfix tables (which contains ALL numeric tables for these targets)
print(f"Appending {len(hotfix)} new hotfix rows...")
final_master = pd.concat([master_clean, hotfix], ignore_index=True)

final_master.to_csv('output/table_2_available_funds.csv', index=False)
print("Saved patched master table_2!")

# Now let's print their labels so we can update analyze_abs.py regex!
for brand in ['TOYOTA', 'HONDA', 'HYUNDAI', 'BMW']:
    accs = metadata[metadata['company_name'].astype(str).str.upper().str.contains(brand)]['accession_number']
    brand_f = hotfix[hotfix['accession_number'].isin(accs)]
    print(f"\n--- {brand} TOP LABELS CONTAINING 'TOTAL' or 'AVAILABLE' or 'COLLECTION' ---")
    mask = brand_f['label'].astype(str).str.contains('Total|Available|Collection|Fund|Deposit', case=False, na=False)
    print(brand_f[mask]['label'].value_counts().head(10).to_string())
