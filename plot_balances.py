import pandas as pd
import matplotlib.pyplot as plt
import glob
import re
import sys

# ── 1. Load all Table 5 CSVs ────────────────────────────────────────────────
# Pass a folder as an argument: python plot_balances.py output/ford
# Defaults to output/carmax if no argument given
folder = sys.argv[1] if len(sys.argv) > 1 else "output/carmax"
files = glob.glob(f"{folder}/table_5_note_balance_*.csv")
if not files:
    raise FileNotFoundError("No table_5_note_balance_*.csv files found in output/")

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

# Metadata columns added by the scraper — exclude from auto-detection
META_COLS = {'accession_number', 'company_name', 'report_period', 'filed_date', 'cik'}
data_cols = [c for c in df.columns if c not in META_COLS]

# ── 2. Auto-detect the label column ─────────────────────────────────────────
# Whichever data column has the most cells containing the word "Class"
def score_label_col(col):
    return df[col].astype(str).str.contains('Class', case=False, na=False).sum()

label_col = max(data_cols, key=score_label_col)
print(f"Auto-detected label column: '{label_col}'")

# ── 3. Filter to Note Balance rows only (skip Note Factor, etc.) ─────────────
mask = (
    df[label_col].astype(str).str.contains('Class', case=False, na=False) &
    df[label_col].astype(str).str.contains('Note Balance', case=False, na=False)
)
df = df[mask].copy()

# ── 4. Auto-detect the balance column ───────────────────────────────────────
# Among remaining data columns, pick the one where the most values are
# large numbers (> 1,000 — i.e. actual dollar balances, not factors/rates)
def score_balance_col(col):
    numeric = pd.to_numeric(
        df[col].astype(str).str.replace(r'[\$,]', '', regex=True),
        errors='coerce'
    )
    return (numeric > 1_000).sum()

balance_col = max(data_cols, key=score_balance_col)
print(f"Auto-detected balance column: '{balance_col}'")

# ── 5. Clean class names ─────────────────────────────────────────────────────
# Remove leading letter prefix ("a. ") and trailing " Note Balance"
df['class_name'] = (
    df[label_col].astype(str)
    .str.replace(r'^[a-zA-Z]\.\s*', '', regex=True)        # strip "a. "
    .str.replace(r'\s*Note Balance\s*$', '', regex=True, flags=re.IGNORECASE)
    .str.strip()
)

# ── 6. Parse balance and date ────────────────────────────────────────────────
df['balance'] = (
    df[balance_col].astype(str)
    .str.replace(r'[\$,]', '', regex=True)   # handle "$1,234,567" style (Ford)
    .pipe(lambda s: pd.to_numeric(s, errors='coerce'))
)
df['date'] = pd.to_datetime(df['report_period'], format='%Y%m%d')

print(df[['class_name', 'balance', 'date']].sort_values('date').head(20))

# ── 7. Plot ──────────────────────────────────────────────────────────────────
# Auto-generate title from company names present in the data
companies = df['company_name'].dropna().unique()
title_label = ', '.join(sorted(companies)) if len(companies) <= 3 else f"{len(companies)} ABS Trusts"

plt.figure(figsize=(12, 6))
for class_name in sorted(df['class_name'].unique()):
    class_df = df[df['class_name'] == class_name].sort_values('date')
    plt.plot(class_df['date'], class_df['balance'], label=class_name, marker='o', markersize=3)

plt.title(f'Note Balances Over Time — {title_label}')
plt.xlabel('Date')
plt.ylabel('Balance ($)')
plt.legend()
plt.ylim(bottom=0)
plt.gca().yaxis.set_major_formatter(
    plt.matplotlib.ticker.FuncFormatter(lambda x, _: f'${x/1e6:.0f}M')
)
plt.tight_layout()
plt.savefig('output/balance_chart.png')
print("Chart saved to output/balance_chart.png")
plt.show()
