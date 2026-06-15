# SEC 10-D ABS Scraper & Analytics Pipeline

A Python pipeline for scraping, cleaning, and analyzing auto-loan Asset-Backed Security (ABS) distribution reports filed with the SEC on Form 10-D. Built for academic research at the University of Georgia.

---

## Overview

This project targets all **Auto Loan** and **Auto Lease** 10-D filings on EDGAR and extracts four standardized financial tables from each filing's Exhibit 99:

| Table | Description |
|---|---|
| Table 2 | Available Funds / Collections |
| Table 3 | Distributions |
| Table 4 | Noteholder Payments |
| Table 5 | Note Balance |

Covered issuers include Ford, CarMax, Ally, Fifth Third, Capital One, Santander, Toyota, BMW, Mercedes-Benz, and more.

### Cleanup Call Analytics

The primary research output focuses on **Cleanup Calls** — the early retirement of ABS trusts once the remaining pool balance falls to ~10% of the original. The analytics pipeline:

1. **Validates the 10% Industry Standard** — confirms cleanup calls are consistently triggered at ~10% of initial pool balance across all issuers.
2. **Proves the Terminal Event** — shows a 1:1 ratio between the Cleanup Call Amount and the Remaining Pool Balance, confirming full debt retirement.
3. **Tranche Waterfalls** — generates per-trust case study plots tracing subordinated tranche lifecycles down to their cleanup call date.

---

## Project Structure

```
├── src/                    # Core pipeline scripts
│   ├── scraper.py          # Multi-threaded EDGAR scraper (Tables 2–5)
│   ├── analyze_abs.py      # Master analytics & aggregation script
│   ├── rescrape_edgar.py   # Targeted re-scrape for missing filings
│   ├── repair_collections.py        # Repairs malformed collections data
│   ├── extract_cleanup_tranche_data.py  # Extracts tranche-level cleanup call data
│   ├── final_abs_repair.py # Final post-processing repair pass
│   ├── verify_all.py       # End-to-end output verification
│   └── plots/
│       ├── honkanen_plots_v7.py   # Main visualization script (latest)
│       └── honkanen_response.py   # Response plots for Dr. Honkanen
├── tests/
│   ├── test_mercedes_abs.py
│   ├── test_sec.py
│   └── test_sec_home.py
├── data/                   # Raw input data (large files gitignored)
│   ├── all_10D_ABS.txt     # Full list of 10-D filing URLs from EDGAR full-text index
│   └── all_ABS.txt         # Filtered ABS URL list
├── deliverables/           # Outputs delivered to Dr. Honkanen
│   ├── For_Dr_Honkanen/    # Final plots and case study CSVs
│   ├── honkanen_deliverable_may2026.zip
│   ├── honkanen_instructions.txt
│   └── Table4 notes.xlsx
├── output/                 # Generated data (gitignored)
├── requirements.txt
└── README.md
```

---

## Setup

```bash
pip install -r requirements.txt
```

**Dependencies:** `pandas`, `matplotlib`, `requests`, `lxml`, `beautifulsoup4`

---

## Usage

### 1. Scrape EDGAR

Fetches all 10-D filings from the URL list and writes Tables 2–5 to `output/`.
Uses 10 parallel threads, respects the SEC's 10 RPS rate limit, and supports resuming interrupted runs.

```bash
python src/scraper.py data/all_10D_ABS.txt
```

Optional — write output to a custom directory:
```bash
python src/scraper.py data/all_10D_ABS.txt my_output_dir/
```

### 2. Analyze & Aggregate

Runs the master analytics pass over the scraped CSVs.

```bash
python src/analyze_abs.py
```

### 3. Generate Plots

Produces all cleanup call visualizations and case study charts into `output/analysis_v7/`.

```bash
python src/plots/honkanen_plots_v7.py
```

**Outputs:**
- `01_scatter_by_brand.png` — Cleanup call scatter by issuer
- `02_call_to_initial_over_time.png` — Cleanup-to-initial ratio time series
- `03_call_to_remaining_over_time.png` — Cleanup-to-remaining ratio time series
- `04_casestudy_*.png` — Individual tranche waterfall plots
- `case_study_pools.csv` — Raw pool data for all case studies

### 4. Verify Output

```bash
python src/verify_all.py
```

---

## Notes

- **Rate limiting**: The scraper sleeps 1.05s per thread × 10 threads ≈ 9.5 RPS, staying under the SEC's 10 RPS cap.
- **Resume support**: Already-processed URLs are tracked in `output/processed_urls.txt` and skipped on re-runs.
- **Large files**: `output/` CSVs (Tables 2–5 are 100–150 MB each) and raw input files are excluded from version control via `.gitignore`.
