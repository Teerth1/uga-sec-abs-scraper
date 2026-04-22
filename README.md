# SEC 10-D ABS Scraper & Analytics Pipeline

A Python scraper and analytics suite for extracting financial data from SEC 10-D Distribution Reports for Asset-Backed Securities (ABS) and visualizing lifecycle payment schedules.

## Overview

This repository contains tools to extract structured data from various auto-loan ABS trusts (including Ford, CarMax, Ally, Fifth Third, Capital One, and Santander). 

It specifically targets:
- **Table 2**: Available Funds
- **Table 3**: Distributions  
- **Table 4**: Noteholder Payments
- **Table 5**: Note Balance

### Cleanup Call Analytics
The primary analytics script (`honkanen_plots_v6.py`) focuses on isolating and analyzing **Cleanup Calls**. It extracts historical data across all scraped issuers to mathematically model and visualize:
1. **The 10% Industry Standard**: Validating that cleanup calls are consistently executed when the initial pool size reaches ~10%.
2. **Terminal Events**: Proving a 1:1 ratio between the Cleanup Call Amount and the Remaining Pool Balance, confirming that the call fully retires the debt.
3. **Tranche Waterfalls**: Generating individual Case Study plots that trace the lifecycle of subordinated tranches down to their terminal cleanup call date.

## Usage

### 1. Run the Scraper
```bash
python scraper.py
```
*Outputs structured CSVs for each table into the `output/` directory.*

### 2. Generate Analytics & Visualizations
```bash
python honkanen_plots_v6.py
```
*Outputs the following to `output/analysis_v6/`:*
- Master Time-Series of all scraped collections (2006-2024)
- Verification scatter plots
- Time-Series of Cleanup Call ratios
- Visual tranche-level Case Studies (`.png`) and raw pool data (`case_study_pools.csv`)

## Requirements

```
pandas
matplotlib
requests
lxml
```

## Data Storage
Note: Due to their size, the raw scraped `output/` directories and generated `.png` plots are explicitly ignored via `.gitignore` to keep the repository lightweight. Only the python scripts and metadata mappings are tracked.
