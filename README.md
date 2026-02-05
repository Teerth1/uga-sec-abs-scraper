# SEC 10-D ABS Scraper

A Python scraper for extracting financial data from SEC 10-D Distribution Reports for Asset-Backed Securities (ABS).

## Overview

This tool extracts structured data from Ford Credit Auto Owner Trust 10-D filings, including:
- **Table 2**: Available Funds
- **Table 3**: Distributions  
- **Table 4**: Noteholder Payments
- **Table 5**: Note Balance

Each table is tagged with metadata (accession number, company name, report period, filed date) for cross-referencing.

## Usage

```bash
python scraper.py
```

Output CSVs are saved to the `output/` directory.

## Requirements

```
pandas
requests
lxml
```

## Project Status

🚧 **In Development** - Currently testing with Ford Credit Auto Owner Trust 2021-A filings.
