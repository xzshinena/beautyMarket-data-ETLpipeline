# Beauty Market Price Data ETL Pipeline

How come everytime I try to purchase an East Asian beauty product, the identical product has a different price depending on where I look. 
I built this tool which takes a search input from the user for the product they want, then scrapes that product info from 10 different suppliers to see where is selling for the cheapest
More formally, it's an ETL data pipeline that scrapes product prices from multiple online stores, computes metrics, and identifies the cheapest venue for each product.

## Setup

```bash
pip install -r requirements.txt
playwright install chromium
```

## Usage

```bash
python main.py --start 2025-01-01 --end 2025-12-31 --compare --sources kiyoko oomomo oliveyoung --search "romand"
```

### Arguments

- `--start` - Start date filter
- `--end` - End date filter
- `--sources` - Space-separated list of sources to scrape
- `--search` - Product search term
- `--compare` - Enable price comparison

### Available Suppliers

- Olive Young
- Sukoshi Mart
- Kiokii &
- Lamour
- Oomomo
- Kiyoko Beauty
- Komiko Beauty
- Axia Station
- M Beauty
- Cosme

## Output

Results are saved to `data/processed/`:

- `prices.csv` - Raw price data
- `daily_metrics.csv` - OHLC, returns, rolling average, volatility
- `price_comparison.csv` - Cheapest venue per product
- `comparison_report.txt` - Human-readable summary

## Pipeline Structure

```
src/
  sources/       - Data source adapters (scrapers)
  pipeline/      - Clean, validate, store modules
  analysis/      - Metrics and comparison logic
  export.py      - CSV/report export
main.py          - CLI entry point
config.py        - Configuration settings
```
