# Market Data Pipeline

A data pipeline that scrapes product prices from multiple online stores, computes metrics, and identifies the cheapest venue for each product.

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

### Available Sources

Shopify stores (JSON): kiyoko, oomomo, sukoshi, komiko, lamour, kiokii, axiastation

Other: oliveyoung (Playwright)

## Output

Results are saved to `data/processed/`:

- `prices.csv` - Raw price data
- `daily_metrics.csv` - OHLC, returns, rolling average, volatility
- `price_comparison.csv` - Cheapest venue per product
- `comparison_report.txt` - Human-readable summary

## Project Structure

```
src/
  sources/       - Data source adapters (scrapers)
  pipeline/      - Clean, validate, store modules
  analysis/      - Metrics and comparison logic
  export.py      - CSV/report export
main.py          - CLI entry point
config.py        - Configuration settings
```
