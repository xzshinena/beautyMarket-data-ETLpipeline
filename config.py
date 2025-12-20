#project settings

from pathlib import Path

project_root = Path(__file__).parent

raw_directory = project_root / "data" / "raw"
processed_directory = project_root / "data" / "processed"
db_path = project_root / "data" / "market_data.db"

valid_currencies = {"CAD" , "RMB", "KRW" , "JPY" , "USD"}

suppliers = {
    "Sephora",
    "YesStyle",
    "Sukoshi Mart",
    "Stylevana",
    "Amazon",
    "Olive Young",
    "Kiokii &",
    "Shoppers Drug Mart",
    "Lamour",
    "Oomomo",
    "Axia Station",
    "Shein",
    "Cosme",
    "Kiyoko Beauty",
    "Komiko Beauty"
}

min_price = 0.01
max_price = 1000000

rolling_window_days = 7

required_columns = {
    "product_name",
    "supplier",
    "price",
    "currency",
    "timestamp"
}
