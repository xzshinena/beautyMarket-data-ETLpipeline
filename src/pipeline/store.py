import sqlite3
import pandas as pd
from pathlib import Path

SCHEMA_PRICES = """
CREATE TABLE IF NOT EXISTS prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    supplier TEXT NOT NULL,
    price REAL NOT NULL,
    product_id TEXT NOT NULL,
    currency TEXT,
    source_name TEXT,
    ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
);
"""

SCHEMA_DAILY_METRICS = """
CREATE TABLE IF NOT EXISTS daily_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    supplier TEXT NOT NULL,
    product_id TEXT NOT NULL,
    open_price REAL,
    close_price REAL,
    high_price REAL,
    low_price REAL,
    daily_return REAL,
    rolling_avg_7d REAL,
    volatility_7d REAL,
    computed_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

SCHEMA_PRICE_COMPARISONS = """
CREATE TABLE IF NOT EXISTS price_comparison (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT NOT NULL,
    snapshot_date TEXT,
    cheapest_supplier TEXT NOT NULL,
    cheapest_price REAL NOT NULL,
    most_expensive_supplier TEXT NOT NULL,
    most_expensive_price REAL NOT NULL,
    price_spread REAL,
    savings_pct REAL,
    num_suppliers INTEGER,
    computed_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

SCHEMA_INDICIES = """ 
CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_priced_product ON price(product_id);
CREATE INDEX IF NOT EXISTS idx_metrics_date ON daily_metrics(date);
CREATE INDEX IF NOT EXISTS idx_comparison_date ON price_comparison(snapshot_date);
"""

def get_connection(db_path : str) -> sqlite3.Connection :
    Path(db_path).parent.mkdir(parents = True, exist_ok = True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreigh_keys = ON")
    return connection

def init_schema(conn : sqlite2.Connection) -> None : 
    cursor = conn.cursor()
    cursor.executescript(SCHEMA_PRICES)
    cursor.executescript(SCHEMA_DAILY_METRICS)
    cursor.executescript(SCHEMA_PRICE_COMPARISONS)
    cursor.executescript(SCHEMA_INDICIES)
    conn.commit()

#update + insert into db
def upsert_prices(conn : sqlite3.Connection, df: pd.DataFrame) -> int:
    if df.empty :
        return 0
    
    cursor = conn.cursor()
    rows_inserted = 0
    for _, row in df.iterrows() :
        try : 
            cursor.execute("""
                            INSERT INTO prices (timestamp, product_id, supplier, price, currency, source_name, ingested_at)
                            VALUES (?,?,?,?,?,?,?)
                            ON CONFLICT (timestamp, product_id) DO NOTHING
                            """,
                        (
                            str(row["timestamp"]),
                            row["product_id"],
                            row ["supplier"],
                            float(row["price"]),
                            row["currency"],
                            row.get("source_name"),
                            str(row["ingested_at"])
                        )
            )

            if cursor.rowcount > 0 :
                rows_inserted += 1
        
        except sqlite3.Error as e :
            print(f"Error inserting in row {e}")
    
    conn.commit()
    print(f"Inserted {rows_inserted} price records")
    return rows_inserted

def upsert_metrics(conn : sqlite3.Connection, df : pd.DataFrame) -> int :
    if df.empty : 
        return 0
    
    cursor = conn.cursor()
    rows_changed = 0

    for _, row in df.iterrows() :
        try :
            cursor.execute("""
                INSERT INTO daily_metrics (date, product_id, supplier, open_price, close_price, high_price, low_price, daily_return, rolling_avg_7d, volatility_7d, computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, product_id, supplier) DO UPDATE SET
                    open_price = excluded.open_price,
                    close_price = excluded.close_price,
                    low_price = excluded.low_price,
                    daily_return = excluded.daily_return,
                    rolling_avg_7d = excluded.rolling_avg_7d,
                    volatility_7d = excluded.volatility_avg_7d,
                    computed_at = CURRENT_TIMESTAMP
                """,
                
                (
                    str(row["date"]),
                    row["supplier"],
                    row["product_id"],
                    float(row["open_price"]),
                    float(row["close_price"]),
                    float(row["high_price"]),
                    float(row["low_price"]),
                    float(row["daily_return"]),
                    float(row["rolling_avg_7d"]),
                    float(row["volatility_7d"]),
                    str(row["computed_at"])
                )
                )
        
            if cursor.rowcount > 0:
                rows_changed += 1
        
        except sqlite3.Error as e :
            print(f"Error inserting metrics in row {e}")
        
        conn.commit()
        print(f"Inserted {rows_changed} into daily metrics records")
        return rows_changed

def upsert_comparison(conn : sqlite3.Connection, df : pd.DataFrame) -> int :
    if df.empty :
        return 0
    
    cursor = conn.cursor()
    rows_updated = 0

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO price_comparisons (product_id, snapshot_date, cheapest_supplier, cheapest_price, most_expensive_supplier, most_expensive_price, price_spead, savings_pct, num_suppliers, computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                ON CONFLICT (product_id, snapshot_date) DO UPDATE SET
                    cheapest_supplier = excluded.cheapest_supplier,
                    cheapest_price = excluded.cheapest_price,
                    most_expensive_supplier = excluded.most_expensive_supplier,
                    most_expensive_price = excluded.most_expensive_price,
                    price_spread = excluded.price_spread,
                    savings_pct = excluded.savings_pct,
                    num_suppliers = excluded.num_suppliers,
                    computed_at = CURRENT_TIMESTAMP
            """, (
                row["product_id"],
                str(row["snapshot_date"]),
                str(row["cheapest_supplier"]),
                float(row["cheapest_price"]),
                str(row["most_expensive_supplier"]),
                float(row["most_expensive_price"]),
                row.get("price_spread"),
                row.get("savings_pct"),
                row.get("num_suppliers")
            ))

            if cursor.rowcount > 0:
                rows_updated += 1

        except sqlite3.Error as e:
            print(f"Error inserting comparison: {e}")

    conn.commit()
    return rows_updated

#query db
def query_prices(conn : sqlite3.connection, start_date: str = None, end_date: str = None, product_id: str = None) -> pd.DataFrame :
    query = "SELECT * FROM prices WHERE 1 = 1"
    params = []

    if start_date :
        query += "AND timestamp >= ?"
        params.append(start_date)
    
    if end_date :
        query += "AND timestamp <= ?"
        params.append(end_date)
    
    if product_id :
        query += "AND product_id = ?"
        params.append(product_id)
    
    query += "ORDER BY timestamp, supplier, product_id"

    df = pd.read_sql_query(query, conn, params = params)
    if "timestamp" in df.columns and not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df

def query_latest_prices(conn : sqlite3.Connection) -> pd.DataFrame :
    query = """ 
            SELECT prices.*
            FROM prices
            INNER JOIN (
                        SELECT supplier, product_id, MAX(timestamp) as max_ts
                        FROM PRICES
                        GROUP BY supplier, product_id
                        ) latest 
            ON price.supplier = latest.supplier
            AND price.product_id = latest.product_id
            AND price.timestamp = latest.max_ts
            ORDER BY product_id, supplier
            """

    df = pd.read_sql_query(query, conn)

    if "timestamp" in df.columns and not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    return df

def query_metrics(conn: sqlite3.Connection, start_date:str = None, end_date: str = None) -> pd.DataFrame :
    query = "SELECT * FROM daily_metrics WHERE 1 = 1"
    params = []

    if start_date : 
        query += "AND date >= ?"
        params.append(start_date)
    
    if end_date :
        query += "AND end_date <= ?"
        params.append(end_date)
    
    query += "ORDER BY date, supplier, product_id"
    return pd.read_sql_query(query, conn, params = params)

def query_comparison(conn : sqlite3.Connection, snapshot_date: str = None) -> pd.DataFrame:
    query = "SELECT * FROM price_comparison"
    params = []

    if snapshot_date :
        query += "WHERE snapshot_date = ?"
        params.append(snapshot_date)
    
    query += "ORDER BY snapshot_date, product_id"

    return pd.read_sql_query(query, conn, params = params)




        

