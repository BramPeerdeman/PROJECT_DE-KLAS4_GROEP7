import sqlite3
import pandas as pd
from pathlib import Path

SDM_PATH = "data/sdm_deproject.db"
SQL_SCRIPT = "data/sdm_create.sql"
SOURCES = {
    "GreatOutdoors/GreatOutdoors/GO_SALES-data.sqlite": ["order_header", "order_details"],
    "GreatOutdoors/GreatOutdoors/SALES_TARGET-data.csv": "sales_target"
}

def full_process():
    # 1. Create
    Path(SDM_PATH).unlink(missing_ok=True)
    with open(SQL_SCRIPT, "r") as f:
        sql = f.read()
    con = sqlite3.connect(SDM_PATH)
    con.executescript(sql)
    con.commit()
    print("DB Created")

    # 2. Load SQLite
    src_path = "GreatOutdoors/GreatOutdoors/GO_SALES-data.sqlite"
    src_con = sqlite3.connect(src_path)
    for table in ["order_header", "order_details"]:
        sdm_cols = [row[1] for row in con.execute(f"PRAGMA table_info({table})")]
        df = pd.read_sql(f"SELECT * FROM {table}", src_con)
        df.columns = df.columns.str.upper()
        sdm_cols_upper = [c.upper() for c in sdm_cols]
        common_cols = [col for col in df.columns if col in sdm_cols_upper]
        df = df[common_cols]
        df.columns = [sdm_cols[sdm_cols_upper.index(col)] for col in df.columns]
        df.to_sql(table, con, if_exists="append", index=False)
        print(f"Loaded {table}: {len(df)} rows")
    src_con.close()

    # 3. Load CSV
    csv_path = "GreatOutdoors/GreatOutdoors/SALES_TARGET-data.csv"
    table = "sales_target"
    sdm_cols = [row[1] for row in con.execute(f"PRAGMA table_info({table})")]
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.upper()
    sdm_cols_upper = [c.upper() for c in sdm_cols]
    common_cols = [col for col in df.columns if col in sdm_cols_upper]
    df = df[common_cols]
    df.columns = [sdm_cols[sdm_cols_upper.index(col)] for col in df.columns]
    df.to_sql(table, con, if_exists="append", index=False)
    print(f"Loaded {table}: {len(df)} rows")

    con.close()

if __name__ == "__main__":
    full_process()
