import sqlite3
import pandas as pd
from pathlib import Path

SDM_PATH = "data/sdm_deproject.db"
SOURCE_PATH = "GreatOutdoors/GreatOutdoors/GO_SALES-data.sqlite"
TABLE = "order_header"

def test_load():
    if not Path(SOURCE_PATH).exists():
        print(f"Source {SOURCE_PATH} not found")
        return
    
    sdm_con = sqlite3.connect(SDM_PATH)
    src_con = sqlite3.connect(SOURCE_PATH)
    
    try:
        # 1. Get SDM columns
        sdm_cols = [row[1] for row in sdm_con.execute(f"PRAGMA table_info({TABLE})")]
        print(f"SDM columns for {TABLE}: {sdm_cols}")
        
        if not sdm_cols:
            print(f"Table {TABLE} does not exist in SDM")
            return

        # 2. Read from source
        df = pd.read_sql(f"SELECT * FROM {TABLE} LIMIT 5", src_con)
        print(f"Source columns: {df.columns.tolist()}")
        
        # 3. Match columns
        df.columns = df.columns.str.upper()
        sdm_cols_upper = [c.upper() for c in sdm_cols]
        
        common_cols = [col for col in df.columns if col in sdm_cols_upper]
        print(f"Common columns: {common_cols}")
        
        if not common_cols:
            print("No matching columns found!")
            return
            
        df = df[common_cols]
        df.columns = [sdm_cols[sdm_cols_upper.index(col)] for col in df.columns]

        # 4. Try to_sql
        df.to_sql(TABLE, sdm_con, if_exists="append", index=False)
        print("Success! Data loaded.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        src_con.close()
        sdm_con.close()

if __name__ == "__main__":
    test_load()
