import sqlite3
from pathlib import Path

SDM_PATH = "data/sdm_deproject.db"
SQL_SCRIPT = "data/sdm_create.sql"

def create_sdm():
    try:
        Path(SDM_PATH).unlink(missing_ok=True)
        print(f"Deleted {SDM_PATH}")
        
        with open(SQL_SCRIPT, "r") as f:
            sql = f.read()
        
        con = sqlite3.connect(SDM_PATH)
        con.executescript(sql)
        con.commit()
        
        tables = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        print(f"Created {len(tables)} tables")
        con.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_sdm()
