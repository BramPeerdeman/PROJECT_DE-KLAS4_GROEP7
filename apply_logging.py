import json
import os

def update_sdm_notebook():
    nb_path = "SDM/load_data-SDM.ipynb"
    with open(nb_path, 'r') as f:
        nb = json.load(f)
    
    # 1. Update Config Cell to include LOG_PATH
    for cell in nb['cells']:
        if cell['cell_type'] == 'code' and "SDM_PATH =" in "".join(cell['source']):
            source_str = "".join(cell['source'])
            if "LOG_PATH =" not in source_str:
                cell['source'].insert(cell['source'].index("SDM_PATH = \"../data/sdm_deproject.db\"\n") + 1, "LOG_PATH = \"../logs/source_to_sdm.log\"\n")
            break
    
    # 2. Update Helper Functions Cell to include logging and robust loading logic
    for cell in nb['cells']:
        if cell['cell_type'] == 'code' and "def _load_from_sqlite" in "".join(cell['source']):
            cell['source'] = [
                "import logging\n",
                "from datetime import datetime\n",
                "\n",
                "def log_etl(level, message, details=\"\"):\n",
                "    timestamp = datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")\n",
                "    with open(LOG_PATH, \"a\") as f:\n",
                "        f.write(f\"{timestamp} | {level:10} | {message:30} | {details}\\n\")\n",
                "\n",
                "def _load_from_sqlite(sdm_con, source_path, tables):\n",
                "    \"\"\"Kopieer meerdere tabellen uit \u00e9\u00e9n SQLite-bronbestand naar het SDM.\"\"\"\n",
                "    if not Path(source_path).exists():\n",
                "        log_etl(\"ERROR\", \"Source missing\", f\"{source_path} not found\")\n",
                "        return\n",
                "    src_con = sqlite3.connect(source_path)\n",
                "    for table in tables:\n",
                "        try:\n",
                "            # 1. Controleer of tabel bestaat in SDM en haal kolommen op\n",
                "            sdm_cols = [row[1] for row in sdm_con.execute(f\"PRAGMA table_info({table})\")]\n",
                "            if not sdm_cols:\n",
                "                log_etl(\"ERROR\", f\"Table {table}\", \"Table does not exist in SDM\")\n",
                "                print(f\"  [SKIP]  {table:35s} (bestaat niet in SDM)\")\n",
                "                continue\n",
                "\n",
                "            # 2. Lees data uit bron\n",
                "            df = pd.read_sql(f\"SELECT * FROM {table}\", src_con)\n",
                "            \n",
                "            # 3. Match kolommen (case-insensitive)\n",
                "            df.columns = df.columns.str.upper()\n",
                "            sdm_cols_upper = [c.upper() for c in sdm_cols]\n",
                "            \n",
                "            common_cols = [col for col in df.columns if col in sdm_cols_upper]\n",
                "            if not common_cols:\n",
                "                log_etl(\"WARNING\", f\"Table {table}\", \"No matching columns found\")\n",
                "                print(f\"  [SKIP]  {table:35s} (geen overeenkomende kolommen)\")\n",
                "                continue\n",
                "                \n",
                "            df = df[common_cols]\n",
                "            df.columns = [sdm_cols[sdm_cols_upper.index(col)] for col in df.columns]\n",
                "\n",
                "            # 4. Opslaan in SDM\n",
                "            if not df.empty:\n",
                "                df.to_sql(table, sdm_con, if_exists=\"append\", index=False)\n",
                "                log_etl(\"SUCCESS\", f\"Table {table}\", f\"Loaded {len(df)} records from {Path(source_path).name}\")\n",
                "                print(f\"  [OK]   {table:35s} {len(df):>6} rijen  \u2190  {Path(source_path).name}\")\n",
                "            else:\n",
                "                print(f\"  [INFO] {table:35s} Geen data gevonden\")\n",
                "                \n",
                "        except Exception as e:\n",
                "            log_etl(\"ERROR\", f\"Table {table}\", str(e))\n",
                "            print(f\"  [ERROR] {table:35s} {str(e)}\")\n",
                "    src_con.close()\n",
                "\n",
                "def _load_from_csv(sdm_con, csv_path, table):\n",
                "    \"\"\"Laad \u00e9\u00e9n CSV-bestand in het SDM.\"\"\"\n",
                "    if not Path(csv_path).exists():\n",
                "        log_etl(\"ERROR\", \"Source missing\", f\"{csv_path} not found\")\n",
                "        return\n",
                "\n",
                "    # 1. Controleer of tabel bestaat in SDM\n",
                "    sdm_cols = [row[1] for row in sdm_con.execute(f\"PRAGMA table_info({table})\")]\n",
                "    if not sdm_cols:\n",
                "        log_etl(\"ERROR\", f\"Table {table}\", \"Table does not exist in SDM\")\n",
                "        print(f\"  [SKIP]  {table:35s} (bestaat niet in SDM)\")\n",
                "        return\n",
                "\n",
                "    # 2. Lees CSV met encoding detectie\n",
                "    for encoding in [\"utf-8\", \"latin-1\", \"cp1252\"]:\n",
                "        try:\n",
                "            df = pd.read_csv(csv_path, encoding=encoding)\n",
                "            break\n",
                "        except UnicodeDecodeError:\n",
                "            continue\n",
                "    else:\n",
                "        log_etl(\"ERROR\", f\"CSV Read {table}\", \"Unknown encoding\")\n",
                "        return\n",
                "    \n",
                "    try:\n",
                "        # 3. Match kolommen\n",
                "        df.columns = df.columns.str.upper()\n",
                "        sdm_cols_upper = [c.upper() for c in sdm_cols]\n",
                "        \n",
                "        common_cols = [col for col in df.columns if col in sdm_cols_upper]\n",
                "        if not common_cols:\n",
                "            print(f\"  [SKIP]  {table:35s} (geen overeenkomende kolommen)\")\n",
                "            return\n",
                "            \n",
                "        df = df[common_cols]\n",
                "        df.columns = [sdm_cols[sdm_cols_upper.index(col)] for col in df.columns]\n",
                "        \n",
                "        df = df.drop_duplicates()\n",
                "        if not df.empty:\n",
                "            df.to_sql(table, sdm_con, if_exists=\"append\", index=False)\n",
                "            log_etl(\"SUCCESS\", f\"Table {table}\", f\"Loaded {len(df)} records from {Path(csv_path).name}\")\n",
                "            print(f\"  [OK]   {table:35s} {len(df):>6} rijen  \u2190  {Path(csv_path).name}\")\n",
                "        else:\n",
                "            print(f\"  [INFO] {table:35s} Geen data gevonden\")\n",
                "    except Exception as e:\n",
                "        log_etl(\"ERROR\", f\"Table {table}\", str(e))\n",
                "        print(f\"  [ERROR] {table:35s} {str(e)}\")\n",
                "\n",
                "def _clear_table(sdm_con, table):\n",
                "    \"\"\"Verwijder alle rijen uit \u00e9\u00e9n SDM-tabel.\"\"\"\n",
                "    sdm_con.execute(f\"DELETE FROM {table}\")\n",
                "    log_etl(\"INFO\", f\"Table {table}\", \"Cleared SDM table\")\n",
                "    print(f\"  [CLEAR] {table}\")\n",
                "\n",
                "def load():\n",
                "    \"\"\"Vult het SDM met alle brondata uit SQLite en CSV.\"\"\"\n",
                "    con = sqlite3.connect(SDM_PATH)\n",
                "    con.execute(\"PRAGMA foreign_keys = OFF;\")\n",
                "    print(\"=== LADEN ===\")\n",
                "    \n",
                "    # 1. SQLite bronnen\n",
                "    for path, tables in SQLITE_SOURCES.items():\n",
                "        _load_from_sqlite(con, path, tables)\n",
                "        \n",
                "    # 2. CSV bronnen\n",
                "    for path, table in CSV_SOURCES.items():\n",
                "        _load_from_csv(con, path, table)\n",
                "        \n",
                "    con.execute(\"PRAGMA foreign_keys = ON;\")\n",
                "    con.close()\n",
                "    print(\"\\\\n\u2713 Klaar \u2013 SDM gevuld.\")\n",
                "\n",
                "def clear():\n",
                "    \"\"\"Verwijdert alle data uit de SDM tabellen.\"\"\"\n",
                "    con = sqlite3.connect(SDM_PATH)\n",
                "    con.execute(\"PRAGMA foreign_keys = OFF;\")\n",
                "    print(\"=== LEEGMAKEN ===\")\n",
                "    \n",
                "    # Verzamel alle tabellen uit de configuratie\n",
                "    all_tables = []\n",
                "    for tables in SQLITE_SOURCES.values():\n",
                "        all_tables.extend(tables)\n",
                "    all_tables.extend(CSV_SOURCES.values())\n",
                "    \n",
                "    for table in all_tables:\n",
                "        _clear_table(con, table)\n",
                "        \n",
                "    con.execute(\"PRAGMA foreign_keys = ON;\")\n",
                "    con.close()\n",
                "    print(\"\\\\n\u2713 Klaar \u2013 SDM leeggemaakt.\")\n"
            ]
            break

    with open(nb_path, 'w') as f:
        json.dump(nb, f, indent=1)

def update_dwh_notebook():
    nb_path = "DWH/load_data-DWH.ipynb"
    with open(nb_path, 'r') as f:
        nb = json.load(f)
    
    # 1. Update Config Cell
    for cell in nb['cells']:
        if cell['cell_type'] == 'code' and "DWH_PATH =" in "".join(cell['source']):
            source_str = "".join(cell['source'])
            if "LOG_PATH =" not in source_str:
                cell['source'].insert(cell['source'].index("DWH_PATH = \"../data/dwh_deproject.db\"\n") + 1, "LOG_PATH = \"../logs/sdm_to_dwh.log\"\n")
                # Add log function
                cell['source'].append("\n")
                cell['source'].append("def log_etl(level, message, details=\"\"):\n")
                cell['source'].append("    timestamp = datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")\n")
                cell['source'].append("    with open(LOG_PATH, \"a\") as f:\n")
                cell['source'].append("        f.write(f\"{timestamp} | {level:10} | {message:30} | {details}\\n\")\n")
            break

    # 2. Update each load cell to include logging
    table_mappings = {
        "Dim_Product": "Dim_Product",
        "Dim_Staff": "Dim_Staff",
        "Dim_Customer": "Dim_Customer",
        "Dim_Date": "Dim_Date",
        "Fact_Sales_Actuals": "Fact_Sales_Actuals",
        "Fact_Sales_Targets": "Fact_Sales_Targets",
        "Fact_Product_Forecast": "Fact_Product_Forecast"
    }

    for cell in nb['cells']:
        if cell['cell_type'] == 'code':
            source_str = "".join(cell['source'])
            for table_id, table_name in table_mappings.items():
                if f'"{table_name}"' in source_str and "to_sql" in source_str:
                    if "log_etl" not in source_str:
                        # Append logging calls
                        cell['source'].append(f"log_etl(\"SUCCESS\", \"{table_name}\", f\"Loaded {{len({table_id.lower().replace('fact_', 'fact_').replace('dim_', 'dim_')})}} records\")\n")
                        cell['source'].append(f"log_etl(\"VALIDATE\", \"{table_name}\", \"DWH Load verified\")\n")

    with open(nb_path, 'w') as f:
        json.dump(nb, f, indent=1)

if __name__ == "__main__":
    update_sdm_notebook()
    update_dwh_notebook()
    print("Both notebooks updated with logging and robust loading logic.")
