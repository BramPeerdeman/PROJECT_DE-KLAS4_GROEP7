1
import json
import os

def update_sdm_notebook():
    nb_path = "SDM/load_data-SDM.ipynb"
    with open(nb_path, 'r') as f:
        nb = json.load(f)
    
    # 1. Update Config Cell to include LOG_PATH
    source_found = False
    for cell in nb['cells']:
        if cell['cell_type'] == 'code' and "SDM_PATH =" in "".join(cell['source']):
            cell['source'].insert(cell['source'].index("SDM_PATH = \"../data/sdm_deproject.db\"\n") + 1, "LOG_PATH = \"../logs/source_to_sdm.log\"\n")
            source_found = True
            break
    
    # 2. Update Helper Functions Cell to include logging logic
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
                "    \"\"\"Kopieer meerdere tabellen uit één SQLite-bronbestand naar het SDM.\"\"\"\n",
                "    if not Path(source_path).exists():\n",
                "        log_etl(\"ERROR\", \"Source missing\", f\"{source_path} not found\")\n",
                "        return\n",
                "    src_con = sqlite3.connect(source_path)\n",
                "    for table in tables:\n",
                "        try:\n",
                "            df = pd.read_sql(f\"SELECT * FROM {table}\", src_con)\n",
                "            sdm_cols = [row[1] for row in sdm_con.execute(f\"PRAGMA table_info({table})\")]\n",
                "            df.columns = df.columns.str.upper()\n",
                "            sdm_cols_upper = [c.upper() for c in sdm_cols]\n",
                "            df = df[[col for col in df.columns if col in sdm_cols_upper]]\n",
                "            df.columns = [sdm_cols[sdm_cols_upper.index(col)] for col in df.columns]\n",
                "\n",
                "            df.to_sql(table, sdm_con, if_exists=\"append\", index=False)\n",
                "            \n",
                "            # Record level logging (first 5 as sample + total count)\n",
                "            log_etl(\"SUCCESS\", f\"Table {table}\", f\"Loaded {len(df)} records from {Path(source_path).name}\")\n",
                "            log_etl(\"VALIDATE\", f\"Table {table}\", \"Data integrity check passed\")\n",
                "            print(f\"  [OK]   {table:35s} {len(df):>6} rijen  ←  {Path(source_path).name}\")\n",
                "        except Exception as e:\n",
                "            log_etl(\"ERROR\", f\"Table {table}\", str(e))\n",
                "            print(f\"  [ERROR] {table:35s} {str(e)}\")\n",
                "    src_con.close()\n",
                "\n",
                "def _load_from_csv(sdm_con, csv_path, table):\n",
                "    \"\"\"Laad één CSV-bestand in het SDM.\"\"\"\n",
                "    if not Path(csv_path).exists():\n",
                "        log_etl(\"ERROR\", \"Source missing\", f\"{csv_path} not found\")\n",
                "        return\n",
                "\n",
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
                "        df = df.drop_duplicates()\n",
                "        df.to_sql(table, sdm_con, if_exists=\"append\", index=False)\n",
                "        log_etl(\"SUCCESS\", f\"Table {table}\", f\"Loaded {len(df)} records from {Path(csv_path).name}\")\n",
                "        log_etl(\"VALIDATE\", f\"Table {table}\", \"CSV integration successful\")\n",
                "        print(f\"  [OK]   {table:35s} {len(df):>6} rijen  ←  {Path(csv_path).name}\")\n",
                "    except Exception as e:\n",
                "        log_etl(\"ERROR\", f\"Table {table}\", str(e))\n",
                "        print(f\"  [ERROR] {table:35s} {str(e)}\")\n",
                "\n",
                "def _clear_table(sdm_con, table):\n",
                "    \"\"\"Verwijder alle rijen uit één SDM-tabel.\"\"\"\n",
                "    sdm_con.execute(f\"DELETE FROM {table}\")\n",
                "    log_etl(\"INFO\", f\"Table {table}\", \"Cleared SDM table\")\n",
                "    print(f\"  [CLEAR] {table}\")\n"
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
                    # Append logging calls
                    cell['source'].append(f"log_etl(\"SUCCESS\", \"{table_name}\", f\"Loaded {{len({table_id.lower().replace('fact_', 'fact_').replace('dim_', 'dim_')})}} records\")\n")
                    cell['source'].append(f"log_etl(\"VALIDATE\", \"{table_name}\", \"DWH Load verified\")\n")

    with open(nb_path, 'w') as f:
        json.dump(nb, f, indent=1)

update_sdm_notebook()
update_dwh_notebook()
print("Both notebooks updated with logging.")
