import duckdb
import os

# 1. Setup paths
INPUT_FILE = "data/raw/pp-complete.csv"
OUTPUT_DIR = "data/parts"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"Created directory: {OUTPUT_DIR}")

print("Partitioning big CSV into yearly files... This might take a minute.")

# 2. Use DuckDB to read the big file and save it as multiple small CSVs partitioned by year
con = duckdb.connect()
con.execute(f"""
    COPY (
        SELECT 
            *, 
            strftime(CAST(column02 AS DATE), '%Y') as sale_year 
        FROM read_csv_auto('{INPUT_FILE}', all_varchar=True)
    ) 
    TO '{OUTPUT_DIR}' 
    (FORMAT CSV, PARTITION_BY (sale_year), OVERWRITE_OR_IGNORE 1);
""")

print(f"Success! Your data is now split into yearly folders in {OUTPUT_DIR}")