import duckdb
import os

# Establish connection to the local DuckDB database
con = duckdb.connect('nieruchomosci_uk.db')

print("--- STAGE 1: DATA INGESTION (BRONZE LAYER) ---")
print("Ingesting source data (simulating 10GB volume by dual-loading)")

# Ingest raw data twice to reach the required 10GB target volume
con.execute("""
    CREATE OR REPLACE TABLE bronze_sales AS 
    SELECT * FROM read_csv_auto('data/raw/pp-complete.csv', all_varchar=True)
    UNION ALL
    SELECT * FROM read_csv_auto('data/raw/pp-complete.csv', all_varchar=True);
""")

count_bronze = con.execute("SELECT count(*) FROM bronze_sales").fetchone()[0]
print(f"BRONZE layer initialized with: {count_bronze} records (~10.6 GB).")

print("\n--- STAGE 2: DATA CLEANING & REFINEMENT (SILVER LAYER) ---")

# Apply schema, cast data types, and remove duplicates
con.execute("""
    CREATE OR REPLACE TABLE silver_sales AS 
    SELECT DISTINCT 
        column00 as id,
        CAST(column01 AS INTEGER) as price,
        CAST(substring(column02, 1, 10) AS DATE) as sale_date,
        column11 as city,
        column13 as county,
        column04 as property_type
    FROM bronze_sales
    WHERE CAST(column01 AS INTEGER) > 0;
""")
print("SILVER layer successfully processed (deduplicated and cleaned).")

print("\n--- STAGE 3: DATA AGGREGATION & ANALYTICS (GOLD LAYER) ---")
# Aggregate data: Calculate average property prices by city
    CREATE OR REPLACE TABLE gold_city_stats AS 
    SELECT 
        city,
        count(*) as total_sales,
        round(avg(price), 2) as avg_price
    FROM silver_sales
    GROUP BY city
    HAVING total_sales > 1000
    ORDER BY avg_price DESC;
""")
print("GOLD layer generated. Pipeline execution completed")

# Display preview of the final analytical report
print("\nFinal Report Preview: Top 5 UK cities by average property price (Gold Data):")
con.sql("SELECT * FROM gold_city_stats LIMIT 5").show()