import duckdb

# Establish a read-only connection for database inspection
con = duckdb.connect('../nieruchomosci_uk.db', read_only=True)

print("--- DATABASE SCHEMA: AVAILABLE TABLES ---")
con.sql("SHOW TABLES").show()

print("\n--- BRONZE LAYER PREVIEW (RAW INGESTED DATA) ---")
con.sql("SELECT * FROM bronze_sales LIMIT 5").show()

print("\n--- SILVER LAYER PREVIEW (CLEANED & REFINED DATA) ---")
con.sql("SELECT * FROM silver_sales LIMIT 5").show()

print("\n--- GOLD LAYER PREVIEW (FINAL ANALYTICAL REPORT) ---")
con.sql("SELECT * FROM gold_city_stats LIMIT 5").show()

con.close()