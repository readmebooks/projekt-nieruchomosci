import duckdb

# Establish a connection to the local DuckDB database
# Using read_only=True to prevent accidental data modification during inspection
con = duckdb.connect('../nieruchomosci_uk.db', read_only=True)

print("--- DBT MODELS: FINAL REFINED TABLES ---")
# This shows all tables currently managed by dbt and Dagster
con.sql("SHOW TABLES").show()

print("\n--- SILVER LAYER (stg_uk_sales) ---")
# Verifying the output of the first dbt transformation
con.sql("SELECT * FROM stg_uk_sales LIMIT 5").show()

print("\n--- GOLD LAYER (avg_price_by_city) ---")
# Final analytical report showing aggregated results
con.sql("SELECT * FROM avg_price_by_city LIMIT 10").show()

con.close()