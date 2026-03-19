import duckdb

con = duckdb.connect('nieruchomosci_uk.db', read_only=True)

print("--- LISTA TABEL W BAZIE ---")
con.sql("SHOW TABLES").show()

print("\n--- PODGLĄD WARSTWY BRONZE (RAW) ---")
con.sql("SELECT * FROM bronze_sales LIMIT 5").show()

print("\n--- PODGLĄD WARSTWY SILVER (CLEANED) ---")
con.sql("SELECT * FROM silver_sales LIMIT 5").show()

print("\n--- PODGLĄD WARSTWY GOLD (ANALYTICS - RAPORT) ---")
con.sql("SELECT * FROM gold_city_stats LIMIT 5").show()

con.close()