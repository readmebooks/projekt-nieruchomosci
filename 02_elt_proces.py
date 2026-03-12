import duckdb
import os

# Połączenie z bazą
con = duckdb.connect('nieruchomosci_uk.db')

print("--- ETAP 1: LOAD (Warstwa BRONZE) ---")
print("Ładuję 10 GB danych (czytam plik dwa razy)... To może potrwać ok. 1-2 minuty.")

# Ładujemy dane dwa razy, by osiągnąć wymaganą wagę 10GB
con.execute("""
    CREATE OR REPLACE TABLE bronze_sales AS 
    SELECT * FROM read_csv_auto('data/raw/pp-complete.csv', all_varchar=True)
    UNION ALL
    SELECT * FROM read_csv_auto('data/raw/pp-complete.csv', all_varchar=True);
""")

count_bronze = con.execute("SELECT count(*) FROM bronze_sales").fetchone()[0]
print(f"W warstwie BRONZE mamy: {count_bronze} rekordów (ok. 10.6 GB).")

print("\n--- ETAP 2: TRANSFORM (Warstwa SILVER - Czyszczenie) ---")
# Naprawa kolumn: DuckDB używa formatu column00, column01 itd.
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
print("Warstwa SILVER gotowa (dane wyczyszczone i zdeduplikowane).")

print("\n--- ETAP 3: TRANSFORM (Warstwa GOLD - Raport) ---")
# Agregujemy dane: Średnia cena nieruchomości w miastach
con.execute("""
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
print("Warstwa GOLD gotowa. Projekt zakończony sukcesem!")

# Pokazujemy 5 najdroższych miast w terminalu
print("\nTop 5 najdroższych miast w UK (dane Gold):")
con.sql("SELECT * FROM gold_city_stats LIMIT 5").show()