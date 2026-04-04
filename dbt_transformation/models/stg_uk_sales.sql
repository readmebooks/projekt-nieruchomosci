{{ config(materialized='table') }}

SELECT 
    column00 as id,
    CAST(column01 AS INTEGER) as price,
    CAST(substring(column02, 1, 10) AS DATE) as sale_date,
    column11 as city,
    column13 as county,
    sale_year -- to jest nasza partycja z folderu!
FROM read_csv_auto('data/parts/*/*.csv', all_varchar=True)
WHERE CAST(column01 AS INTEGER) > 0