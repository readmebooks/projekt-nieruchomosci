
  
    
    

    create  table
      "nieruchomosci_uk"."main"."stg_uk_sales__dbt_tmp"
  
    as (
      

/* Staging model for UK property sales data.
  Loads partitioned CSV files from the local data directory.
*/

SELECT 
    column00 as id,
    CAST(column01 AS INTEGER) as price,
    CAST(substring(column02, 1, 10) AS DATE) as sale_date,
    column11 as city,
    column13 as county,
    sale_year -- this column is automatically added by DuckDB from the folder structure
FROM read_csv_auto('../data/parts/*/*.csv', all_varchar=True)
WHERE CAST(column01 AS INTEGER) > 0
    );
  
  