

/* Gold layer model: Aggregating average property prices per city.
  This view is optimized for reporting and data visualization.
*/

SELECT 
    city,
    sale_year,
    round(avg(price), 2) as average_price,
    count(*) as total_transactions
FROM "nieruchomosci_uk"."main"."stg_uk_sales"
GROUP BY city, sale_year
ORDER BY sale_year DESC, average_price DESC