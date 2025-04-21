   
  -- % diff in txn amounts per sellers, sequentially
  WITH monthly_sales AS (
    SELECT seller,
		DATE_FORMAT(
            STR_TO_DATE(
                SUBSTRING_INDEX(saledate, 'GMT', 1),
                '%a %b %d %Y %H:%i:%s '
            ),
            '%Y-%m'
        ) AS month,
        SUM(sellingprice) AS total_sales
    FROM car_prices
    WHERE saledate IS NOT NULL
    GROUP BY
        seller,  DATE_FORMAT(
            STR_TO_DATE(
                SUBSTRING_INDEX(saledate, 'GMT', 1),
                '%a %b %d %Y %H:%i:%s '
            ),
            '%Y-%m')
),
sales_with_lag AS (
    SELECT seller, month, total_sales,
        LAG(total_sales) OVER (PARTITION BY seller ORDER BY month) AS prev_month_sales
    FROM monthly_sales
) 
SELECT
    seller, month, total_sales, prev_month_sales,
    ROUND(
        CASE 
            WHEN prev_month_sales = 0 OR prev_month_sales IS NULL THEN NULL
            ELSE ((total_sales - prev_month_sales) / prev_month_sales) * 100
        END, 
    2) AS percent_change
FROM sales_with_lag
ORDER BY seller, month;
  

-- monthly inflow per sellers
 SELECT 
 DATE_FORMAT(
            STR_TO_DATE(
                SUBSTRING_INDEX(saledate, 'GMT', 1),
                '%a %b %d %Y %H:%i:%s '
            ),
            '%Y-%m'
        ) AS month, seller, sum(sellingprice)
 FROM car_prices
 GROUP BY DATE_FORMAT(
            STR_TO_DATE(
                SUBSTRING_INDEX(saledate, 'GMT', 1),
                '%a %b %d %Y %H:%i:%s '
            ),
            '%Y-%m'), 
            seller
 ORDER BY sum(sellingprice);
 
 -- car price segmentation for market analysis
    SELECT sellingprice, NTILE(3) OVER (ORDER BY sellingprice) quartile,
    CASE WHEN NTILE(3) OVER (ORDER BY sellingprice) = 1 THEN 'lowpoint'
		WHEN NTILE(3) OVER (ORDER BY sellingprice) = 2 THEN 'midpoint'
        WHEN NTILE(3) OVER (ORDER BY sellingprice) = 3 THEN 'highpoint'
	END AS car_segments
    FROM car_prices;
    
    
-- self join that produces the % change in monthly sales per seller
SELECT
    curr.seller,
    curr.month AS current_month,
    prev.month AS previous_month,
    curr.total_sales AS current_sales,
    prev.total_sales AS previous_sales,
    ROUND(-- calculates % monthly change
        CASE
            WHEN prev.total_sales IS NULL OR prev.total_sales = 0 THEN NULL
            ELSE ((curr.total_sales - prev.total_sales) / prev.total_sales) * 100
        END,
        2
    ) AS percent_change
FROM (
    SELECT
        seller,
        DATE_FORMAT(STR_TO_DATE(SUBSTRING_INDEX(saledate, 'GMT', 1), '%a %b %d %Y %H:%i:%s '), '%Y-%m-01') AS month,
        SUM(sellingprice) AS total_sales
    FROM car_prices
    WHERE saledate IS NOT NULL
    GROUP BY
        seller, DATE_FORMAT(STR_TO_DATE(SUBSTRING_INDEX(saledate, 'GMT', 1), '%a %b %d %Y %H:%i:%s '), '%Y-%m-01')
) AS curr
LEFT JOIN (
    SELECT
        seller,
        DATE_FORMAT(STR_TO_DATE(SUBSTRING_INDEX(saledate, 'GMT', 1), '%a %b %d %Y %H:%i:%s '), '%Y-%m-01') AS month,
        SUM(sellingprice) AS total_sales
    FROM car_prices
    WHERE saledate IS NOT NULL
    GROUP BY
        seller, DATE_FORMAT(STR_TO_DATE(SUBSTRING_INDEX(saledate, 'GMT', 1), '%a %b %d %Y %H:%i:%s '), '%Y-%m-01')
) AS prev
ON curr.seller = prev.seller
   AND DATE_SUB(curr.month, INTERVAL 1 MONTH) = prev.month -- joins each current-month record to the same seller's previous month, by subtracting one month from curr.month
ORDER BY curr.seller, curr.month;

