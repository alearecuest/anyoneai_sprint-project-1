-- TODO: 
-- This query will return a table with the revenue by month and year. 
-- It will have different columns: 
--      month_no, with the month numbers going from 01 to 12; 
--      month, with the 3 first letters of each month (e.g. Jan, Feb); 
--      Year2016, with the revenue per month of 2016 (0.00 if it doesn't exist); 
--      Year2017, with the revenue per month of 2017 (0.00 if it doesn't exist) and 
--      Year2018, with the revenue per month of 2018 (0.00 if it doesn't exist).

WITH delivered_orders AS (
    SELECT DISTINCT
        o.order_id,
        STRFTIME('%m', o.order_delivered_customer_date) AS month_no,
        CASE STRFTIME('%m', o.order_delivered_customer_date)
            WHEN '01' THEN 'Jan'
            WHEN '02' THEN 'Feb'
            WHEN '03' THEN 'Mar'
            WHEN '04' THEN 'Apr'
            WHEN '05' THEN 'May'
            WHEN '06' THEN 'Jun'
            WHEN '07' THEN 'Jul'
            WHEN '08' THEN 'Aug'
            WHEN '09' THEN 'Sep'
            WHEN '10' THEN 'Oct'
            WHEN '11' THEN 'Nov'
            WHEN '12' THEN 'Dec'
        END AS month,
        STRFTIME('%Y', o.order_delivered_customer_date) AS year
    FROM olist_orders o
    WHERE o.order_status = 'delivered' 
        AND o.order_delivered_customer_date IS NOT NULL
),
min_payment_per_order AS (
    SELECT 
        p.order_id,
        MIN(p.payment_value) AS min_payment
    FROM olist_order_payments p
    GROUP BY p.order_id
)
SELECT 
    d.month_no,
    d.month,
    COALESCE(ROUND(SUM(CASE WHEN d.year = '2016' THEN m.min_payment END), 2), 0.0) AS Year2016,
    COALESCE(ROUND(SUM(CASE WHEN d.year = '2017' THEN m.min_payment END), 2), 0.0) AS Year2017,
    COALESCE(ROUND(SUM(CASE WHEN d.year = '2018' THEN m.min_payment END), 2), 0.0) AS Year2018
FROM delivered_orders d
INNER JOIN min_payment_per_order m ON d.order_id = m.order_id
GROUP BY d.month_no, d.month
ORDER BY d.month_no;