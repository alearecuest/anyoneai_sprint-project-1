-- TODO: 
-- This query will return a table with the differences between the real 
-- and estimated delivery times by month and year. 
-- It will have different columns: 
--      month_no, with the month numbers going FROM 01 to 12; 
--      month, with the 3 first letters of each month (e.g. Jan, Feb); 
--      Year2016_real_time, with the average delivery time per month of 2016 (NaN if it doesn't exist); 
--      Year2017_real_time, with the average delivery time per month of 2017 (NaN if it doesn't exist); 
--      Year2018_real_time, with the average delivery time per month of 2018 (NaN if it doesn't exist); 
--      Year2016_estimated_time, with the average estimated delivery time per month of 2016 (NaN if it doesn't exist); 
--      Year2017_estimated_time, with the average estimated delivery time per month of 2017 (NaN if it doesn't exist) and 
--      Year2018_estimated_time, with the average estimated delivery time per month of 2018 (NaN if it doesn't exist).

WITH delivery_times AS (
    SELECT DISTINCT
        o.order_id,
        STRFTIME('%m', o.order_purchase_timestamp) AS month_no,
        CASE STRFTIME('%m', o.order_purchase_timestamp)
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
        STRFTIME('%Y', o.order_purchase_timestamp) AS year,
        JULIANDAY(o.order_delivered_customer_date) - JULIANDAY(o.order_purchase_timestamp) AS real_time,
        JULIANDAY(o.order_estimated_delivery_date) - JULIANDAY(o.order_purchase_timestamp) AS estimated_time
    FROM olist_orders o
    WHERE o.order_status = 'delivered' 
        AND o.order_delivered_customer_date IS NOT NULL
)
SELECT 
    month_no,
    month,
    AVG(CASE WHEN year = '2016' THEN real_time END) AS Year2016_real_time,
    AVG(CASE WHEN year = '2017' THEN real_time END) AS Year2017_real_time,
    AVG(CASE WHEN year = '2018' THEN real_time END) AS Year2018_real_time,
    AVG(CASE WHEN year = '2016' THEN estimated_time END) AS Year2016_estimated_time,
    AVG(CASE WHEN year = '2017' THEN estimated_time END) AS Year2017_estimated_time,
    AVG(CASE WHEN year = '2018' THEN estimated_time END) AS Year2018_estimated_time
FROM delivery_times
GROUP BY month_no, month
ORDER BY month_no;