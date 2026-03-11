-- TODO: 
-- This query will return a table with two columns: order_status and Amount. 
-- The first one will have the different order status classes 
-- and the second one the total amount of each.

SELECT 
    order_status,
    COUNT(*) AS Amount
FROM olist_orders
GROUP BY order_status
ORDER BY order_status;