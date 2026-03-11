-- TODO: 
-- This query will return a table with two columns: customer_state and Revenue. 
-- The first one will have the letters that identify the top 10 states 
-- with most revenue and the second one the total revenue of each.

SELECT 
    c.customer_state,
    ROUND(SUM(p.payment_value), 2) AS Revenue
FROM olist_orders o
JOIN olist_customers c ON o.customer_id = c.customer_id
JOIN olist_order_payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered' 
    AND o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY Revenue DESC
LIMIT 10;