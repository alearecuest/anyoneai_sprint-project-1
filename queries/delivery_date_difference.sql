-- TODO: 
-- This query will return a table with two columns: State and Delivery_Difference. 
-- The first one will have the letters that identify the states, 
-- and the second one the average difference between the estimated delivery date 
-- and the date when the items were actually delivered to the customer.

SELECT 
    c.customer_state AS State,
    CAST(AVG(
        JULIANDAY(STRFTIME('%Y-%m-%d', o.order_estimated_delivery_date)) - 
        JULIANDAY(STRFTIME('%Y-%m-%d', o.order_delivered_customer_date))
    ) AS INTEGER) AS Delivery_Difference
FROM olist_orders o
JOIN olist_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered' 
    AND o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY Delivery_Difference ASC, State ASC;