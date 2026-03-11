-- TODO:
-- This query will return a table with the top 10 revenue categories
-- in English, the number of orders and their total revenue.

SELECT 
    t.product_category_name_english AS Category,
    COUNT(DISTINCT o.order_id) AS Num_order,
    ROUND(SUM(p.payment_value), 2) AS Revenue
FROM olist_orders o
JOIN olist_order_items oi ON o.order_id = oi.order_id
JOIN olist_products pr ON oi.product_id = pr.product_id
JOIN product_category_name_translation t ON pr.product_category_name = t.product_category_name
JOIN olist_order_payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered' 
    AND o.order_delivered_customer_date IS NOT NULL
    AND t.product_category_name_english IS NOT NULL
GROUP BY t.product_category_name_english
ORDER BY Revenue DESC
LIMIT 10;