CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET}_aggregated_table 
AS 

WITH orders AS ( 
SELECT
  orders.*, 
  customers.customer_state, 
  order_items.price AS product_price,
  products.product_category_name,
FROM `chinwee-datatalkcamp.{BIGQUERY_DATASET}.olist_orders` orders
LEFT JOIN `chinwee-datatalkcamp.{BIGQUERY_DATASET}.olist_customers` customers
       ON orders.customer_id = customers.customer_id
LEFT JOIN `chinwee-datatalkcamp.{BIGQUERY_DATASET}.olist_order_items` order_items
       ON orders.order_id = order_items.order_id
LEFT JOIN `chinwee-datatalkcamp.{BIGQUERY_DATASET}.olist_products` products
       ON order_items.product_id = products.product_id
)

SELECT
  DATE_TRUNC(date(order_purchase_timestamp), MONTH) AS DATE_MONTH,
  customer_state,
  product_category_name,
  COUNT(DISTINCT order_id) AS order_count,
  SUM(product_price) AS order_value,
FROM orders
GROUP BY 1,2,3
ORDER BY 1,2,3;