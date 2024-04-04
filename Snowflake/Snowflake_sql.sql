-- 1.	How many different products does each store sell per day? 

-- 2.	How many different stores does each customer visit by store? 

-- 3.	On which days are the most products sold, separated by store? The least amount of products? 


--- 1

SELECT s.store_id,
       d.date_day,
       COUNT(DISTINCT p.product_id) AS num_products_sold
FROM DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_fact_order AS o
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_orderline AS ol ON o.order_number = ol.order_number
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_customer AS c ON o.customer_key = c.customer_key
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_employee AS e ON o.employee_key = e.employee_key
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_date AS d ON o.date_key = d.date_day
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_store AS s ON o.store_key = s.store_key
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_product AS p ON ol.product_key = p.product_key
GROUP BY s.store_id,
         d.date_day;

--- 2

SELECT c.customer_pk,
       COUNT(DISTINCT s.store_id) AS num_stores_visited
FROM DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_fact_order AS o
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_customer AS c ON o.customer_key = c.customer_key
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_store AS s ON o.store_key = s.store_key
GROUP BY c.customer_pk;

--- 3

-- Query to find the days with the most products sold by store
SELECT s.store_id,
       d.date_day              AS most_products_sold_day,
       COUNT(ol.order_line_id) AS num_products_sold
FROM DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_fact_order AS o
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_orderline AS ol ON o.order_number = ol.order_number
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_date AS d ON o.date_key = d.date_day
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_store AS s ON o.store_key = s.store_key
GROUP BY s.store_id,
         d.date_day
HAVING COUNT(ol.order_line_id) = (SELECT MAX(product_count)
                                  FROM (SELECT s.store_id,
                                               d.date_day,
                                               COUNT(ol.order_line_id) AS product_count
                                        FROM DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_fact_order AS o
                                                 LEFT JOIN
                                             DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_orderline AS ol
                                             ON o.order_number = ol.order_number
                                                 LEFT JOIN
                                             DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_date AS d ON o.date_key = d.date_day
                                                 LEFT JOIN
                                             DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_store AS s
                                             ON o.store_key = s.store_key
                                        GROUP BY s.store_id,
                                                 d.date_day) AS counts_per_day
                                  WHERE counts_per_day.store_id = s.store_id);


-- Query to find the days with the least products sold by store

SELECT s.store_id,
       d.date_day              AS least_products_sold_day,
       COUNT(ol.order_line_id) AS num_products_sold
FROM DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_fact_order AS o
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_orderline AS ol ON o.order_number = ol.order_number
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_date AS d ON o.date_key = d.date_day
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_store AS s ON o.store_key = s.store_key
GROUP BY s.store_id,
         d.date_day
HAVING COUNT(ol.order_line_id) = (SELECT MIN(product_count)
                                  FROM (SELECT s.store_id,
                                               d.date_day,
                                               COUNT(ol.order_line_id) AS product_count
                                        FROM DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_fact_order AS o
                                                 LEFT JOIN
                                             DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_orderline AS ol
                                             ON o.order_number = ol.order_number
                                                 LEFT JOIN
                                             DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_date AS d ON o.date_key = d.date_day
                                                 LEFT JOIN
                                             DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_store AS s
                                             ON o.store_key = s.store_key
                                        GROUP BY s.store_id,
                                                 d.date_day) AS counts_per_day
                                  WHERE counts_per_day.store_id = s.store_id);




Data Validation - Check that your data is being transformed correctly with dbt.  (Extra credit?) 

Lessons learned - 

Lesson #1 - In the event you are missing a comma in your select statement dbt will do one of three things.
1.	Transform the two columns into a hash value
2.	Omit one of the first column from the end table
3.	Give you an error

Lesson #2 - In the event you change your database in the profiles.yml file, dbt will still use your old database setting until you clear your cache.  Now sometimes dbt will decide to start using your new one and other times your existing one.   Makes sense right?

--- Validation Airbyte vs DBT
SELECT o.order_number,
       ol.order_line_id,
       d.date_day,
       c.customer_pk,
       e.employee_id,
       s.store_id,
       p.product_id,
       ol.order_line_qty,
       ol.order_line_price,
       o.order_total_price,
       o.order_points_earned
FROM DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_fact_order AS o
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_orderline AS ol ON o.order_number = ol.order_number
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_customer AS c ON o.customer_key = c.customer_key
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_employee AS e ON o.employee_key = e.employee_key
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_date AS d ON o.date_key = d.date_day
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_store AS s ON o.store_key = s.store_key
         LEFT JOIN
     DBT_TEST_DW_SAMSSUBSANDWHICHS.ss_dim_product AS p ON ol.product_key = p.product_key
WHERE o.order_number = 1
GROUP BY o.order_number,
         ol.order_line_id,
         d.date_day,
         c.customer_pk,
         e.employee_id,
         s.store_id,
         p.product_id,
         ol.order_line_qty,
         ol.order_line_price,
         o.order_total_price,
         o.order_points_earned;

-- Different SCHEMA SAMSSUBSSANDWHICHS
SELECT
    o.order_number,
    ol.order_line_id,
    o.order_method,
    d.date_id,
    p.product_id,
    c.customer_id,
    c.customer_pk,
    s.store_id,
    e.employee_id,
    o.order_total_price,
    o.order_points_earned,
    ol.order_line_qty
FROM orders o
INNER JOIN order_line ol ON ol.order_number = o.order_number
INNER JOIN product p ON  ol.product_id = p.product_id
INNER JOIN customer c ON o.customer_pk = c.customer_pk
INNER JOIN employee e ON o.employee_pk = e.employee_pk
INNER JOIN store s ON o.store_pk = s.store_pk
WHERE o.order_number = 1;
