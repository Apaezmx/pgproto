-- ===================================================================
-- pgproto Feature Showcase Queries
-- ===================================================================

-- 1. Simple Scalar Extraction (Order ID)
-- Using the #> operator with a path array (First element is Message Name)
SELECT 
    id, 
    data #> '{example.Order, order_id}' AS order_id 
FROM orders;

-- 2. Nested Field Extraction (Customer ID)
SELECT 
    id, 
    data #> '{example.Order, customer, id}' AS customer_id 
FROM orders;

-- 3. Map Field Extraction (Map values by key)
-- Extracting 'priority' from the metadata map
SELECT 
    id, 
    data #> '{example.Order, metadata, priority}' AS priority 
FROM orders;

-- 4. Filtering using GIN Index
-- Find order 1001 using the containment operator @>
-- (Binary contains Order ID 1001)
EXPLAIN ANALYZE 
SELECT id 
FROM orders 
WHERE data @> '08e907'::protobuf; -- order_id: 1001

-- 5. Using extraction in the WHERE clause
SELECT 
    id, 
    data #> '{example.Order, customer, id}' AS cust_id
FROM orders 
WHERE (data #> '{example.Order, customer, id}') = 42;


-- 6. Complex Nested Navigation (Repeated & Submessages)
-- Accessing the quantity of the first item (Index 0)
SELECT 
    id, 
    data #> '{example.Order, items, 0, quantity}' AS first_item_qty 
FROM orders;


-- 7. Human-Readable Output
SELECT 
    id, 
    pb_to_json(data, 'example.Order'::text) AS json_data 
FROM orders;
