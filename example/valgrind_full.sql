-- 1. Simple Scalar Extraction (Order ID)
SELECT id, data #> '{example.Order, order_id}' AS order_id FROM orders;

-- 2. Nested Field Extraction (Customer ID)
SELECT id, data #> '{example.Order, customer, id}' AS customer_id FROM orders;

-- 3. Map Field Extraction (Map values by key)
SELECT id, data #> '{example.Order, metadata, priority}' AS priority FROM orders;

-- 4. Filtering using GIN Index
SELECT id FROM orders WHERE data @> '\x08e907'::protobuf;

-- 5. Using extraction in the WHERE clause
SELECT id, data #> '{example.Order, customer, id}' AS cust_id FROM orders WHERE (data #> '{example.Order, customer, id}') = 42;

-- 6. Complex Nested Navigation (Repeated & Submessages)
SELECT id, data #> '{example.Order, items, 0, quantity}' AS first_item_qty FROM orders;

-- 7. Human-Readable Output
SELECT id, pb_to_json(data, 'example.Order'::text) AS json_data FROM orders;
