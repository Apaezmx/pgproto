#!/bin/bash
# ⏱️ pgproto Performance Benchmark Script

DB_NAME=${DB_NAME:-pgproto_showcase}
DB_USER=${DB_USER:-postgres}
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}

PSQL="psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -t -A"

echo "🧹 Pre-cleaning old benchmark tables..."
$PSQL -c "DROP TABLE IF EXISTS order_items_native CASCADE; DROP TABLE IF EXISTS orders_native CASCADE; DROP TABLE IF EXISTS bench_proto CASCADE; DROP TABLE IF EXISTS bench_jsonb CASCADE; DROP TABLE IF EXISTS converted_cache CASCADE;"

echo "📂 Creating benchmark tables..."
$PSQL -c "
CREATE TABLE bench_proto (id serial PRIMARY KEY, data protobuf);
CREATE TABLE bench_jsonb (id serial PRIMARY KEY, data jsonb);
CREATE TABLE orders_native (
    id serial PRIMARY KEY,
    order_id int4,
    customer_id int4,
    customer_email text,
    shipping_street text,
    shipping_city text,
    shipping_zip text
);
CREATE TABLE order_items_native (
    id serial PRIMARY KEY,
    order_id int4 REFERENCES orders_native(id),
    product_id text,
    quantity int4,
    cents int4
);
CREATE TABLE converted_cache (
    id serial PRIMARY KEY,
    j jsonb
);
"

echo "📥 Loading base static fixture (mock data)..."
$PSQL -c "CREATE TABLE IF NOT EXISTS tmp_load (id int, val text); TRUNCATE tmp_load;"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\copy tmp_load (id, val) FROM 'example/bench_data.csv' WITH CSV HEADER;"

echo "🚀 Generating 100,000 rows of synthetic data..."
$PSQL -c "
INSERT INTO bench_proto (data)
SELECT val::protobuf FROM tmp_load CROSS JOIN generate_series(1, 10000);

INSERT INTO bench_jsonb (data)
SELECT pb_to_json(val::protobuf, 'example.Order')::jsonb FROM tmp_load CROSS JOIN generate_series(1, 10000);
"

echo "⚡ Indexing bench_proto for fast JOIN..."
$PSQL -c "CREATE INDEX idx_proto ON bench_proto ((data #> '{example.Order, order_id}'::text[]));"

echo "⚡ Caching pb_to_json results..."
$PSQL -c "INSERT INTO converted_cache (j) SELECT pb_to_json(data, 'example.Order')::jsonb FROM bench_proto;"

echo "🚀 Populating native relational tables (Normalized 1:N)..."
$PSQL -c "
INSERT INTO orders_native (order_id, customer_id, customer_email, shipping_street, shipping_city, shipping_zip)
SELECT 
    (p.data #> '{example.Order, order_id}'::text[]),
    (p.data #> '{example.Order, customer, id}'::text[]),
    (c.j -> 'customer' ->> 'email'),
    (c.j -> 'shippingAddress' ->> 'street'),
    (c.j -> 'shippingAddress' ->> 'city'),
    (c.j -> 'shippingAddress' ->> 'zip')
FROM converted_cache c JOIN bench_proto p ON c.id = p.id;

INSERT INTO order_items_native (order_id, product_id, quantity, cents)
SELECT 
    o.id,
    (item ->> 'productId'),
    (item ->> 'quantity')::int,
    (item ->> 'cents')::int
FROM orders_native o
JOIN converted_cache c ON o.id = c.id
CROSS JOIN jsonb_array_elements(c.j -> 'items') as item;
"

echo ""
echo "📊 --- Storage Size Comparison ---"
PROTO_SIZE=$($PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('bench_proto'));")
JSONB_SIZE=$($PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('bench_jsonb'));")
NATIVE_SIZE=$($PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('orders_native') + pg_total_relation_size('order_items_native'));")

echo "Protobuf Size: $PROTO_SIZE"
echo "JSONB Size:    $JSONB_SIZE"
echo "Native Size:   $NATIVE_SIZE"

echo "📈 --- Query Latency Comparison (Indexes) ---"
$PSQL -c "CREATE INDEX IF NOT EXISTS idx_proto ON bench_proto ((data #> '{example.Order, order_id}'::text[]));"
$PSQL -c "CREATE INDEX IF NOT EXISTS idx_jsonb ON bench_jsonb ((data ->> 'orderId'));"
$PSQL -c "CREATE INDEX IF NOT EXISTS idx_native_order ON orders_native (order_id);"
$PSQL -c "CREATE INDEX IF NOT EXISTS idx_native_items ON order_items_native (order_id);"

echo "Running indexed lookups..."
# order_id in the fixture is 1001
PROTO_LATENCY=$($PSQL -c "EXPLAIN ANALYZE SELECT * FROM bench_proto WHERE (data #> '{example.Order, order_id}'::text[]) = 1001;" | grep 'Execution Time' | awk '{print $3}')
JSONB_LATENCY=$($PSQL -c "EXPLAIN ANALYZE SELECT * FROM bench_jsonb WHERE data ->> 'orderId' = '1001';" | grep 'Execution Time' | awk '{print $3}')
NATIVE_SINGLE_LATENCY=$($PSQL -c "EXPLAIN ANALYZE SELECT * FROM orders_native WHERE order_id = 1001;" | grep 'Execution Time' | awk '{print $3}')
NATIVE_JOIN_LATENCY=$($PSQL -c "EXPLAIN ANALYZE SELECT * FROM orders_native o JOIN order_items_native i ON o.id = i.order_id WHERE o.order_id = 1001;" | grep 'Execution Time' | awk '{print $3}')

echo "Protobuf Latency:        ${PROTO_LATENCY} ms"
echo "JSONB Latency:           ${JSONB_LATENCY} ms"
echo "Native (Single Tbl):     ${NATIVE_SINGLE_LATENCY} ms"
echo "Native (Joined Full):    ${NATIVE_JOIN_LATENCY} ms"
