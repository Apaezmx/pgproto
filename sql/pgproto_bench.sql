-- Large Payload Benchmark (1KB)
DROP TABLE IF EXISTS pb_bench_large;
CREATE TABLE pb_bench_large (id serial, pb_data protobuf, jsonb_data jsonb);

-- Insert 100,000 rows with 1KB payload
-- Tag 1 = 42, Tag 2 = 1000 'a's, Tag 3 = 120
-- Hex generated previously
INSERT INTO pb_bench_large (pb_data, jsonb_data)
SELECT ('\x082a12e807' || repeat('61', 1000) || '1878')::protobuf, 
       ('{"id": 42, "text": "' || repeat('a', 1000) || '", "val": 120}')::jsonb
FROM generate_series(1, 100000);

-- Query Tag 1 (Beginning)
\timing on
SELECT count(pb_get_int32(pb_data, 1)) FROM pb_bench_large;

-- Query Tag 3 (End, requires skipping 1KB)
SELECT count(pb_get_int32(pb_data, 3)) FROM pb_bench_large;

-- Query JSONB equivalent for Tag 1
SELECT count((jsonb_data->>'id')::int) FROM pb_bench_large;

-- Query JSONB equivalent for Tag 3
SELECT count((jsonb_data->>'val')::int) FROM pb_bench_large;
\timing off

-- EXPLAIN ANALYZE for Tag 3 vs JSONB Tag 3
EXPLAIN ANALYZE SELECT pb_get_int32(pb_data, 3) FROM pb_bench_large;
EXPLAIN ANALYZE SELECT (jsonb_data->>'val')::int FROM pb_bench_large;

