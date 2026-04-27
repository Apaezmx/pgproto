#!/bin/bash
N=${1:-10}
echo "Running $N queries in parallel for Protobuf..."
seq 1 $N | xargs -I {} -P $N psql -U postgres -d pgproto_showcase -t -A -c "EXPLAIN ANALYZE SELECT * FROM bench_proto WHERE (data #> '{example.Order, order_id}'::text[]) = 1001;" | grep 'Execution Time' | awk '{print $3}' > /tmp/proto_results.txt

echo "Running $N queries in parallel for JSONB..."
seq 1 $N | xargs -I {} -P $N psql -U postgres -d pgproto_showcase -t -A -c "EXPLAIN ANALYZE SELECT * FROM bench_jsonb WHERE data ->> 'orderId' = '1001';" | grep 'Execution Time' | awk '{print $3}' > /tmp/jsonb_results.txt

echo "📊 Results:"
awk '{ sum += $1 } END { if (NR > 0) print "Protobuf Average: " sum / NR " ms" }' /tmp/proto_results.txt
awk '{ sum += $1 } END { if (NR > 0) print "JSONB Average: " sum / NR " ms" }' /tmp/jsonb_results.txt
