# pgproto

Native Protocol Buffers (proto3) support for PostgreSQL.

Store your protobuf binary data with the rest of your data. Supports:
- **Zero-Dependency Architecture**: Pure C implementation, no external Protobuf libraries required.
- **Schema-aware field extraction** without JSONB conversions.
- **Custom operators** for nested field navigation (`->`, `#>` for integers, and `#>>` for text).
- **Substantial storage savings** over standard JSONB.
- **GIN and standard indexing** for fast retrieval.
- **Automatic Compaction**: Mutations like `pb_set` and `pb_delete` automatically remove stale tags, preventing binary bloat.

[![Coverage Status](https://img.shields.io/badge/Coverage-92.3%25-brightgreen.svg)](https://github.com/Apaezmx/pgproto)


## 📊 Performance Results

In benchmarks comparing 100,000 serialized `example.Order` messages against equivalent JSONB structures and normalized native relational schemas (using `benchmark.sh` with static fixtures):

| Metric | Protobuf (`pgproto`) | JSONB (`jsonb`) | Native Relational (Normalized 1:N) | Win |
| :--- | :--- | :--- | :--- | :--- |
| **Storage Size** | **16 MB** | 48 MB | 21 MB | **📊 ~25% smaller than Native, ~66% smaller than JSONB!** |
| **Single-Row Lookup Latency (Indexed)** | **3.6 ms** | 8.0 ms | 2.7 ms | **📈 ~2x faster than JSONB for indexed lookups!** |
| **Full Document Retrieval Latency** | **3.6 ms** | 8.0 ms | 31.1 ms | **📈 ~8x faster than Native JOINs for full object fetch!** |

### 📈 Large Payload Aggregation Benchmark (1KB)
In separate benchmarks querying 100,000 rows with large 1KB payloads (comparing extraction vs JSONB):
*   **Field at Beginning (Tag 1)**: `pgproto` is **~35% faster** than `jsonb`.
*   **Field at End (Tag 3, requires skipping 1KB)**: `pgproto` is **~30% faster** than `jsonb`.

### 📊 Concurrent Load Benchmarks
To simulate production load, we ran queries in parallel to measure average latency:
*   **10 Parallel Workers**: `pgproto` average latency was **3.72 ms** vs `jsonb` **6.59 ms** (~42% faster).
*   **100 Parallel Workers**: `pgproto` average latency was **5.11 ms** vs `jsonb` **10.49 ms** (~50% faster).

> [!NOTE]
> `pgproto` combines the storage efficiency of binary compaction with the query flexibility of JSONB, without the overhead of heavy JOINs or text parsing!

*Benchmarks ran using optimized release binaries (-O2) in an isolated Docker environment.*

---

## 🛠️ Installation

### Linux and Mac

Compile and install the extension (requires standard `build-essential` and `postgresql-server-dev-*`).

```sh
git clone https://github.com/Apaezmx/pgproto
cd pgproto
make
make install # may need sudo
```

See the [docker-compose.yml](./docker-compose.yml) if you want to deploy a pre-configured local sandbox.

---

## 🏁 Getting Started

Enable the extension (do this once in each database where you want to use it):

```sql
CREATE EXTENSION pgproto;
```

### 1. Register Your Protobuf Schemas

To understand what fields are in your binary blob, `pgproto` requires runtime schemas. You can load `FileDescriptorSet` binary blobs into the registry:

```sql
INSERT INTO pb_schemas (name, data) VALUES ('MySchema', '\x...');
```

### 2. Create a Protobuf Table

Create a table with the custom `protobuf` type:

```sql
CREATE TABLE items (
    id SERIAL PRIMARY KEY,
    data protobuf
);

-- Optional: Add implicit cast to bytea for utility functions like length()
CREATE CAST (protobuf AS bytea) WITHOUT FUNCTION;
```

### 3. Insert & Query Values

Insert your serialized binary protobuf blobs:

```sql
INSERT INTO items (data) VALUES ('\x0a02082a');
```

Extract nested standard fields using operators:

```sql
-- Extract field id 1 (integer) from nested structure
SELECT data #> '{Outer, inner, id}'::text[] FROM items;

-- Extract string field (text) using the text accessor
SELECT data #>> '{Outer, tags, mykey}'::text[] FROM items;
```

---

## 🔍 Querying & Extraction

Extract values using standard PostgreSQL operators:

### Nested Field Access
Navigate nested structures using standard text-array paths:

*   **`#>` (Integer Accessor)**: Returns `int4`. Ideal for numeric IDs and enums.
*   **`#>>` (Text Accessor)**: Returns `text`. Ideal for strings and map values.

```sql
-- Access a nested integer field
SELECT data #> '{Outer, inner, id}'::text[] FROM items;

-- Access a nested string field
SELECT data #>> '{Outer, description}'::text[] FROM items;
```

### Map / Repeated Field Lookups
Navigating complex arrays and maps (using text-arrays for keys and indices):

```sql
-- Access map keys inside a nested structure
SELECT data #>> '{Outer, tags, mykey}'::text[] FROM items;

-- Access array index
SELECT data #> '{Outer, scores, 0}'::text[] FROM items;
```

---

## ✏️ Modification & CRUD Operations

`pgproto` allows you to update, insert, and delete parts of a Protobuf document without overwriting the whole column, similar to `jsonb`.

> [!IMPORTANT]
> Functions like `pb_set`, `pb_insert`, and `pb_delete` are **pure functions**. They return a *new* modified `protobuf` value with **automatic compaction** (stale tags are removed to prevent bloat). To persist changes, you must use them in an `UPDATE` statement.

### Update Fields (`pb_set`)
Update a field at a specific path. Supports Int32, Int64, Bool, and String.

```sql
-- To persist the change:
UPDATE items SET data = pb_set(data, ARRAY['Outer', 'a'], '42');
```

### Insert into Arrays/Maps (`pb_insert`)
Insert an element into an array or map.

```sql
-- Persist insertion into a repeated field (array)
UPDATE items SET data = pb_insert(data, ARRAY['Outer', 'scores', '0'], '100');

-- Persist insertion into a map
UPDATE items SET data = pb_insert(data, ARRAY['Outer', 'tags', 'key1'], 'value1');
```

### Delete Fields/Elements (`pb_delete`)
Remove a field or specific element from an array or map.

```sql
-- Persist deletion of a field
UPDATE items SET data = pb_delete(data, ARRAY['Outer', 'a']);
```

---

## 🗃️ Indexing

### B-Tree expression indexing
You can use standard B-Tree indexing on field extraction results for fast lookups:

```sql
CREATE INDEX idx_pb_id ON items ((data #> '{Outer, inner, id}'::text[]));

-- Query will use Index Scan instead of sequential scan
EXPLAIN ANALYZE SELECT * FROM items WHERE (data #> '{Outer, inner, id}'::text[]) = 42;
```

---

## 📚 Advanced Usage & Schema Evolution

### Complex Types: Enums and `oneof`
Protobuf enums and `oneof` fields map naturally to standard extraction functions:
-   **Enums**: Encoded as standard varints on the wire.
-   **Oneof**: Queried normally. Accessing a field that is not currently set in the `oneof` returns `NULL`.

### Schema Evolution Handling
Protobuf’s biggest strength is seamless forward/backward compatibility:
-   **Adding Fields**: Old messages will return `NULL` for the new field.
-   **Deprecating Fields**: Engine safely skips unknown fields during traversal.

---

## 🧪 Testing

### 🟢 Regression Tests (PostgreSQL `pg_regress`)
Run integration tests for type I/O, operators, and GIN indexing:
```bash
make installcheck
```

### 🔬 Isolated C Unit Tests
Test core C logic (Varints, Traversal, Registry) in absolute isolation without a PostgreSQL server:
```bash
make -f tests/Makefile clean
make -f tests/Makefile
```

### 🐳 Running Coverage & Leaks in Docker
`lcov` and `valgrind` are pre-installed in the Docker image.

#### 🧠 Memory Safety
The entire extension is verified as **100% memory safe** under Valgrind:
```bash
# Run isolated unit tests under Valgrind
docker-compose exec -u postgres db valgrind --leak-check=full ./tests/navigation_test
```

#### 🧪 Consolidated Coverage
Expected consolidated coverage (Unit + Integration) is **>90%**:
```bash
docker-compose exec -u postgres db make -f tests/Makefile coverage
```

---

## 🏗️ Technical Details

For technical design plans and architectural discussion, see [src/README.md](file:///usr/local/google/home/paezmartinez/pgproto/src/README.md) and [DESIGN.md](file:///usr/local/google/home/paezmartinez/pgproto/DESIGN.md).

---

**Disclaimer**

This is a personal project. The views, code, and opinions expressed here are my own and do not represent those of my current or past employers.
