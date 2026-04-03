# pgproto

Native Protocol Buffers (proto3) support for PostgreSQL.

Store your protobuf binary data with the rest of your data. Supports:
- Schema-aware field extraction without JSONB conversions.
- Custom operators for nested field navigation (`->` and `#>`).
- Substantial storage savings over standard JSONB.
- GIN and standard indexing for fast retrieval.

[![Coverage Status](https://img.shields.io/badge/Coverage-87.0%25-brightgreen.svg)](https://github.com/Apaezmx/pgproto)


## 📊 Performance Results

In benchmarks comparing 100,000 serialized `example.Order` messages against equivalent JSONB structures and normalized native relational schemas (using `benchmark.sh` with static fixtures):

| Metric | Protobuf (`pgproto`) | JSONB (`jsonb`) | Native Relational (Normalized 1:N) | Win |
| :--- | :--- | :--- | :--- | :--- |
| **Storage Size** | **16 MB** | 46 MB | 25 MB | **📊 ~35% smaller than Native, ~65% smaller than JSONB!** |
| **Single-Row Lookup Latency (Indexed)** | 5.9 ms | 8.1 ms | **3.5 ms** | Native is fastest for flat lookups, but `pgproto` is close! |
| **Full Document Retrieval Latency** | **5.9 ms** | 8.1 ms | 33.1 ms | **📈 ~5x faster than Native JOINs for full object fetch!** |

> [!NOTE]
> `pgproto` combines the storage efficiency of binary compaction with the query flexibility of JSONB, without the overhead of heavy JOINs or text parsing!

*Benchmarks ran using un-optimized debug binaries on standard Linux environments.*

---

## 🛠️ Installation

### Linux and Mac

Compile and install the extension (requires standard `build-essential` and `postgresql-server-dev-*`).

```sh
cd pgproto
make
make install # may need sudo
```

See the [docker-compose.yml](file:///usr/local/google/home/paezmartinez/pgproto/docker-compose.yml) if you want to deploy a pre-configured local sandbox.

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
```

---

## 🔍 Querying & Extraction

Extract values using standard PostgreSQL operators:

### Nested Field Access
Navigate nested structures using standard text-array paths:

```sql
-- Access a nested field deep in protobuf hierarchy
SELECT data #> '{Outer, inner, id}'::text[] FROM items;
```

### Map / Repeated Field Lookups
Navigating complex arrays and maps (using text-arrays for keys and indices):

```sql
-- Access map keys inside a nested structure
SELECT data #> '{Outer, tags, mykey}'::text[] FROM items;
```

---

## ✏️ Modification & CRUD Operations

`pgproto` allows you to update, insert, and delete parts of a Protobuf document without overwriting the whole column, similar to `jsonb`.

> [!IMPORTANT]
> Functions like `pb_set`, `pb_insert`, and `pb_delete` are **pure functions**. They do not modify the database in place. They return a *new* modified `protobuf` value. To persist changes, you must use them in an `UPDATE` statement and assign the return value back to the column.
> The `pb_to_json` function seen in some examples is **not necessary** for the operation itself; it is only used to display the binary result in a human-readable format.

### Update Fields (`pb_set`)
Update a field at a specific path. Currently supports singular primitive types (Int32, Float, Bool, String).

```sql
-- To persist the change, use it in an UPDATE statement:
UPDATE items SET data = pb_set(data, ARRAY['Outer', 'a'], '42');

-- To view the result without persisting (returns JSON for display):
SELECT pb_to_json(pb_set(data, ARRAY['Outer', 'a'], '42'), 'Outer') FROM items;
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

-- Persist deletion from an array
UPDATE items SET data = pb_delete(data, ARRAY['Outer', 'scores', '0']);
```

### Merge Messages (`||` Operator)
Merge two protobuf messages of the same type. Concatenation of wire format results in standard Protobuf merge (scalars overwrite, arrays append).

```sql
-- Persist merge result
UPDATE items SET data = data || other_data;
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
-   **Enums**: Encoded as standard varints on the wire. Extract them using `pb_get_int32` or the shorthand `->` operators.
-   **Oneof**: Since `oneof` fields are just regular fields with a semantic constraint, you can query their values normally.

### Schema Evolution Handling
Protobuf’s biggest strength is seamless forward/backward compatibility:
-   **Adding Fields**: You can safely add new fields to your `.proto` definition. Old messages in the database without the field will return `NULL` or default values when read using the new schema.
-   **Deprecating Fields**: Deprecated fields can still be read if they exist in the binary data. If you remove a field from the schema, the engine will safely skip it during traversal.

To update a schema in the registry without breaking existing data:
```sql
-- Update using ON CONFLICT (re-registering is safe!)
INSERT INTO pb_schemas (name, data) VALUES ('MySchema', '\x...')
ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data;
```

---

## 🧪 Testing

### 🟢 Regression Tests (PostgreSQL `pg_regress`)
Run the standard PostgreSQL regression tests to verify type I/O, operators, and GIN indexing:

```bash
make installcheck
```

### 🛒 eCommerce Testbench Sandbox (Docker)
We provide an isolated, ready-to-use testing sandbox with a pre-configured schema (`order.proto`) and sample records. This environment demonstrates advanced features like **Maps**, **Nested Navigation**, and **Human-Readable JSON conversion**.

To spin it up and run queries:
```bash
# 1. Build and start the container
docker-compose -f example/docker-compose.yml up -d --build

# 2. Run showcase queries
docker-compose -f example/docker-compose.yml exec db psql -U postgres -d pgproto_showcase -f /workspace/example/queries.sql
```

See [example/README.md](file:///usr/local/google/home/paezmartinez/pgproto/example/README.md) for more details.

---

### 🐳 Running Coverage & Leaks in Docker (Recommended)

You can run both coverage capture and memory leak analysis directly inside your running Docker workspace.

#### 1. 🏗️ Prerequisites (Install Tools)
Install `lcov` and `valgrind` inside the running container as `root`:
```bash
docker-compose -f example/docker-compose.yml exec -u root db apt-get update
docker-compose -f example/docker-compose.yml exec -u root db apt-get install -y lcov valgrind
```

#### 2. 🧪 Coverage Run
Recompile the extension with profiling flags and capture data:
```bash
# Recompile inside container
docker-compose -f example/docker-compose.yml exec -u postgres db make clean
docker-compose -f example/docker-compose.yml exec -u postgres db make COPT="-O0 -fprofile-arcs -ftest-coverage"
docker-compose -f example/docker-compose.yml exec -u root db make install

# Run tests to generate trace data
docker-compose -f example/docker-compose.yml exec -u postgres db make installcheck

# Capture output (ignores negative hit counter overflows)
docker-compose -f example/docker-compose.yml exec -u postgres db lcov --capture --directory . --output-file coverage.info --ignore-errors negative,inconsistent
```

#### 3. 🧠 Memory Leak Analysis
Run showcase queries through `valgrind` to verify memory safety:
```bash
docker-compose -f example/docker-compose.yml exec -u postgres db valgrind --leak-check=full --log-file=/workspace/valgrind.log psql -U postgres -d pgproto_showcase -f /workspace/example/valgrind_full.sql
```
Check `valgrind.log` for memory leaks reports!

---

## 🏗️ Technical Details

For historical design plans, caching mechanisms, and deeper architectural discussion, see [DESIGN.md](file:///usr/local/google/home/paezmartinez/pgproto/DESIGN.md).
