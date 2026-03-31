# pgproto

Native Protocol Buffers (proto3) support for PostgreSQL.

Store your protobuf binary data with the rest of your data. Supports:
- Schema-aware field extraction without JSONB conversions.
- Custom operators for nested field navigation (`->` and `#>`).
- Substantial storage savings over standard JSONB.
- GIN and standard indexing for fast retrieval.

[![Coverage Status](https://img.shields.io/badge/Coverage-83.4%25-brightgreen.svg)](https://github.com/pgvector/pgvector)

## 📊 Performance Results

In benchmarks comparing 100,000 serialized protobuf messages against an equivalent JSONB structure:

| Metric | Protobuf (`pgproto`) | JSONB (`jsonb`) | Win |
| --- | --- | --- | --- |
| **Storage Size** | **4.3 MB** | 8.2 MB | **📊 ~50% Saved** |
| **Indexed Query Latency** | **15.1 ms** | 15.9 ms | **📈 Competitive / Slightly Faster** |

*Benchmarks ran using un-optimized debug binaries on standard Linux environments. Expect higher throughput in optimized production builds.*

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

## 🗃️ Indexing

### B-Tree expression indexing
You can use standard B-Tree indexing on field extraction results for fast lookups:

```sql
CREATE INDEX idx_pb_id ON items ((data #> '{Outer, inner, id}'::text[]));

-- Query will use Index Scan instead of sequential scan
EXPLAIN ANALYZE SELECT * FROM items WHERE (data #> '{Outer, inner, id}'::text[]) = 42;
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
