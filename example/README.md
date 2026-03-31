# pgproto eCommerce Testbench

This is a clean, isolated environment to evaluate the `pgproto` extension capabilities with realistic eCommerce payloads.

## 🚀 Getting Started

### 1. Start the Environment
Run this command from the repository root:
```bash
docker-compose -f example/docker-compose.yml up -d --build
```
This will build standard PostgreSQL 18 with `pgproto` installed, and automatically initialize it using `initdb.d/01_setup.sql`.

### 2. View Initialization Logs
The setup script creates standard eCommerce tables, registers the `example.Order` schema, and inserts a few sample records:
```bash
docker-compose -f example/docker-compose.yml logs db
```

---

## 🧭 Navigating Data

You can run queries using standard `psql` inside the container:
```bash
docker-compose -f example/docker-compose.yml exec db psql -U postgres -d pgproto_showcase -f /workspace/example/queries.sql
```

Available queries demonstrate:
*   **Scalar Extraction**: Reading plain fields like `order_id`.
*   **Nested Field Navigation**: Reversing path arrays like `'{customer, id}'`.
*   **Maps Feature Navigation**: Extracting values using string keys.
*   **GIN Indexing Filtering**: Performance check for containment operations.

### Example Queries

#### Scalar Extraction
```sql
SELECT id, data #> '{order_id}' AS order_id FROM orders;
```

#### Map Values by Key
```sql
SELECT id, data #> '{metadata, priority}' AS priority FROM orders;
```

---

## 📜 Schema Lifecycle & Registration

### 🛠️ Generating Schema Blobs
The `pb_register_schema` function expects a binary `FileDescriptorSet` blob. You can generate this using `protoc`:

```bash
protoc --descriptor_set_out=order.desc order.proto
```

To register it in the database, you can load the file via `psql` or use Hex encoding (as seen in `01_setup.sql`). 

### 🔄 Schema Lifecycle and Persistence

-   **Persistence**: Registered schemas are stored in the `pb_schemas` table and are persistent across service restarts.
-   **Rollovers**: You do **not** need to reload schemas on every rollout unless you change the schema definition.
-   **Overrides**: The table has a `UNIQUE` constraint on `name`. To override a schema, you must `DELETE FROM pb_schemas WHERE name = '...'` first.
-   **Existing Data**: Changing a schema definition does not alter binary data in the DB. The extension interprets binary on-the-fly using the currently registered schema.

---

## 🧹 Cleanup
To tear down the showcase environment without affecting your development environment:
```bash
docker-compose -f example/docker-compose.yml down -v
```
