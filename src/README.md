# `src/` Directory Overview

This directory contains the pure C source code for the `pgproto` PostgreSQL extension. The extension is built as a zero-dependency, schema-agnostic Protobuf engine optimized for high-performance storage and querying.

## 🏗️ Architecture
The extension is implemented in pure C99 without any external Protobuf libraries (like `upb` or C++ Protobuf). It uses an on-the-fly binary descriptor parser to resolve field metadata directly from `FileDescriptorSet` blobs stored in the database.

## 📂 File Distribution

### 🛠️ Core & Entry
*   **[`pgproto.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/pgproto.c)**: The main entry point for the extension. Contains module magic (`PG_MODULE_MAGIC`) and boilerplate.
*   **[`pgproto.h`](file:///usr/local/google/home/paezmartinez/pgproto/src/pgproto.h)**: The central internal header. Defines the Protobuf wire format types, `PbFieldLookup` structures, and shared inline functions for high-performance varint decoding and encoding.

### 📥 Type Handler
*   **[`io.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/io.c)**: Implements standard PostgreSQL Type Input/Output handlers. Manages the hex-encoded string representation used in SQL queries.

### 📜 Registry
*   **[`registry.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/registry.c)**: The core schema engine. Implements a custom Protobuf binary parser that traverses descriptor blobs to resolve field names to tag numbers and types. Manages the session-level schema cache.

### 🧭 Navigation
*   **[`navigation.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/navigation.c)**: The "hot-path" querying engine. Implements sequential wire-format scanning to perform nested field extraction, array indexing, and map key lookups without decoding the entire message.

### ✏️ Mutation
*   **[`mutation.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/mutation.c)**: Implements high-performance modification operations. Uses a "last-tag-wins" append strategy for updates and tag-filtering for deletions to maintain high speed and memory efficiency.

### 📄 JSON Conversion
*   **[`json.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/json.c)**: Implements dynamic Protobuf-to-JSON translation. Recursively decodes binary messages into human-readable JSON using metadata from the registry.

### 🔍 Indexing & GIN
*   **[`gin.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/gin.c)**: Implements GIN index support. Extracts tag-value pairs from Protobuf blobs to enable blazing-fast indexed lookups (e.g., using the `@>` operator).
