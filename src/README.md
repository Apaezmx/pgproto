# `src/` Directory Overview

This directory contains the pure C source code for the `pgproto` PostgreSQL extension. The extension is built as a zero-dependency, schema-agnostic Protobuf engine optimized for high-performance storage and querying.

## 🏗️ Architecture
The extension is implemented in pure C99 without any external Protobuf libraries (like `upb` or C++ Protobuf). It uses an on-the-fly binary descriptor parser to resolve field metadata directly from `FileDescriptorSet` blobs stored in the database.

The codebase follows a **DRY (Don't Repeat Yourself)** architecture, leveraging internal unified helpers for path traversal, tag filtering, and value encoding to ensure robustness and memory safety.

## 📂 File Distribution

### 🛠️ Core & Entry
*   **[`pgproto.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/pgproto.c)**: The main entry point for the extension.
*   **[`pgproto.h`](file:///usr/local/google/home/paezmartinez/pgproto/src/pgproto.h)**: The central internal header. Defines Protobuf wire format types and shared inline functions for varint decoding. Supports conditional inclusion for isolated unit testing via the `PGPROTO_UNIT_TEST` macro.

### 📥 Type Handler
*   **[`io.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/io.c)**: Implements PostgreSQL Type Input/Output handlers for hex-encoded Protobuf blobs.

### 📜 Registry
*   **[`registry.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/registry.c)**: The schema engine. Implements a binary descriptor parser to resolve field names to tags.

### 🧭 Navigation
*   **[`navigation.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/navigation.c)**: The querying engine. Uses unified path traversal helpers to extract integers (`#>`) and text (`#>>`) from nested structures, maps, and arrays.

### ✏️ Mutation
*   **[`mutation.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/mutation.c)**: Implements modification operations (`pb_set`, `pb_insert`, `pb_delete`) with **automatic compaction** to prevent binary bloat.

### 📄 JSON Conversion
*   **[`json.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/json.c)**: Implements dynamic Protobuf-to-JSON translation for human-readable display.

### 🔍 Indexing & GIN
*   **[`gin.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/gin.c)**: Implements GIN index support for blazing-fast containment queries (`@>`).

## 🧪 Testing
For isolated C unit tests and memory safety verification, see the root **[`tests/`](file:///usr/local/google/home/paezmartinez/pgproto/tests/)** directory.
