# `src/` Directory Overview

This directory contains the C source code for the `pgproto` PostgreSQL extension. The code is modularized into functional components.

## 📂 File Distribution

### 🛠️ Core & Entry
*   **[`pgproto.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/pgproto.c)**: The main entry point for the extension. Contains module magic (`PG_MODULE_MAGIC`) and global state definitions.
*   **[`pgproto.h`](file:///usr/local/google/home/paezmartinez/pgproto/src/pgproto.h)**: Common internal header file. Includes PostgreSQL and `upb` headers, defines shared structures, and provides utility functions (like `decode_varint`).

### 📥 Type Handler
*   **[`io.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/io.c)**: Contains standard PostgreSQL Type Input/Output handlers (`protobuf_in`, `protobuf_out`). Used for translating between text representation (hex strings) and binary storage.

### 📜 Registry
*   **[`registry.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/registry.c)**: Manages Protobuf Schema registration and server-side caching (`upb_DefPool`). Contains `pb_register_schema` and background loading routines.

### 🧭 Navigation
*   **[`navigation.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/navigation.c)**: Implements nested field extraction by path traversal. This powers the querying engine (resolving tags, array offsets, and maps).

### 🔍 Indexing & GIN
*   **[`gin.c`](file:///usr/local/google/home/paezmartinez/pgproto/src/gin.c)**: Implements support functions for Generalized Inverted Index (GIN) lookups over Protobuf binary data. Includes extraction and consistency checks.
