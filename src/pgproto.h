#ifndef PGPROTO_H
#define PGPROTO_H

#include "postgres.h"
#include "varatt.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/array.h"
#include "utils/lsyscache.h"

#include "upb/reflection/def_pool.h"
#include "upb/reflection/message_def.h"
#include "upb/reflection/field_def.h"
#include "google/protobuf/descriptor.upb.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Custom Varlena Structure */
typedef struct {
    int32 vl_len_;    // Internal Postgres header (do not access directly)
    char  data[1];     // Flexible data member
} ProtobufData;

/* Global Schema Pool (Defined in pgproto.c) */
extern upb_DefPool *s_def_pool;

/* Protobuf Wire Types (proto3) */
typedef enum {
    PB_WIRE_VARINT = 0,
    PB_WIRE_FIXED64 = 1,
    PB_WIRE_LENGTH_DELIMITED = 2,
    PB_WIRE_START_GROUP = 3,         // Deprecated
    PB_WIRE_END_GROUP = 4,           // Deprecated
    PB_WIRE_FIXED32 = 5
} PbWireType;

#define PB_WIRE_TYPE_MASK 0x07
#define PB_FIELD_NUM_SHIFT 3
#define PB_VARINT_CONT_MASK 0x80
#define PB_VARINT_DATA_MASK 0x7F

/* Helpers for hex parsing and protobuf wire format */
static inline int
hex_val(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static inline uint64
decode_varint(const char **ptr, const char *end)
{
    uint64 result = 0;
    int shift = 0;
    while (*ptr < end) {
        unsigned char b = (unsigned char) **ptr;
        (*ptr)++;
        result |= ((uint64)(b & 0x7F)) << shift;
        if (!(b & 0x80)) {
            return result;
        }
        shift += 7;
        if (shift >= 64) {
            elog(ERROR, "Varint too large");
        }
    }
    elog(ERROR, "Unexpected end of varint");
    return 0; // Unreachable
}

/* Schema Registry API */
void load_all_schemas_from_db(upb_DefPool *pool);

#endif /* PGPROTO_H */
