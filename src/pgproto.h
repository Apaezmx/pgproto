#ifndef PGPROTO_H
#define PGPROTO_H

#ifdef PGPROTO_UNIT_TEST
#include "tests/postgres_mock.h"
#else
#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "lib/stringinfo.h"
#endif

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* 
 * ProtobufData: Custom Varlena structure for storing raw Protobuf binary data in PostgreSQL.
 * It follows the standard PostgreSQL 'varlena' header convention.
 */
typedef struct {
    int32 vl_len_;    /* PostgreSQL varlena length header */
    char  data[1];     /* Raw Protobuf wire format bytes */
} ProtobufData;

/* 
 * PbWireType: Standard Protobuf wire types as defined in the Protobuf encoding specification.
 */
typedef enum {
    PB_WIRE_VARINT = 0,
    PB_WIRE_FIXED64 = 1,
    PB_WIRE_LENGTH_DELIMITED = 2,
    PB_WIRE_START_GROUP = 3,
    PB_WIRE_END_GROUP = 4,
    PB_WIRE_FIXED32 = 5
} PbWireType;

#define PB_WIRE_TYPE_MASK 0x07
#define PB_FIELD_NUM_SHIFT 3
#define PB_FIELD_TAG(num, wire) (((uint32_t)(num) << PB_FIELD_NUM_SHIFT) | (uint32_t)(wire))

/* DescriptorProto Tags (from descriptor.proto) */
#define PB_FILE_DESCRIPTOR_SET_FILE          1
#define PB_FILE_DESCRIPTOR_PROTO_NAME        1
#define PB_FILE_DESCRIPTOR_PROTO_PACKAGE     2
#define PB_FILE_DESCRIPTOR_PROTO_MESSAGE_TYPE 4

#define PB_DESCRIPTOR_PROTO_NAME             1
#define PB_DESCRIPTOR_PROTO_FIELD            2
#define PB_DESCRIPTOR_PROTO_NESTED_TYPE      3

#define PB_FIELD_DESCRIPTOR_PROTO_NAME       1
#define PB_FIELD_DESCRIPTOR_PROTO_NUMBER     3
#define PB_FIELD_DESCRIPTOR_PROTO_LABEL      4
#define PB_FIELD_DESCRIPTOR_PROTO_TYPE       5
#define PB_FIELD_DESCRIPTOR_PROTO_TYPE_NAME  6

/* Map Entry Tags */
#define PB_MAP_ENTRY_KEY                     1
#define PB_MAP_ENTRY_VALUE                   2

/* Fixed-size wire types sizes */
#define PB_WIRE_FIXED64_SIZE                 8
#define PB_WIRE_FIXED32_SIZE                 4

/* Hexadecimal conversion constants */
#define HEX_CHAR_BITS                        4
#define HEX_PREFIX_LEN                       2

/* 
 * PbType: Protobuf field types as defined in descriptor.proto.
 */
typedef enum {
    PB_TYPE_DOUBLE = 1,
    PB_TYPE_FLOAT = 2,
    PB_TYPE_INT64 = 3,
    PB_TYPE_UINT64 = 4,
    PB_TYPE_INT32 = 5,
    PB_TYPE_FIXED64 = 6,
    PB_TYPE_FIXED32 = 7,
    PB_TYPE_BOOL = 8,
    PB_TYPE_STRING = 9,
    PB_TYPE_GROUP = 10,
    PB_TYPE_MESSAGE = 11,
    PB_TYPE_BYTES = 12,
    PB_TYPE_UINT32 = 13,
    PB_TYPE_ENUM = 14,
    PB_TYPE_SFIXED32 = 15,
    PB_TYPE_SFIXED64 = 16,
    PB_TYPE_SINT32 = 17,
    PB_TYPE_SINT64 = 18
} PbType;

/* 
 * PbFieldLookup: Structure returned by the registry to describe a field's metadata.
 */
typedef struct {
    char name[256];       /* Field name */
    uint32_t number;      /* Field tag number */
    PbType type;          /* Protobuf type */
    char type_name[256];  /* Fully qualified submessage name (if type == MESSAGE) */
    bool is_repeated;     /* True if field is 'repeated' */
    bool is_map;          /* True if field is a 'map' */
    bool found;           /* True if the lookup succeeded */
} PbFieldLookup;

/**
 * decode_varint: Decodes a Base128 Varint from a byte stream.
 * 
 * @param ptr: Pointer to the start of the byte stream (advanced by the function).
 * @param end: Pointer to the end of the byte stream.
 * @return: The decoded 64-bit unsigned integer.
 * 
 * Failure Modes:
 * - Errors via elog(ERROR) if the stream ends unexpectedly or the varint exceeds 64 bits.
 */
static inline uint64
decode_varint(const char **ptr, const char *end)
{
    uint64 result = 0;
    int shift = 0;
    while (*ptr < end) {
        unsigned char b = (unsigned char) **ptr;
        (*ptr)++;
        result |= ((uint64)(b & 0x7F)) << shift;
        if (!(b & 0x80)) return result;
        shift += 7;
        if (shift >= 64) elog(ERROR, "Varint too large");
    }
    return 0;
}

/**
 * encode_varint: Encodes a 64-bit integer into a Base128 Varint and appends to a StringInfo buffer.
 * 
 * @param val: The 64-bit value to encode.
 * @param buf: The PostgreSQL StringInfo buffer to append to.
 */
static inline void
encode_varint(uint64 val, StringInfo buf)
{
    do {
        unsigned char b = (unsigned char)(val & 0x7F);
        val >>= 7;
        if (val) b |= 0x80;
        appendStringInfoChar(buf, (char)b);
    } while (val);
}

#define PB_WIRE_TYPE_MASK 0x07

/**
 * hex_val: Converts a hex character to its integer value.
 */
static inline int
hex_val(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/**
 * skip_field: Skips a field in the Protobuf stream based on its wire type.
 * 
 * @param wire_type: The Protobuf wire type (0, 1, 2, 5).
 * @param ptr: Pointer to the start of the field value (advanced by the function).
 * @param end: Pointer to the end of the byte stream.
 */
static inline void
skip_field(int wire_type, const char **ptr, const char *end)
{
    switch (wire_type) {
        case PB_WIRE_VARINT:
            decode_varint(ptr, end);
            break;
        case PB_WIRE_FIXED64:
            *ptr += 8;
            break;
        case PB_WIRE_LENGTH_DELIMITED:
            {
                uint64 len = decode_varint(ptr, end);
                *ptr += len;
            }
            break;
        case PB_WIRE_FIXED32:
            *ptr += 4;
            break;
    }
}

typedef enum {
    PB_LOOKUP_OK = 0,
    PB_LOOKUP_MSG_NOT_FOUND = 1,
    PB_LOOKUP_FIELD_NOT_FOUND = 2
} PbLookupStatus;

/* Registry API */

/**
 * pgproto_lookup_field: Resolves a field name to its metadata in a given message type.
 * 
 * @param message_name: Fully qualified message name (e.g., "example.Order").
 * @param field_name: Name of the field to look up.
 * @param out: Pointer to the lookup structure to populate.
 * @return: PB_LOOKUP_OK if found, otherwise an error status.
 */
PbLookupStatus pgproto_lookup_field(const char *message_name, const char *field_name, PbFieldLookup *out);

/**
 * pgproto_lookup_field_by_number: Resolves a field tag number to its metadata in a given message type.
 * 
 * @param message_name: Fully qualified message name.
 * @param field_number: Tag number of the field.
 * @param out: Pointer to the lookup structure to populate.
 * @return: PB_LOOKUP_OK if found, otherwise an error status.
 */
PbLookupStatus pgproto_lookup_field_by_number(const char *message_name, uint32_t field_number, PbFieldLookup *out);

/**
 * pgproto_LoadAllSchemasFromDb: Loads all registered schemas into the session cache.
 * Must be called before any dynamic lookup if s_schema_loaded is false.
 */
void pgproto_LoadAllSchemasFromDb(void);

#endif
