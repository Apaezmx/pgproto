#include "pgproto.h"

/*
 * Extract a single key from a protobuf stream and format it as a string
 * suitable for GIN indexing.
 */
static void
extract_single_key(const char **ptr, const char *end, char *key_str)
{
    uint64 key = decode_varint(ptr, end);
    int field_num = key >> PB_FIELD_NUM_SHIFT;
    int wire_type = key & PB_WIRE_TYPE_MASK;
    
    uint64 val = 0;
    
    switch (wire_type) {
        case PB_WIRE_VARINT:
            val = decode_varint(ptr, end);
            sprintf(key_str, "%d=%lu", field_num, val);
            break;
        case PB_WIRE_FIXED64:
            *ptr += 8;
            sprintf(key_str, "%d=64bit", field_num);
            break;
        case PB_WIRE_LENGTH_DELIMITED:
            {
                uint64 len = decode_varint(ptr, end);
                *ptr += len;
                sprintf(key_str, "%d=len_delim", field_num);
            }
            break;
        case PB_WIRE_FIXED32:
            *ptr += 4;
            sprintf(key_str, "%d=32bit", field_num);
            break;
        default:
            elog(ERROR, "Unsupported wire type %d in GIN extract", wire_type);
    }
}

PG_FUNCTION_INFO_V1(protobuf_gin_extract_value);

/*
 * GIN Extract Value function for Protobuf.
 * Iterates over the binary Protobuf data and extracts field numbers/wire types as keys.
 * Formats them as strings (e.g., "1=42", "2=len_delim") for indexing.
 */
Datum
protobuf_gin_extract_value(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int32 *nkeys = (int32 *) PG_GETARG_POINTER(1);
    bool **nullFlags = (bool **) PG_GETARG_POINTER(2);
    
    const char *ptr = data->data;
    const char *end = (const char *) data + VARSIZE(data);
    
    Datum *entries = NULL;
    int max_entries = 0;
    int cur_entries = 0;

    while (ptr < end) {
        char key_str[64];
        extract_single_key(&ptr, end, key_str);

        if (max_entries == 0) {
            max_entries = 8;
            entries = (Datum *) palloc(max_entries * sizeof(Datum));
        } else if (cur_entries >= max_entries) {
            max_entries *= 2;
            entries = (Datum *) repalloc(entries, max_entries * sizeof(Datum));
        }
        
        entries[cur_entries++] = PointerGetDatum(cstring_to_text(key_str));
    }

    *nkeys = cur_entries;
    *nullFlags = NULL; // No nulls supported
    
    PG_RETURN_POINTER(entries);
}

PG_FUNCTION_INFO_V1(protobuf_gin_extract_query);

Datum
protobuf_gin_extract_query(PG_FUNCTION_ARGS)
{
    ProtobufData *query = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int32 *nkeys = (int32 *) PG_GETARG_POINTER(1);
    bool **nullFlags = (bool **) PG_GETARG_POINTER(5);

    const char *ptr = query->data;
    const char *end = (const char *) query + VARSIZE(query);
    
    Datum *entries = NULL;
    int max_entries = 0;
    int cur_entries = 0;

    while (ptr < end) {
        char key_str[64];
        extract_single_key(&ptr, end, key_str);

        if (max_entries == 0) {
            max_entries = 8;
            entries = (Datum *) palloc(max_entries * sizeof(Datum));
        } else if (cur_entries >= max_entries) {
            max_entries *= 2;
            entries = (Datum *) repalloc(entries, max_entries * sizeof(Datum));
        }
        
        entries[cur_entries++] = PointerGetDatum(cstring_to_text(key_str));
    }

    *nkeys = cur_entries;
    *nullFlags = NULL;
    
    PG_RETURN_POINTER(entries);
}

PG_FUNCTION_INFO_V1(protobuf_gin_consistent);

Datum
protobuf_gin_consistent(PG_FUNCTION_ARGS)
{
    bool *check = (bool *) PG_GETARG_POINTER(0);
    int32 nkeys = PG_GETARG_INT32(3);
    bool *res = (bool *) PG_GETARG_POINTER(5);
    
    bool match = true;
    for (int i = 0; i < nkeys; i++) {
        if (!check[i]) {
            match = false;
            break;
        }
    }
    *res = match;
    PG_RETURN_BOOL(match);
}
