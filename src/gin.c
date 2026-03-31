#include "pgproto.h"

PG_FUNCTION_INFO_V1(protobuf_gin_extract_value);

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
        uint64 key = decode_varint(&ptr, end);
        int field_num = key >> 3;
        int wire_type = key & 0x07;
        
        uint64 val = 0;
        char key_str[64];
        
        switch (wire_type) {
            case 0: // Varint
                val = decode_varint(&ptr, end);
                sprintf(key_str, "%d=%lu", field_num, val);
                break;
            case 1: // 64-bit
                ptr += 8;
                sprintf(key_str, "%d=64bit", field_num);
                break;
            case 2: // Length-delimited
                {
                    uint64 len = decode_varint(&ptr, end);
                    ptr += len;
                    sprintf(key_str, "%d=len_delim", field_num);
                }
                break;
            case 5: // 32-bit
                ptr += 4;
                sprintf(key_str, "%d=32bit", field_num);
                break;
            default:
                elog(ERROR, "Unsupported wire type %d in GIN extract", wire_type);
        }

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
        uint64 key = decode_varint(&ptr, end);
        int field_num = key >> 3;
        int wire_type = key & 0x07;
        
        uint64 val = 0;
        char key_str[64];
        
        switch (wire_type) {
            case 0:
                val = decode_varint(&ptr, end);
                sprintf(key_str, "%d=%lu", field_num, val);
                break;
            case 1:
                ptr += 8;
                sprintf(key_str, "%d=64bit", field_num);
                break;
            case 2:
                {
                    uint64 len = decode_varint(&ptr, end);
                    ptr += len;
                    sprintf(key_str, "%d=len_delim", field_num);
                }
                break;
            case 5:
                ptr += 4;
                sprintf(key_str, "%d=32bit", field_num);
                break;
            default:
                elog(ERROR, "Unsupported wire type %d in GIN extract_query", wire_type);
        }

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
