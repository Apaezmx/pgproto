#include "postgres.h"
#include "varatt.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "catalog/pg_type_d.h"
#include <ctype.h>
#include <stdlib.h>
#include "executor/spi.h"
#include "utils/array.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"


#include "upb/reflection/def_pool.h"
#include "upb/reflection/message_def.h"
#include "upb/reflection/field_def.h"
#include "google/protobuf/descriptor.upb.h"



PG_MODULE_MAGIC;

#include <string.h>
#include <stdio.h>

typedef struct {
    int32 vl_len_;    // Internal Postgres header (do not access directly)
    char  data[1];     // Flexible data member
} ProtobufData;

static upb_DefPool *s_def_pool = NULL;

static void load_all_schemas_from_db(upb_DefPool *pool);

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

PG_FUNCTION_INFO_V1(protobuf_in);
Datum
protobuf_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    size_t len = strlen(str);
    size_t i;
    ProtobufData *result;
    size_t data_len;

    if (len >= 2 && str[0] == '\\' && str[1] == 'x') {
        str += 2;
        len -= 2;
    }

    if (len % 2 != 0) {
        elog(ERROR, "Invalid hex string length: %zu", len);
    }

    data_len = len / 2;
    result = (ProtobufData *) palloc(VARHDRSZ + data_len);
    SET_VARSIZE(result, VARHDRSZ + data_len);

    for (i = 0; i < data_len; i++) {
        int hi = hex_val(str[2 * i]);
        int lo = hex_val(str[2 * i + 1]);
        if (hi < 0 || lo < 0) {
            elog(ERROR, "Invalid character in hex string");
        }
        result->data[i] = (hi << 4) | lo;
    }

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(protobuf_out);
Datum
protobuf_out(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    size_t len = VARSIZE(data) - VARHDRSZ;
    char *result_str;
    size_t i;

    result_str = palloc(len * 2 + 3);
    result_str[0] = '\\';
    result_str[1] = 'x';

    for (i = 0; i < len; i++) {
        sprintf(result_str + 2 + i * 2, "%02x", (unsigned char) data->data[i]);
    }
    result_str[len * 2 + 2] = '\0';

    PG_RETURN_CSTRING(result_str);
}

PG_FUNCTION_INFO_V1(pb_get_int32);
Datum
pb_get_int32(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int32 target_tag = PG_GETARG_INT32(1);
    const char *ptr = data->data;
    const char *end = (const char *) data + VARSIZE(data);

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = key >> 3;
        int wire_type = key & 0x07;

        if (field_num == target_tag) {
            if (wire_type == 0) { // Varint
                uint64 val = decode_varint(&ptr, end);
                PG_RETURN_INT32((int32) val);
            } else {
                elog(ERROR, "Expected varint wire type for field %d, got %d", target_tag, wire_type);
            }
        }

        switch (wire_type) {
            case 0:
                decode_varint(&ptr, end);
                break;
            case 1:
                ptr += 8;
                break;
            case 2:
                {
                    uint64 len = decode_varint(&ptr, end);
                    ptr += len;
                }
                break;
            case 5:
                ptr += 4;
                break;
            default:
                elog(ERROR, "Unsupported wire type %d", wire_type);
        }
    }

    PG_RETURN_NULL();
}


PG_FUNCTION_INFO_V1(protobuf_contains);
Datum
protobuf_contains(PG_FUNCTION_ARGS)
{
    ProtobufData *base = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ProtobufData *query = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));
    const char *q_ptr = query->data;
    const char *q_end = (const char *) query + VARSIZE(query);
    bool match_all = true;

    while (q_ptr < q_end) {
        uint64 q_key = decode_varint(&q_ptr, q_end);
        int q_field_num = q_key >> 3;
        int q_wire_type = q_key & 0x07;
        uint64 q_val = 0;
        bool q_has_val = false;

        if (q_wire_type == 0) {
             q_val = decode_varint(&q_ptr, q_end);
             q_has_val = true;
        } else if (q_wire_type == 1) { q_ptr += 8; }
        else if (q_wire_type == 2) { uint64 len = decode_varint(&q_ptr, q_end); q_ptr += len; }
        else if (q_wire_type == 5) { q_ptr += 4; }

        const char *b_ptr = base->data;
        const char *b_end = (const char *) base + VARSIZE(base);
        bool found_tag = false;

        while (b_ptr < b_end) {
            uint64 b_key = decode_varint(&b_ptr, b_end);
            int b_field_num = b_key >> 3;
            int b_wire_type = b_key & 0x07;

            if (b_field_num == q_field_num) {
                if (b_wire_type == 0 && q_has_val) {
                    uint64 b_val = decode_varint(&b_ptr, b_end);
                    if (b_val == q_val) {
                         found_tag = true;
                         break;
                    }
                } else if (b_wire_type == 0) { decode_varint(&b_ptr, b_end); }
                else if (b_wire_type == 1) { b_ptr += 8; }
                else if (b_wire_type == 2) { uint64 len = decode_varint(&b_ptr, b_end); b_ptr += len; }
                else if (b_wire_type == 5) { b_ptr += 4; }
                
                if (!q_has_val) { // We only check tag existence if no value was provided (or wire type wasn't 0)
                     found_tag = true;
                     break;
                }
            } else {
                if (b_wire_type == 0) { decode_varint(&b_ptr, b_end); }
                else if (b_wire_type == 1) { b_ptr += 8; }
                else if (b_wire_type == 2) { uint64 len = decode_varint(&b_ptr, b_end); b_ptr += len; }
                else if (b_wire_type == 5) { b_ptr += 4; }
            }
        }

        if (!found_tag) {
            match_all = false;
            break;
        }
    }

    PG_RETURN_BOOL(match_all);
}






PG_FUNCTION_INFO_V1(pb_register_schema);

Datum
pb_register_schema(PG_FUNCTION_ARGS)
{
    text *name = PG_GETARG_TEXT_P(0);
    bytea *data = PG_GETARG_BYTEA_P(1);
    char *name_str = text_to_cstring(name);
    
    int ret;
    Oid argtypes[2];
    Datum Values[2];
    char Nulls[2] = {' ', ' '}; // Not nulls

    upb_Arena *arena;
    size_t data_len;
    char *data_ptr;
    google_protobuf_FileDescriptorSet *set;
    size_t file_count;
    const google_protobuf_FileDescriptorProto *const *files;

    argtypes[0] = TEXTOID;
    Values[0] = PointerGetDatum(name);
    
    argtypes[1] = BYTEAOID;
    Values[1] = PointerGetDatum(data);

    SPI_connect();
    
    ret = SPI_execute_with_args("INSERT INTO pb_schemas (name, data) VALUES ($1, $2)",
                                2, argtypes, Values, Nulls, false, 0);
                                
    if (ret != SPI_OK_INSERT) {
        elog(ERROR, "Failed to insert schema into database: %d", ret);
    }

    SPI_finish();

    // 2. Load into Session Cache (upb_DefPool)
    if (s_def_pool == NULL) {
        s_def_pool = upb_DefPool_New();
        if (s_def_pool == NULL) {
            elog(ERROR, "Failed to create upb_DefPool");
        }
    }

    arena = upb_Arena_New();
    if (arena == NULL) {
        elog(ERROR, "Failed to create upb_Arena");
    }

    data_len = VARSIZE_ANY_EXHDR(data);
    data_ptr = VARDATA_ANY(data);

    set = google_protobuf_FileDescriptorSet_parse(data_ptr, data_len, arena);
    if (set == NULL) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to parse FileDescriptorSet binary");
    }

    file_count = 0;
    files = google_protobuf_FileDescriptorSet_file(set, &file_count);

    for (size_t i = 0; i < file_count; i++) {
        upb_Status status;
        const upb_FileDef *file_def;
        
        upb_Status_Clear(&status);
        file_def = upb_DefPool_AddFile(s_def_pool, files[i], &status);
        if (file_def == NULL) {
            upb_Arena_Free(arena);
            elog(ERROR, "Failed to add schema to upb_DefPool: %s", upb_Status_ErrorMessage(&status));
        }
    }


    upb_Arena_Free(arena);

    elog(INFO, "Successfully registered and cached schema: %s", name_str);
    pfree(name_str);

    PG_RETURN_VOID();
}

static void
load_all_schemas_from_db(upb_DefPool *pool)
{
    int ret;
    
    SPI_connect();
    ret = SPI_execute("SELECT data FROM public.pb_schemas", true, 0);
    
    if (ret == SPI_OK_SELECT) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        
        for (int i = 0; i < tuptable->numvals; i++) {
            HeapTuple tuple = tuptable->vals[i];
            bool is_null;
            Datum data_datum = SPI_getbinval(tuple, tupdesc, 1, &is_null);
            
            if (!is_null) {
                bytea *data = DatumGetByteaP(data_datum);
                size_t data_len = VARSIZE_ANY_EXHDR(data);
                char *data_ptr = VARDATA_ANY(data);
                
                upb_Arena *arena = upb_Arena_New();
                if (arena) {
                    google_protobuf_FileDescriptorSet *set = google_protobuf_FileDescriptorSet_parse(data_ptr, data_len, arena);
                    if (set) {
                        size_t file_count = 0;
                        const google_protobuf_FileDescriptorProto *const *files = google_protobuf_FileDescriptorSet_file(set, &file_count);
                        
                        for (size_t j = 0; j < file_count; j++) {
                            upb_Status status;
                            
                            upb_Status_Clear(&status);
                            if (!upb_DefPool_AddFile(pool, files[j], &status)) {
                                elog(WARNING, "Failed to add file to DefPool: %s", upb_Status_ErrorMessage(&status));
                            }
                        }
                    } else {
                        elog(WARNING, "Failed to parse FileDescriptorSet");
                    }
                    upb_Arena_Free(arena);
                } else {
                    elog(WARNING, "Failed to create arena for schema loading");
                }
            }
        }
    }
    
    SPI_finish();
}


PG_FUNCTION_INFO_V1(pb_get_int32_by_name);
Datum
pb_get_int32_by_name(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    text *msg_name_text = PG_GETARG_TEXT_P(1);
    text *field_name_text = PG_GETARG_TEXT_P(2);
    char *msg_name = text_to_cstring(msg_name_text);
    char *field_name = text_to_cstring(field_name_text);

    const upb_MessageDef *msg_def;
    const upb_FieldDef *field_def;
    uint32_t field_number;

    const char *ptr;
    const char *end;

    if (s_def_pool == NULL) {
        s_def_pool = upb_DefPool_New();
        if (s_def_pool) {
            load_all_schemas_from_db(s_def_pool);
        }
    }

    if (s_def_pool == NULL) {
        elog(ERROR, "Failed to initialize Schema Registry");
    }

    msg_def = upb_DefPool_FindMessageByName(s_def_pool, msg_name);
    if (!msg_def) {
        elog(ERROR, "Message not found in schema registry: %s", msg_name);
    }

    field_def = upb_MessageDef_FindFieldByName(msg_def, field_name);
    if (!field_def) {
        elog(ERROR, "Field not found in message %s: %s", msg_name, field_name);
    }

    field_number = upb_FieldDef_Number(field_def);

    pfree(msg_name);
    pfree(field_name);

    // Now call existing extraction logic
    ptr = data->data;
    end = (const char *) data + VARSIZE(data);

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = key >> 3;
        int wire_type = key & 0x07;

        if (field_num == field_number) {
            if (wire_type == 0) { // Varint
                uint64 val = decode_varint(&ptr, end);
                PG_RETURN_INT32((int32) val);
            } else {
                elog(ERROR, "Expected varint wire type for field %u, got %d", field_number, wire_type);
            }
        }

        switch (wire_type) {
            case 0:
                decode_varint(&ptr, end);
                break;
            case 1:
                ptr += 8;
                break;
            case 2:
                {
                    uint64 len = decode_varint(&ptr, end);
                    ptr += len;
                }
                break;
            case 5:
                ptr += 4;
                break;
            default:
                elog(ERROR, "Unsupported wire type %d", wire_type);
        }
    }

    PG_RETURN_NULL();
}




PG_FUNCTION_INFO_V1(pb_get_int32_by_name_dot);
Datum
pb_get_int32_by_name_dot(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    text *full_name_text = PG_GETARG_TEXT_P(1);
    char *full_name = text_to_cstring(full_name_text);
    char *dot = strchr(full_name, '.');
    char *msg_name;
    char *field_name;

    const upb_MessageDef *msg_def;
    const upb_FieldDef *field_def;
    uint32_t field_number;

    const char *ptr;
    const char *end;

    if (!dot) {
        elog(ERROR, "Name must be in the format 'MessageName.FieldName'");
    }

    *dot = '\0';
    msg_name = full_name;
    field_name = dot + 1;

    if (s_def_pool == NULL) {
        s_def_pool = upb_DefPool_New();
        if (s_def_pool) {
            load_all_schemas_from_db(s_def_pool);
        }
    }

    if (s_def_pool == NULL) {
        elog(ERROR, "Failed to initialize Schema Registry");
    }

    msg_def = upb_DefPool_FindMessageByName(s_def_pool, msg_name);
    if (!msg_def) {
        elog(ERROR, "Message not found in schema registry: %s", msg_name);
    }

    field_def = upb_MessageDef_FindFieldByName(msg_def, field_name);
    if (!field_def) {
        elog(ERROR, "Field not found in message %s: %s", msg_name, field_name);
    }

    field_number = upb_FieldDef_Number(field_def);

    ptr = data->data;
    end = (const char *) data + VARSIZE(data);

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = key >> 3;
        int wire_type = key & 0x07;

        if (field_num == field_number) {
            if (wire_type == 0) { // Varint
                uint64 val = decode_varint(&ptr, end);
                pfree(full_name);
                PG_RETURN_INT32((int32) val);
            } else {
                elog(ERROR, "Expected varint wire type for field %u, got %d", field_number, wire_type);
            }
        }

        switch (wire_type) {
            case 0:
                decode_varint(&ptr, end);
                break;
            case 1:
                ptr += 8;
                break;
            case 2:
                {
                    uint64 len = decode_varint(&ptr, end);
                    ptr += len;
                }
                break;
            case 5:
                ptr += 4;
                break;
            default:
                elog(ERROR, "Unsupported wire type %d", wire_type);
        }
    }

    pfree(full_name);
    PG_RETURN_NULL();
}



/* 
 * GIN Support Functions (Schema-less Tag Numbers)
 */

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
    // StrategyNumber strategy = PG_GETARG_UINT16(2);
    // bool **pmatch = (bool **) PG_GETARG_POINTER(3);
    // Pointer **extra_data = (Pointer **) PG_GETARG_POINTER(4);
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
    // StrategyNumber strategy = PG_GETARG_UINT16(1);
    // Datum query = PG_GETARG_DATUM(2);
    int32 nkeys = PG_GETARG_INT32(3);
    // Pointer *extra_data = (Pointer *) PG_GETARG_POINTER(4);
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



PG_FUNCTION_INFO_V1(pb_get_int32_by_path);

Datum
pb_get_int32_by_path(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ArrayType *path_array = PG_GETARG_ARRAYTYPE_P(1);
    
    int16 typlen;
    bool typbyval;
    char typalign;
    Datum *elems;
    bool *nulls;
    int nelems;

    get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
    deconstruct_array(path_array, TEXTOID, typlen, typbyval, typalign, &elems, &nulls, &nelems);
    
    if (nelems == 0) PG_RETURN_NULL();

    char *msg_name = text_to_cstring(DatumGetTextPP(elems[0]));

    if (s_def_pool == NULL) {
        s_def_pool = upb_DefPool_New();
        if (s_def_pool) {
            load_all_schemas_from_db(s_def_pool);
        }
    }

    if (s_def_pool == NULL) {
        elog(ERROR, "Failed to initialize Schema Registry");
    }

    const upb_MessageDef *msg_def = upb_DefPool_FindMessageByName(s_def_pool, msg_name);
    if (!msg_def) {
        elog(ERROR, "Message not found in schema registry: %s", msg_name);
    }

    const char *ptr = data->data;
    const char *end = (const char *) data + VARSIZE(data);

    for (int i = 1; i < nelems; i++) {
        char *field_name = text_to_cstring(DatumGetTextPP(elems[i]));
        const upb_FieldDef *field_def = upb_MessageDef_FindFieldByName(msg_def, field_name);
        if (!field_def) {
            pfree(field_name);
            pfree(msg_name);
            PG_RETURN_NULL();
        }

        uint32_t field_number = upb_FieldDef_Number(field_def);
        bool is_repeated = upb_FieldDef_IsRepeated(field_def);
        bool is_map = upb_FieldDef_IsMap(field_def);
        
        int target_index = 0;
        bool has_index = false;
        char *map_key = NULL;
        
        if (is_repeated && !is_map && i + 1 < nelems) {
            char *next_elem = text_to_cstring(DatumGetTextPP(elems[i + 1]));
            if (isdigit(next_elem[0])) {
                target_index = atoi(next_elem);
                has_index = true;
                i++; // Consume index
            }
            pfree(next_elem);
        } else if (is_map && i + 1 < nelems) {
            map_key = text_to_cstring(DatumGetTextPP(elems[i + 1]));
            i++; // Consume key
        }

        bool found = false;
        int current_idx = 0;

        while (ptr < end) {
            uint64 key = decode_varint(&ptr, end);
            int field_num = key >> 3;
            int wire_type = key & 0x07;

            if (field_num == field_number) {
                if (is_map) {
                    if (wire_type == 2) {
                        uint64 len = decode_varint(&ptr, end);
                        const char *entry_ptr = ptr;
                        const char *entry_end = ptr + len;
                        ptr += len; // Advance main pointer over entry
                        
                        const upb_MessageDef *entry_def = upb_FieldDef_MessageSubDef(field_def);
                        const upb_FieldDef *key_field = upb_MessageDef_FindFieldByNumber(entry_def, 1);
                        
                        bool key_matched = false;
                        uint64 val = 0;
                        bool val_found = false;

                        while (entry_ptr < entry_end) {
                            uint64 entry_key = decode_varint(&entry_ptr, entry_end);
                            int entry_num = entry_key >> 3;
                            int entry_wire = entry_key & 0x07;

                            if (entry_num == 1) { // Key
                                if (upb_FieldDef_Type(key_field) == kUpb_FieldType_String) {
                                    uint64 key_len = decode_varint(&entry_ptr, entry_end);
                                    if (key_len == strlen(map_key) && memcmp(entry_ptr, map_key, key_len) == 0) {
                                        key_matched = true;
                                    }
                                    entry_ptr += key_len;
                                } else if (upb_FieldDef_Type(key_field) == kUpb_FieldType_Int32) {
                                    uint64 key_val = decode_varint(&entry_ptr, entry_end);
                                    if (key_val == atoi(map_key)) {
                                        key_matched = true;
                                    }
                                }
                            } else if (entry_num == 2) { // Value
                                if (entry_wire == 0) {
                                    val = decode_varint(&entry_ptr, entry_end);
                                    val_found = true;
                                } else {
                                     if (entry_wire == 1) entry_ptr += 8;
                                     else if (entry_wire == 2) { uint64 l = decode_varint(&entry_ptr, entry_end); entry_ptr += l; }
                                     else if (entry_wire == 5) entry_ptr += 4;
                                }
                            } else {
                                 if (entry_wire == 0) decode_varint(&entry_ptr, entry_end);
                                 else if (entry_wire == 1) entry_ptr += 8;
                                 else if (entry_wire == 2) { uint64 l = decode_varint(&entry_ptr, entry_end); entry_ptr += l; }
                                 else if (entry_wire == 5) entry_ptr += 4;
                            }
                        }

                        if (key_matched && val_found) {
                            found = true;
                            if (i == nelems - 1) {
                                pfree(field_name);
                                pfree(msg_name);
                                if (map_key) pfree(map_key);
                                PG_RETURN_INT32((int32) val);
                            } else {
                                elog(ERROR, "Map value traversal beyond int32 not supported yet");
                            }
                        }
                    }
                } else if (is_repeated) {
                    if (wire_type == 2) { 
                        uint64 len = decode_varint(&ptr, end);
                        if (upb_FieldDef_Type(field_def) == kUpb_FieldType_Message) {
                            if (current_idx == target_index) {
                                found = true;
                                end = ptr + len;
                                msg_def = upb_FieldDef_MessageSubDef(field_def);
                                break; 
                            } else {
                                ptr += len; 
                                current_idx++;
                            }
                        } else {
                            const char *packed_end = ptr + len;
                            while (ptr < packed_end && current_idx < target_index) {
                                decode_varint(&ptr, packed_end); 
                                current_idx++;
                            }
                            if (ptr < packed_end) {
                                found = true;
                                uint64 val = decode_varint(&ptr, packed_end);
                                if (i == nelems - 1) {
                                    pfree(field_name);
                                    pfree(msg_name);
                                    PG_RETURN_INT32((int32) val);
                                } else {
                                    elog(ERROR, "Cannot traverse into primitive element");
                                }
                            }
                            ptr = packed_end; 
                            break; 
                        }
                    } else { 
                        if (current_idx == target_index) {
                            found = true;
                            uint64 val = decode_varint(&ptr, end); 
                            if (i == nelems - 1) {
                                pfree(field_name);
                                pfree(msg_name);
                                PG_RETURN_INT32((int32) val);
                            } else {
                                elog(ERROR, "Cannot traverse into primitive element");
                            }
                        } else {
                            if (wire_type == 0) decode_varint(&ptr, end);
                            else if (wire_type == 1) ptr += 8;
                            else if (wire_type == 2) { uint64 len = decode_varint(&ptr, end); ptr += len; }
                            else if (wire_type == 5) ptr += 4;
                            current_idx++;
                        }
                    }
                } else {
                    found = true;
                    if (i == nelems - 1) {
                        if (wire_type == 0) {
                            uint64 val = decode_varint(&ptr, end);
                            pfree(field_name);
                            pfree(msg_name);
                            PG_RETURN_INT32((int32) val);
                        } else {
                            elog(ERROR, "Expected varint wire type for field %s, got %d", field_name, wire_type);
                        }
                    } else {
                        if (wire_type == 2) {
                            uint64 len = decode_varint(&ptr, end);
                            end = ptr + len;
                            msg_def = upb_FieldDef_MessageSubDef(field_def);
                            break;
                        } else {
                            elog(ERROR, "Expected length-delimited wire type for submessage %s, got %d", field_name, wire_type);
                        }
                    }
                }
            } else {
                if (wire_type == 0) decode_varint(&ptr, end);
                else if (wire_type == 1) ptr += 8;
                else if (wire_type == 2) { uint64 len = decode_varint(&ptr, end); ptr += len; }
                else if (wire_type == 5) ptr += 4;
            }
        }

        if (map_key) pfree(map_key);
        pfree(field_name);
        if (!found) {
            pfree(msg_name);
            PG_RETURN_NULL();
        }
    }

    pfree(msg_name);
    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pgproto_hello);



Datum
pgproto_hello(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text("pgproto environment is ready."));
}
