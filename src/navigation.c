#include "pgproto.h"
#include <string.h>
#include <ctype.h>

PG_FUNCTION_INFO_V1(pb_get_int32);

/**
 * pb_get_int32: Extracts an int32 value from a Protobuf message by its tag number.
 * 
 * Inputs:
 * - data (protobuf): The Protobuf binary data.
 * - target_tag (int32): The tag number to look for.
 * 
 * Summary:
 * Scans the Protobuf wire format sequentially. When the tag is found, it decodes
 * the Varint value. If the tag is not found, returns NULL.
 */
Datum
pb_get_int32(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    int32 target_tag = PG_GETARG_INT32(1);
    const char *ptr = data->data;
    const char *end = (const char *) data + VARSIZE(data);

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = (int)(key >> PB_FIELD_NUM_SHIFT);
        int wire_type = (int)(key & PB_WIRE_TYPE_MASK);

        if (field_num == target_tag) {
            if (wire_type == PB_WIRE_VARINT) {
                uint64 val = decode_varint(&ptr, end);
                PG_RETURN_INT32((int32) val);
            } else {
                elog(ERROR, "Expected varint wire type for field %d, got %d", target_tag, wire_type);
            }
        }
        skip_field(wire_type, &ptr, end);
    }
    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(protobuf_contains);

/**
 * protobuf_contains: Implementation of the @> operator.
 * Checks if the 'base' protobuf contains all tag-value pairs present in 'query'.
 */
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
        int q_field_num = (int)(q_key >> PB_FIELD_NUM_SHIFT);
        int q_wire_type = (int)(q_key & PB_WIRE_TYPE_MASK);
        uint64 q_val = 0;
        bool q_has_val = false;

        if (q_wire_type == PB_WIRE_VARINT) {
             q_val = decode_varint(&q_ptr, q_end);
             q_has_val = true;
        } else skip_field(q_wire_type, &q_ptr, q_end);

        const char *b_ptr = base->data;
        const char *b_end = (const char *) base + VARSIZE(base);
        bool found_tag = false;

        while (b_ptr < b_end) {
            uint64 b_key = decode_varint(&b_ptr, b_end);
            int b_field_num = (int)(b_key >> PB_FIELD_NUM_SHIFT);
            int b_wire_type = (int)(b_key & PB_WIRE_TYPE_MASK);

            if (b_field_num == q_field_num) {
                if (b_wire_type == PB_WIRE_VARINT && q_has_val) {
                    uint64 b_val = decode_varint(&b_ptr, b_end);
                    if (b_val == q_val) { found_tag = true; break; }
                } else if (!q_has_val) { found_tag = true; break; }
                else skip_field(b_wire_type, &b_ptr, b_end);
            } else skip_field(b_wire_type, &b_ptr, b_end);
        }
        if (!found_tag) { match_all = false; break; }
    }
    PG_RETURN_BOOL(match_all);
}

PG_FUNCTION_INFO_V1(pb_get_int32_by_name);

/**
 * pb_get_int32_by_name: Extracts an int32 value by message and field name.
 */
Datum
pb_get_int32_by_name(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    text *msg_name_text = PG_GETARG_TEXT_P(1);
    text *field_name_text = PG_GETARG_TEXT_P(2);
    char *msg_name = text_to_cstring(msg_name_text);
    char *field_name = text_to_cstring(field_name_text);

    uint32_t field_number;
    PbFieldLookup lookup;
    const char *ptr;
    const char *end;

    pgproto_LoadAllSchemasFromDb();
    PbLookupStatus status = pgproto_lookup_field(msg_name, field_name, &lookup);
    if (status != PB_LOOKUP_OK) {
        if (status == PB_LOOKUP_MSG_NOT_FOUND) {
            elog(ERROR, "Message not found in schema registry: %s", msg_name);
        } else {
            elog(ERROR, "Field %s not found in message %s", field_name, msg_name);
        }
    }
    field_number = lookup.number;

    pfree(msg_name); pfree(field_name);

    ptr = data->data;
    end = (const char *) data + VARSIZE(data);

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = (int)(key >> PB_FIELD_NUM_SHIFT);
        int wire_type = (int)(key & PB_WIRE_TYPE_MASK);

        if (field_num == (int)field_number) {
            if (wire_type == PB_WIRE_VARINT) {
                uint64 val = decode_varint(&ptr, end);
                PG_RETURN_INT32((int32) val);
            } else {
                elog(ERROR, "Expected varint wire type for field %s, got %d", field_name, wire_type);
            }
        }
        skip_field(wire_type, &ptr, end);
    }
    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pb_get_int32_by_name_dot);

/**
 * pb_get_int32_by_name_dot: Extracts an int32 value using dot notation (e.g., "Message.Field").
 */
Datum
pb_get_int32_by_name_dot(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    text *path_text = PG_GETARG_TEXT_P(1);
    char *path = text_to_cstring(path_text);
    
    char *dot = strchr(path, '.');
    if (!dot) { pfree(path); elog(ERROR, "Path must be in format 'Message.Field'"); }
    
    *dot = '\0';
    char *msg_name = path;
    char *field_name = dot + 1;
    uint32_t field_number;
    PbFieldLookup lookup;

    pgproto_LoadAllSchemasFromDb();
    PbLookupStatus status = pgproto_lookup_field(msg_name, field_name, &lookup);
    if (status != PB_LOOKUP_OK) {
        if (status == PB_LOOKUP_MSG_NOT_FOUND) {
            elog(ERROR, "Message not found in schema registry: %s", msg_name);
        } else {
            elog(ERROR, "Field %s not found in message %s", field_name, msg_name);
        }
    }
    field_number = lookup.number;

    const char *ptr = data->data;
    const char *end = (const char *) data + VARSIZE(data);

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = (int)(key >> PB_FIELD_NUM_SHIFT);
        int wire_type = (int)(key & PB_WIRE_TYPE_MASK);
        if (field_num == (int)field_number) {
            if (wire_type == PB_WIRE_VARINT) {
                uint64 val = decode_varint(&ptr, end);
                pfree(path); PG_RETURN_INT32((int32) val);
            } else {
                pfree(path);
                elog(ERROR, "Expected varint wire type for field %s, got %d", field_name, wire_type);
            }
        }
        skip_field(wire_type, &ptr, end);
    }
    pfree(path); PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pb_get_int32_by_path);

/**
 * pb_get_int32_by_path: Implementation of the #> operator for path navigation.
 * 
 * Inputs:
 * - data (protobuf)
 * - path (text[]): E.g., ARRAY['Outer', 'inner', 'id'] or ARRAY['Outer', 'scores', '0']
 * 
 * Summary:
 * Iteratively resolves each path element. If it's a message, it nests the parser
 * into the submessage's length-delimited blob. Supports array indexing and map key lookups.
 */
Datum
pb_get_int32_by_path(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ArrayType *path_array = PG_GETARG_ARRAYTYPE_P(1);
    
    int16 typlen; bool typbyval; char typalign; Datum *elems; bool *nulls; int nelems;
    get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
    deconstruct_array(path_array, TEXTOID, typlen, typbyval, typalign, &elems, &nulls, &nelems);
    
    if (nelems == 0) PG_RETURN_NULL();

    char current_msg[512];
    char *msg_name_str = text_to_cstring(DatumGetTextPP(elems[0]));
    strncpy(current_msg, msg_name_str, 511);
    current_msg[511] = '\0';
    pfree(msg_name_str);

    pgproto_LoadAllSchemasFromDb();

    const char *ptr = data->data;
    const char *end = (const char *) data + VARSIZE(data);

    for (int i = 1; i < nelems; i++) {
        char *field_name = text_to_cstring(DatumGetTextPP(elems[i]));
        PbFieldLookup lookup;
        if (pgproto_lookup_field(current_msg, field_name, &lookup) != PB_LOOKUP_OK) { pfree(field_name); PG_RETURN_NULL(); }

        uint32_t field_number = lookup.number;
        bool is_repeated = lookup.is_repeated;
        bool is_map = lookup.is_map;
        int target_index = 0; char *map_key = NULL;
        
        if (is_repeated && !is_map && i + 1 < nelems) {
            char *next_elem = text_to_cstring(DatumGetTextPP(elems[i + 1]));
            if (isdigit(next_elem[0])) { target_index = atoi(next_elem); i++; }
            pfree(next_elem);
        } else if (is_map && i + 1 < nelems) {
            map_key = text_to_cstring(DatumGetTextPP(elems[i + 1])); i++;
        }

        bool found = false; int current_idx = 0;
        while (ptr < end) {
            uint64 key = decode_varint(&ptr, end);
            int field_num = (int)(key >> PB_FIELD_NUM_SHIFT);
            int wire_type = (int)(key & PB_WIRE_TYPE_MASK);

            if (field_num == (int)field_number) {
                if (is_map) {
                    if (wire_type == PB_WIRE_LENGTH_DELIMITED) {
                        uint64 len = decode_varint(&ptr, end);
                        const char *entry_ptr = ptr; const char *entry_end = ptr + len; ptr += len; 
                        bool key_matched = false; uint64 val = 0; bool val_found = false;
                        while (entry_ptr < entry_end) {
                            uint64 entry_key = decode_varint(&entry_ptr, entry_end);
                            int entry_num = (int)(entry_key >> 3); int entry_wire = (int)(entry_key & 0x07);
                            if (entry_num == 1) { // key
                                if (entry_wire == 2) { // string
                                    uint64 key_len = decode_varint(&entry_ptr, entry_end);
                                    if (key_len == strlen(map_key) && memcmp(entry_ptr, map_key, key_len) == 0) key_matched = true;
                                    entry_ptr += key_len;
                                } else if (entry_wire == 0) { if (decode_varint(&entry_ptr, entry_end) == atoi(map_key)) key_matched = true; }
                            } else if (entry_num == 2) { // value
                                if (entry_wire == 0) { val = decode_varint(&entry_ptr, entry_end); val_found = true; }
                                else skip_field(entry_wire, &entry_ptr, entry_end);
                            } else skip_field(entry_wire, &entry_ptr, entry_end);
                        }
                        if (key_matched && val_found) {
                            found = true;
                            if (i == nelems - 1) {
                                pfree(field_name); if (map_key) pfree(map_key);
                                PG_RETURN_INT32((int32) val);
                            }
                        }
                    }
                } else if (is_repeated) {
                    if (wire_type == PB_WIRE_LENGTH_DELIMITED && lookup.type != PB_TYPE_MESSAGE) {
                        uint64 len = decode_varint(&ptr, end);
                        const char *packed_end = ptr + len;
                        while (ptr < packed_end && current_idx < target_index) { decode_varint(&ptr, packed_end); current_idx++; }
                        if (ptr < packed_end) {
                            found = true; uint64 val = decode_varint(&ptr, packed_end);
                            if (i == nelems - 1) { pfree(field_name); PG_RETURN_INT32((int32) val); }
                        }
                        ptr = packed_end; break;
                    } else {
                        if (current_idx == target_index) {
                            found = true;
                            if (lookup.type == PB_TYPE_MESSAGE) {
                                uint64 len = decode_varint(&ptr, end); const char *next_end = ptr + len;
                                strncpy(current_msg, lookup.type_name, 511);
                                ptr = ptr; end = next_end;
                                break;
                            } else {
                                uint64 val = decode_varint(&ptr, end);
                                if (i == nelems - 1) { pfree(field_name); PG_RETURN_INT32((int32) val); }
                            }
                        } else { skip_field(wire_type, &ptr, end); current_idx++; }
                    }
                } else {
                    found = true;
                    if (i == nelems - 1) {
                        if (wire_type == PB_WIRE_VARINT) {
                            uint64 val = decode_varint(&ptr, end);
                            pfree(field_name); PG_RETURN_INT32((int32) val);
                        } else elog(ERROR, "Expected varint wire type for field %s, got %d", field_name, wire_type);
                    } else {
                        if (wire_type == PB_WIRE_LENGTH_DELIMITED) {
                            uint64 len = decode_varint(&ptr, end);
                            const char *next_end = ptr + len;
                            strncpy(current_msg, lookup.type_name, 511);
                            end = next_end;
                            break;
                        } else elog(ERROR, "Expected length-delimited wire type for submessage %s, got %d", field_name, wire_type);
                    }
                }
            } else skip_field(wire_type, &ptr, end);
        }
        if (map_key) pfree(map_key); pfree(field_name);
        if (!found) PG_RETURN_NULL();
    }
    PG_RETURN_NULL();
}
