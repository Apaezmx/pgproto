#include "pgproto.h"

/*
 * Skip a protobuf field based on its wire type.
 * Updates the pointer to point past the skipped field.
 */
static void
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
        default:
            elog(ERROR, "Unsupported wire type %d in field skip", wire_type);
    }
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
        int field_num = key >> PB_FIELD_NUM_SHIFT;
        int wire_type = key & PB_WIRE_TYPE_MASK;

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
        int q_field_num = q_key >> PB_FIELD_NUM_SHIFT;
        int q_wire_type = q_key & PB_WIRE_TYPE_MASK;
        uint64 q_val = 0;
        bool q_has_val = false;

        if (q_wire_type == PB_WIRE_VARINT) {
             q_val = decode_varint(&q_ptr, q_end);
             q_has_val = true;
        } else if (q_wire_type == PB_WIRE_FIXED64) { q_ptr += 8; }
        else if (q_wire_type == PB_WIRE_LENGTH_DELIMITED) { uint64 len = decode_varint(&q_ptr, q_end); q_ptr += len; }
        else if (q_wire_type == PB_WIRE_FIXED32) { q_ptr += 4; }

        const char *b_ptr = base->data;
        const char *b_end = (const char *) base + VARSIZE(base);
        bool found_tag = false;

        while (b_ptr < b_end) {
            uint64 b_key = decode_varint(&b_ptr, b_end);
            int b_field_num = b_key >> PB_FIELD_NUM_SHIFT;
            int b_wire_type = b_key & PB_WIRE_TYPE_MASK;

            if (b_field_num == q_field_num) {
                if (b_wire_type == PB_WIRE_VARINT && q_has_val) {
                    uint64 b_val = decode_varint(&b_ptr, b_end);
                    if (b_val == q_val) {
                         found_tag = true;
                         break;
                    }
                } else if (b_wire_type == PB_WIRE_VARINT) { decode_varint(&b_ptr, b_end); }
                else if (b_wire_type == PB_WIRE_FIXED64) { b_ptr += 8; }
                else if (b_wire_type == PB_WIRE_LENGTH_DELIMITED) { uint64 len = decode_varint(&b_ptr, b_end); b_ptr += len; }
                else if (b_wire_type == PB_WIRE_FIXED32) { b_ptr += 4; }
                
                if (!q_has_val) { 
                     found_tag = true;
                     break;
                }
            } else {
                if (b_wire_type == PB_WIRE_VARINT) { decode_varint(&b_ptr, b_end); }
                else if (b_wire_type == PB_WIRE_FIXED64) { b_ptr += 8; }
                else if (b_wire_type == PB_WIRE_LENGTH_DELIMITED) { uint64 len = decode_varint(&b_ptr, b_end); b_ptr += len; }
                else if (b_wire_type == PB_WIRE_FIXED32) { b_ptr += 4; }
            }
        }

        if (!found_tag) {
            match_all = false;
            break;
        }
    }

    PG_RETURN_BOOL(match_all);
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

    ptr = data->data;
    end = (const char *) data + VARSIZE(data);

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = key >> PB_FIELD_NUM_SHIFT;
        int wire_type = key & PB_WIRE_TYPE_MASK;

        if (field_num == field_number) {
            if (wire_type == PB_WIRE_VARINT) { // Varint
                uint64 val = decode_varint(&ptr, end);
                PG_RETURN_INT32((int32) val);
            } else {
                elog(ERROR, "Expected varint wire type for field %u, got %d", field_number, wire_type);
            }
        }

        skip_field(wire_type, &ptr, end);
    }

    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pb_get_int32_by_name_dot);

Datum
pb_get_int32_by_name_dot(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    text *path_text = PG_GETARG_TEXT_P(1);
    char *path = text_to_cstring(path_text);
    
    char *dot = strchr(path, '.');
    if (!dot) {
        pfree(path);
        elog(ERROR, "Path must be in format 'Message.Field'");
    }
    
    *dot = '\0';
    char *msg_name = path;
    char *field_name = dot + 1;

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
        pfree(path);
        elog(ERROR, "Failed to initialize Schema Registry");
    }

    msg_def = upb_DefPool_FindMessageByName(s_def_pool, msg_name);
    if (!msg_def) {
        pfree(path);
        elog(ERROR, "Message not found in schema registry: %s", msg_name);
    }

    field_def = upb_MessageDef_FindFieldByName(msg_def, field_name);
    if (!field_def) {
        pfree(path);
        PG_RETURN_NULL();
    }

    field_number = upb_FieldDef_Number(field_def);

    ptr = data->data;
    end = (const char *) data + VARSIZE(data);

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = key >> PB_FIELD_NUM_SHIFT;
        int wire_type = key & PB_WIRE_TYPE_MASK;

        if (field_num == field_number) {
            if (wire_type == PB_WIRE_VARINT) { // Varint
                uint64 val = decode_varint(&ptr, end);
                pfree(path);
                PG_RETURN_INT32((int32) val);
            } else {
                pfree(path);
                elog(ERROR, "Expected varint wire type for field %u, got %d", field_number, wire_type);
            }
        }

        skip_field(wire_type, &ptr, end);
    }

    pfree(path);
    PG_RETURN_NULL();
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
            int field_num = key >> PB_FIELD_NUM_SHIFT;
            int wire_type = key & PB_WIRE_TYPE_MASK;

            if (field_num == field_number) {
                if (is_map) {
                    /*
                     * Map fields are encoded as a repeated sequence of submessages.
                     * Each submessage (map entry) contains:
                     * - Field 1: The key
                     * - Field 2: The value
                     * We must parse this submessage to find the matching key.
                     */
                    if (wire_type == PB_WIRE_LENGTH_DELIMITED) {
                        uint64 len = decode_varint(&ptr, end);
                        const char *entry_ptr = ptr;
                        const char *entry_end = ptr + len;
                        ptr += len; 
                        
                        const upb_MessageDef *entry_def = upb_FieldDef_MessageSubDef(field_def);
                        const upb_FieldDef *key_field = upb_MessageDef_FindFieldByNumber(entry_def, 1);
                        
                        bool key_matched = false;
                        uint64 val = 0;
                        bool val_found = false;

                        while (entry_ptr < entry_end) {
                            uint64 entry_key = decode_varint(&entry_ptr, entry_end);
                            int entry_num = entry_key >> PB_FIELD_NUM_SHIFT;
                            int entry_wire = entry_key & PB_WIRE_TYPE_MASK;

                            if (entry_num == 1) { 
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
                            } else if (entry_num == 2) { 
                                if (entry_wire == PB_WIRE_VARINT) {
                                    val = decode_varint(&entry_ptr, entry_end);
                                    val_found = true;
                                } else {
                                     skip_field(entry_wire, &entry_ptr, entry_end);
                                }
                            } else {
                                 skip_field(entry_wire, &entry_ptr, entry_end);
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
                    if (wire_type == PB_WIRE_LENGTH_DELIMITED) { 
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
                            skip_field(wire_type, &ptr, end);
                            current_idx++;
                        }
                    }
                } else {
                    found = true;
                    if (i == nelems - 1) {
                        if (wire_type == PB_WIRE_VARINT) {
                            uint64 val = decode_varint(&ptr, end);
                            pfree(field_name);
                            pfree(msg_name);
                            PG_RETURN_INT32((int32) val);
                        } else {
                            elog(ERROR, "Expected varint wire type for field %s, got %d", field_name, wire_type);
                        }
                    } else {
                        if (wire_type == PB_WIRE_LENGTH_DELIMITED) {
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
                skip_field(wire_type, &ptr, end);
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
