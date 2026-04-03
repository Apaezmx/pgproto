#include "pgproto.h"
#include <string.h>
#include "upb/reflection/message.h"
#include "upb/collections/array.h"
#include "upb/collections/map.h"
#include "upb/wire/decode.h"
#include "upb/wire/encode.h"
#include "upb/message/copy.h"


PG_FUNCTION_INFO_V1(pb_set);

Datum
pb_set(PG_FUNCTION_ARGS)
{
    ProtobufData *data;
    ArrayType *path_array;
    text *new_val_text;
    bool create_if_missing;

    int16 typlen;
    bool typbyval;
    char typalign;
    Datum *elems;
    bool *nulls;
    int nelems;

    char *msg_name;
    const upb_MessageDef *msg_def;
    const upb_MiniTable *mini_table;
    upb_Arena *arena;
    upb_Message *msg;
    const char *buf;
    size_t size;
    upb_DecodeStatus status;
    char *out_buf;
    size_t out_size;
    upb_EncodeStatus enc_status;
    ProtobufData *result;

    data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    path_array = PG_GETARG_ARRAYTYPE_P(1);
    new_val_text = PG_GETARG_TEXT_P(2);
    create_if_missing = PG_GETARG_BOOL(3);

    get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
    deconstruct_array(path_array, TEXTOID, typlen, typbyval, typalign, &elems, &nulls, &nelems);

    if (nelems == 0) {
        PG_RETURN_POINTER(data);
    }

    msg_name = text_to_cstring(DatumGetTextPP(elems[0]));

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

    mini_table = upb_MessageDef_MiniTable(msg_def);
    
    arena = upb_Arena_New();
    msg = upb_Message_New(mini_table, arena);

    buf = data->data;
    size = VARSIZE(data) - VARHDRSZ;

    status = upb_Decode(buf, size, msg, mini_table, NULL, 0, arena);
    if (status != kUpb_DecodeStatus_Ok) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to decode protobuf message: %d", status);
    }

    {
        char *field_name;
        const upb_FieldDef *field_def;
        upb_CType ctype;
        upb_MessageValue val;
        char *new_val_str;

        if (nelems != 2) {
            elog(ERROR, "Only paths of length 2 are supported in this prototype (message_name, field_name)");
        }

        field_name = text_to_cstring(DatumGetTextPP(elems[1]));
        field_def = upb_MessageDef_FindFieldByName(msg_def, field_name);
        
        if (!field_def) {
            elog(ERROR, "Field %s not found in message %s", field_name, msg_name);
        }

        ctype = upb_FieldDef_CType(field_def);
        new_val_str = text_to_cstring(new_val_text);

        switch (ctype) {
            case kUpb_CType_Int32:
                val.int32_val = atoi(new_val_str);
                break;
            case kUpb_CType_Float:
                val.float_val = atof(new_val_str);
                break;
            case kUpb_CType_Bool:
                if (strcmp(new_val_str, "true") == 0) val.bool_val = true;
                else if (strcmp(new_val_str, "false") == 0) val.bool_val = false;
                else elog(ERROR, "Invalid boolean value: %s", new_val_str);
                break;
            case kUpb_CType_String:
                {
                    char *arena_str = upb_Arena_Malloc(arena, strlen(new_val_str) + 1);
                    strcpy(arena_str, new_val_str);
                    val.str_val.data = arena_str;
                    val.str_val.size = strlen(new_val_str);
                }
                break;
            default:
                elog(ERROR, "Unsupported type for modification: %d", ctype);
        }

        upb_Message_SetFieldByDef(msg, field_def, val, arena);
        pfree(field_name);
        pfree(new_val_str);
    }
    (void)create_if_missing;

    out_buf = NULL;
    out_size = 0;
    enc_status = upb_Encode(msg, mini_table, 0, arena, &out_buf, &out_size);
    
    if (enc_status != kUpb_EncodeStatus_Ok) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to encode protobuf message: %d", enc_status);
    }

    result = (ProtobufData *) palloc(VARHDRSZ + out_size);
    SET_VARSIZE(result, VARHDRSZ + out_size);
    memcpy(result->data, out_buf, out_size);

    upb_Arena_Free(arena);
    pfree(msg_name);

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(pb_insert);

Datum
pb_insert(PG_FUNCTION_ARGS)
{
    ProtobufData *data;
    ArrayType *path_array;
    text *new_val_text;

    int16 typlen;
    bool typbyval;
    char typalign;
    Datum *elems;
    bool *nulls;
    int nelems;

    char *msg_name;
    const upb_MessageDef *msg_def;
    const upb_MiniTable *mini_table;
    upb_Arena *arena;
    upb_Message *msg;
    const char *buf;
    size_t size;
    upb_DecodeStatus status;
    char *out_buf;
    size_t out_size;
    upb_EncodeStatus enc_status;
    ProtobufData *result;

    data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    path_array = PG_GETARG_ARRAYTYPE_P(1);
    new_val_text = PG_GETARG_TEXT_P(2);

    get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
    deconstruct_array(path_array, TEXTOID, typlen, typbyval, typalign, &elems, &nulls, &nelems);

    if (nelems != 3) {
        elog(ERROR, "pb_insert requires a path of length 3 (message_name, field_name, index/key)");
    }

    msg_name = text_to_cstring(DatumGetTextPP(elems[0]));

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

    mini_table = upb_MessageDef_MiniTable(msg_def);
    
    arena = upb_Arena_New();
    msg = upb_Message_New(mini_table, arena);

    buf = data->data;
    size = VARSIZE(data) - VARHDRSZ;

    status = upb_Decode(buf, size, msg, mini_table, NULL, 0, arena);
    if (status != kUpb_DecodeStatus_Ok) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to decode protobuf message: %d", status);
    }

    {
        char *field_name;
        const upb_FieldDef *field_def;
        char *key_or_idx_str;

        field_name = text_to_cstring(DatumGetTextPP(elems[1]));
        field_def = upb_MessageDef_FindFieldByName(msg_def, field_name);
        
        if (!field_def) {
            elog(ERROR, "Field %s not found in message %s", field_name, msg_name);
        }

        key_or_idx_str = text_to_cstring(DatumGetTextPP(elems[2]));

        if (upb_FieldDef_IsRepeated(field_def) && !upb_FieldDef_IsMap(field_def)) {
            // Array Insertion
            int index = atoi(key_or_idx_str);
            upb_MutableMessageValue mut_val = upb_Message_Mutable(msg, field_def, arena);
            upb_Array *arr = mut_val.array;
            size_t arr_size = upb_Array_Size(arr);

            if (index < 0 || index > arr_size) {
                elog(ERROR, "Array index out of bounds: %d, size: %zu", index, arr_size);
            }

            if (upb_Array_Insert(arr, index, 1, arena)) {
                upb_MessageValue new_elem_val;
                char *new_val_str = text_to_cstring(new_val_text);
                upb_CType ctype = upb_FieldDef_CType(field_def);

                // Parse value based on type
                switch (ctype) {
                    case kUpb_CType_Int32:
                        new_elem_val.int32_val = atoi(new_val_str);
                        break;
                    case kUpb_CType_String:
                        {
                            char *arena_str = upb_Arena_Malloc(arena, strlen(new_val_str) + 1);
                            strcpy(arena_str, new_val_str);
                            new_elem_val.str_val.data = arena_str;
                            new_elem_val.str_val.size = strlen(new_val_str);
                        }
                        break;
                    // Add more types as needed
                    default:
                        elog(ERROR, "Unsupported type for array insertion: %d", ctype);
                }
                upb_Array_Set(arr, index, new_elem_val);
                pfree(new_val_str);
            } else {
                elog(ERROR, "Failed to insert into array");
            }
        } else if (upb_FieldDef_IsMap(field_def)) {
            // Map Insertion
            upb_MutableMessageValue mut_val = upb_Message_Mutable(msg, field_def, arena);
            upb_Map *map = mut_val.map;
            upb_MessageValue key_val;
            
            // Assume string keys for now for prototype
            key_val.str_val.data = key_or_idx_str;
            key_val.str_val.size = strlen(key_or_idx_str);

            if (upb_Map_Get(map, key_val, NULL)) {
                 elog(ERROR, "cannot replace existing key");
            }

            // Parse value
            {
                upb_MessageValue new_elem_val;
                char *new_val_str = text_to_cstring(new_val_text);
                
                const upb_MessageDef *entry_def = upb_FieldDef_MessageSubDef(field_def);
                const upb_FieldDef *val_field_def = upb_MessageDef_FindFieldByNumber(entry_def, 2); // 2 is value
                upb_CType val_ctype = upb_FieldDef_CType(val_field_def);

                switch (val_ctype) {
                    case kUpb_CType_Int32:
                        new_elem_val.int32_val = atoi(new_val_str);
                        break;
                    case kUpb_CType_String:
                        {
                            char *arena_str = upb_Arena_Malloc(arena, strlen(new_val_str) + 1);
                            strcpy(arena_str, new_val_str);
                            new_elem_val.str_val.data = arena_str;
                            new_elem_val.str_val.size = strlen(new_val_str);
                        }
                        break;
                    default:
                        elog(ERROR, "Unsupported map value type: %d", val_ctype);
                }

                upb_Map_Insert(map, key_val, new_elem_val, arena);
                pfree(new_val_str);
            }
        } else {
            elog(ERROR, "Field %s is not a repeated or map field", field_name);
        }

        pfree(field_name);
        pfree(key_or_idx_str);
    }

    out_buf = NULL;
    out_size = 0;
    enc_status = upb_Encode(msg, mini_table, 0, arena, &out_buf, &out_size);
    
    if (enc_status != kUpb_EncodeStatus_Ok) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to encode protobuf message: %d", enc_status);
    }

    result = (ProtobufData *) palloc(VARHDRSZ + out_size);
    SET_VARSIZE(result, VARHDRSZ + out_size);
    memcpy(result->data, out_buf, out_size);

    upb_Arena_Free(arena);
    pfree(msg_name);

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(pb_delete);

Datum
pb_delete(PG_FUNCTION_ARGS)
{
    ProtobufData *data;
    ArrayType *path_array;

    int16 typlen;
    bool typbyval;
    char typalign;
    Datum *elems;
    bool *nulls;
    int nelems;

    char *msg_name;
    const upb_MessageDef *msg_def;
    const upb_MiniTable *mini_table;
    upb_Arena *arena;
    upb_Message *msg;
    const char *buf;
    size_t size;
    upb_DecodeStatus status;
    char *out_buf;
    size_t out_size;
    upb_EncodeStatus enc_status;
    ProtobufData *result;

    data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    path_array = PG_GETARG_ARRAYTYPE_P(1);

    get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
    deconstruct_array(path_array, TEXTOID, typlen, typbyval, typalign, &elems, &nulls, &nelems);

    if (nelems < 2 || nelems > 3) {
        elog(ERROR, "pb_delete requires a path of length 2 or 3 (message_name, field_name [, index/key])");
    }

    msg_name = text_to_cstring(DatumGetTextPP(elems[0]));

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

    mini_table = upb_MessageDef_MiniTable(msg_def);
    
    arena = upb_Arena_New();
    msg = upb_Message_New(mini_table, arena);

    buf = data->data;
    size = VARSIZE(data) - VARHDRSZ;

    status = upb_Decode(buf, size, msg, mini_table, NULL, 0, arena);
    if (status != kUpb_DecodeStatus_Ok) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to decode protobuf message: %d", status);
    }

    {
        char *field_name;
        const upb_FieldDef *field_def;

        field_name = text_to_cstring(DatumGetTextPP(elems[1]));
        field_def = upb_MessageDef_FindFieldByName(msg_def, field_name);
        
        if (!field_def) {
            elog(ERROR, "Field %s not found in message %s", field_name, msg_name);
        }

        if (nelems == 2) {
            // Delete/Clear field from message
            upb_Message_ClearFieldByDef(msg, field_def);
        } else if (nelems == 3) {
            char *key_or_idx_str = text_to_cstring(DatumGetTextPP(elems[2]));

            if (upb_FieldDef_IsRepeated(field_def) && !upb_FieldDef_IsMap(field_def)) {
                // Array Deletion
                int index = atoi(key_or_idx_str);
                upb_MutableMessageValue mut_val = upb_Message_Mutable(msg, field_def, arena);
                upb_Array *arr = mut_val.array;
                size_t arr_size = upb_Array_Size(arr);

                if (index < 0 || index >= arr_size) {
                    elog(ERROR, "Array index out of bounds: %d, size: %zu", index, arr_size);
                }

                upb_Array_Delete(arr, index, 1);
            } else if (upb_FieldDef_IsMap(field_def)) {
                // Map Deletion
                upb_MutableMessageValue mut_val = upb_Message_Mutable(msg, field_def, arena);
                upb_Map *map = mut_val.map;
                upb_MessageValue key_val;
                
                // Assume string keys for now
                key_val.str_val.data = key_or_idx_str;
                key_val.str_val.size = strlen(key_or_idx_str);

                upb_Map_Delete(map, key_val, NULL);
            } else {
                elog(ERROR, "Field %s is not a repeated or map field", field_name);
            }
            pfree(key_or_idx_str);
        }

        pfree(field_name);
    }

    out_buf = NULL;
    out_size = 0;
    enc_status = upb_Encode(msg, mini_table, 0, arena, &out_buf, &out_size);
    
    if (enc_status != kUpb_EncodeStatus_Ok) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to encode protobuf message: %d", enc_status);
    }

    result = (ProtobufData *) palloc(VARHDRSZ + out_size);
    SET_VARSIZE(result, VARHDRSZ + out_size);
    memcpy(result->data, out_buf, out_size);

    upb_Arena_Free(arena);
    pfree(msg_name);

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(pb_merge);

Datum
pb_merge(PG_FUNCTION_ARGS)
{
    ProtobufData *data1;
    ProtobufData *data2;
    size_t size1;
    size_t size2;
    ProtobufData *result;

    data1 = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    data2 = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));

    size1 = VARSIZE(data1) - VARHDRSZ;
    size2 = VARSIZE(data2) - VARHDRSZ;

    result = (ProtobufData *) palloc(VARHDRSZ + size1 + size2);
    SET_VARSIZE(result, VARHDRSZ + size1 + size2);
    
    memcpy(result->data, data1->data, size1);
    memcpy(result->data + size1, data2->data, size2);

    PG_RETURN_POINTER(result);
}


