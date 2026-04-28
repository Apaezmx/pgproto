#include "pgproto.h"
#include <string.h>

/**
 * mutation_filter_tag: Copies Protobuf data while filtering out a specific tag.
 */
static void
mutation_filter_tag(const char *ptr, const char *end, uint32_t tag_to_skip, StringInfo buf)
{
    while (ptr < end) {
        const char *start = ptr;
        uint64 key = decode_varint(&ptr, end);
        int field_num = (int)(key >> PB_FIELD_NUM_SHIFT);
        int wire_type = (int)(key & PB_WIRE_TYPE_MASK);
        if (field_num == (int)tag_to_skip) skip_field(wire_type, &ptr, end);
        else { skip_field(wire_type, &ptr, end); appendBinaryStringInfo(buf, start, (int)(ptr - start)); }
    }
}

/**
 * mutation_encode_value: Encodes a scalar value with a customizable error prefix.
 */
static void
mutation_encode_value(PbFieldLookup *lookup, const char *val_str, StringInfo buf, const char *err_prefix)
{
    if (lookup->type == PB_TYPE_INT32 || lookup->type == PB_TYPE_INT64) {
        encode_varint(PB_FIELD_TAG(lookup->number, PB_WIRE_VARINT), buf);
        encode_varint((uint64)atoll(val_str), buf);
    } else if (lookup->type == PB_TYPE_BOOL) {
        encode_varint(PB_FIELD_TAG(lookup->number, PB_WIRE_VARINT), buf);
        bool bval = (strcasecmp(val_str, "true") == 0 || (isdigit(val_str[0]) && atoi(val_str) != 0));
        encode_varint(bval ? 1 : 0, buf);
    } else if (lookup->type == PB_TYPE_STRING) {
        encode_varint(PB_FIELD_TAG(lookup->number, PB_WIRE_LENGTH_DELIMITED), buf);
        encode_varint((uint64)strlen(val_str), buf);
        appendStringInfoString(buf, val_str);
    } else {
        elog(ERROR, "%s: %d", err_prefix, lookup->type);
    }
}

/**
 * mutation_get_path_info: Helper to extract path and resolve field metadata with exact error parity.
 */
static void
mutation_get_path_info(ArrayType *path_array, const char *func_name, char **msg_name, char **field_name, char **key_str, PbFieldLookup *lookup)
{
    int16 typlen; bool typbyval; char typalign; Datum *elems; bool *nulls; int nelems;
    get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
    deconstruct_array(path_array, TEXTOID, typlen, typbyval, typalign, &elems, &nulls, &nelems);

    if (strcmp(func_name, "pb_set") == 0) {
        if (nelems != 2) elog(ERROR, "Only paths of length 2 are supported in this prototype (message_name, field_name)");
    } else if (strcmp(func_name, "pb_insert") == 0) {
        if (nelems != 3) elog(ERROR, "pb_insert requires a path of length 3 (message_name, field_name, index/key)");
    } else if (strcmp(func_name, "pb_delete") == 0) {
        if (nelems < 2 || nelems > 3) elog(ERROR, "pb_delete requires a path of length 2 or 3 (message_name, field_name [, index/key])");
    }

    *msg_name = text_to_cstring(DatumGetTextPP(elems[0]));
    *field_name = text_to_cstring(DatumGetTextPP(elems[1]));
    if (key_str && nelems > 2) *key_str = text_to_cstring(DatumGetTextPP(elems[2]));

    pgproto_LoadAllSchemasFromDb();
    PbLookupStatus status = pgproto_lookup_field(*msg_name, *field_name, lookup);
    if (status != PB_LOOKUP_OK) {
        if (status == PB_LOOKUP_MSG_NOT_FOUND) {
            elog(ERROR, "Message not found in schema registry: %s", *msg_name);
        } else {
            elog(ERROR, "Field %s not found in message %s", *field_name, *msg_name);
        }
    }
}

PG_FUNCTION_INFO_V1(pb_set);
Datum
pb_set(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ArrayType *path_array = PG_GETARG_ARRAYTYPE_P(1);
    char *new_val_str = text_to_cstring(PG_GETARG_TEXT_P(2));
    char *msg_name, *field_name;
    PbFieldLookup lookup;

    mutation_get_path_info(path_array, "pb_set", &msg_name, &field_name, NULL, &lookup);
    
    StringInfoData buf; initStringInfo(&buf);
    mutation_filter_tag(data->data, data->data + VARSIZE(data) - VARHDRSZ, lookup.number, &buf);
    mutation_encode_value(&lookup, new_val_str, &buf, "Unsupported type for modification");

    ProtobufData *result = (ProtobufData *) palloc(VARHDRSZ + buf.len);
    SET_VARSIZE(result, VARHDRSZ + buf.len);
    memcpy(result->data, buf.data, buf.len);
    pfree(buf.data);
    pfree(msg_name); pfree(field_name); pfree(new_val_str);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(pb_insert);
Datum
pb_insert(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ArrayType *path_array = PG_GETARG_ARRAYTYPE_P(1);
    char *new_val_str = text_to_cstring(PG_GETARG_TEXT_P(2));
    char *msg_name, *field_name, *key_str = NULL;
    PbFieldLookup lookup;

    mutation_get_path_info(path_array, "pb_insert", &msg_name, &field_name, &key_str, &lookup);
    
    StringInfoData buf; initStringInfo(&buf);
    appendBinaryStringInfo(&buf, data->data, (int)(VARSIZE(data) - VARHDRSZ));
    
    if (lookup.is_map) {
        StringInfoData entry_buf; initStringInfo(&entry_buf);
        PbFieldLookup key_lookup, val_lookup;
        pgproto_lookup_field(lookup.type_name, "key", &key_lookup);
        pgproto_lookup_field(lookup.type_name, "value", &val_lookup);
        mutation_encode_value(&key_lookup, key_str, &entry_buf, "Unsupported map key type");
        mutation_encode_value(&val_lookup, new_val_str, &entry_buf, "Unsupported map value type");
        encode_varint(PB_FIELD_TAG(lookup.number, PB_WIRE_LENGTH_DELIMITED), &buf);
        encode_varint((uint64)entry_buf.len, &buf);
        appendBinaryStringInfo(&buf, entry_buf.data, entry_buf.len);
        pfree(entry_buf.data);
    } else if (lookup.is_repeated) {
        mutation_encode_value(&lookup, new_val_str, &buf, "Unsupported type for array insertion");
    } else {
        elog(ERROR, "Field %s is not a repeated or map field", field_name);
    }

    ProtobufData *result = (ProtobufData *) palloc(VARHDRSZ + buf.len);
    SET_VARSIZE(result, VARHDRSZ + buf.len);
    memcpy(result->data, buf.data, buf.len);
    pfree(buf.data);
    pfree(msg_name); pfree(field_name); pfree(new_val_str); if (key_str) pfree(key_str);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(pb_delete);
Datum
pb_delete(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ArrayType *path_array = PG_GETARG_ARRAYTYPE_P(1);
    char *msg_name, *field_name;
    PbFieldLookup lookup;

    mutation_get_path_info(path_array, "pb_delete", &msg_name, &field_name, NULL, &lookup);
    
    StringInfoData buf; initStringInfo(&buf);
    mutation_filter_tag(data->data, data->data + VARSIZE(data) - VARHDRSZ, lookup.number, &buf);

    ProtobufData *result = (ProtobufData *) palloc(VARHDRSZ + buf.len);
    SET_VARSIZE(result, VARHDRSZ + buf.len);
    memcpy(result->data, buf.data, buf.len);
    pfree(buf.data);
    pfree(msg_name); pfree(field_name);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(pb_merge);
Datum
pb_merge(PG_FUNCTION_ARGS)
{
    ProtobufData *data1 = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ProtobufData *data2 = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(1));
    size_t size1 = VARSIZE(data1) - VARHDRSZ;
    size_t size2 = VARSIZE(data2) - VARHDRSZ;
    ProtobufData *result = (ProtobufData *) palloc(VARHDRSZ + size1 + size2);
    SET_VARSIZE(result, VARHDRSZ + size1 + size2);
    memcpy(result->data, data1->data, (int)size1);
    memcpy(result->data + size1, data2->data, (int)size2);
    PG_RETURN_POINTER(result);
}
