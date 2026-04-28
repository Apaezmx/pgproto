#include "pgproto.h"
#include <string.h>
#include "lib/stringinfo.h"

PG_FUNCTION_INFO_V1(pb_set);

/**
 * pb_set: Sets a field in a Protobuf message.
 * 
 * Inputs:
 * - data (protobuf): Original Protobuf data.
 * - path (text[]): [message_type, field_name].
 * - value (text): New value for the field (string representation).
 * 
 * Summary:
 * Implementation follows the "Last Tag Wins" rule of Protobuf. It appends the
 * new field-tag and value at the end of the existing binary blob. This is a very
 * efficient O(1) "set" operation.
 */
Datum
pb_set(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ArrayType *path_array = PG_GETARG_ARRAYTYPE_P(1);
    text *new_val_text = PG_GETARG_TEXT_P(2);

    int16 typlen; bool typbyval; char typalign; Datum *elems; bool *nulls; int nelems;
    get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
    deconstruct_array(path_array, TEXTOID, typlen, typbyval, typalign, &elems, &nulls, &nelems);

    if (nelems != 2) elog(ERROR, "Only paths of length 2 are supported in this prototype (message_name, field_name)");

    char *msg_name = text_to_cstring(DatumGetTextPP(elems[0]));
    char *field_name = text_to_cstring(DatumGetTextPP(elems[1]));
    char *new_val_str = text_to_cstring(new_val_text);

    pgproto_LoadAllSchemasFromDb();
    PbFieldLookup lookup;
    PbLookupStatus status = pgproto_lookup_field(msg_name, field_name, &lookup);
    if (status != PB_LOOKUP_OK) {
        if (status == PB_LOOKUP_MSG_NOT_FOUND) {
            elog(ERROR, "Message not found in schema registry: %s", msg_name);
        } else {
            elog(ERROR, "Field %s not found in message %s", field_name, msg_name);
        }
    }

    size_t old_size = VARSIZE(data) - VARHDRSZ;
    StringInfoData buf; initStringInfo(&buf);
    appendBinaryStringInfo(&buf, data->data, (int)old_size);
    
    if (lookup.type == PB_TYPE_INT32 || lookup.type == PB_TYPE_INT64 || lookup.type == PB_TYPE_BOOL) {
        encode_varint(PB_FIELD_TAG(lookup.number, PB_WIRE_VARINT), &buf);
        encode_varint((uint64)atoll(new_val_str), &buf);
    } else if (lookup.type == PB_TYPE_STRING) {
        encode_varint(PB_FIELD_TAG(lookup.number, PB_WIRE_LENGTH_DELIMITED), &buf);
        encode_varint((uint64)strlen(new_val_str), &buf);
        appendStringInfoString(&buf, new_val_str);
    } else elog(ERROR, "Unsupported type for modification: %d", lookup.type);

    ProtobufData *result = (ProtobufData *) palloc(VARHDRSZ + buf.len);
    SET_VARSIZE(result, VARHDRSZ + buf.len);
    memcpy(result->data, buf.data, buf.len);
    pfree(msg_name); pfree(field_name); pfree(new_val_str);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(pb_insert);

/**
 * pb_insert: Inserts an element into a repeated or map field.
 */
Datum
pb_insert(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ArrayType *path_array = PG_GETARG_ARRAYTYPE_P(1);
    text *new_val_text = PG_GETARG_TEXT_P(2);
    int16 typlen; bool typbyval; char typalign; Datum *elems; bool *nulls; int nelems;
    get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
    deconstruct_array(path_array, TEXTOID, typlen, typbyval, typalign, &elems, &nulls, &nelems);
    if (nelems != 3) {
        elog(ERROR, "pb_insert requires a path of length 3 (message_name, field_name, index/key)");
    }
    char *msg_name = text_to_cstring(DatumGetTextPP(elems[0]));
    char *field_name = text_to_cstring(DatumGetTextPP(elems[1]));
    char *new_val_str = text_to_cstring(new_val_text);
    pgproto_LoadAllSchemasFromDb();
    PbFieldLookup lookup;
    PbLookupStatus status = pgproto_lookup_field(msg_name, field_name, &lookup);
    if (status != PB_LOOKUP_OK) {
        if (status == PB_LOOKUP_MSG_NOT_FOUND) {
            elog(ERROR, "Message not found in schema registry: %s", msg_name);
        } else {
            elog(ERROR, "Field %s not found in message %s", field_name, msg_name);
        }
    }
    size_t old_size = VARSIZE(data) - VARHDRSZ;
    StringInfoData buf; initStringInfo(&buf);
    appendBinaryStringInfo(&buf, data->data, (int)old_size);
    if (lookup.is_map) {
        char *key_str = text_to_cstring(DatumGetTextPP(elems[2]));
        StringInfoData entry_buf; initStringInfo(&entry_buf);
        
        encode_varint(PB_FIELD_TAG(1, PB_WIRE_LENGTH_DELIMITED), &entry_buf);
        encode_varint((uint64)strlen(key_str), &entry_buf);
        appendStringInfoString(&entry_buf, key_str);
        
        encode_varint(PB_FIELD_TAG(2, PB_WIRE_VARINT), &entry_buf);
        encode_varint((uint64)atoll(new_val_str), &entry_buf);
        
        encode_varint(PB_FIELD_TAG(lookup.number, PB_WIRE_LENGTH_DELIMITED), &buf);
        encode_varint((uint64)entry_buf.len, &buf);
        appendBinaryStringInfo(&buf, entry_buf.data, entry_buf.len);
        pfree(key_str);
    } else if (lookup.is_repeated) {
        if (lookup.type == PB_TYPE_INT32) {
            encode_varint(PB_FIELD_TAG(lookup.number, PB_WIRE_VARINT), &buf);
            encode_varint((uint64)atoll(new_val_str), &buf);
        } else elog(ERROR, "Unsupported type for array insertion: %d", lookup.type);
    } else elog(ERROR, "Field not repeated or map");
    ProtobufData *result = (ProtobufData *) palloc(VARHDRSZ + buf.len);
    SET_VARSIZE(result, VARHDRSZ + buf.len);
    memcpy(result->data, buf.data, buf.len);
    pfree(msg_name); pfree(field_name); pfree(new_val_str);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(pb_delete);

/**
 * pb_delete: Removes a field or array/map element from a Protobuf message.
 * 
 * Summary:
 * Scans the message and copies all fields to a new buffer, EXCEPT the field matching
 * the target tag. This effectively deletes the field from the message.
 */
Datum
pb_delete(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    ArrayType *path_array = PG_GETARG_ARRAYTYPE_P(1);
    int16 typlen; bool typbyval; char typalign; Datum *elems; bool *nulls; int nelems;
    get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
    deconstruct_array(path_array, TEXTOID, typlen, typbyval, typalign, &elems, &nulls, &nelems);
    if (nelems < 2 || nelems > 3) {
        elog(ERROR, "pb_delete requires a path of length 2 or 3 (message_name, field_name [, index/key])");
    }
    char *msg_name = text_to_cstring(DatumGetTextPP(elems[0]));
    char *field_name = text_to_cstring(DatumGetTextPP(elems[1]));
    pgproto_LoadAllSchemasFromDb();
    PbFieldLookup lookup;
    PbLookupStatus status = pgproto_lookup_field(msg_name, field_name, &lookup);
    if (status != PB_LOOKUP_OK) {
        if (status == PB_LOOKUP_MSG_NOT_FOUND) {
            elog(ERROR, "Message not found in schema registry: %s", msg_name);
        } else {
            elog(ERROR, "Field %s not found in message %s", field_name, msg_name);
        }
    }
    size_t old_size = VARSIZE(data) - VARHDRSZ;
    const char *ptr = data->data; const char *end = ptr + old_size;
    StringInfoData buf; initStringInfo(&buf);
    while (ptr < end) {
        const char *start = ptr;
        uint64 key = decode_varint(&ptr, end);
        int field_num = (int)(key >> 3);
        int wire_type = (int)(key & 0x07);
        if (field_num == (int)lookup.number) skip_field(wire_type, &ptr, end);
        else { skip_field(wire_type, &ptr, end); appendBinaryStringInfo(&buf, start, (int)(ptr - start)); }
    }
    ProtobufData *result = (ProtobufData *) palloc(VARHDRSZ + buf.len);
    SET_VARSIZE(result, VARHDRSZ + buf.len);
    memcpy(result->data, buf.data, buf.len);
    pfree(msg_name); pfree(field_name);
    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(pb_merge);

/**
 * pb_merge: Implements the || operator. Concatenates two Protobuf blobs.
 * Valid Protobuf messages can be merged by simple concatenation.
 */
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
