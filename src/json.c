#include "pgproto.h"
#include <string.h>
#include "lib/stringinfo.h"

/**
 * pb_to_json_inner: Recursive helper to convert a Protobuf binary blob to JSON.
 * 
 * @param ptr: Start of the binary blob.
 * @param end: End of the binary blob.
 * @param msg_name: Name of the message type for field resolution.
 * @param buf: StringInfo buffer to append the JSON string to.
 * 
 * Summary:
 * Iterates through the binary stream. For each field, it performs a reverse
 * lookup (tag -> name) in the registry and formats the value according to its type.
 * Nested messages trigger a recursive call.
 */
static void
pb_to_json_inner(const char *ptr, const char *end, const char *msg_name, StringInfo buf)
{
    appendStringInfoChar(buf, '{');
    bool first = true;

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = (int)(key >> 3);
        int wire_type = (int)(key & 0x07);
        
        PbFieldLookup lookup;
        if (pgproto_lookup_field_by_number(msg_name, (uint32_t)field_num, &lookup)) {
            if (!first) appendStringInfoString(buf, ",");
            appendStringInfo(buf, "\"%s\":", lookup.name);
            
            if (wire_type == PB_WIRE_VARINT) { // Varint
                uint64 val = decode_varint(&ptr, end);
                if (lookup.type == PB_TYPE_BOOL) appendStringInfoString(buf, val ? "true" : "false");
                else appendStringInfo(buf, "%ld", (long)val);
            } else if (wire_type == PB_WIRE_LENGTH_DELIMITED) { // Length-delimited
                uint64 len = decode_varint(&ptr, end);
                if (lookup.type == PB_TYPE_MESSAGE) {
                    pb_to_json_inner(ptr, ptr + len, lookup.type_name, buf);
                } else {
                    appendStringInfoChar(buf, '"');
                    appendBinaryStringInfo(buf, ptr, (int)len);
                    appendStringInfoChar(buf, '"');
                }
                ptr += len;
            } else {
                skip_field(wire_type, &ptr, end);
                appendStringInfoString(buf, "null");
            }
            first = false;
        } else {
            skip_field(wire_type, &ptr, end);
        }
    }
    appendStringInfoChar(buf, '}');
}

PG_FUNCTION_INFO_V1(pb_to_json);

/**
 * pb_to_json: SQL function to convert a Protobuf binary blob to a JSON text.
 * 
 * Inputs:
 * - data (protobuf): Raw Protobuf binary data.
 * - type (text): Fully qualified message type name.
 * 
 * Summary:
 * Initializes the registry, prepares a StringInfo buffer, and calls the recursive
 * encoder to produce a compact JSON representation.
 */
Datum
pb_to_json(PG_FUNCTION_ARGS)
{
    ProtobufData *pb_data = (ProtobufData *) PG_GETARG_VARLENA_P(0);
    text *message_type_text = PG_GETARG_TEXT_P(1);
    char *message_type_str = text_to_cstring(message_type_text);
    
    pgproto_LoadAllSchemasFromDb();

    PbFieldLookup dummy;
    if (pgproto_lookup_field(message_type_str, "", &dummy) == PB_LOOKUP_MSG_NOT_FOUND) {
        elog(ERROR, "Protobuf schema not found: %s", message_type_str);
    }
    
    StringInfoData buf;
    initStringInfo(&buf);
    
    size_t data_len = VARSIZE(pb_data) - VARHDRSZ;
    pb_to_json_inner(pb_data->data, pb_data->data + data_len, message_type_str, &buf);
    
    pfree(message_type_str);
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
