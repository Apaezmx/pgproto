#include "pgproto.h"
#include <string.h>

/**
 * pb_to_json_inner: Recursive helper to convert a Protobuf binary blob to JSON.
 * Grouping logic: Contiguous repeated fields are grouped into a single JSON array.
 */
static void
pb_to_json_inner(const char *ptr, const char *end, const char *msg_name, StringInfo buf)
{
    appendStringInfoChar(buf, '{');
    bool first = true;
    int last_field_num = -1;
    bool in_repeated_group = false;

    while (ptr < end) {
        uint64 key = decode_varint(&ptr, end);
        int field_num = (int)(key >> 3);
        int wire_type = (int)(key & 0x07);
        
        PbFieldLookup lookup;
        if (pgproto_lookup_field_by_number(msg_name, (uint32_t)field_num, &lookup) == PB_LOOKUP_OK) {
            if (field_num == last_field_num && lookup.is_repeated) {
                appendStringInfoChar(buf, ',');
            } else {
                if (in_repeated_group) appendStringInfoChar(buf, ']');
                if (!first) appendStringInfoChar(buf, ',');
                appendStringInfo(buf, "\"%s\":", lookup.name);
                if (lookup.is_repeated) {
                    appendStringInfoChar(buf, '[');
                    in_repeated_group = true;
                } else {
                    in_repeated_group = false;
                }
            }
            
            if (wire_type == PB_WIRE_VARINT) {
                uint64 val = decode_varint(&ptr, end);
                if (lookup.type == PB_TYPE_BOOL) appendStringInfoString(buf, val ? "true" : "false");
                else appendStringInfo(buf, "%ld", (long)val);
            } else if (wire_type == PB_WIRE_LENGTH_DELIMITED) {
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
            last_field_num = field_num;
        } else {
            if (in_repeated_group) { appendStringInfoChar(buf, ']'); in_repeated_group = false; }
            skip_field(wire_type, &ptr, end);
            last_field_num = -1;
        }
    }
    if (in_repeated_group) appendStringInfoChar(buf, ']');
    appendStringInfoChar(buf, '}');
}

PG_FUNCTION_INFO_V1(pb_to_json);
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
    StringInfoData buf; initStringInfo(&buf);
    size_t data_len = VARSIZE(pb_data) - VARHDRSZ;
    pb_to_json_inner(pb_data->data, pb_data->data + data_len, message_type_str, &buf);
    pfree(message_type_str);
    PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
