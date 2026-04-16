#include "src/pgproto.h"
#include "third_party/upb/upb/json/encode.h"
#include "third_party/upb/upb/wire/decode.h"
#include "third_party/upb/upb/mem/arena.h"

PG_FUNCTION_INFO_V1(pb_to_json);

Datum
pb_to_json(PG_FUNCTION_ARGS)
{
    ProtobufData *pb_data = (ProtobufData *) PG_GETARG_VARLENA_P(0);
    text *message_type_text = PG_GETARG_TEXT_P(1);
    char *message_type_str = text_to_cstring(message_type_text);
    
    // 1. Resolve message definition
    if (!s_def_pool) {
        s_def_pool = upb_DefPool_New();
        pgproto_LoadAllSchemasFromDb(s_def_pool);
    }
    
    const upb_MessageDef *msg_def = upb_DefPool_FindMessageByName(s_def_pool, message_type_str);
    if (!msg_def) {
        elog(ERROR, "Protobuf schema not found: %s", message_type_str);
    }
    
    const upb_MiniTable *mini_table = upb_MessageDef_MiniTable(msg_def);
    if (!mini_table) {
        elog(ERROR, "Could not get mini table for %s", message_type_str);
    }
    
    // 2. Decode binary to upb_Message
    upb_Arena *arena = upb_Arena_New();
    if (!arena) {
        elog(ERROR, "Failed to create UPB arena");
    }
    
    upb_Message *msg = upb_Message_New(mini_table, arena);
    if (!msg) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to create message instance for %s", message_type_str);
    }
    
    size_t data_len = VARSIZE(pb_data) - VARHDRSZ;
    upb_DecodeStatus status = upb_Decode(pb_data->data, data_len, msg, mini_table, NULL, 0, arena);
    if (status != kUpb_DecodeStatus_Ok) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to decode protobuf data: %s", upb_DecodeStatus_String(status));
    }
    
    // 3. Convert to JSON
    upb_Status json_status;
    upb_Status_Clear(&json_status);
    
    // First pass override: get required size
    size_t json_len = upb_JsonEncode(msg, msg_def, s_def_pool, 0, NULL, 0, &json_status);
    if (!upb_Status_IsOk(&json_status)) {
        upb_Arena_Free(arena);
        elog(ERROR, "Failed to calculate JSON size: %s", upb_Status_ErrorMessage(&json_status));
    }
    
    char *json_buf = palloc(json_len + 1);
    upb_JsonEncode(msg, msg_def, s_def_pool, 0, json_buf, json_len + 1, &json_status);
    if (!upb_Status_IsOk(&json_status)) {
        upb_Arena_Free(arena);
        pfree(json_buf);
        elog(ERROR, "Failed to encode to JSON: %s", upb_Status_ErrorMessage(&json_status));
    }
    
    upb_Arena_Free(arena);
    
    text *result_text = cstring_to_text(json_buf);
    pfree(json_buf);
    
    PG_RETURN_TEXT_P(result_text);
}
