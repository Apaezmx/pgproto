#include "pgproto.h"

/* 
 * SchemaEntry: A linked list entry for the in-memory session cache of registered schemas.
 */
typedef struct SchemaEntry {
    char *name;       /* Unique schema identifier (usually the .proto filename) */
    char *data;       /* Raw FileDescriptorSet binary blob */
    size_t len;       /* Length of the binary blob */
    struct SchemaEntry *next;
} SchemaEntry;

static SchemaEntry *s_schemas = NULL; /* Head of the session schema cache */
bool s_schema_loaded = false;         /* Flag to track if schemas have been loaded from the DB */

/**
 * pgproto_LoadAllSchemasFromDb: Loads all registered schemas from the PostgreSQL 'pb_schemas' table.
 * It clears the existing cache and populates it using SPI (Server Programming Interface).
 * 
 * Summary:
 * 1. Checks if schemas are already loaded in this backend session.
 * 2. Connects via SPI and selects all from 'pb_schemas'.
 * 3. Iterates over tuples, Toasting/copying binary data into the linked list.
 */
void
pgproto_LoadAllSchemasFromDb(void)
{
    int ret;
    if (s_schema_loaded) return;

    // Clear existing schemas
    while (s_schemas) {
        SchemaEntry *next = s_schemas->next;
        free(s_schemas->name);
        free(s_schemas->data);
        free(s_schemas);
        s_schemas = next;
    }

    SPI_connect();
    ret = SPI_execute("SELECT name, data FROM public.pb_schemas", true, 0);
    if (ret != SPI_OK_SELECT) {
        SPI_finish();
        return;
    }

    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    
    for (int i = 0; i < SPI_processed; i++) {
        HeapTuple tuple = tuptable->vals[i];
        bool is_null_name, is_null_data;
        Datum name_datum = SPI_getbinval(tuple, tupdesc, 1, &is_null_name);
        Datum data_datum = SPI_getbinval(tuple, tupdesc, 2, &is_null_data);
        
        if (is_null_name || is_null_data) continue;

        char *name = text_to_cstring(DatumGetTextPP(name_datum));
        bytea *data = DatumGetByteaP(data_datum);
        size_t len = VARSIZE_ANY_EXHDR(data);
        
        SchemaEntry *entry = (SchemaEntry *) malloc(sizeof(SchemaEntry));
        entry->name = strdup(name);
        entry->data = (char *) malloc(len);
        memcpy(entry->data, VARDATA_ANY(data), len);
        entry->len = len;
        entry->next = s_schemas;
        s_schemas = entry;
        
        pfree(name);
    }
    
    SPI_finish();
    s_schema_loaded = true;
}

/**
 * scan_fields: Scans a DescriptorProto (message definition) for a specific field by name or number.
 * 
 * @param ptr: Start of the DescriptorProto fields.
 * @param end: End of the DescriptorProto fields.
 * @param target_name: Optional field name to match.
 * @param target_number: Optional tag number to match.
 * @param out: Populate with field metadata if found.
 * @return: True if found.
 * 
 * Summary:
 * This is a raw wire-format traversal of the FieldDescriptorProto repeated field.
 * It decodes field tags (name, number, type, etc.) on the fly.
 */
static bool
scan_fields(const char *ptr, const char *end, const char *target_name, uint32_t target_number, PbFieldLookup *out)
{
    const char *f_ptr = ptr;
    while (f_ptr < end) {
        uint64 key = decode_varint(&f_ptr, end);
        int field_num = key >> 3;
        int wire_type = key & 0x07;
        
        if (field_num == 2 && wire_type == 2) { // repeated FieldDescriptorProto field
            uint64 len = decode_varint(&f_ptr, end);
            const char *f_end = f_ptr + len;
            const char *curr_f_ptr = f_ptr;
            
            uint32_t f_num = 0;
            PbType f_type = 0;
            char f_name[256] = {0};
            char f_type_name[256] = {0};
            bool f_is_repeated = false;

            while (curr_f_ptr < f_end) {
                uint64 f_key = decode_varint(&curr_f_ptr, f_end);
                int fn = f_key >> 3;
                int fwt = f_key & 0x07;
                if (fn == 1 && fwt == 2) { // name
                    uint64 nl = decode_varint(&curr_f_ptr, f_end);
                    size_t to_copy = nl < 255 ? nl : 255;
                    memcpy(f_name, curr_f_ptr, to_copy);
                    f_name[to_copy] = '\0';
                    curr_f_ptr += nl;
                } else if (fn == 3 && fwt == 0) { // number
                    f_num = (uint32_t) decode_varint(&curr_f_ptr, f_end);
                } else if (fn == 4 && fwt == 0) { // label
                    uint64 label = decode_varint(&curr_f_ptr, f_end);
                    f_is_repeated = (label == 3);
                } else if (fn == 5 && fwt == 0) { // type
                    f_type = (PbType) decode_varint(&curr_f_ptr, f_end);
                } else if (fn == 6 && fwt == 2) { // type_name
                    uint64 nl = decode_varint(&curr_f_ptr, f_end);
                    size_t to_copy = nl < 255 ? nl : 255;
                    memcpy(f_type_name, curr_f_ptr, to_copy);
                    f_type_name[to_copy] = '\0';
                    curr_f_ptr += nl;
                } else {
                    skip_field(fwt, &curr_f_ptr, f_end);
                }
            }
            if ((target_name && strcmp(f_name, target_name) == 0) || (target_number > 0 && f_num == target_number)) {
                strcpy(out->name, f_name);
                out->number = f_num;
                out->type = f_type;
                strcpy(out->type_name, f_type_name);
                out->is_repeated = f_is_repeated;
                out->is_map = (f_is_repeated && f_type == PB_TYPE_MESSAGE && strstr(f_type_name, "Entry") != NULL);
                out->found = true;
                return true;
            }
            f_ptr = f_end;
        } else {
            skip_field(wire_type, &f_ptr, end);
        }
    }
    return false;
}

/**
 * scan_messages: Recursively scans a FileDescriptorProto for a message type.
 */
static bool
scan_messages(const char *ptr, const char *end, const char *prefix, const char *target_msg, const char *target_field_name, uint32_t target_field_number, PbFieldLookup *out)
{
    const char *m_ptr = ptr;
    while (m_ptr < end) {
        uint64 key = decode_varint(&m_ptr, end);
        int field_num = key >> 3;
        int wire_type = key & 0x07;
        
        if ((field_num == 4 || field_num == 3) && wire_type == 2) { // repeated DescriptorProto message_type (4) or nested_type (3)
            uint64 len = decode_varint(&m_ptr, end);
            const char *msg_inner_end = m_ptr + len;
            const char *name_ptr = m_ptr;
            char m_name[256] = {0};
            
            // Find name first
            while (name_ptr < msg_inner_end) {
                uint64 mk = decode_varint(&name_ptr, msg_inner_end);
                if ((mk >> 3) == 1 && (mk & 0x07) == 2) {
                    uint64 nl = decode_varint(&name_ptr, msg_inner_end);
                    size_t to_copy = nl < 255 ? nl : 255;
                    memcpy(m_name, name_ptr, to_copy);
                    m_name[to_copy] = '\0';
                    break;
                }
                skip_field(mk & 0x07, &name_ptr, msg_inner_end);
            }
            
            char full_name[512];
            if (prefix && prefix[0]) snprintf(full_name, 512, "%s.%s", prefix, m_name);
            else strcpy(full_name, m_name);
            
            if (strcmp(full_name, target_msg) == 0) {
                if (target_field_name == NULL && target_field_number == 0) return true;
                if (scan_fields(m_ptr, msg_inner_end, target_field_name, target_field_number, out)) return true;
            }
            // Recurse for nested types
            if (scan_messages(m_ptr, msg_inner_end, full_name, target_msg, target_field_name, target_field_number, out)) return true;
            
            m_ptr = msg_inner_end;
        } else {
            skip_field(wire_type, &m_ptr, end);
        }
    }
    return false;
}

/**
 * pgproto_lookup_internal: Orchestrates the lookup of a field within the loaded schema blobs.
 */
static PbLookupStatus
pgproto_lookup_internal(const char *message_name, const char *field_name, uint32_t field_number, PbFieldLookup *out)
{
    SchemaEntry *entry = s_schemas;
    bool msg_type_found = false;
    out->found = false;
    const char *m_name = message_name;
    if (m_name && m_name[0] == '.') m_name++;

    while (entry) {
        const char *ptr = entry->data;
        const char *end = ptr + entry->len;
        while (ptr < end) {
            uint64 key = decode_varint(&ptr, end);
            if ((key >> 3) == 1 && (key & 0x07) == 2) { // FileDescriptorProto
                uint64 len = decode_varint(&ptr, end);
                const char *f_end = ptr + len;
                const char *f_ptr = ptr;
                char package[256] = {0};
                while (f_ptr < f_end) {
                    uint64 fk = decode_varint(&f_ptr, f_end);
                    if ((fk >> 3) == 2 && (fk & 0x07) == 2) {
                        uint64 nl = decode_varint(&f_ptr, f_end);
                        size_t to_copy = nl < 255 ? nl : 255;
                        memcpy(package, f_ptr, to_copy);
                        package[to_copy] = '\0';
                        break;
                    }
                    skip_field(fk & 0x07, &f_ptr, f_end);
                }
                
                // scan_messages needs to tell us if it at least found the message type
                if (scan_messages(ptr, f_end, package, m_name, field_name, field_number, out)) {
                    return PB_LOOKUP_OK;
                }
                
                // Second pass to see if message type exists in this file
                if (!msg_type_found) {
                    PbFieldLookup dummy;
                    if (scan_messages(ptr, f_end, package, m_name, NULL, 0, &dummy)) {
                        msg_type_found = true;
                    }
                }
                ptr = f_end;
            } else skip_field(key & 0x07, &ptr, end);
        }
        entry = entry->next;
    }
    
    return msg_type_found ? PB_LOOKUP_FIELD_NOT_FOUND : PB_LOOKUP_MSG_NOT_FOUND;
}

PbLookupStatus pgproto_lookup_field(const char *message_name, const char *field_name, PbFieldLookup *out) {
    return pgproto_lookup_internal(message_name, field_name, 0, out);
}

PbLookupStatus pgproto_lookup_field_by_number(const char *message_name, uint32_t field_number, PbFieldLookup *out) {
    return pgproto_lookup_internal(message_name, NULL, field_number, out);
}

PG_FUNCTION_INFO_V1(pb_register_schema);

/**
 * pb_register_schema: SQL function to register a new Protobuf schema in the database.
 * 
 * Inputs:
 * - name (text): Unique name for the schema.
 * - data (bytea): Binary FileDescriptorSet blob.
 * 
 * Summary:
 * 1. Inserts the schema into the 'pb_schemas' table.
 * 2. Forces a reload of the session cache.
 */
Datum
pb_register_schema(PG_FUNCTION_ARGS)
{
    text *name = PG_GETARG_TEXT_P(0);
    bytea *data = PG_GETARG_BYTEA_P(1);
    char *name_str = text_to_cstring(name);
    
    int ret;
    Oid argtypes[2];
    Datum Values[2];
    char Nulls[2] = {' ', ' '};

    argtypes[0] = TEXTOID;
    Values[0] = PointerGetDatum(name);
    argtypes[1] = BYTEAOID;
    Values[1] = PointerGetDatum(data);

    SPI_connect();
    ret = SPI_execute_with_args("INSERT INTO pb_schemas (name, data) VALUES ($1, $2)",
                                2, argtypes, Values, Nulls, false, 0);
    if (ret != SPI_OK_INSERT) elog(ERROR, "Failed to insert schema");
    SPI_finish();

    s_schema_loaded = false;
    pgproto_LoadAllSchemasFromDb();

    elog(INFO, "Successfully registered schema: %s", name_str);
    pfree(name_str);
    PG_RETURN_VOID();
}
