#include "pgproto.h"

void
pgproto_LoadAllSchemasFromDb(upb_DefPool *pool)
{
    /*
     * Loads all registered protobuf schemas from the 'pb_schemas' table
     * into the provided upb_DefPool. This is called during extension initialization
     * and whenever a new schema is registered to keep the in-memory pool up to date.
     * 
     * It uses Server Programming Interface (SPI) to query the database.
     */
    int ret;
    
    SPI_connect();
    ret = SPI_execute("SELECT data FROM public.pb_schemas", true, 0);
    
    if (ret != SPI_OK_SELECT) {
        elog(WARNING, "Failed to select schemas from database: %d", ret);
        SPI_finish();
        return;
    }

    TupleDesc tupdesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    
    for (int i = 0; i < SPI_processed; i++) {
        HeapTuple tuple = tuptable->vals[i];
        bool is_null;
        Datum data_datum = SPI_getbinval(tuple, tupdesc, 1, &is_null);
        
        if (is_null) {
            continue;
        }

        bytea *data = DatumGetByteaP(data_datum);
        size_t data_len = VARSIZE_ANY_EXHDR(data);
        char *data_ptr = VARDATA_ANY(data);
        
        upb_Arena *arena = upb_Arena_New();
        if (!arena) {
            elog(WARNING, "Failed to create arena for schema loading");
            continue;
        }

        google_protobuf_FileDescriptorSet *set = google_protobuf_FileDescriptorSet_parse(data_ptr, data_len, arena);
        if (!set) {
            elog(WARNING, "Failed to parse FileDescriptorSet");
            upb_Arena_Free(arena);
            continue;
        }

        size_t file_count = 0;
        const google_protobuf_FileDescriptorProto *const *files = google_protobuf_FileDescriptorSet_file(set, &file_count);
        
        for (size_t j = 0; j < file_count; j++) {
            upb_Status status;
            
            upb_Status_Clear(&status);
            if (!upb_DefPool_AddFile(pool, files[j], &status)) {
                elog(WARNING, "Failed to add file to DefPool: %s", upb_Status_ErrorMessage(&status));
            }
        }
        upb_Arena_Free(arena);
    }
    
    SPI_finish();
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
