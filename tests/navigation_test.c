#include "src/pgproto.h"

/* Mocking registry for navigation testing */
PbLookupStatus pgproto_lookup_field(const char *message_name, const char *field_name, PbFieldLookup *out) {
    if (strcmp(field_name, "id") == 0 || strcmp(field_name, "inner") == 0) {
        strcpy(out->name, field_name);
        if (strcmp(field_name, "id") == 0) {
            out->number = 1;
            out->type = 5; // INT32
        } else {
            out->number = 1;
            out->type = 11; // MESSAGE
            strcpy(out->type_name, "Inner");
        }
        out->is_repeated = false;
        out->found = true;
        return PB_LOOKUP_OK;
    }
    return PB_LOOKUP_FIELD_NOT_FOUND;
}
void pgproto_LoadAllSchemasFromDb(void) {}

/* Static functions need to be accessible for unit testing */
#define static
#include "src/navigation.c"
#undef static

#include <assert.h>
#include <stdio.h>

void test_pb_get_int32() {
    printf("Testing pb_get_int32 (isolated)...\n");
    
    ProtobufData *data = malloc(VARHDRSZ + 2);
    SET_VARSIZE(data, VARHDRSZ + 2);
    data->data[0] = 0x08;
    data->data[1] = 0x2a;
    
    Datum args[2];
    args[0] = (Datum)data;
    args[1] = (Datum)1;
    
    MockFunctionCallInfo fcinfo;
    fcinfo.args = args;
    fcinfo.nargs = 2;
    
    Datum result = pb_get_int32(&fcinfo);
    assert((int32)result == 42);
    
    free(data);
    printf("pb_get_int32 tests passed!\n");
}

void test_protobuf_contains() {
    printf("Testing protobuf_contains (isolated)...\n");
    
    // Base: Tag 1=42, Tag 2=100
    ProtobufData *base = malloc(VARHDRSZ + 4);
    SET_VARSIZE(base, VARHDRSZ + 4);
    base->data[0] = 0x08; base->data[1] = 0x2a;
    base->data[2] = 0x10; base->data[3] = 0x64;
    
    // Query: Tag 1=42
    ProtobufData *query = malloc(VARHDRSZ + 2);
    SET_VARSIZE(query, VARHDRSZ + 2);
    query->data[0] = 0x08; query->data[1] = 0x2a;
    
    Datum args[2];
    args[0] = (Datum)base;
    args[1] = (Datum)query;
    
    MockFunctionCallInfo fcinfo;
    fcinfo.args = args;
    fcinfo.nargs = 2;
    
    Datum result = protobuf_contains(&fcinfo);
    assert((bool)result == true);
    
    // Query: Tag 3=1 (Not in base)
    query->data[0] = 0x18; query->data[1] = 0x01;
    result = protobuf_contains(&fcinfo);
    assert((bool)result == false);
    
    free(base); free(query);
    printf("protobuf_contains tests passed!\n");
}

int main() {
    test_pb_get_int32();
    test_protobuf_contains();
    printf("All navigation unit tests passed!\n");
    return 0;
}

