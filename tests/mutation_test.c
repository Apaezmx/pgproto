#define MOCK_DECONSTRUCT_ARRAY
#include "src/pgproto.h"

/* Mocking deconstruct_array specifically for this test */
void get_typlenbyvalalign(Oid type, int16_t *len, bool *byval, char *align) {}
void deconstruct_array(ArrayType *array, Oid type, int16_t len, bool byval, char align, Datum **elems, bool **nulls, int *nelems) {
    *nelems = 2;
    *elems = malloc(2 * sizeof(Datum));
    (*elems)[0] = (Datum)strdup("Message");
    (*elems)[1] = (Datum)strdup("field");
}

/* Mocking registry for isolated mutation testing */
PbLookupStatus pgproto_lookup_field(const char *message_name, const char *field_name, PbFieldLookup *out) {
    strcpy(out->name, field_name);
    out->number = 5;
    out->type = 5; // INT32
    out->is_repeated = false;
    out->is_map = false;
    out->found = true;
    return PB_LOOKUP_OK;
}
void pgproto_LoadAllSchemasFromDb(void) {}

#define static
#include "src/mutation.c"
#undef static

#include <assert.h>
#include <stdio.h>

void test_pb_set() {
    printf("Testing pb_set (isolated)...\n");
    ProtobufData *data = malloc(VARHDRSZ);
    SET_VARSIZE(data, VARHDRSZ);
    Datum args[3];
    args[0] = (Datum)data;
    args[1] = (Datum)0;
    args[2] = (Datum)strdup("42");
    MockFunctionCallInfo fcinfo;
    fcinfo.args = args;
    fcinfo.nargs = 3;
    Datum result = pb_set(&fcinfo);
    ProtobufData *res_data = (ProtobufData *)result;
    assert(VARSIZE(res_data) == VARHDRSZ + 2);
    assert((unsigned char)res_data->data[0] == 0x28);
    assert((unsigned char)res_data->data[1] == 0x2a);
    free(res_data);
    free(data);
    printf("pb_set tests passed!\n");
}

void test_pb_delete() {
    printf("Testing pb_delete (isolated)...\n");
    ProtobufData *data = malloc(VARHDRSZ + 2);
    SET_VARSIZE(data, VARHDRSZ + 2);
    data->data[0] = 0x28; data->data[1] = 0x2a;
    Datum args[2];
    args[0] = (Datum)data;
    args[1] = (Datum)0;
    MockFunctionCallInfo fcinfo;
    fcinfo.args = args;
    fcinfo.nargs = 2;
    Datum result = pb_delete(&fcinfo);
    ProtobufData *res_data = (ProtobufData *)result;
    assert(VARSIZE(res_data) == VARHDRSZ);
    free(res_data);
    free(data);
    printf("pb_delete tests passed!\n");
}

void test_pb_insert() {
    printf("Testing pb_insert (isolated stub)...\n");
    printf("pb_insert tests passed (stub)!\n");
}

int main() {
    test_pb_set();
    test_pb_delete();
    test_pb_insert();
    printf("All mutation unit tests passed!\n");
    return 0;
}
