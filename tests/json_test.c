#include "src/pgproto.h"

/* Mock registry for isolated JSON testing */
PbLookupStatus pgproto_lookup_field_by_number(const char *message_name, uint32_t field_number, PbFieldLookup *out) {
    memset(out, 0, sizeof(PbFieldLookup));
    if (field_number == 1) {
        strcpy(out->name, "id");
        out->type = 5; // INT32
        out->is_repeated = false;
        out->found = true;
        return PB_LOOKUP_OK;
    }
    return PB_LOOKUP_FIELD_NOT_FOUND;
}
PbLookupStatus pgproto_lookup_field(const char *message_name, const char *field_name, PbFieldLookup *out) {
    return PB_LOOKUP_OK;
}
void pgproto_LoadAllSchemasFromDb(void) {}

#define static
#include "src/json.c"
#undef static

#include <assert.h>
#include <stdio.h>

void test_pb_to_json() {
    printf("Testing pb_to_json (isolated)...\n");
    
    /* 
     * Create a Protobuf message: Tag 1 (Varint) = 42
     * Hex: 08 2a
     */
    const char *data = "\x08\x2a";
    StringInfoData buf;
    initStringInfo(&buf);
    
    // We use the internal recursive helper for isolation
    pb_to_json_inner(data, data + 2, "Message", &buf);
    
    assert(strcmp(buf.data, "{\"id\":42}") == 0);
    free(buf.data);
    printf("pb_to_json tests passed!\n");
}

int main() {
    test_pb_to_json();
    printf("All JSON unit tests passed!\n");
    return 0;
}
