#include "src/pgproto.h"

/* Static functions need to be accessible for unit testing */
#define static
#include "src/registry.c"
#undef static

#include <assert.h>
#include <stdio.h>

void test_scan_fields() {
    printf("Testing scan_fields...\n");
    
    /* 
     * Manual construction of a DescriptorProto containing one field:
     * FieldDescriptorProto (Tag 2 in DescriptorProto)
     * - name (Tag 1): "id"
     * - number (Tag 3): 1
     * - type (Tag 5): 5 (INT32)
     */
    StringInfoData buf; 
    initStringInfo(&buf);
    
    StringInfoData f_buf; 
    initStringInfo(&f_buf);
    
    // name = "id"
    encode_varint(PB_FIELD_TAG(1, PB_WIRE_LENGTH_DELIMITED), &f_buf);
    encode_varint(2, &f_buf);
    appendStringInfoString(&f_buf, "id");
    
    // number = 1
    encode_varint(PB_FIELD_TAG(3, PB_WIRE_VARINT), &f_buf);
    encode_varint(1, &f_buf);
    
    // type = 5 (INT32)
    encode_varint(PB_FIELD_TAG(5, PB_WIRE_VARINT), &f_buf);
    encode_varint(5, &f_buf);
    
    // Wrap in Tag 2 of DescriptorProto
    encode_varint(PB_FIELD_TAG(2, PB_WIRE_LENGTH_DELIMITED), &buf);
    encode_varint(f_buf.len, &buf);
    appendBinaryStringInfo(&buf, f_buf.data, f_buf.len);
    
    PbFieldLookup out;
    bool found = scan_fields(buf.data, buf.data + buf.len, "id", 0, &out);
    assert(found);
    assert(out.number == 1);
    assert(out.type == 5);
    assert(strcmp(out.name, "id") == 0);
    
    // Test lookup by number
    memset(&out, 0, sizeof(out));
    found = scan_fields(buf.data, buf.data + buf.len, NULL, 1, &out);
    assert(found);
    assert(out.number == 1);
    assert(strcmp(out.name, "id") == 0);

    free(buf.data);
    free(f_buf.data);
    printf("scan_fields tests passed!\n");
}

int main() {
    test_scan_fields();
    printf("All registry unit tests passed!\n");
    return 0;
}
