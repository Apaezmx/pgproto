#include "src/pgproto.h"
#include "src/io.c"
#include <assert.h>
#include <stdio.h>

void test_hex_conversion() {
    printf("Testing Hex conversion...\n");
    
    /* 
     * Test protobuf_in (Hex -> binary)
     * Input: "\x082a"
     */
    Datum args[1];
    args[0] = (Datum)"\\x082a";
    
    MockFunctionCallInfo fcinfo;
    fcinfo.args = args;
    fcinfo.nargs = 1;
    
    Datum result = protobuf_in(&fcinfo);
    ProtobufData *data = (ProtobufData *)result;
    
    assert(VARSIZE(data) == VARHDRSZ + 2);
    assert((unsigned char)data->data[0] == 0x08);
    assert((unsigned char)data->data[1] == 0x2a);
    
    /* 
     * Test protobuf_out (binary -> Hex)
     */
    args[0] = (Datum)data;
    result = protobuf_out(&fcinfo);
    char *hex_out = (char *)result;
    
    assert(strcmp(hex_out, "\\x082a") == 0);
    
    free(data);
    free(hex_out);
    printf("Hex conversion tests passed!\n");
}

int main() {
    test_hex_conversion();
    printf("All IO unit tests passed!\n");
    return 0;
}
