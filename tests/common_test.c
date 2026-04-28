#include "src/pgproto.h"
#include <assert.h>
#include <stdio.h>

void test_varint() {
    printf("Testing Varint...\n");
    const char *data = "\x08";
    const char *ptr = data;
    assert(decode_varint(&ptr, data + 1) == 8);
    
    data = "\xac\x02";
    ptr = data;
    assert(decode_varint(&ptr, data + 2) == 300);
    
    StringInfoData buf;
    initStringInfo(&buf);
    encode_varint(300, &buf);
    assert(buf.len == 2);
    assert((unsigned char)buf.data[0] == 0xac);
    assert((unsigned char)buf.data[1] == 0x02);
    free(buf.data);
    printf("Varint tests passed!\n");
}

void test_hex() {
    printf("Testing Hex...\n");
    assert(hex_val('0') == 0);
    assert(hex_val('9') == 9);
    assert(hex_val('a') == 10);
    assert(hex_val('f') == 15);
    assert(hex_val('A') == 10);
    assert(hex_val('F') == 15);
    assert(hex_val('g') == -1);
    printf("Hex tests passed!\n");
}

int main() {
    test_varint();
    test_hex();
    printf("All common utility tests passed!\n");
    return 0;
}
