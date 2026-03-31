#include "pgproto.h"

PG_FUNCTION_INFO_V1(protobuf_in);

Datum
protobuf_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);
    size_t len = strlen(str);
    size_t i;
    ProtobufData *result;
    size_t data_len;

    if (len >= 2 && str[0] == '\\' && str[1] == 'x') {
        str += 2;
        len -= 2;
    }

    if (len % 2 != 0) {
        elog(ERROR, "Invalid hex string length: %zu", len);
    }

    data_len = len / 2;
    result = (ProtobufData *) palloc(VARHDRSZ + data_len);
    SET_VARSIZE(result, VARHDRSZ + data_len);

    for (i = 0; i < data_len; i++) {
        int hi = hex_val(str[2 * i]);
        int lo = hex_val(str[2 * i + 1]);
        if (hi < 0 || lo < 0) {
            elog(ERROR, "Invalid character in hex string");
        }
        result->data[i] = (hi << 4) | lo;
    }

    PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(protobuf_out);

Datum
protobuf_out(PG_FUNCTION_ARGS)
{
    ProtobufData *data = (ProtobufData *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
    size_t len = VARSIZE(data) - VARHDRSZ;
    char *result_str;
    size_t i;

    result_str = palloc(len * 2 + 3);
    result_str[0] = '\\';
    result_str[1] = 'x';

    for (i = 0; i < len; i++) {
        sprintf(result_str + 2 + i * 2, "%02x", (unsigned char) data->data[i]);
    }
    result_str[len * 2 + 2] = '\0';

    PG_RETURN_CSTRING(result_str);
}
