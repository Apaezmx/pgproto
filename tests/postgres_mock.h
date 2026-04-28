#ifndef POSTGRES_MOCK_H
#define POSTGRES_MOCK_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdarg.h>
#include <strings.h>

/* Basic Types */
typedef uint64_t uint64;
typedef int32_t int32;
typedef uint32_t uint32;
typedef int16_t int16;
typedef uint16_t uint16;
typedef uint8_t uint8;
typedef uintptr_t Datum;
typedef uint32_t Oid;

typedef void bytea;
typedef void text;
#define TEXTOID 25
#define BYTEAOID 17

/* Varlena Mock */
#define VARHDRSZ 4
#define SET_VARSIZE(ptr, len) (*(int32 *)(ptr) = (len))
#define VARSIZE(ptr) (*(int32 *)(ptr))
#define VARSIZE_ANY_EXHDR(ptr) (VARSIZE(ptr) - VARHDRSZ)
#define VARDATA_ANY(ptr) (((struct { int32 l; char data[1]; } *)(ptr))->data)

/* Postgres Utility Mock */
#define ERROR 20
#define WARNING 19
#define INFO 18
#define NOTICE 17

static inline void elog(int level, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    printf("ELOG(%d): ", level);
    vprintf(fmt, args);
    printf("\n");
    va_end(args);
    if (level == ERROR) exit(1);
}

#define palloc(sz) malloc(sz)
#define pfree(ptr) free(ptr)
#define repalloc(ptr, sz) realloc(ptr, sz)

typedef struct {
    Datum *args;
    int nargs;
} MockFunctionCallInfo;

#define PG_FUNCTION_ARGS void* fcinfo
#define PG_RETURN_INT32(x) return (Datum)(x)
#define PG_RETURN_TEXT_P(x) return (Datum)(x)
#define PG_RETURN_BOOL(x) return (Datum)(x)
#define PG_RETURN_POINTER(x) return (Datum)(x)
#define PG_RETURN_NULL() return (Datum)0
#define PG_RETURN_VOID() return (Datum)0
#define PG_GETARG_DATUM(n) (((MockFunctionCallInfo*)fcinfo)->args[n])
#define PG_GETARG_INT32(n) (int32)(((MockFunctionCallInfo*)fcinfo)->args[n])
#define PG_GETARG_POINTER(n) (void*)(((MockFunctionCallInfo*)fcinfo)->args[n])
#define PG_GETARG_VARLENA_P(n) (void*)(((MockFunctionCallInfo*)fcinfo)->args[n])
#define PG_GETARG_ARRAYTYPE_P(n) (void*)(((MockFunctionCallInfo*)fcinfo)->args[n])
#define PG_GETARG_CSTRING(n) (char*)(((MockFunctionCallInfo*)fcinfo)->args[n])
#define PG_GETARG_TEXT_P(n) (text*)(((MockFunctionCallInfo*)fcinfo)->args[n])
#define PG_GETARG_BYTEA_P(n) (bytea*)(((MockFunctionCallInfo*)fcinfo)->args[n])
#define PG_DETOAST_DATUM(d) (void*)(d)
#define PG_RETURN_CSTRING(x) return (Datum)(x)

#define PG_FUNCTION_INFO_V1(name)

#define cstring_to_text(s) (void*)strdup(s)
#define text_to_cstring(t) (char*)(t)
#define DatumGetTextPP(d) (text*)(d)
#define DatumGetByteaP(d) (bytea*)(d)
#define PointerGetDatum(p) (Datum)(p)

/* StringInfo Mock */
typedef struct {
    char *data;
    int len;
    int maxlen;
} StringInfoData;
typedef StringInfoData* StringInfo;

static inline void initStringInfo(StringInfo str) { 
    str->maxlen = 1024;
    str->data = malloc(str->maxlen); 
    str->len = 0; 
    str->data[0] = '\0';
}
static inline void appendBinaryStringInfo(StringInfo str, const char *data, int len) { 
    if (str->len + len + 1 >= str->maxlen) {
        str->maxlen = (str->len + len + 1) * 2;
        str->data = realloc(str->data, str->maxlen);
    }
    memcpy(str->data + str->len, data, len); 
    str->len += len; 
    str->data[str->len] = '\0';
}

static inline void appendStringInfoChar(StringInfo str, char c) { 
    if (str->len + 2 >= str->maxlen) {
        str->maxlen *= 2;
        str->data = realloc(str->data, str->maxlen);
    }
    str->data[str->len++] = c; 
    str->data[str->len] = '\0';
}

static inline void appendStringInfoString(StringInfo str, const char *s) { 
    int l = strlen(s); 
    appendBinaryStringInfo(str, s, l);
}
static inline void appendStringInfo(StringInfo str, const char *fmt, ...) {
    va_list args;
    char buf[1024];
    va_start(args, fmt);
    int n = vsnprintf(buf, 1024, fmt, args);
    va_end(args);
    appendBinaryStringInfo(str, buf, n);
}

/* SPI Mock */
typedef struct {
    void *tupdesc;
    void **vals;
} SPITupleTable;

typedef struct {
    int processed;
    SPITupleTable *tuptable;
} SPIGlobal;

static SPIGlobal mock_spi = {0, NULL};
#define SPI_processed mock_spi.processed
#define SPI_tuptable mock_spi.tuptable

#define SPI_OK_SELECT 1
#define SPI_OK_INSERT 1
static inline int SPI_connect() { return 0; }
static inline int SPI_finish() { return 0; }
static inline int SPI_execute(const char *src, bool read_only, long tcount) { return SPI_OK_SELECT; }
static inline int SPI_execute_with_args(const char *src, int nargs, Oid *argtypes, Datum *Values, const char *Nulls, bool read_only, long tcount) { return SPI_OK_INSERT; }
static inline Datum SPI_getbinval(void *tuple, void *tupdesc, int fnumber, bool *isnull) { *isnull = false; return (Datum)0; }

typedef struct {
    int natts;
} TupleDescData;
typedef TupleDescData* TupleDesc;
typedef void* HeapTuple;

/* Array Mock */
typedef void ArrayType;
#ifndef MOCK_DECONSTRUCT_ARRAY
static inline void get_typlenbyvalalign(Oid type, int16_t *len, bool *byval, char *align) {}
static inline void deconstruct_array(ArrayType *array, Oid type, int16_t len, bool byval, char align, Datum **elems, bool **nulls, int *nelems) {
    *nelems = 0; *elems = NULL; *nulls = NULL;
}
#endif


#endif
