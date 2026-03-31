#include "pgproto.h"

PG_MODULE_MAGIC;

/* Global Schema Pool Definition */
upb_DefPool *s_def_pool = NULL;

PG_FUNCTION_INFO_V1(pgproto_hello);

Datum
pgproto_hello(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text("pgproto environment is ready."));
}
