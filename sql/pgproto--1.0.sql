-- Create shell type
CREATE TYPE protobuf;

-- Define I/O functions
CREATE FUNCTION protobuf_in(cstring) RETURNS protobuf
    AS 'MODULE_PATHNAME', 'protobuf_in'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION protobuf_out(protobuf) RETURNS cstring
    AS 'MODULE_PATHNAME', 'protobuf_out'
    LANGUAGE C IMMUTABLE STRICT;

-- Define final type
CREATE TYPE protobuf (
    INPUT = protobuf_in,
    OUTPUT = protobuf_out,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = int4,
    STORAGE = extended
);


-- Define tag extraction functions
CREATE FUNCTION pb_get_int32(protobuf, int4) RETURNS int4
    AS 'MODULE_PATHNAME', 'pb_get_int32'
    LANGUAGE C IMMUTABLE STRICT;

-- Define Schema Registry Table
CREATE TABLE pb_schemas (
    id serial PRIMARY KEY,
    name text UNIQUE,
    data bytea
);

-- Define schema registration function
CREATE FUNCTION pb_register_schema(name text, data bytea) RETURNS void
    AS 'MODULE_PATHNAME', 'pb_register_schema'
    LANGUAGE C IMMUTABLE STRICT;

-- Define name-based extraction function (3-arg)
CREATE FUNCTION pb_get_int32_by_name(protobuf, text, text) RETURNS int4
    AS 'MODULE_PATHNAME', 'pb_get_int32_by_name'
    LANGUAGE C IMMUTABLE STRICT;

-- Define name-based extraction function (2-arg) using 'Message.Field'
CREATE FUNCTION pb_get_int32_by_name_dot(protobuf, text) RETURNS int4
    AS 'MODULE_PATHNAME', 'pb_get_int32_by_name_dot'
    LANGUAGE C IMMUTABLE STRICT;

-- Define shorthand operator ->
CREATE OPERATOR -> (
    LEFTARG = protobuf,
    RIGHTARG = text,
    FUNCTION = pb_get_int32_by_name_dot
);





-- Define contains function
CREATE FUNCTION protobuf_contains(protobuf, protobuf) RETURNS bool
    AS 'MODULE_PATHNAME', 'protobuf_contains'
    LANGUAGE C IMMUTABLE STRICT;

-- Define contains operator
CREATE OPERATOR @> (
    LEFTARG = protobuf,
    RIGHTARG = protobuf,
    FUNCTION = protobuf_contains,
    RESTRICT = eqsel
);


-- Define GIN support functions
CREATE FUNCTION protobuf_gin_extract_value(protobuf, internal, internal) RETURNS internal
    AS 'MODULE_PATHNAME', 'protobuf_gin_extract_value'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION protobuf_gin_extract_query(protobuf, internal, int2, internal, internal, internal) RETURNS internal
    AS 'MODULE_PATHNAME', 'protobuf_gin_extract_query'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION protobuf_gin_consistent(internal, int2, protobuf, int4, internal, internal) RETURNS bool
    AS 'MODULE_PATHNAME', 'protobuf_gin_consistent'
    LANGUAGE C IMMUTABLE STRICT;


-- Define GIN Operator Class
CREATE OPERATOR CLASS protobuf_gin_ops
    DEFAULT FOR TYPE protobuf USING gin AS
    OPERATOR 1 @>,
    FUNCTION 1 bttextcmp(text, text),
    FUNCTION 2 protobuf_gin_extract_value(protobuf, internal, internal),
    FUNCTION 3 protobuf_gin_extract_query(protobuf, internal, int2, internal, internal, internal),

    FUNCTION 4 protobuf_gin_consistent(internal, int2, protobuf, int4, internal, internal),

    STORAGE text;


CREATE FUNCTION pb_get_int32_by_path(protobuf, text[]) RETURNS int4
    AS 'MODULE_PATHNAME', 'pb_get_int32_by_path'
    LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;


CREATE OPERATOR #> (
    LEFTARG = protobuf,
    RIGHTARG = text[],
    FUNCTION = pb_get_int32_by_path
);


CREATE FUNCTION pb_to_json(protobuf, text) RETURNS text
    AS 'MODULE_PATHNAME', 'pb_to_json'
    LANGUAGE C IMMUTABLE STRICT;


-- Keep placeholder function for verification

CREATE FUNCTION pgproto_hello() RETURNS text
     AS 'MODULE_PATHNAME', 'pgproto_hello'
     LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pb_set(protobuf, text[], text, bool DEFAULT true) RETURNS protobuf
    AS 'MODULE_PATHNAME', 'pb_set'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pb_insert(protobuf, text[], text) RETURNS protobuf
    AS 'MODULE_PATHNAME', 'pb_insert'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pb_delete(protobuf, text[]) RETURNS protobuf
    AS 'MODULE_PATHNAME', 'pb_delete'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION pb_merge(protobuf, protobuf) RETURNS protobuf
    AS 'MODULE_PATHNAME', 'pb_merge'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR || (
    LEFTARG = protobuf,
    RIGHTARG = protobuf,
    FUNCTION = pb_merge
);

-- Add cast to bytea for convenience (e.g., for length() function)
CREATE CAST (protobuf AS bytea) WITHOUT FUNCTION;


