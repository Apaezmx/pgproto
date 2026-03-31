CREATE EXTENSION pgproto;

-- Create table with custom type
CREATE TABLE pb_test (id serial, data protobuf);

-- Register test schema first to avoid session caching issues in tests
INSERT INTO pb_schemas (name, data) VALUES ('scratch/test.proto', decode('0AFE010A12736372617463682F746573742E70726F746F22170A05496E6E6572120E0A0269641801200128055202696422C6010A054F75746572121C0A05696E6E657218012001280B32062E496E6E65725205696E6E657212240A047461677318022003280B32102E4F757465722E54616773456E74727952047461677312160A0673636F726573180320032805520673636F726573120E0A01611804200128054800520161120E0A016218052001280948005201621A370A0954616773456E74727912100A036B657918012001280952036B657912140A0576616C7565180220012805520576616C75653A02380142080A0663686F696365620670726F746F33', 'hex'));

-- Insert valid protobuf (Tag 1, Varint 42 => 0x08 0x2a)
INSERT INTO pb_test (data) VALUES ('\x082a');

-- Select it back (Hex output)
SELECT data FROM pb_test;

-- Extract Tag 1
SELECT pb_get_int32(data, 1) FROM pb_test;

-- Extract Tag 2 (it doesn't exist, should return NULL)
SELECT pb_get_int32(data, 2) FROM pb_test;

-- Insert multi-field protobuf: Tag 1 = 42, Tag 2 = Varint 120 (0x10 0x78)
-- Payload: 0x08 0x2a 0x10 0x78
INSERT INTO pb_test (data) VALUES ('\x082a1078');

-- Extract from multi-field
SELECT pb_get_int32(data, 1) AS tag1, pb_get_int32(data, 2) AS tag2 FROM pb_test WHERE id = 2;

-- Negative test for name-based extraction (should fail because message is not registered or found)
-- We wrap it in a function or just run it and expect error. In pg_regress, we can just run it and let the output match expected errors!
SELECT pb_get_int32_by_name(data, 'UnregisteredMessage', 'id') FROM pb_test WHERE id = 1;

-- Test shorthand operator -> (Dot Notation)
SELECT data -> 'UnregisteredMessage.id'::text FROM pb_test WHERE id = 1;


-- Test GIN Indexing (Purity)
CREATE TABLE pb_test_gin (id serial, data protobuf);

-- Insert row with Tag 1 = 42
INSERT INTO pb_test_gin (data) VALUES ('\x082a');

-- Insert row with Tag 1 = 100
INSERT INTO pb_test_gin (data) VALUES ('\x0864');

-- Create GIN index
CREATE INDEX pb_gin_idx ON pb_test_gin USING gin (data);

-- Verify contains operator @> sequentially (or via index if small enough)
SELECT * FROM pb_test_gin WHERE data @> '\x082a'::protobuf;


-- Nested Path Navigation Test

-- Insert valid nested protobuf (Outer.inner.id = 42)
INSERT INTO pb_test (data) VALUES ('\x0a02082a'::protobuf);

-- Test #> with valid path
SELECT data #> '{Outer, inner, id}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Test #> with invalid path (unknown field)
SELECT data #> '{Outer, inner, unknown}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Test #> with invalid message name
SELECT data #> '{UnknownMessage, id}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;-- Map and Repeated Field Navigation Tests

-- Insert expanded protobuf data
-- Text format: inner { id: 42 } tags { key: "foo", value: 100 } tags { key: "bar", value: 200 } scores: 10 scores: 20 choice { a: 30 }
-- Hex: \x0A02082A12070A03666F6F106412080A0362617210C8011A020A14201E
INSERT INTO pb_test (data) VALUES ('\x0A02082A12070A03666F6F106412080A0362617210C8011A020A14201E'::protobuf);

-- Test array index (scores[0] => 10)
SELECT data #> '{Outer, scores, 0}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Test array index (scores[1] => 20)
SELECT data #> '{Outer, scores, 1}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Test map key lookup (tags["foo"] => 100)
SELECT data #> '{Outer, tags, foo}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Test map key lookup (tags["bar"] => 200)
SELECT data #> '{Outer, tags, bar}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Test oneof (choice a => 30)
SELECT data #> '{Outer, a}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Edge Case: Out of bounds array index
SELECT data #> '{Outer, scores, 5}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Edge Case: Typo in map key (should return NULL / empty)
SELECT data #> '{Outer, tags, unknown_key}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Edge Case: Invalid path length (one element only - message name, should return NULL if we expect field)
SELECT data #> '{Outer}'::text[] FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;

-- Edge Case: Corrupt protobuf binary (short read / truncated)
INSERT INTO pb_test (data) VALUES ('\x0A0208'::protobuf); -- Truncated inner message
SELECT pb_get_int32(data, 1) FROM (SELECT data FROM pb_test ORDER BY id DESC LIMIT 1) s;


