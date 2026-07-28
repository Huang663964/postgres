/* src/test/modules/test_buffer_frame/test_buffer_frame--1.0.sql */

\echo Use "CREATE EXTENSION test_buffer_frame" to load this file. \quit

CREATE FUNCTION test_buffer_frame_buffer_id(regclass, int4)
RETURNS int4
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_reader(regclass, int4, text, text, boolean)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_writer(regclass, int4, text, text, text)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_two_buffers(regclass, int4, regclass, int4, text)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_cross_pin(regclass, int4, regclass, int4, text)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_dirty_reader(regclass, int4, text, text)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_flush(regclass, text)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_is_dirty(regclass, int4)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_cleanup(regclass, int4, text, text)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_state(int4)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION test_buffer_frame_broadcast(int4)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT PARALLEL UNSAFE;
