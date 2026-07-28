/*-------------------------------------------------------------------------
 *
 * test_buffer_frame.c
 *	  Test support for shared buffer frame pin gates.
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/modules/test_buffer_frame/test_buffer_frame.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/injection_point.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

static Buffer
read_test_buffer(Oid relid, int32 block, Relation *rel)
{
	BlockNumber nblocks;

	if (block < 0)
		elog(ERROR, "invalid block number: %d", block);

	*rel = table_open(relid, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(*rel);
	if ((BlockNumber) block >= nblocks)
		elog(ERROR, "block %d is past relation size %u", block, nblocks);

	return ReadBuffer(*rel, (BlockNumber) block);
}

static void
run_test_injection_point(text *point)
{
	char	   *name = text_to_cstring(point);

	if (name[0] != '\0')
		INJECTION_POINT(name, NULL);
}

static BufferDesc *
get_test_buffer_desc(int32 buffer)
{
	if (buffer <= 0 || buffer > NBuffers)
		elog(ERROR, "invalid shared buffer ID: %d", buffer);

	return GetBufferDescriptor(buffer - 1);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_buffer_id);
Datum
test_buffer_frame_buffer_id(PG_FUNCTION_ARGS)
{
	Relation	rel;
	Buffer		buffer;

	buffer = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1), &rel);
	ReleaseBuffer(buffer);
	table_close(rel, AccessShareLock);

	PG_RETURN_INT32(buffer);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_reader);
Datum
test_buffer_frame_reader(PG_FUNCTION_ARGS)
{
	Relation	rel;
	Buffer		buffer;
	Page		page;
	uint16		pd_lower;
	uint16		pd_upper;
	uint16		pd_special;
	bool		nested = PG_GETARG_BOOL(4);

	buffer = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1), &rel);

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	pd_lower = ((PageHeader) page)->pd_lower;
	pd_upper = ((PageHeader) page)->pd_upper;
	pd_special = ((PageHeader) page)->pd_special;
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	run_test_injection_point(PG_GETARG_TEXT_PP(2));

	if (nested)
	{
		IncrBufferRefCount(buffer);
		ReleaseBuffer(buffer);
		run_test_injection_point(PG_GETARG_TEXT_PP(3));
	}

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	if (page != BufferGetPage(buffer) ||
		pd_lower != ((PageHeader) page)->pd_lower ||
		pd_upper != ((PageHeader) page)->pd_upper ||
		pd_special != ((PageHeader) page)->pd_special)
		elog(ERROR, "buffer page pointer changed while pinned");
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	ReleaseBuffer(buffer);
	table_close(rel, AccessShareLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_writer);
Datum
test_buffer_frame_writer(PG_FUNCTION_ARGS)
{
	Relation	rel;
	Buffer		buffer;
	char	   *action = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (strcmp(action, "normal") != 0 &&
		strcmp(action, "read") != 0 &&
		strcmp(action, "incr") != 0 &&
		strcmp(action, "release") != 0 &&
		strcmp(action, "error") != 0)
		elog(ERROR, "unknown writer action: %s", action);

	buffer = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1), &rel);
	run_test_injection_point(PG_GETARG_TEXT_PP(2));
	TestOnlyBeginBufferWriteIntent(buffer);
	run_test_injection_point(PG_GETARG_TEXT_PP(3));

	if (strcmp(action, "read") == 0)
		(void) ReadBuffer(rel, PG_GETARG_INT32(1));
	else if (strcmp(action, "incr") == 0)
		IncrBufferRefCount(buffer);
	else if (strcmp(action, "release") == 0)
		ReleaseBuffer(buffer);
	else if (strcmp(action, "error") == 0)
		elog(ERROR, "deliberate buffer write-intent test error");

	TestOnlyEndBufferWriteIntent(buffer);
	ReleaseBuffer(buffer);
	table_close(rel, AccessShareLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_two_buffers);
Datum
test_buffer_frame_two_buffers(PG_FUNCTION_ARGS)
{
	Relation	target_rel;
	Relation	other_rel;
	Buffer		target;
	Buffer		other;
	char	   *action = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (strcmp(action, "begin_with_other") != 0 &&
		strcmp(action, "pin_other_after_begin") != 0)
		elog(ERROR, "unknown two-buffer action: %s", action);

	target = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1),
							  &target_rel);

	if (strcmp(action, "begin_with_other") == 0)
	{
		other = read_test_buffer(PG_GETARG_OID(2), PG_GETARG_INT32(3),
								 &other_rel);
		TestOnlyBeginBufferWriteIntent(target);

		TestOnlyEndBufferWriteIntent(target);
		ReleaseBuffer(other);
		table_close(other_rel, AccessShareLock);
	}
	else
	{
		TestOnlyBeginBufferWriteIntent(target);
		other = read_test_buffer(PG_GETARG_OID(2), PG_GETARG_INT32(3),
								 &other_rel);

		ReleaseBuffer(other);
		table_close(other_rel, AccessShareLock);
		TestOnlyEndBufferWriteIntent(target);
	}

	ReleaseBuffer(target);
	table_close(target_rel, AccessShareLock);
	elog(ERROR, "two-buffer action unexpectedly succeeded");
}

PG_FUNCTION_INFO_V1(test_buffer_frame_cross_pin);
Datum
test_buffer_frame_cross_pin(PG_FUNCTION_ARGS)
{
	Relation	held_rel;
	Relation	target_rel;
	Buffer		held;
	Buffer		target;

	held = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1), &held_rel);
	run_test_injection_point(PG_GETARG_TEXT_PP(4));
	target = read_test_buffer(PG_GETARG_OID(2), PG_GETARG_INT32(3),
							  &target_rel);

	ReleaseBuffer(target);
	table_close(target_rel, AccessShareLock);
	ReleaseBuffer(held);
	table_close(held_rel, AccessShareLock);
	elog(ERROR, "cross-buffer pin unexpectedly succeeded");
}

PG_FUNCTION_INFO_V1(test_buffer_frame_dirty_reader);
Datum
test_buffer_frame_dirty_reader(PG_FUNCTION_ARGS)
{
	Relation	rel;
	Buffer		buffer;

	buffer = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1), &rel);
	run_test_injection_point(PG_GETARG_TEXT_PP(2));

	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	MarkBufferDirty(buffer);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	run_test_injection_point(PG_GETARG_TEXT_PP(3));
	ReleaseBuffer(buffer);
	table_close(rel, AccessShareLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_flush);
Datum
test_buffer_frame_flush(PG_FUNCTION_ARGS)
{
	Relation	rel;
	char	   *mode = text_to_cstring(PG_GETARG_TEXT_PP(1));

	rel = table_open(PG_GETARG_OID(0), AccessShareLock);
	if (strcmp(mode, "relation") == 0)
		FlushRelationBuffers(rel);
	else if (strcmp(mode, "relations") == 0)
	{
		SMgrRelation smgr = RelationGetSmgr(rel);

		FlushRelationsAllBuffers(&smgr, 1);
	}
	else if (strcmp(mode, "database") == 0)
		FlushDatabaseBuffers(MyDatabaseId);
	else
		elog(ERROR, "unknown flush mode: %s", mode);
	table_close(rel, AccessShareLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_is_dirty);
Datum
test_buffer_frame_is_dirty(PG_FUNCTION_ARGS)
{
	Relation	rel;
	Buffer		buffer;
	bool		dirty;

	buffer = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1), &rel);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	dirty = BufferIsDirty(buffer);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
	table_close(rel, AccessShareLock);

	PG_RETURN_BOOL(dirty);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_cleanup);
Datum
test_buffer_frame_cleanup(PG_FUNCTION_ARGS)
{
	Relation	rel;
	Buffer		buffer;

	buffer = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1), &rel);
	run_test_injection_point(PG_GETARG_TEXT_PP(2));
	LockBufferForCleanup(buffer);
	run_test_injection_point(PG_GETARG_TEXT_PP(3));
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
	table_close(rel, AccessShareLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_state);
Datum
test_buffer_frame_state(PG_FUNCTION_ARGS)
{
	int32		buffer = PG_GETARG_INT32(0);
	BufferDesc *buf = get_test_buffer_desc(buffer);
	ProcNumber	owner;
	BufferFrameId frame_id;
	uint32		buf_state;

	buf_state = LockBufHdr(buf);
	owner = (ProcNumber)
		pg_atomic_read_u32(&BufferWriteIntentOwners[buf->buf_id]);
	frame_id = pg_atomic_read_u32(&BufferFrameIds[buf->buf_id]);
	UnlockBufHdr(buf, buf_state);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%d:%c:%u:%u",
											  owner,
											  (buf_state & BM_PIN_COUNT_WAITER) ?
											  't' : 'f',
											  BUF_STATE_GET_REFCOUNT(buf_state),
											  frame_id)));
}

PG_FUNCTION_INFO_V1(test_buffer_frame_broadcast);
Datum
test_buffer_frame_broadcast(PG_FUNCTION_ARGS)
{
	BufferDesc *buf = get_test_buffer_desc(PG_GETARG_INT32(0));

	ConditionVariableBroadcast(BufferDescriptorGetIOCV(buf));
	PG_RETURN_VOID();
}
