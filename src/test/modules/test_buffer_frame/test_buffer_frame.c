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
#include "common/hashfn.h"
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

static bool
pin_test_buffer_by_tag(FunctionCallInfo fcinfo, int first_arg, Buffer *buffer)
{
	int32		buffer_id = PG_GETARG_INT32(first_arg);
	Oid			database = PG_GETARG_OID(first_arg + 1);
	Oid			tablespace = PG_GETARG_OID(first_arg + 2);
	Oid			relfilenumber = PG_GETARG_OID(first_arg + 3);
	int16		fork = PG_GETARG_INT16(first_arg + 4);
	int64		block = PG_GETARG_INT64(first_arg + 5);
	RelFileLocator locator;

	if (buffer_id <= 0 || buffer_id > NBuffers)
		elog(ERROR, "invalid shared buffer ID: %d", buffer_id);
	if (fork < 0 || fork > MAX_FORKNUM)
		elog(ERROR, "invalid fork number: %d", fork);
	if (block < 0 || block > MaxBlockNumber)
		elog(ERROR, "invalid block number: " INT64_FORMAT, block);

	locator.spcOid = tablespace;
	locator.dbOid = database;
	locator.relNumber = relfilenumber;
	*buffer = (Buffer) buffer_id;

	return ReadRecentBuffer(locator, (ForkNumber) fork, (BlockNumber) block,
							*buffer);
}

static void
lock_test_page_pair_shared(Buffer first, Buffer second)
{
	if (first < second)
	{
		LockBuffer(first, BUFFER_LOCK_SHARE);
		LockBuffer(second, BUFFER_LOCK_SHARE);
	}
	else
	{
		LockBuffer(second, BUFFER_LOCK_SHARE);
		LockBuffer(first, BUFFER_LOCK_SHARE);
	}
}

static void
unlock_test_page_pair_shared(Buffer first, Buffer second)
{
	if (first < second)
	{
		LockBuffer(second, BUFFER_LOCK_UNLOCK);
		LockBuffer(first, BUFFER_LOCK_UNLOCK);
	}
	else
	{
		LockBuffer(first, BUFFER_LOCK_UNLOCK);
		LockBuffer(second, BUFFER_LOCK_UNLOCK);
	}
}

static bool
test_buffer_is_dirty(Buffer buffer)
{
	BufferDesc *desc;

	Assert(BufferIsValid(buffer) && !BufferIsLocal(buffer));
	desc = GetBufferDescriptor(buffer - 1);
	return (pg_atomic_read_u32(&desc->state) & BM_DIRTY) != 0;
}

static void
lock_test_page_pair(Buffer target, Buffer source)
{
	if (target < source)
	{
		LockBuffer(target, BUFFER_LOCK_EXCLUSIVE);
		LockBuffer(source, BUFFER_LOCK_SHARE);
	}
	else
	{
		LockBuffer(source, BUFFER_LOCK_SHARE);
		LockBuffer(target, BUFFER_LOCK_EXCLUSIVE);
	}
}

static void
unlock_test_page_pair(Buffer target, Buffer source)
{
	if (target < source)
	{
		LockBuffer(source, BUFFER_LOCK_UNLOCK);
		LockBuffer(target, BUFFER_LOCK_UNLOCK);
	}
	else
	{
		LockBuffer(target, BUFFER_LOCK_UNLOCK);
		LockBuffer(source, BUFFER_LOCK_UNLOCK);
	}
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

PG_FUNCTION_INFO_V1(test_buffer_frame_page_digest);
Datum
test_buffer_frame_page_digest(PG_FUNCTION_ARGS)
{
	Buffer		buffer;
	uint64		digest;
	bool		clean;

	if (!pin_test_buffer_by_tag(fcinfo, 0, &buffer))
		PG_RETURN_NULL();

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	clean = !test_buffer_is_dirty(buffer);
	if (clean)
		digest = hash_bytes_extended((unsigned char *) BufferGetPage(buffer),
									 BLCKSZ, 0);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);

	if (!clean)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) digest);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_pages_equal_by_tag);
Datum
test_buffer_frame_pages_equal_by_tag(PG_FUNCTION_ARGS)
{
	Buffer		first;
	Buffer		second;
	bool		clean;
	bool		equal = false;

	if (PG_GETARG_INT32(0) == PG_GETARG_INT32(6))
		elog(ERROR, "page comparison requires distinct buffers");
	if (!pin_test_buffer_by_tag(fcinfo, 0, &first))
		PG_RETURN_NULL();
	if (!pin_test_buffer_by_tag(fcinfo, 6, &second))
	{
		ReleaseBuffer(first);
		PG_RETURN_NULL();
	}

	lock_test_page_pair_shared(first, second);
	clean = !test_buffer_is_dirty(first) && !test_buffer_is_dirty(second);
	if (clean)
		equal = memcmp(BufferGetPage(first), BufferGetPage(second),
					   BLCKSZ) == 0;
	unlock_test_page_pair_shared(first, second);

	ReleaseBuffer(second);
	ReleaseBuffer(first);

	if (!clean)
		PG_RETURN_NULL();
	PG_RETURN_BOOL(equal);
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

PG_FUNCTION_INFO_V1(test_buffer_frame_copy_page);
Datum
test_buffer_frame_copy_page(PG_FUNCTION_ARGS)
{
	Relation	target_rel;
	Relation	source_rel;
	Buffer		target;
	Buffer		source;

	target = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1),
							  &target_rel);
	source = read_test_buffer(PG_GETARG_OID(2), PG_GETARG_INT32(3),
							  &source_rel);
	if (BufferIsLocal(target) || BufferIsLocal(source))
		elog(ERROR, "test page copy requires shared buffers");
	if (target == source)
		elog(ERROR, "test page copy requires distinct buffers");

	lock_test_page_pair(target, source);
	memcpy(BufferGetPage(target), BufferGetPage(source), BLCKSZ);
	MarkBufferDirty(target);
	unlock_test_page_pair(target, source);

	ReleaseBuffer(source);
	ReleaseBuffer(target);
	table_close(source_rel, AccessShareLock);
	table_close(target_rel, AccessShareLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_attach);
Datum
test_buffer_frame_attach(PG_FUNCTION_ARGS)
{
	Relation	target_rel;
	Relation	source_rel;
	Buffer		target;
	Buffer		source;
	BufferTag	source_tag;
	char	   *action = text_to_cstring(PG_GETARG_TEXT_PP(5));

	if (strcmp(action, "normal") != 0 && strcmp(action, "error") != 0)
		elog(ERROR, "unknown attach action: %s", action);

	target = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1),
							  &target_rel);
	source = read_test_buffer(PG_GETARG_OID(2), PG_GETARG_INT32(3),
							  &source_rel);
	if (target == source)
		elog(ERROR, "test-only frame attach requires distinct buffers");

	if (BufferIsLocal(source))
		ClearBufferTag(&source_tag);
	else
		source_tag = GetBufferDescriptor(source - 1)->tag;
	ReleaseBuffer(source);

	TestOnlyBeginBufferWriteIntent(target);
	TestOnlyAttachBufferFrame(target, source, &source_tag);
	run_test_injection_point(PG_GETARG_TEXT_PP(4));
	if (strcmp(action, "error") == 0)
		elog(ERROR, "deliberate error after test-only frame attach");
	TestOnlyEndBufferWriteIntent(target);

	ReleaseBuffer(target);
	table_close(source_rel, AccessShareLock);
	table_close(target_rel, AccessShareLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_detach);
Datum
test_buffer_frame_detach(PG_FUNCTION_ARGS)
{
	Relation	rel;
	Buffer		buffer;
	char	   *action = text_to_cstring(PG_GETARG_TEXT_PP(4));

	if (strcmp(action, "normal") != 0 && strcmp(action, "error") != 0)
		elog(ERROR, "unknown detach action: %s", action);

	buffer = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1), &rel);
	run_test_injection_point(PG_GETARG_TEXT_PP(2));
	TestOnlyBeginBufferWriteIntent(buffer);
	TestOnlyDetachBufferFrame(buffer);
	run_test_injection_point(PG_GETARG_TEXT_PP(3));
	if (strcmp(action, "error") == 0)
		elog(ERROR, "deliberate error after test-only frame detach");
	TestOnlyEndBufferWriteIntent(buffer);

	ReleaseBuffer(buffer);
	table_close(rel, AccessShareLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_mapping_state);
Datum
test_buffer_frame_mapping_state(PG_FUNCTION_ARGS)
{
	int32		buffer = PG_GETARG_INT32(0);
	BufferDesc *buf = get_test_buffer_desc(buffer);
	BufferFrameId frame_id;
	uint32		attachments;
	uint32		home_attachments;
	uint32		generation;
	uint32		nonidentity;
	uint32		buf_state;

	buf_state = LockBufHdr(buf);
	frame_id = pg_atomic_read_u32(&BufferFrameIds[buf->buf_id]);
	attachments =
		pg_atomic_read_u32(&BufferFrameAttachmentCounts[frame_id]);
	home_attachments =
		pg_atomic_read_u32(&BufferFrameAttachmentCounts[buf->buf_id]);
	generation = pg_atomic_read_u32(&BufferFrameGenerations[buf->buf_id]);
	nonidentity = pg_atomic_read_u32(BufferNonIdentityFrameCount);
	UnlockBufHdr(buf, buf_state);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%u:%u:%u:%u:%u",
											  frame_id,
											  attachments,
											  home_attachments,
											  generation,
											  nonidentity)));
}

PG_FUNCTION_INFO_V1(test_buffer_frame_evict);
Datum
test_buffer_frame_evict(PG_FUNCTION_ARGS)
{
	Buffer		buffer = PG_GETARG_INT32(0);
	bool		flushed;

	if (buffer <= 0 || buffer > NBuffers)
		elog(ERROR, "invalid shared buffer ID: %d", buffer);

	PG_RETURN_BOOL(EvictUnpinnedBuffer(buffer, &flushed));
}

PG_FUNCTION_INFO_V1(test_buffer_frame_pressure);
Datum
test_buffer_frame_pressure(PG_FUNCTION_ARGS)
{
	Relation	rel;
	Buffer	   *buffers;
	BlockNumber block;
	BlockNumber nblocks;
	int			npins = NBuffers - 1;

	rel = table_open(PG_GETARG_OID(0), AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(rel);
	if (nblocks < (BlockNumber) npins)
		elog(ERROR,
			 "test relation has %u blocks, expected at least %d",
			 nblocks, npins);
	buffers = palloc(sizeof(Buffer) * npins);

	/*
	 * Keep NBuffers - 1 pages pinned.  With a test-shared pair consuming two
	 * protected descriptors, clock sweep must fail after one bounded pass.
	 * Once the pair is detached, the same request must fit and return
	 * normally.
	 */
	for (block = 0; block < (BlockNumber) npins; block++)
		buffers[block] = ReadBuffer(rel, block);

	while (block > 0)
		ReleaseBuffer(buffers[--block]);
	table_close(rel, AccessShareLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_drop_buffers);
Datum
test_buffer_frame_drop_buffers(PG_FUNCTION_ARGS)
{
	Relation	rel;
	SMgrRelation smgr;
	ForkNumber	fork = MAIN_FORKNUM;
	BlockNumber first = 0;

	rel = table_open(PG_GETARG_OID(0), AccessExclusiveLock);
	smgr = RelationGetSmgr(rel);
	DropRelationBuffers(smgr, &fork, 1, &first);
	table_close(rel, AccessExclusiveLock);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_private_mutation);
Datum
test_buffer_frame_private_mutation(PG_FUNCTION_ARGS)
{
	Relation	target_rel;
	Relation	source_rel;
	Buffer		target;
	Buffer		source;
	unsigned char *target_page;
	unsigned char *source_page;
	unsigned char target_byte;
	unsigned char source_byte;
	bool		isolated;

	target = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1),
							  &target_rel);
	source = read_test_buffer(PG_GETARG_OID(2), PG_GETARG_INT32(3),
							  &source_rel);
	if (BufferIsLocal(target) || BufferIsLocal(source))
		elog(ERROR, "private mutation check requires shared buffers");
	if (target == source)
		elog(ERROR, "private mutation check requires distinct buffers");

	lock_test_page_pair(target, source);
	target_page = (unsigned char *) BufferGetPage(target);
	source_page = (unsigned char *) BufferGetPage(source);
	if (target_page == source_page)
		elog(ERROR, "target still points to source frame");

	target_byte = target_page[BLCKSZ - 1];
	source_byte = source_page[BLCKSZ - 1];
	if (target_byte != source_byte)
		elog(ERROR, "detached target bytes differ from source");

	target_page[BLCKSZ - 1] ^= 0x5a;
	isolated = source_page[BLCKSZ - 1] == source_byte &&
		target_page[BLCKSZ - 1] != source_page[BLCKSZ - 1];
	target_page[BLCKSZ - 1] = target_byte;
	unlock_test_page_pair(target, source);

	ReleaseBuffer(source);
	ReleaseBuffer(target);
	table_close(source_rel, AccessShareLock);
	table_close(target_rel, AccessShareLock);

	PG_RETURN_BOOL(isolated);
}

PG_FUNCTION_INFO_V1(test_buffer_frame_pages_alias);
Datum
test_buffer_frame_pages_alias(PG_FUNCTION_ARGS)
{
	Relation	target_rel;
	Relation	source_rel;
	Buffer		target;
	Buffer		source;
	Page		target_page;
	Page		source_page;
	bool		aliased;

	target = read_test_buffer(PG_GETARG_OID(0), PG_GETARG_INT32(1),
							  &target_rel);
	source = read_test_buffer(PG_GETARG_OID(2), PG_GETARG_INT32(3),
							  &source_rel);
	if (BufferIsLocal(target) || BufferIsLocal(source))
		elog(ERROR, "page alias check requires shared buffers");
	if (target == source)
		elog(ERROR, "page alias check requires distinct buffers");

	if (target < source)
	{
		LockBuffer(target, BUFFER_LOCK_SHARE);
		LockBuffer(source, BUFFER_LOCK_SHARE);
	}
	else
	{
		LockBuffer(source, BUFFER_LOCK_SHARE);
		LockBuffer(target, BUFFER_LOCK_SHARE);
	}

	target_page = BufferGetPage(target);
	source_page = BufferGetPage(source);
	aliased = target_page == source_page &&
		memcmp(target_page, source_page, BLCKSZ) == 0;

	if (target < source)
	{
		LockBuffer(source, BUFFER_LOCK_UNLOCK);
		LockBuffer(target, BUFFER_LOCK_UNLOCK);
	}
	else
	{
		LockBuffer(target, BUFFER_LOCK_UNLOCK);
		LockBuffer(source, BUFFER_LOCK_UNLOCK);
	}

	ReleaseBuffer(source);
	ReleaseBuffer(target);
	table_close(source_rel, AccessShareLock);
	table_close(target_rel, AccessShareLock);

	PG_RETURN_BOOL(aliased);
}
