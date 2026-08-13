/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buf_internals.h"

/* entry for buffer lookup hashtable */
typedef struct
{
	BufferTag	key;			/* Tag of a disk page */
	int			id;				/* Associated buffer ID */
} BufferLookupEnt;

static HTAB *SharedBufHash;

typedef struct DBBranchFrameCandidateEnt
{
	DBBranchFrameCandidateKey key;
	DBBranchFrameCandidate candidate;
}			DBBranchFrameCandidateEnt;

typedef struct DBBranchFrameCandidateSlot
{
	bool		active;
	DBBranchFrameCandidateKey key;
	uint32		tag_generation;
}			DBBranchFrameCandidateSlot;

typedef struct DBBranchFrameCandidateControl
{
	LWLock		lock;
	DBBranchFramePromotionStats stats;
}			DBBranchFrameCandidateControl;

static HTAB *DBBranchFrameCandidateHash;
static DBBranchFrameCandidateControl * DBBranchFrameCandidateCtl;
static DBBranchFrameCandidateSlot * DBBranchFrameCandidateSlots;
DBBranchFramePromotionStats *DBBranchFrameStats;

static bool
DBBranchFrameCandidatesEqual(const DBBranchFrameCandidate * left,
							 const DBBranchFrameCandidate * right)
{
	return left->source_buf_id == right->source_buf_id &&
		left->source_tag_generation == right->source_tag_generation &&
		BufferTagsEqual(&left->source_tag, &right->source_tag);
}

static bool
DBBranchFrameCandidateKeysEqual(const DBBranchFrameCandidateKey * left,
								const DBBranchFrameCandidateKey * right)
{
	return memcmp(left, right, sizeof(*left)) == 0;
}

/* Caller holds DBBranchFrameCandidateCtl->lock exclusively. */
static void
DBBranchFrameCandidateForgetSlot(int buf_id)
{
	DBBranchFrameCandidateSlot *slot;
	DBBranchFrameCandidateEnt *entry;

	Assert(buf_id >= 0 && buf_id < NBuffers);

	slot = &DBBranchFrameCandidateSlots[buf_id];
	if (!slot->active)
		return;

	entry = hash_search(DBBranchFrameCandidateHash, &slot->key,
						HASH_FIND, NULL);
	if (entry != NULL &&
		entry->candidate.source_buf_id == buf_id &&
		entry->candidate.source_tag_generation == slot->tag_generation)
		hash_search(DBBranchFrameCandidateHash, &slot->key,
					HASH_REMOVE, NULL);

	slot->active = false;
}


/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than NBuffers)
 */
Size
BufTableShmemSize(int size)
{
	Size		shmem_size;

	shmem_size = hash_estimate_size(size, sizeof(BufferLookupEnt));
	shmem_size = add_size(shmem_size,
						  hash_estimate_size(size,
											 sizeof(DBBranchFrameCandidateEnt)));
	shmem_size = add_size(shmem_size,
						  mul_size(NBuffers,
								   sizeof(DBBranchFrameCandidateSlot)));
	shmem_size = add_size(shmem_size,
						  MAXALIGN(sizeof(DBBranchFrameCandidateControl)));

	return shmem_size;
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than NBuffers)
 */
void
InitBufTable(int size)
{
	HASHCTL		info;
	bool		found_control;
	bool		found_slots;

	/* assume no locking is needed yet */

	/* BufferTag maps to Buffer */
	info.keysize = sizeof(BufferTag);
	info.entrysize = sizeof(BufferLookupEnt);
	info.num_partitions = NUM_BUFFER_PARTITIONS;

	SharedBufHash = ShmemInitHash("Shared Buffer Lookup Table",
								  size, size,
								  &info,
								  HASH_ELEM | HASH_BLOBS | HASH_PARTITION);

	/*
	 * Page-frame candidates use one short-lived lock.  Promotion is disabled
	 * in normal backends until the immutable lifecycle is complete, so a
	 * partitioned second lock hierarchy would add risk without current value.
	 */
	DBBranchFrameCandidateCtl = (DBBranchFrameCandidateControl *)
		ShmemInitStruct("DB Branch Frame Candidate Control",
						sizeof(DBBranchFrameCandidateControl),
						&found_control);
	DBBranchFrameCandidateSlots = (DBBranchFrameCandidateSlot *)
		ShmemInitStruct("DB Branch Frame Candidate Slots",
						mul_size(NBuffers,
								 sizeof(DBBranchFrameCandidateSlot)),
						&found_slots);

	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(DBBranchFrameCandidateKey);
	info.entrysize = sizeof(DBBranchFrameCandidateEnt);
	DBBranchFrameCandidateHash =
		ShmemInitHash("DB Branch Frame Candidate Table",
					  size, size, &info, HASH_ELEM | HASH_BLOBS);

	if (!found_control)
	{
		LWLockInitialize(&DBBranchFrameCandidateCtl->lock,
						 LWTRANCHE_BUFFER_MAPPING);
		pg_atomic_init_u64(&DBBranchFrameCandidateCtl->stats.attempts, 0);
		pg_atomic_init_u64(&DBBranchFrameCandidateCtl->stats.registrations, 0);
		pg_atomic_init_u64(&DBBranchFrameCandidateCtl->stats.hash_matches, 0);
		pg_atomic_init_u64(&DBBranchFrameCandidateCtl->stats.full_mismatches, 0);
		pg_atomic_init_u64(&DBBranchFrameCandidateCtl->stats.promotions, 0);
		pg_atomic_init_u64(&DBBranchFrameCandidateCtl->stats.skips, 0);
	}
	if (!found_slots)
		MemSet(DBBranchFrameCandidateSlots, 0,
			   mul_size(NBuffers, sizeof(DBBranchFrameCandidateSlot)));

	Assert(found_control == found_slots);
	DBBranchFrameStats = &DBBranchFrameCandidateCtl->stats;
}

bool
DBBranchFrameCandidateLookup(const DBBranchFrameCandidateKey * key,
							 DBBranchFrameCandidate * candidate)
{
	DBBranchFrameCandidateEnt *entry;

	LWLockAcquire(&DBBranchFrameCandidateCtl->lock, LW_SHARED);
	entry = hash_search(DBBranchFrameCandidateHash, (void *) key,
						HASH_FIND, NULL);
	if (entry != NULL)
		*candidate = entry->candidate;
	LWLockRelease(&DBBranchFrameCandidateCtl->lock);

	return entry != NULL;
}

/*
 * Replace a weak candidate only if the table still contains expected.  A
 * NULL expected means that the key must still be absent.
 */
bool
DBBranchFrameCandidateReplace(const DBBranchFrameCandidateKey * key,
							  const DBBranchFrameCandidate * expected,
							  const DBBranchFrameCandidate * replacement)
{
	DBBranchFrameCandidateEnt *entry;
	DBBranchFrameCandidate old_candidate;
	DBBranchFrameCandidateSlot *old_slot;
	DBBranchFrameCandidateSlot *replacement_slot;
	bool		found;
	bool		had_old = false;

	Assert(replacement->source_buf_id >= 0 &&
		   replacement->source_buf_id < NBuffers);

	LWLockAcquire(&DBBranchFrameCandidateCtl->lock, LW_EXCLUSIVE);
	entry = hash_search(DBBranchFrameCandidateHash, (void *) key,
						HASH_FIND, NULL);
	if ((expected == NULL && entry != NULL) ||
		(expected != NULL &&
		 (entry == NULL ||
		  !DBBranchFrameCandidatesEqual(&entry->candidate, expected))))
	{
		LWLockRelease(&DBBranchFrameCandidateCtl->lock);
		return false;
	}

	if (entry != NULL)
	{
		old_candidate = entry->candidate;
		had_old = true;
		hash_search(DBBranchFrameCandidateHash, (void *) key,
					HASH_REMOVE, NULL);
	}

	DBBranchFrameCandidateForgetSlot(replacement->source_buf_id);

	if (had_old && old_candidate.source_buf_id != replacement->source_buf_id)
	{
		old_slot = &DBBranchFrameCandidateSlots[old_candidate.source_buf_id];
		if (old_slot->active &&
			old_slot->tag_generation == old_candidate.source_tag_generation &&
			DBBranchFrameCandidateKeysEqual(&old_slot->key, key))
			old_slot->active = false;
	}

	entry = hash_search(DBBranchFrameCandidateHash, (void *) key,
						HASH_ENTER_NULL, &found);
	if (entry == NULL)
	{
		LWLockRelease(&DBBranchFrameCandidateCtl->lock);
		return false;
	}
	Assert(!found);

	entry->candidate = *replacement;
	replacement_slot =
		&DBBranchFrameCandidateSlots[replacement->source_buf_id];
	replacement_slot->key = *key;
	replacement_slot->tag_generation =
		replacement->source_tag_generation;
	replacement_slot->active = true;

	LWLockRelease(&DBBranchFrameCandidateCtl->lock);
	return true;
}

/*
 * Check the reverse slot used by the candidate table.  This is intentionally
 * lock-based for the first product slice: hint updates are optional and the
 * existing lock avoids another per-buffer atomic side array.
 */
bool
DBBranchFrameCandidateIsRegistered(int buf_id, uint32 tag_generation)
{
	DBBranchFrameCandidateSlot *slot;
	bool		registered;

	if (buf_id < 0 || buf_id >= NBuffers)
		return false;

	LWLockAcquire(&DBBranchFrameCandidateCtl->lock, LW_SHARED);
	slot = &DBBranchFrameCandidateSlots[buf_id];
	registered = slot->active && slot->tag_generation == tag_generation;
	LWLockRelease(&DBBranchFrameCandidateCtl->lock);

	return registered;
}

void
DBBranchFrameCandidateUnregister(int buf_id)
{
	if (buf_id < 0 || buf_id >= NBuffers)
		return;

	LWLockAcquire(&DBBranchFrameCandidateCtl->lock, LW_EXCLUSIVE);
	DBBranchFrameCandidateForgetSlot(buf_id);
	LWLockRelease(&DBBranchFrameCandidateCtl->lock);
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
BufTableHashCode(BufferTag *tagPtr)
{
	return get_hash_value(SharedBufHash, tagPtr);
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)
		return -1;

	return result->id;
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int
BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	BufferLookupEnt *result;
	bool		found;

	Assert(buf_id >= 0);		/* -1 is reserved for not-in-table */
	Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_ENTER,
									&found);

	if (found)					/* found something already in the table */
		return result->id;

	result->id = buf_id;

	return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;
	int			buf_id;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)				/* shouldn't happen */
		elog(ERROR, "shared buffer hash table corrupted");

	buf_id = result->id;
	DBBranchFrameCandidateUnregister(buf_id);
	pg_atomic_fetch_add_u32(&BufferTagGenerations[buf_id], 1);

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(SharedBufHash,
									tagPtr,
									hashcode,
									HASH_REMOVE,
									NULL);
	if (!result)
		elog(ERROR, "shared buffer hash table corrupted");
}
