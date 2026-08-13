/*-------------------------------------------------------------------------
 *
 * buf_init.c
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/aio.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"

BufferDescPadded *BufferDescriptors;
char	   *BufferBlocks;
pg_atomic_uint32 *BufferFrameIds;
pg_atomic_uint32 *BufferNonIdentityFrameCount;
pg_atomic_uint32 *BufferFrameAttachmentCounts;
pg_atomic_uint32 *BufferFrameGenerations;
pg_atomic_uint32 *BufferTagGenerations;
pg_atomic_uint32 *BufferWriteIntentOwners;
ConditionVariableMinimallyPadded *BufferIOCVArray;
WritebackContext BackendWritebackContext;
CkptSortItem *CkptBufferIds;


/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Buffer Replacement:
 *		see freelist.c.  A buffer cannot be replaced while in
 *		use either by data manager or during IO.
 *
 *
 * Synchronization/Locking:
 *
 * IO_IN_PROGRESS -- this is a flag in the buffer descriptor.
 *		It must be set when an IO is initiated and cleared at
 *		the end of the IO.  It is there to make sure that one
 *		process doesn't start to use a buffer while another is
 *		faulting it in.  see WaitIO and related routines.
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a BufferAlloc().
 *		Pins must be released before end of transaction.  For efficiency the
 *		shared refcount isn't increased if an individual backend pins a buffer
 *		multiple times. Check the PrivateRefCount infrastructure in bufmgr.c.
 */


/*
 * Initialize shared buffer pool
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
void
BufferManagerShmemInit(void)
{
	bool		foundBufs,
				foundDescs,
				foundFrameIds,
				foundWriteIntentOwners,
				foundIOCV,
				foundBufCkpt;
	pg_atomic_uint32 *frameMap;
	Size		frameMapEntries;

	/* Align descriptors to a cacheline boundary. */
	BufferDescriptors = (BufferDescPadded *)
		ShmemInitStruct("Buffer Descriptors",
						NBuffers * sizeof(BufferDescPadded),
						&foundDescs);

	/*
	 * ponytail: keep NFrames == NBuffers; add an allocator only when
	 * descriptors must outnumber physical frames.
	 */

	/*
	 * Keep the test-only frame metadata in one allocation:
	 *
	 * non-identity count, descriptor mappings, frame attachment counts,
	 * descriptor mapping generations, descriptor tag generations.
	 */
	frameMapEntries = add_size(mul_size((Size) NBuffers, 4), 1);
	frameMap = (pg_atomic_uint32 *)
		ShmemInitStruct("Buffer Frame IDs",
						mul_size(frameMapEntries,
								 sizeof(pg_atomic_uint32)),
						&foundFrameIds);
	BufferNonIdentityFrameCount = &frameMap[0];
	BufferFrameIds = &frameMap[1];
	BufferFrameAttachmentCounts = &frameMap[1 + NBuffers];
	BufferFrameGenerations = &frameMap[1 + (2 * NBuffers)];
	BufferTagGenerations = &frameMap[1 + (3 * NBuffers)];

	BufferWriteIntentOwners = (pg_atomic_uint32 *)
		ShmemInitStruct("Buffer Write Intent Owners",
						mul_size(NBuffers, sizeof(pg_atomic_uint32)),
						&foundWriteIntentOwners);

	/* Align buffer pool on IO page size boundary. */
	BufferBlocks = (char *)
		TYPEALIGN(PG_IO_ALIGN_SIZE,
				  ShmemInitStruct("Buffer Blocks",
								  NBuffers * (Size) BLCKSZ + PG_IO_ALIGN_SIZE,
								  &foundBufs));

	/* Align condition variables to cacheline boundary. */
	BufferIOCVArray = (ConditionVariableMinimallyPadded *)
		ShmemInitStruct("Buffer IO Condition Variables",
						NBuffers * sizeof(ConditionVariableMinimallyPadded),
						&foundIOCV);

	/*
	 * The array used to sort to-be-checkpointed buffer ids is located in
	 * shared memory, to avoid having to allocate significant amounts of
	 * memory at runtime. As that'd be in the middle of a checkpoint, or when
	 * the checkpointer is restarted, memory allocation failures would be
	 * painful.
	 */
	CkptBufferIds = (CkptSortItem *)
		ShmemInitStruct("Checkpoint BufferIds",
						NBuffers * sizeof(CkptSortItem), &foundBufCkpt);

	if (foundDescs || foundFrameIds || foundWriteIntentOwners ||
		foundBufs || foundIOCV || foundBufCkpt)
	{
		/* should find all of these, or none of them */
		Assert(foundDescs && foundFrameIds && foundWriteIntentOwners &&
			   foundBufs && foundIOCV && foundBufCkpt);
		/* note: this path is only taken in EXEC_BACKEND case */
	}
	else
	{
		int			i;

		pg_atomic_init_u32(BufferNonIdentityFrameCount, 0);

		/*
		 * Initialize all the buffer headers.
		 */
		for (i = 0; i < NBuffers; i++)
		{
			BufferDesc *buf = GetBufferDescriptor(i);

			ClearBufferTag(&buf->tag);

			pg_atomic_init_u32(&buf->state, 0);
			pg_atomic_init_u32(&BufferFrameIds[i], i);
			pg_atomic_init_u32(&BufferFrameAttachmentCounts[i], 1);
			pg_atomic_init_u32(&BufferFrameGenerations[i], 0);
			pg_atomic_init_u32(&BufferTagGenerations[i], 0);
			pg_atomic_init_u32(&BufferWriteIntentOwners[i],
							   INVALID_PROC_NUMBER);
			buf->wait_backend_pgprocno = INVALID_PROC_NUMBER;

			buf->buf_id = i;

			pgaio_wref_clear(&buf->io_wref);

			/*
			 * Initially link all the buffers together as unused. Subsequent
			 * management of this list is done by freelist.c.
			 */
			buf->freeNext = i + 1;

			LWLockInitialize(BufferDescriptorGetContentLock(buf),
							 LWTRANCHE_BUFFER_CONTENT);

			ConditionVariableInit(BufferDescriptorGetIOCV(buf));
		}

		/* Correct last entry of linked list */
		GetBufferDescriptor(NBuffers - 1)->freeNext = FREENEXT_END_OF_LIST;
	}

#ifdef USE_ASSERT_CHECKING
	if (!foundDescs)
	{
		int			i;

		/*
		 * These are initialization invariants, not EXEC_BACKEND attach
		 * invariants: another backend may have an active write intent (and a
		 * later test helper may publish a non-identity frame).
		 */
		Assert(pg_atomic_read_u32(BufferNonIdentityFrameCount) == 0);
		for (i = 0; i < NBuffers; i++)
		{
			Assert(pg_atomic_read_u32(&BufferFrameIds[i]) == (uint32) i);
			Assert(pg_atomic_read_u32(&BufferFrameAttachmentCounts[i]) == 1);
			Assert(pg_atomic_read_u32(&BufferFrameGenerations[i]) == 0);
			Assert(pg_atomic_read_u32(&BufferTagGenerations[i]) == 0);
			Assert(pg_atomic_read_u32(&BufferWriteIntentOwners[i]) ==
				   INVALID_PROC_NUMBER);
		}
	}
#endif

	/* Init other shared buffer-management stuff */
	StrategyInitialize(!foundDescs);

	/* Initialize per-backend file flush context */
	WritebackContextInit(&BackendWritebackContext,
						 &backend_flush_after);
}

/*
 * BufferManagerShmemSize
 *
 * compute the size of shared memory for the buffer pool including
 * data pages, buffer descriptors, hash tables, etc.
 */
Size
BufferManagerShmemSize(void)
{
	Size		size = 0;

	/* size of buffer descriptors */
	size = add_size(size, mul_size(NBuffers, sizeof(BufferDescPadded)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/*
	 * Descriptor-to-frame identifiers plus test-only attachment counts and
	 * mapping generations and descriptor tag generations.
	 */
	size = add_size(size,
					mul_size(add_size(mul_size((Size) NBuffers, 4), 1),
							 sizeof(pg_atomic_uint32)));

	/* write-intent owner for each shared buffer */
	size = add_size(size, mul_size(NBuffers, sizeof(pg_atomic_uint32)));
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of data pages, plus alignment padding */
	size = add_size(size, PG_IO_ALIGN_SIZE);
	size = add_size(size, mul_size(NBuffers, BLCKSZ));

	/* size of stuff controlled by freelist.c */
	size = add_size(size, StrategyShmemSize());

	/* size of I/O condition variables */
	size = add_size(size, mul_size(NBuffers,
								   sizeof(ConditionVariableMinimallyPadded)));
	/* to allow aligning the above */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of checkpoint sort array in bufmgr.c */
	size = add_size(size, mul_size(NBuffers, sizeof(CkptSortItem)));

	return size;
}
