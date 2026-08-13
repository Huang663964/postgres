/*-------------------------------------------------------------------------
 *
 * test_buffer_frame_legacy.c
 *	  Deliberately old ABI marker for the buffer-frame extension test.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

extern PGDLLEXPORT const Pg_magic_struct *Pg_magic_func(void);

const Pg_magic_struct *
Pg_magic_func(void)
{
	static const Pg_magic_struct magic =
	{
		.len = sizeof(Pg_magic_struct),
		.abi_fields =
		{
			PG_VERSION_NUM / 100,
			FUNC_MAX_ARGS,
			INDEX_MAX_KEYS,
			NAMEDATALEN,
			FLOAT8PASSBYVAL,
			"PostgreSQL",
		},
		.name = NULL,
		.version = NULL,
	};

	return &magic;
}
