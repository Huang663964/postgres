/*-------------------------------------------------------------------------
 *
 * pg_dbbranch.h
 *		definition of the DB Branch metadata system catalog (pg_dbbranch)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * src/include/catalog/pg_dbbranch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_DBBRANCH_H
#define PG_DBBRANCH_H

#include "access/xlogdefs.h"
#include "catalog/genbki.h"
#include "catalog/pg_dbbranch_d.h"	/* IWYU pragma: export */

/* ----------------
 *		pg_dbbranch definition. cpp turns this into
 *		typedef struct FormData_pg_dbbranch
 * ----------------
 */
CATALOG(pg_dbbranch,8785,DbBranchRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(8786,DbBranchRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid			source_db_oid BKI_LOOKUP(pg_database);
	Oid			branch_db_oid BKI_LOOKUP(pg_database);
	XLogRecPtr	redo_ptr;
	XLogRecPtr	branch_lsn;
	int64		wal_records_scanned;
	int64		wal_source_records;
	int64		wal_other_db_records;
	int64		wal_mixed_records;
	int64		wal_global_records;
	int64		wal_source_fpi_blocks;
	int64		wal_source_non_fpi_records;
	float8		clone_elapsed_ms;
	float8		replay_elapsed_ms;

#ifdef CATALOG_VARLEN
	timestamptz created_at BKI_FORCE_NOT_NULL;
	timestamptz ready_at BKI_FORCE_NOT_NULL;
	text		clone_result BKI_FORCE_NOT_NULL;
	text		replay_method BKI_FORCE_NOT_NULL;
	text		status BKI_FORCE_NOT_NULL;
	text		failure BKI_FORCE_NOT_NULL;
#endif
} FormData_pg_dbbranch;

typedef FormData_pg_dbbranch *Form_pg_dbbranch;

DECLARE_UNIQUE_INDEX_PKEY(pg_dbbranch_branch_index, 8787, DbBranchBranchIndexId, pg_dbbranch, btree(branch_db_oid oid_ops));
DECLARE_INDEX(pg_dbbranch_source_index, 8788, DbBranchSourceIndexId, pg_dbbranch, btree(source_db_oid oid_ops));

#endif /* PG_DBBRANCH_H */
