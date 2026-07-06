# Copyright (c) 2026, PostgreSQL Global Development Group

# Prototype harness for DB Branch.  The primary path creates a real,
# connectable branch when the filesystem supports FICLONE.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 10');
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_source;');
$node->safe_psql(
	'dbbranch_source',
	qq(
CREATE TABLE users (id int PRIMARY KEY, name text NOT NULL);
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
CHECKPOINT;
INSERT INTO users VALUES (3, 'carol');
));

my $source_rows = $node->safe_psql(
	'dbbranch_source',
	'SELECT count(*) FROM users;');
is($source_rows, '3', 'source database baseline is ready for DB Branch');

my $wal_start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn();');
my $stderr = '';
my $result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_target FROM DATABASE dbbranch_source],
	stderr => \$stderr);

my @metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 1, 'branch creation writes one metadata file');

open my $metadata_fh, '<', $metadata_files[0]
  or die "could not open $metadata_files[0]: $!";
my $metadata = do { local $/; <$metadata_fh> };
close $metadata_fh;

like($metadata, qr/^source_db_name=dbbranch_source$/m, 'metadata records source database');
like($metadata, qr/^branch_name=dbbranch_target$/m, 'metadata records branch database');
like($metadata, qr/^redo_ptr=[0-9A-F]+\/[0-9A-F]+$/m, 'metadata records redo pointer');
unlike($metadata, qr/^redo_ptr=0\/0$/m, 'redo pointer is valid');
like($metadata, qr/^branch_lsn=[0-9A-F]+\/[0-9A-F]+$/m, 'metadata records branch LSN');
unlike($metadata, qr/^branch_lsn=0\/0$/m, 'branch LSN is valid');
like($metadata, qr/^wal_range_bytes=[0-9]+$/m, 'metadata records WAL range size');
like($metadata, qr/^wal_records_scanned=[0-9]+$/m, 'metadata records scanned WAL count');
like($metadata, qr/^wal_source_records=[0-9]+$/m, 'metadata records source WAL count');
like($metadata, qr/^wal_other_db_records=[0-9]+$/m, 'metadata records other DB WAL count');
like($metadata, qr/^wal_mixed_records=[0-9]+$/m, 'metadata records mixed WAL count');
like($metadata, qr/^wal_global_records=[0-9]+$/m, 'metadata records global WAL count');
like($metadata, qr/^wal_source_fpi_blocks=[0-9]+$/m, 'metadata records source FPI block count');
like($metadata, qr/^wal_source_non_fpi_records=[0-9]+$/m, 'metadata records source non-FPI record count');
like($metadata, qr/^wal_replayed_records=[0-9]+$/m, 'metadata records replayed WAL count');
like($metadata, qr/^source_blocking_ms=[0-9]+(\.[0-9]+)?$/m, 'metadata records source blocking time');
like($metadata, qr/^clone_elapsed_ms=[0-9]+(\.[0-9]+)?$/m, 'metadata records clone elapsed time');
like($metadata, qr/^replay_elapsed_ms=[0-9]+(\.[0-9]+)?$/m, 'metadata records replay elapsed time');
my ($wal_records_scanned) = $metadata =~ /^wal_records_scanned=([0-9]+)$/m;
my ($wal_source_records) = $metadata =~ /^wal_source_records=([0-9]+)$/m;
my ($wal_other_db_records) = $metadata =~ /^wal_other_db_records=([0-9]+)$/m;
my ($wal_mixed_records) = $metadata =~ /^wal_mixed_records=([0-9]+)$/m;
my ($wal_global_records) = $metadata =~ /^wal_global_records=([0-9]+)$/m;
my ($wal_source_fpi_blocks) = $metadata =~ /^wal_source_fpi_blocks=([0-9]+)$/m;
my ($wal_source_non_fpi_records) = $metadata =~ /^wal_source_non_fpi_records=([0-9]+)$/m;
my ($wal_replayed_records) = $metadata =~ /^wal_replayed_records=([0-9]+)$/m;
ok(
	$wal_records_scanned >= $wal_source_records,
	'metadata WAL scan count covers source WAL count');
ok($wal_source_records > 0, 'metadata records source WAL after last checkpoint');
ok($wal_replayed_records > 0, 'metadata records replayed source WAL');
ok($wal_replayed_records <= $wal_source_records, 'metadata replayed WAL count is bounded by source WAL count');
ok($wal_source_fpi_blocks > 0, 'metadata records restored source full-page images');
ok($wal_mixed_records <= $wal_source_records, 'metadata mixed WAL count is bounded by source WAL count');
ok(
	$wal_records_scanned >= $wal_source_records + $wal_other_db_records + $wal_global_records,
	'metadata WAL scan count covers classified WAL counts');
like($metadata, qr/^clone_path=base\/[0-9]+$/m, 'metadata records branch storage path');
like($metadata, qr/^wal_pin=released$/m, 'metadata records released WAL pin');
like($metadata, qr/^replay_method=rmgr_redo$/m, 'metadata records rmgr redo replay method');

my ($clone_path) = $metadata =~ /^clone_path=(.+)$/m;
if ($result == 0)
{
	like($metadata, qr/^clone_result=(done|copy_fallback)$/m, 'metadata records storage clone success');
	like($metadata, qr/^cleanup=not_needed$/m, 'metadata records no failed clone cleanup needed');
	like($metadata, qr/^status_history=CREATING,COPYING,REPLAYING,READY$/m, 'metadata records READY transition');
	like($metadata, qr/^status=READY$/m, 'metadata final state is READY');
	like($metadata, qr/^failure=$/m, 'metadata records no failure');
	ok(-d $node->data_dir . '/' . $clone_path, 'branch storage path is installed');

	my $branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_target' AND datallowconn;]);
	is($branch_count, '1', 'ready branch is connectable');

	my $branch_rows = $node->safe_psql(
		'dbbranch_target',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is($branch_rows, '1:alice,2:bob,3:carol', 'ready branch can read cloned source rows');

	my $source_oid = $node->safe_psql(
		'postgres',
		q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_source';]);
	my $branch_oid = $node->safe_psql(
		'postgres',
		q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_target';]);
	is($clone_path, 'base/' . $branch_oid, 'branch storage path uses branch database OID');
	my $source_locator = $node->safe_psql(
		'postgres',
		q[SELECT dattablespace || '/' || oid FROM pg_database WHERE datname = 'dbbranch_source';]);
	my $branch_locator = $node->safe_psql(
		'postgres',
		q[SELECT dattablespace || '/' || oid FROM pg_database WHERE datname = 'dbbranch_target';]);
	my $wal_end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn();');
	my $waldump = '';
	my $catalog_state = $node->safe_psql(
		'postgres',
		q[SELECT status || '|' || replay_method || '|' || source_db_oid || '|' || branch_db_oid || '|' || (redo_ptr IS NOT NULL) || '|' || (branch_lsn IS NOT NULL) || '|' || (redo_ptr <= branch_lsn) || '|' || (wal_records_scanned >= 0) || '|' || (wal_source_records >= 0) || '|' || (wal_records_scanned >= wal_source_records) || '|' || (source_blocking_ms >= 0) || '|' || (clone_elapsed_ms >= 0) || '|' || (replay_elapsed_ms >= 0) || '|' || failure FROM pg_dbbranch WHERE branch_db_oid = (SELECT oid FROM pg_database WHERE datname = 'dbbranch_target');]);
	is(
		$catalog_state,
		'READY|rmgr_redo|' . $source_oid . '|' . $branch_oid . '|true|true|true|true|true|true|true|true|true|',
		'pg_dbbranch records READY metadata with replay method, WAL scan counts, and timings');

	my $catalog_clone_result = $node->safe_psql(
		'postgres',
		q[SELECT clone_result IN ('done', 'copy_fallback') FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_clone_result,
		't',
		'pg_dbbranch records storage clone result');

	my $catalog_ready_times = $node->safe_psql(
		'postgres',
		q[SELECT created_at IS NOT NULL AND ready_at IS NOT NULL AND created_at <= ready_at FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_ready_times,
		't',
		'pg_dbbranch records creation and ready timestamps');

	my $catalog_wal_range = $node->safe_psql(
		'postgres',
		q[SELECT wal_range_bytes >= 0 AND wal_range_bytes::numeric = branch_lsn - redo_ptr FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_wal_range,
		't',
		'pg_dbbranch records WAL range bytes');

	my $catalog_wal_replay = $node->safe_psql(
		'postgres',
		q[SELECT wal_replayed_records > 0 AND wal_replayed_records <= wal_source_records FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_wal_replay,
		't',
		'pg_dbbranch records replayed WAL count');

	my $catalog_wal_classification = $node->safe_psql(
		'postgres',
		q[SELECT (wal_other_db_records >= 0) || '|' || (wal_mixed_records >= 0) || '|' || (wal_global_records >= 0) || '|' || (wal_source_fpi_blocks >= 0) || '|' || (wal_source_non_fpi_records >= 0) || '|' || (wal_mixed_records <= wal_source_records) || '|' || (wal_records_scanned >= wal_source_records + wal_other_db_records + wal_global_records) FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_wal_classification,
		'true|true|true|true|true|true|true',
		'pg_dbbranch exposes WAL classification counts');

	ok(
		PostgreSQL::Test::Utils::run_log(
			[ 'pg_waldump', '-p', $node->data_dir, '-r', 'Database', '-s', $wal_start, '-e', $wal_end ],
			'>' => \$waldump),
		'db branch WAL can be dumped');
	like(
		$waldump,
		qr/CREATE_FILE_COPY.*copy dir \Q$source_locator\E to \Q$branch_locator\E/,
		'db branch creation records database file-copy WAL');

	$node->restart;
	my $branch_rows_after_restart = $node->safe_psql(
		'dbbranch_target',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is($branch_rows_after_restart, '1:alice,2:bob,3:carol', 'ready branch survives restart');
	my $catalog_state_after_restart = $node->safe_psql(
		'postgres',
		q[SELECT status || '|' || replay_method || '|' || source_db_oid || '|' || branch_db_oid || '|' || (redo_ptr IS NOT NULL) || '|' || (branch_lsn IS NOT NULL) || '|' || (redo_ptr <= branch_lsn) || '|' || (wal_records_scanned >= 0) || '|' || (wal_source_records >= 0) || '|' || (wal_records_scanned >= wal_source_records) || '|' || (source_blocking_ms >= 0) || '|' || (clone_elapsed_ms >= 0) || '|' || (replay_elapsed_ms >= 0) || '|' || failure FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_state_after_restart,
		'READY|rmgr_redo|' . $source_oid . '|' . $branch_oid . '|true|true|true|true|true|true|true|true|true|',
		'pg_dbbranch READY metadata with WAL scan counts and timings survives restart');

	$node->safe_psql('dbbranch_target', q[INSERT INTO users VALUES (4, 'dora');]);
	my $source_after_branch_write = $node->safe_psql(
		'dbbranch_source',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is($source_after_branch_write, '1:alice,2:bob,3:carol', 'branch writes do not affect source');

	$node->safe_psql('dbbranch_source', q[INSERT INTO users VALUES (6, 'frank');]);
	my $branch_after_source_write = $node->safe_psql(
		'dbbranch_target',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is(
		$branch_after_source_write,
		'1:alice,2:bob,3:carol,4:dora',
		'source writes after branch creation do not affect branch');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_target;]);
	my $catalog_after_drop = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[ OR source_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is($catalog_after_drop, '0', 'dropping branch removes pg_dbbranch metadata');
	ok(!-e $node->data_dir . '/' . $clone_path, 'dropping branch removes branch storage path');
}
else
{
	is($result, 3, 'db branch fails explicitly when FICLONE is unavailable');
	like(
		$stderr,
		qr/db_branch/,
		'internal entry reports storage clone failure');
	like($metadata, qr/^clone_result=failed$/m, 'metadata records storage clone failure');
	like($metadata, qr/^cleanup=done$/m, 'metadata records failed clone cleanup');
	like($metadata, qr/^status_history=CREATING,COPYING,FAILED$/m, 'metadata records failed transition');
	like($metadata, qr/^status=FAILED$/m, 'metadata final state is FAILED');
	like(
		$metadata,
		qr/^failure=.+$/m,
		'metadata records storage clone failure');
	ok(!-e $node->data_dir . '/' . $clone_path, 'failed branch cleanup removes branch storage path');

	my $branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_target';]);
	is($branch_count, '0', 'failed branch is not connectable');
}

my $slot_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%']);
is($slot_count, '0', 'db branch releases WAL pin slot');

SKIP:
{
	skip 'Injection points not supported by this build', 4
	  if ($ENV{enable_injection_points} // '') ne 'yes'
	  || !$node->check_extension('injection_points');

	$node->safe_psql('postgres', q[CREATE EXTENSION IF NOT EXISTS injection_points;]);
	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_replay_gate_source;]);
	$node->safe_psql(
		'dbbranch_replay_gate_source',
		q[
CREATE TABLE gate_rows (id int PRIMARY KEY);
INSERT INTO gate_rows VALUES (1);
CHECKPOINT;
UPDATE gate_rows SET id = 1 WHERE id = 1;
]);
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-before-replay', 'wait');]);

	my $gate_branch = $node->background_psql('postgres', on_error_stop => 1);
	$gate_branch->query_until(
		qr/start_gate_branch/,
		q(\echo start_gate_branch
CREATE BRANCH dbbranch_replay_gate_target FROM DATABASE dbbranch_replay_gate_source;
\echo finish_gate_branch
));
	$node->wait_for_event('client backend', 'db-branch-before-replay');

	my $pin_active = $node->safe_psql(
		'postgres',
		q[SELECT count(*) = 1 AND bool_and(active) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($pin_active, 't', 'db branch WAL pin is active before replay');

	$node->safe_psql('dbbranch_replay_gate_source', q[INSERT INTO gate_rows VALUES (2);]);
	my $gate_source_rows = $node->safe_psql(
		'dbbranch_replay_gate_source',
		q[SELECT count(*) FROM gate_rows;]);
	is($gate_source_rows, '2', 'source accepts writes after clone before replay');

	$node->safe_psql('postgres', q[SELECT injection_points_wakeup('db-branch-before-replay');]);
	$gate_branch->query_until(qr/finish_gate_branch/, '');
	$gate_branch->quit;

	my $gate_branch_rows = $node->safe_psql(
		'dbbranch_replay_gate_target',
		q[SELECT count(*) FROM gate_rows;]);
	is($gate_branch_rows, '1', 'branch excludes source writes after branch LSN');

	my $gate_slot_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($gate_slot_count, '0', 'db branch releases WAL pin after replay resumes');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-before-replay');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_replay_gate_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_replay_gate_source;]);
}

$stderr = '';
$result = $node->psql(
	'postgres',
	q[BEGIN; CREATE BRANCH dbbranch_xact_target FROM DATABASE dbbranch_source; ROLLBACK;],
	stderr => \$stderr);

is($result, 3, 'db branch rejects explicit transaction block');
like(
	$stderr,
	qr/CREATE BRANCH cannot run inside a transaction block/,
	'db branch reports transaction block restriction');

my $xact_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_xact_target';]);
is($xact_branch_count, '0', 'transaction-block rejection creates no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 1, 'transaction-block rejection writes no metadata file');

$node->safe_psql('postgres', q[
CREATE ROLE dbbranch_no_createdb LOGIN;
CREATE ROLE dbbranch_createdb LOGIN CREATEDB;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_no_createdb_target FROM DATABASE dbbranch_source],
	stderr => \$stderr,
	extra_params => [ '--username' => 'dbbranch_no_createdb' ]);

is($result, 3, 'db branch requires CREATEDB privilege');
like(
	$stderr,
	qr/permission denied to create database/,
	'db branch reports missing CREATEDB privilege');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_not_owner_target FROM DATABASE dbbranch_source],
	stderr => \$stderr,
	extra_params => [ '--username' => 'dbbranch_createdb' ]);

is($result, 3, 'db branch requires source database ownership');
like(
	$stderr,
	qr/permission denied to copy database "dbbranch_source"/,
	'db branch reports missing source ownership');

my $permission_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname IN ('dbbranch_no_createdb_target', 'dbbranch_not_owner_target');]);
is($permission_branch_count, '0', 'permission failures create no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 1, 'permission failures write no metadata file');

$node->safe_psql('postgres', q[DROP ROLE dbbranch_no_createdb, dbbranch_createdb;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_existing_target;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_existing_target FROM DATABASE dbbranch_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects an existing target database name');
like(
	$stderr,
	qr/database "dbbranch_existing_target" already exists/,
	'db branch reports existing target database name');

$node->safe_psql('postgres', q[
CREATE DATABASE dbbranch_no_conn_source;
ALTER DATABASE dbbranch_no_conn_source WITH ALLOW_CONNECTIONS false;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_no_conn_target FROM DATABASE dbbranch_no_conn_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects source database with disabled connections');
like(
	$stderr,
	qr/source database "dbbranch_no_conn_source" is not accepting connections/,
	'db branch reports source allow-connection restriction');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_missing_target FROM DATABASE dbbranch_missing_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects missing source database');
like(
	$stderr,
	qr/source database "dbbranch_missing_source" does not exist/,
	'db branch reports missing source database');

my $control_reject_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname IN ('dbbranch_no_conn_target', 'dbbranch_missing_target')]);
is($control_reject_branch_count, '0', 'control validation failures create no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 1, 'control validation failures write no metadata file');

$node->safe_psql('postgres', q[
DROP DATABASE dbbranch_existing_target;
ALTER DATABASE dbbranch_no_conn_source WITH ALLOW_CONNECTIONS true;
DROP DATABASE dbbranch_no_conn_source;
]);

$node->safe_psql('postgres', q[
CREATE FUNCTION dbbranch_wrapper() RETURNS oid LANGUAGE sql AS $$
  SELECT pg_create_database_branch('dbbranch_source', 'dbbranch_func_target')
$$;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[SELECT dbbranch_wrapper();],
	stderr => \$stderr);

is($result, 3, 'disabled SQL wrapper cannot create a branch');
like(
	$stderr,
	qr/pg_create_database_branch\(\) is disabled; use CREATE BRANCH instead/,
	'disabled SQL wrapper reports CREATE BRANCH replacement');

my $func_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_func_target';]);
is($func_branch_count, '0', 'disabled SQL wrapper creates no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 1, 'disabled SQL wrapper writes no metadata file');

$node->safe_psql('postgres', 'CREATE ROLE dbbranch_setting_role LOGIN;');
$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_setting_source;');
$node->safe_psql(
	'postgres',
	q[
ALTER DATABASE dbbranch_setting_source SET work_mem = '64MB';
ALTER DATABASE dbbranch_setting_source CONNECTION LIMIT 7;
ALTER ROLE dbbranch_setting_role IN DATABASE dbbranch_setting_source
	SET maintenance_work_mem = '32MB';
]);
$node->safe_psql(
	'dbbranch_setting_source',
	q[
CREATE TABLE setting_rows (id int PRIMARY KEY);
INSERT INTO setting_rows VALUES (1);
CREATE FUNCTION branch_login_proc() RETURNS event_trigger AS $$
BEGIN
  NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
CREATE EVENT TRIGGER branch_login_trigger
	ON login EXECUTE PROCEDURE branch_login_proc();
ALTER EVENT TRIGGER branch_login_trigger ENABLE ALWAYS;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_setting_target FROM DATABASE dbbranch_setting_source],
	stderr => \$stderr);

is($result, 0, 'db branch copies database-level settings');

my $setting_login_flag = $node->safe_psql(
	'postgres',
	q[
SELECT s.dathasloginevt::text || '|' || t.dathasloginevt::text
FROM pg_database s, pg_database t
WHERE s.datname = 'dbbranch_setting_source'
  AND t.datname = 'dbbranch_setting_target';
]);
is($setting_login_flag, 'true|true', 'branch copies source login event flag');

my $setting_login_trigger = $node->safe_psql(
	'dbbranch_setting_target',
	q[
SELECT count(*)
FROM pg_event_trigger
WHERE evtname = 'branch_login_trigger'
  AND evtevent = 'login'
  AND evtenabled = 'A';
]);
is($setting_login_trigger, '1', 'branch copies source login event trigger');

my $setting_work_mem = $node->safe_psql(
	'dbbranch_setting_target',
	q[SHOW work_mem;]);
is($setting_work_mem, '64MB', 'branch applies copied database-level setting');

my $setting_branch_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_setting_target';]);
my $setting_connlimit = $node->safe_psql(
	'postgres',
	q[SELECT datconnlimit FROM pg_database WHERE datname = 'dbbranch_setting_target';]);
is($setting_connlimit, '7', 'branch copies source connection limit');

my $setting_role_work_mem = $node->safe_psql(
	'dbbranch_setting_target',
	q[SHOW maintenance_work_mem;],
	extra_params => [ '--username' => 'dbbranch_setting_role' ]);
is($setting_role_work_mem, '32MB', 'branch applies copied role-in-database setting');

my $setting_rows = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_db_role_setting WHERE setdatabase = ]
	  . $setting_branch_oid
	  . q[;]);
is($setting_rows, '2', 'branch has copied database and role-in-database settings');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_setting_target;]);
my $setting_rows_after_drop = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_db_role_setting WHERE setdatabase = ]
	  . $setting_branch_oid
	  . q[;]);
is($setting_rows_after_drop, '0', 'dropping branch removes copied database settings');

$node->safe_psql(
	'postgres',
	q[ALTER DATABASE dbbranch_setting_source RESET work_mem;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_setting_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_setting_role;]);

my $tablespace_dir = $node->basedir . '/dbbranch_ts';
mkdir($tablespace_dir) or die "could not create $tablespace_dir: $!";
$node->safe_psql('postgres', "CREATE TABLESPACE dbbranch_ts LOCATION '$tablespace_dir';");
$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_ts_source TABLESPACE dbbranch_ts;');
$node->safe_psql(
	'dbbranch_ts_source',
	q[
CREATE TABLE ts_rows (id int PRIMARY KEY, name text NOT NULL);
INSERT INTO ts_rows VALUES (1, 'ts');
CHECKPOINT;
INSERT INTO ts_rows VALUES (2, 'redo');
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_ts_target FROM DATABASE dbbranch_ts_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports non-default source tablespace');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 3, 'tablespace branch writes separate metadata file');

my $tablespace_metadata = '';
for my $path (@metadata_files)
{
	open my $fh, '<', $path or die "could not open $path: $!";
	my $contents = do { local $/; <$fh> };
	close $fh;
	if ($contents =~ /^branch_name=dbbranch_ts_target$/m)
	{
		$tablespace_metadata = $contents;
		last;
	}
}

like($tablespace_metadata, qr/^wal_range_bytes=[1-9][0-9]*$/m, 'tablespace metadata records WAL range');
like($tablespace_metadata, qr/^wal_records_scanned=[1-9][0-9]*$/m, 'tablespace metadata records scanned WAL count');
like($tablespace_metadata, qr/^wal_source_records=[1-9][0-9]*$/m, 'tablespace metadata records source WAL count');
like($tablespace_metadata, qr/^clone_path=pg_tblspc\/[0-9]+\/[^\/]+\/[0-9]+$/m,
	'tablespace metadata records tablespace branch storage path');
like($tablespace_metadata, qr/^wal_pin=released$/m, 'tablespace metadata records released WAL pin');
like($tablespace_metadata, qr/^clone_result=(done|copy_fallback)$/m, 'tablespace metadata records clone success');
like($tablespace_metadata, qr/^cleanup=not_needed$/m, 'tablespace metadata records no cleanup');
like($tablespace_metadata, qr/^replay_method=rmgr_redo$/m, 'tablespace metadata records rmgr replay');
like($tablespace_metadata, qr/^status=READY$/m, 'tablespace metadata final state is READY');
like($tablespace_metadata, qr/^failure=$/m, 'tablespace metadata records no failure');
my ($tablespace_clone_path) = $tablespace_metadata =~ /^clone_path=(.+)$/m;
ok(-d $node->data_dir . '/' . $tablespace_clone_path, 'tablespace branch storage path is installed');

my $tablespace_rows = $node->safe_psql(
	'dbbranch_ts_target',
	q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM ts_rows;]);
is($tablespace_rows, '1:ts,2:redo', 'tablespace branch reads cloned and replayed rows');

my $same_tablespace = $node->safe_psql(
	'postgres',
	q[SELECT s.dattablespace = t.dattablespace FROM pg_database s, pg_database t WHERE s.datname = 'dbbranch_ts_source' AND t.datname = 'dbbranch_ts_target';]);
is($same_tablespace, 't', 'tablespace branch keeps source default tablespace');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ts_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ts_source;]);
$node->safe_psql('postgres', q[DROP TABLESPACE dbbranch_ts;]);


$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_unlogged_source;');
$node->safe_psql(
	'dbbranch_unlogged_source',
	q[
CREATE UNLOGGED TABLE cache_entries (id int PRIMARY KEY);
INSERT INTO cache_entries VALUES (1);
CHECKPOINT;
INSERT INTO cache_entries VALUES (2);
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_unlogged_target FROM DATABASE dbbranch_unlogged_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports unlogged source relations');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 4, 'unlogged branch writes separate metadata file');

my $unlogged_metadata = '';

for my $path (@metadata_files)
{
	open my $fh, '<', $path or die "could not open $path: $!";
	my $contents = do { local $/; <$fh> };
	close $fh;
	if ($contents =~ /^branch_name=dbbranch_unlogged_target$/m)
	{
		$unlogged_metadata = $contents;
		last;
	}
}

like($unlogged_metadata, qr/^wal_pin=released$/m, 'unlogged metadata records released WAL pin');
like($unlogged_metadata, qr/^clone_result=(done|copy_fallback)$/m, 'unlogged metadata records clone success');
like($unlogged_metadata, qr/^cleanup=not_needed$/m, 'unlogged metadata records no cleanup');
like($unlogged_metadata, qr/^replay_method=rmgr_redo$/m, 'unlogged metadata records rmgr replay');
like($unlogged_metadata, qr/^status=READY$/m, 'unlogged metadata final state is READY');
like($unlogged_metadata, qr/^failure=$/m, 'unlogged metadata records no failure');
my ($unlogged_clone_path) = $unlogged_metadata =~ /^clone_path=(.+)$/m;
ok(-d $node->data_dir . '/' . $unlogged_clone_path, 'unlogged branch storage path is installed');

my $unlogged_rows = $node->safe_psql(
	'dbbranch_unlogged_target',
	q[SELECT string_agg(id::text, ',' ORDER BY id) FROM cache_entries;]);
is($unlogged_rows, '1,2', 'unlogged branch reads flushed unlogged rows');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_unlogged_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_unlogged_source;]);

my $writer = $node->background_psql('dbbranch_source', on_error_stop => 1);
$writer->query_safe(q[BEGIN; INSERT INTO users VALUES (5, 'erin');]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_busy_target FROM DATABASE dbbranch_source],
	stderr => \$stderr);

is($result, 3, 'db branch internal entry rejects busy source database');
like(
	$stderr,
	qr/source database "dbbranch_source" is being accessed by other users/,
	'conservative source drain blocks active writer');

$writer->query_safe('ROLLBACK;');
$writer->quit;

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 5, 'busy branch attempt writes separate metadata file');

my $busy_metadata = '';
for my $path (@metadata_files)
{
	open my $fh, '<', $path or die "could not open $path: $!";
	my $contents = do { local $/; <$fh> };
	close $fh;
	if ($contents =~ /^branch_name=dbbranch_busy_target$/m)
	{
		$busy_metadata = $contents;
		last;
	}
}

like($busy_metadata, qr/^wal_pin=not_started$/m, 'busy source metadata records WAL pin not started');
like($busy_metadata, qr/^clone_result=not_started$/m, 'busy source metadata records clone not started');
like($busy_metadata, qr/^cleanup=not_started$/m, 'busy source metadata records cleanup not started');
like($busy_metadata, qr/^replay_method=not_started$/m, 'busy source metadata records replay not started');
like($busy_metadata, qr/^status=FAILED$/m, 'busy source metadata final state is FAILED');
like(
	$busy_metadata,
	qr/^failure=source database is being accessed by other users$/m,
	'busy source metadata records drain failure');

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_prepared_source;');
$node->safe_psql(
	'dbbranch_prepared_source',
	q[
CREATE TABLE prepared_rows (id int PRIMARY KEY);
BEGIN;
INSERT INTO prepared_rows VALUES (1);
PREPARE TRANSACTION 'dbbranch_prepared_xact';
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_prepared_target FROM DATABASE dbbranch_prepared_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects source database with prepared transaction');
like(
	$stderr,
	qr/source database "dbbranch_prepared_source" has prepared transactions/,
	'db branch reports prepared transaction limitation');

$node->safe_psql('dbbranch_prepared_source', q[ROLLBACK PREPARED 'dbbranch_prepared_xact';]);

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 6, 'prepared transaction rejection writes separate metadata file');

my $prepared_metadata = '';
for my $path (@metadata_files)
{
	open my $fh, '<', $path or die "could not open $path: $!";
	my $contents = do { local $/; <$fh> };
	close $fh;
	if ($contents =~ /^branch_name=dbbranch_prepared_target$/m)
	{
		$prepared_metadata = $contents;
		last;
	}
}

like($prepared_metadata, qr/^wal_pin=not_started$/m, 'prepared metadata records WAL pin not started');
like($prepared_metadata, qr/^clone_result=not_started$/m, 'prepared metadata records clone not started');
like($prepared_metadata, qr/^cleanup=not_started$/m, 'prepared metadata records cleanup not started');
like($prepared_metadata, qr/^replay_method=not_started$/m, 'prepared metadata records replay not started');
like($prepared_metadata, qr/^status=FAILED$/m, 'prepared metadata final state is FAILED');
like(
	$prepared_metadata,
	qr/^failure=source database has prepared transactions$/m,
	'prepared metadata records drain failure');
my ($prepared_clone_path) = $prepared_metadata =~ /^clone_path=(.*)$/m;
ok($prepared_clone_path eq '' || !-e $node->data_dir . '/' . $prepared_clone_path,
	'prepared rejection does not create clone staging path');

$node->safe_psql(
	'postgres',
	q[
CREATE DATABASE dbbranch_invalid_source;
UPDATE pg_database SET datconnlimit = -2 WHERE datname = 'dbbranch_invalid_source';
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_invalid_target FROM DATABASE dbbranch_invalid_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects invalid source database');
like(
	$stderr,
	qr/cannot use invalid database "dbbranch_invalid_source" as source/,
	'db branch reports invalid source database');

my $invalid_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_invalid_target';]);
is($invalid_branch_count, '0', 'invalid source rejection creates no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 6, 'invalid source rejection writes no metadata file');

$node->safe_psql('postgres', 'DROP DATABASE dbbranch_invalid_source;');

my $rel_tablespace_dir = $node->basedir . '/dbbranch_rel_ts';
mkdir($rel_tablespace_dir) or die "could not create $rel_tablespace_dir: $!";
$node->safe_psql('postgres', "CREATE TABLESPACE dbbranch_rel_ts LOCATION '$rel_tablespace_dir';");
$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_rel_ts_source;');
$node->safe_psql(
	'dbbranch_rel_ts_source',
	q[
CREATE TABLE rel_ts_rows (id int PRIMARY KEY, name text NOT NULL) TABLESPACE dbbranch_rel_ts;
INSERT INTO rel_ts_rows VALUES (1, 'rel-ts');
CHECKPOINT;
INSERT INTO rel_ts_rows VALUES (2, 'redo');
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_rel_ts_target FROM DATABASE dbbranch_rel_ts_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports relations in non-default tablespaces');

my $rel_tablespace_rows = $node->safe_psql(
	'dbbranch_rel_ts_target',
	q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM rel_ts_rows;]);
is($rel_tablespace_rows, '1:rel-ts,2:redo',
	'relation tablespace branch reads cloned and replayed rows');

my $rel_tablespace_path = $node->safe_psql(
	'dbbranch_rel_ts_target',
	q[SELECT pg_relation_filepath('rel_ts_rows');]);
like($rel_tablespace_path, qr/^pg_tblspc\/[0-9]+\/[^\/]+\/[0-9]+\/[0-9]+$/,
	'relation tablespace file stays under pg_tblspc');
ok(-f $node->data_dir . '/' . $rel_tablespace_path,
	'relation tablespace branch file exists');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_rel_ts_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_rel_ts_source;]);
$node->safe_psql('postgres', q[DROP TABLESPACE dbbranch_rel_ts;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_wal_source;');
$node->safe_psql(
	'dbbranch_wal_source',
	q[
CREATE TABLE wal_rows (id int PRIMARY KEY, payload text NOT NULL);
CREATE SEQUENCE wal_seq CACHE 1;
INSERT INTO wal_rows VALUES (1, repeat('a', 9000)), (2, repeat('b', 9000));
SELECT nextval('wal_seq');
CHECKPOINT;
UPDATE wal_rows SET payload = repeat('c', 9000) WHERE id = 1;
DELETE FROM wal_rows WHERE id = 2;
INSERT INTO wal_rows VALUES (3, repeat('d', 9000));
SELECT nextval('wal_seq');
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_wal_target FROM DATABASE dbbranch_wal_source],
	stderr => \$stderr);

is($result, 0, 'db branch handles heap, toast, btree, and sequence state');

my $wal_rows = $node->safe_psql(
	'dbbranch_wal_target',
	q[
SELECT string_agg(id || ':' || substr(payload, 1, 1) || ':' || length(payload), ',' ORDER BY id)
FROM wal_rows;
]);
is($wal_rows, '1:c:9000,3:d:9000',
	'branch sees replayed update, delete, insert, and TOAST values');

my $wal_index_lookup = $node->safe_psql(
	'dbbranch_wal_target',
	q[
SET enable_seqscan = off;
SELECT substr(payload, 1, 1) || ':' || length(payload) FROM wal_rows WHERE id = 3;
]);
is($wal_index_lookup, 'd:9000', 'branch btree index lookup sees replayed row');

my $source_sequence = $node->safe_psql(
	'dbbranch_wal_source',
	q[SELECT last_value FROM wal_seq;]);
my $branch_sequence = $node->safe_psql(
	'dbbranch_wal_target',
	q[SELECT last_value FROM wal_seq;]);
is($branch_sequence, $source_sequence,
	'branch sequence durable state matches source');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_wal_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_wal_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_partition_source;');
$node->safe_psql(
	'dbbranch_partition_source',
	q[
CREATE TABLE part_rows (
	id int,
	bucket int,
	payload text NOT NULL,
	PRIMARY KEY (bucket, id)
) PARTITION BY RANGE (bucket);
CREATE TABLE part_rows_low PARTITION OF part_rows FOR VALUES FROM (0) TO (10);
CREATE TABLE part_rows_high PARTITION OF part_rows FOR VALUES FROM (10) TO (20);
INSERT INTO part_rows VALUES (1, 1, 'low-before'), (2, 11, 'high-before');
CHECKPOINT;
INSERT INTO part_rows VALUES (3, 2, 'low-redo'), (4, 12, 'high-redo');
UPDATE part_rows SET payload = 'high-updated' WHERE bucket = 11 AND id = 2;
DELETE FROM part_rows WHERE bucket = 1 AND id = 1;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_partition_target FROM DATABASE dbbranch_partition_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports partitioned table WAL replay');

my $partition_rows = $node->safe_psql(
	'dbbranch_partition_target',
	q[
SELECT string_agg(tableoid::regclass::text || ':' || id || ':' || payload, ',' ORDER BY id)
FROM part_rows;
]);
is($partition_rows, 'part_rows_high:2:high-updated,part_rows_low:3:low-redo,part_rows_high:4:high-redo',
	'branch reads replayed rows from both partitions');

my $partition_index_lookup = $node->safe_psql(
	'dbbranch_partition_target',
	q[
SET enable_seqscan = off;
SELECT payload FROM part_rows WHERE bucket = 12 AND id = 4;
]);
is($partition_index_lookup, 'high-redo',
	'branch partition index lookup sees replayed row');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_partition_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_partition_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_toast_source;');
$node->safe_psql(
	'dbbranch_toast_source',
	q[
CREATE TABLE toast_rows (id int PRIMARY KEY, payload text NOT NULL);
ALTER TABLE toast_rows ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO toast_rows
SELECT 1, string_agg(md5(g::text), '') FROM generate_series(1, 256) g;
CHECKPOINT;
INSERT INTO toast_rows
SELECT 2, string_agg(md5(g::text), '') FROM generate_series(257, 512) g;
UPDATE toast_rows SET payload = payload || md5('dbbranch-toast-update') WHERE id = 1;
]);

my $toast_expected = $node->safe_psql(
	'dbbranch_toast_source',
	q[SELECT string_agg(id || ':' || length(payload) || ':' || md5(payload), ',' ORDER BY id) FROM toast_rows;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_toast_target FROM DATABASE dbbranch_toast_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports TOAST WAL replay');

my $toast_rows = $node->safe_psql(
	'dbbranch_toast_target',
	q[SELECT string_agg(id || ':' || length(payload) || ':' || md5(payload), ',' ORDER BY id) FROM toast_rows;]);
is($toast_rows, $toast_expected, 'branch reads cloned and replayed TOAST values');

my $toast_index_lookup = $node->safe_psql(
	'dbbranch_toast_target',
	q[
SET enable_seqscan = off;
SELECT length(payload) > 8000 FROM toast_rows WHERE id = 2;
]);
is($toast_index_lookup, 't', 'branch index lookup fetches replayed TOAST value');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_toast_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_toast_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_drop_source;');
$node->safe_psql(
	'dbbranch_drop_source',
	q[
CREATE TABLE drop_rows (id int PRIMARY KEY, payload text NOT NULL);
INSERT INTO drop_rows VALUES (1, 'before');
CHECKPOINT;
INSERT INTO drop_rows VALUES (2, 'after');
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_drop_target FROM DATABASE dbbranch_drop_source],
	stderr => \$stderr);

is($result, 0, 'db branch can be created before dropping source database');

my $drop_source_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_drop_source';]);
my $drop_branch_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_drop_target';]);

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_source;]);

my $drop_branch_rows = $node->safe_psql(
	'dbbranch_drop_target',
	q[SELECT string_agg(id || ':' || payload, ',' ORDER BY id) FROM drop_rows;]);
is($drop_branch_rows, '1:before,2:after',
	'branch remains readable after source database is dropped');

my $drop_catalog_rows = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_dbbranch WHERE source_db_oid = ]
	  . $drop_source_oid
	  . q[ OR branch_db_oid = ]
	  . $drop_branch_oid
	  . q[;]);
is($drop_catalog_rows, '0',
	'dropping source removes pg_dbbranch catalog metadata');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_target;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_ddl_source;');
$node->safe_psql(
	'dbbranch_ddl_source',
	q[
CREATE TABLE ddl_base (id int PRIMARY KEY);
INSERT INTO ddl_base VALUES (1);
CHECKPOINT;
CREATE TABLE ddl_after (id int PRIMARY KEY, payload text NOT NULL);
INSERT INTO ddl_after VALUES (2, 'created-after-checkpoint');
CREATE INDEX ddl_after_payload_idx ON ddl_after(payload);
CREATE VIEW ddl_after_view AS SELECT id, payload FROM ddl_after;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_ddl_target FROM DATABASE dbbranch_ddl_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports post-checkpoint DDL');

my $ddl_rows = $node->safe_psql(
	'dbbranch_ddl_target',
	q[SELECT string_agg(id || ':' || payload, ',' ORDER BY id) FROM ddl_after;]);
is($ddl_rows, '2:created-after-checkpoint',
	'branch reads table created after checkpoint');

my $ddl_view_rows = $node->safe_psql(
	'dbbranch_ddl_target',
	q[SELECT string_agg(id || ':' || payload, ',' ORDER BY id) FROM ddl_after_view;]);
is($ddl_view_rows, '2:created-after-checkpoint',
	'branch reads view created after checkpoint');

my $ddl_index_lookup = $node->safe_psql(
	'dbbranch_ddl_target',
	q[
SET enable_seqscan = off;
SELECT id FROM ddl_after WHERE payload = 'created-after-checkpoint';
]);
is($ddl_index_lookup, '2',
	'branch index lookup works on table created after checkpoint');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ddl_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ddl_source;]);

$node->append_conf('postgresql.conf', 'full_page_writes = off');
$node->reload;
$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_nonfpi_source;');
$node->safe_psql(
	'dbbranch_nonfpi_source',
	q[
CREATE TABLE nonfpi_rows (id int PRIMARY KEY);
CHECKPOINT;
INSERT INTO nonfpi_rows VALUES (1);
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_nonfpi_target FROM DATABASE dbbranch_nonfpi_source],
	stderr => \$stderr);

is($result, 0, 'db branch replays source WAL records without full-page images');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
my $nonfpi_metadata = '';
for my $path (@metadata_files)
{
	open my $fh, '<', $path or die "could not open $path: $!";
	my $contents = do { local $/; <$fh> };
	close $fh;
	if ($contents =~ /^branch_name=dbbranch_nonfpi_target$/m)
	{
		$nonfpi_metadata = $contents;
		last;
	}
}

like(
	$nonfpi_metadata,
	qr/^wal_source_non_fpi_records=[1-9][0-9]*$/m,
	'non-FPI metadata records source WAL outside FPI replay');
like(
	$nonfpi_metadata,
	qr/^clone_result=(done|copy_fallback)$/m,
	'non-FPI metadata records storage clone success');
like(
	$nonfpi_metadata,
	qr/^replay_method=rmgr_redo$/m,
	'non-FPI metadata records rmgr redo replay method');
like(
	$nonfpi_metadata,
	qr/^status_history=CREATING,COPYING,REPLAYING,READY$/m,
	'non-FPI metadata records READY transition');
like(
	$nonfpi_metadata,
	qr/^failure=$/m,
	'non-FPI metadata records no failure');

my $nonfpi_rows = $node->safe_psql(
	'dbbranch_nonfpi_target',
	q[SELECT count(*) FROM nonfpi_rows;]);
is($nonfpi_rows, '1', 'non-FPI branch reads rmgr-replayed rows');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_nonfpi_target;]);

$node->safe_psql('postgres', q[
CREATE ROLE dbbranch_template_createdb LOGIN CREATEDB;
CREATE DATABASE dbbranch_template_source;
ALTER DATABASE dbbranch_template_source IS_TEMPLATE true;
]);
$node->safe_psql(
	'dbbranch_template_source',
	q[
CREATE TABLE template_rows (id int PRIMARY KEY);
INSERT INTO template_rows VALUES (1);
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_template_target FROM DATABASE dbbranch_template_source],
	stderr => \$stderr,
	extra_params => [ '--username' => 'dbbranch_template_createdb' ]);

is($result, 0, 'db branch allows CREATEDB role to copy template source');

my $template_branch_owner = $node->safe_psql(
	'postgres',
	q[SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = 'dbbranch_template_target';]);
is($template_branch_owner, 'dbbranch_template_createdb', 'template branch is owned by creator');

my $template_rows = $node->safe_psql(
	'dbbranch_template_target',
	q[SELECT count(*) FROM template_rows;]);
is($template_rows, '1', 'template branch copies source data');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_template_target;]);
$node->safe_psql('postgres', q[ALTER DATABASE dbbranch_template_source IS_TEMPLATE false;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_template_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_template_createdb;]);


$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_cmdtag_source;]);
$node->safe_psql(
	'dbbranch_cmdtag_source',
	q[
CREATE TABLE cmdtag_rows (id int PRIMARY KEY);
INSERT INTO cmdtag_rows VALUES (1);
]);
command_like(
	[
		$node->installed_command('psql'),
		'--no-psqlrc',
		'--set' => 'ON_ERROR_STOP=1',
		'--dbname' => $node->connstr('postgres'),
		'--command' =>
		  'CREATE BRANCH dbbranch_cmdtag_target FROM DATABASE dbbranch_cmdtag_source'
	],
	qr/^CREATE BRANCH$/m,
	'CREATE BRANCH reports its own command tag');
my $cmdtag_rows = $node->safe_psql(
	'dbbranch_cmdtag_target',
	q[SELECT count(*) FROM cmdtag_rows;]);
is($cmdtag_rows, '1', 'command-tag branch copies source data');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_cmdtag_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_cmdtag_source;]);

my $minimal_node = PostgreSQL::Test::Cluster->new('minimal');
$minimal_node->init;
$minimal_node->append_conf('postgresql.conf', qq[
wal_level = minimal
max_wal_senders = 0
]);
$minimal_node->start;
$minimal_node->safe_psql('postgres', q[CREATE DATABASE dbbranch_minimal_source;]);
$minimal_node->safe_psql(
	'dbbranch_minimal_source',
	q[
CREATE TABLE minimal_rows (id int PRIMARY KEY);
INSERT INTO minimal_rows VALUES (1);
]);

$stderr = '';
$result = $minimal_node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_minimal_target FROM DATABASE dbbranch_minimal_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects wal_level=minimal');
like(
	$stderr,
	qr/CREATE BRANCH can only be used if "wal_level" >= "replica"/,
	'db branch reports wal_level requirement');

my $minimal_branch_count = $minimal_node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_minimal_target';]);
is($minimal_branch_count, '0', 'wal_level rejection creates no branch database');

my @minimal_metadata_files = glob $minimal_node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @minimal_metadata_files, 0, 'wal_level rejection writes no metadata file');

$minimal_node->stop;

done_testing();
