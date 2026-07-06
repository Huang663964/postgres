# Copyright (c) 2026, PostgreSQL Global Development Group

# Prototype harness for DB Branch.  The primary path creates a real,
# connectable branch when the filesystem supports FICLONE.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
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
my ($wal_records_scanned) = $metadata =~ /^wal_records_scanned=([0-9]+)$/m;
my ($wal_source_records) = $metadata =~ /^wal_source_records=([0-9]+)$/m;
my ($wal_other_db_records) = $metadata =~ /^wal_other_db_records=([0-9]+)$/m;
my ($wal_mixed_records) = $metadata =~ /^wal_mixed_records=([0-9]+)$/m;
my ($wal_global_records) = $metadata =~ /^wal_global_records=([0-9]+)$/m;
my ($wal_source_fpi_blocks) = $metadata =~ /^wal_source_fpi_blocks=([0-9]+)$/m;
my ($wal_source_non_fpi_records) = $metadata =~ /^wal_source_non_fpi_records=([0-9]+)$/m;
ok(
	$wal_records_scanned >= $wal_source_records,
	'metadata WAL scan count covers source WAL count');
ok($wal_source_records > 0, 'metadata records source WAL after last checkpoint');
ok($wal_source_fpi_blocks > 0, 'metadata records restored source full-page images');
ok($wal_source_non_fpi_records <= $wal_source_records, 'metadata source non-FPI count is bounded by source WAL count');
ok($wal_mixed_records <= $wal_source_records, 'metadata mixed WAL count is bounded by source WAL count');
ok(
	$wal_records_scanned >= $wal_source_records + $wal_other_db_records + $wal_global_records,
	'metadata WAL scan count covers classified WAL counts');
like($metadata, qr/^clone_path=base\/pg_dbbranch_[0-9]+_[0-9a-f]+$/m, 'metadata records clone staging path');
like($metadata, qr/^wal_pin=released$/m, 'metadata records released WAL pin');
like($metadata, qr/^replay_method=fpi_restore\+source_flush$/m, 'metadata records FPI restore replay method');

my ($clone_path) = $metadata =~ /^clone_path=(.+)$/m;
if ($result == 0)
{
	like($metadata, qr/^clone_result=(done|copy_fallback)$/m, 'metadata records storage clone success');
	like($metadata, qr/^cleanup=not_needed$/m, 'metadata records no failed clone cleanup needed');
	like($metadata, qr/^status_history=CREATING,COPYING,REPLAYING,READY$/m, 'metadata records READY transition');
	like($metadata, qr/^status=READY$/m, 'metadata final state is READY');
	like($metadata, qr/^failure=$/m, 'metadata records no failure');
	ok(!-e $node->data_dir . '/' . $clone_path, 'clone staging path is installed, not left behind');

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
		q[SELECT status || '|' || replay_method || '|' || source_db_oid || '|' || branch_db_oid || '|' || (redo_ptr IS NOT NULL) || '|' || (branch_lsn IS NOT NULL) || '|' || (redo_ptr <= branch_lsn) || '|' || (wal_records_scanned >= 0) || '|' || (wal_source_records >= 0) || '|' || (wal_records_scanned >= wal_source_records) || '|' || failure FROM pg_dbbranch WHERE branch_db_oid = (SELECT oid FROM pg_database WHERE datname = 'dbbranch_target');]);
	is(
		$catalog_state,
		'READY|fpi_restore+source_flush|' . $source_oid . '|' . $branch_oid . '|true|true|true|true|true|true|',
		'pg_dbbranch records READY metadata with replay method and WAL scan counts');

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
		q[SELECT status || '|' || replay_method || '|' || source_db_oid || '|' || branch_db_oid || '|' || (redo_ptr IS NOT NULL) || '|' || (branch_lsn IS NOT NULL) || '|' || (redo_ptr <= branch_lsn) || '|' || (wal_records_scanned >= 0) || '|' || (wal_source_records >= 0) || '|' || (wal_records_scanned >= wal_source_records) || '|' || failure FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_state_after_restart,
		'READY|fpi_restore+source_flush|' . $source_oid . '|' . $branch_oid . '|true|true|true|true|true|true|',
		'pg_dbbranch READY metadata with WAL scan counts survives restart');

	$node->safe_psql('dbbranch_target', q[INSERT INTO users VALUES (4, 'dora');]);
	my $source_after_branch_write = $node->safe_psql(
		'dbbranch_source',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is($source_after_branch_write, '1:alice,2:bob,3:carol', 'branch writes do not affect source');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_target;]);
	my $catalog_after_drop = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[ OR source_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is($catalog_after_drop, '0', 'dropping branch removes pg_dbbranch metadata');

	mkdir($node->data_dir . '/' . $clone_path)
	  or die "could not create stale clone path $clone_path: $!";
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_target FROM DATABASE dbbranch_source],
		stderr => \$stderr);
	is($result, 0, 'db branch removes stale clone staging path before retry');
	my $retry_branch_rows = $node->safe_psql(
		'dbbranch_target',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is($retry_branch_rows, '1:alice,2:bob,3:carol', 'retried branch reads cloned source rows');
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_target;]);
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
	ok(!-e $node->data_dir . '/' . $clone_path, 'failed branch cleanup removes clone staging path');

	my $branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_target';]);
	is($branch_count, '0', 'failed branch is not connectable');
}

my $slot_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%']);
is($slot_count, '0', 'db branch releases WAL pin slot');

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

my $tablespace_dir = $node->basedir . '/dbbranch_ts';
mkdir($tablespace_dir) or die "could not create $tablespace_dir: $!";
$node->safe_psql('postgres', "CREATE TABLESPACE dbbranch_ts LOCATION '$tablespace_dir';");
$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_ts_source TABLESPACE dbbranch_ts;');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_ts_target FROM DATABASE dbbranch_ts_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects non-default source tablespace');
like(
	$stderr,
	qr/db_branch currently supports only pg_default tablespace/,
	'db branch reports tablespace limitation before clone');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 2, 'tablespace rejection writes separate metadata file');

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

like($tablespace_metadata, qr/^wal_range_bytes=0$/m, 'tablespace metadata records zero WAL range');
like($tablespace_metadata, qr/^wal_records_scanned=0$/m, 'tablespace metadata records zero scanned WAL count');
like($tablespace_metadata, qr/^wal_source_records=0$/m, 'tablespace metadata records zero source WAL count');
like($tablespace_metadata, qr/^wal_other_db_records=0$/m, 'tablespace metadata records zero other DB WAL count');
like($tablespace_metadata, qr/^wal_mixed_records=0$/m, 'tablespace metadata records zero mixed WAL count');
like($tablespace_metadata, qr/^wal_global_records=0$/m, 'tablespace metadata records zero global WAL count');
like($tablespace_metadata, qr/^wal_source_fpi_blocks=0$/m, 'tablespace metadata records zero source FPI block count');
like($tablespace_metadata, qr/^wal_source_non_fpi_records=0$/m, 'tablespace metadata records zero source non-FPI record count');
like($tablespace_metadata, qr/^wal_pin=not_started$/m, 'tablespace metadata records WAL pin not started');
like($tablespace_metadata, qr/^clone_result=not_started$/m, 'tablespace metadata records clone not started');
like($tablespace_metadata, qr/^cleanup=not_started$/m, 'tablespace metadata records cleanup not started');
like($tablespace_metadata, qr/^replay_method=not_started$/m, 'tablespace metadata records replay not started');
like($tablespace_metadata, qr/^status=FAILED$/m, 'tablespace metadata final state is FAILED');
like(
	$tablespace_metadata,
	qr/^failure=db_branch currently supports only pg_default tablespace$/m,
	'tablespace metadata records limitation');
my ($tablespace_clone_path) = $tablespace_metadata =~ /^clone_path=(.+)$/m;
ok(!-e $node->data_dir . '/' . $tablespace_clone_path, 'tablespace rejection does not create clone staging path');


$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_unlogged_source;');
$node->safe_psql(
	'dbbranch_unlogged_source',
	q[CREATE UNLOGGED TABLE cache_entries (id int PRIMARY KEY); INSERT INTO cache_entries VALUES (1);]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_unlogged_target FROM DATABASE dbbranch_unlogged_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects unlogged source relations');
like(
	$stderr,
	qr/db_branch currently does not support unlogged relations/,
	'db branch reports unlogged relation limitation before clone');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 3, 'unlogged rejection writes separate metadata file');

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

like($unlogged_metadata, qr/^wal_pin=not_started$/m, 'unlogged metadata records WAL pin not started');
like($unlogged_metadata, qr/^clone_result=not_started$/m, 'unlogged metadata records clone not started');
like($unlogged_metadata, qr/^cleanup=not_started$/m, 'unlogged metadata records cleanup not started');
like($unlogged_metadata, qr/^replay_method=not_started$/m, 'unlogged metadata records replay not started');
like($unlogged_metadata, qr/^status=FAILED$/m, 'unlogged metadata final state is FAILED');
like(
	$unlogged_metadata,
	qr/^failure=db_branch currently does not support unlogged relations$/m,
	'unlogged metadata records limitation');
my ($unlogged_clone_path) = $unlogged_metadata =~ /^clone_path=(.+)$/m;
ok(!-e $node->data_dir . '/' . $unlogged_clone_path, 'unlogged rejection does not create clone staging path');

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
is(scalar @metadata_files, 4, 'busy branch attempt writes separate metadata file');

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
is(scalar @metadata_files, 5, 'prepared transaction rejection writes separate metadata file');

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
is(scalar @metadata_files, 5, 'invalid source rejection writes no metadata file');

$node->safe_psql('postgres', 'DROP DATABASE dbbranch_invalid_source;');

done_testing();
