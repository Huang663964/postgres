# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', q[lock_timeout = '1s']);
$node->start;

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_dml_gate_source;]);
$node->safe_psql(
	'dbbranch_dml_gate_source',
	q[
CREATE TABLE dml_gate_rows (id int PRIMARY KEY);
INSERT INTO dml_gate_rows VALUES (1);
]);
$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_dml_gate_other;]);
$node->safe_psql(
	'dbbranch_dml_gate_other',
	q[CREATE TABLE other_rows (id int PRIMARY KEY);]);

my $source_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_dml_gate_source';]);
my $other_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_dml_gate_other';]);

my $reader = $node->background_psql('dbbranch_dml_gate_source', on_error_stop => 1);
$reader->query_safe(q[BEGIN; SELECT count(*) FROM dml_gate_rows;]);
my $reader_gate = $node->safe_psql(
	'postgres',
	q[
SELECT count(*)
FROM pg_locks
WHERE locktype = 'object'
  AND classid = 'pg_dbbranch'::regclass
  AND objid = ] . $source_oid . q[
  AND objsubid = 0
  AND mode = 'RowExclusiveLock';
]);
is($reader_gate, '0', 'active source reader takes no DB Branch writer gate');

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_reader_gate_target FROM DATABASE dbbranch_dml_gate_source]);
my $reader_branch_rows = $node->safe_psql(
	'dbbranch_reader_gate_target',
	q[SELECT string_agg(id::text, ',' ORDER BY id) FROM dml_gate_rows;]);
is($reader_branch_rows, '1', 'db branch succeeds while source reader is active');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_reader_gate_target;]);
$reader->query_safe(q[COMMIT;]);
$reader->quit;

my $other_writer = $node->background_psql('dbbranch_dml_gate_other', on_error_stop => 1);
$other_writer->query_safe(q[BEGIN; INSERT INTO other_rows VALUES (1);]);
my $other_gate = $node->safe_psql(
	'postgres',
	q[
SELECT count(*) > 0
FROM pg_locks
WHERE locktype = 'object'
  AND classid = 'pg_dbbranch'::regclass
  AND objid = ] . $other_oid . q[
  AND objsubid = 0
  AND mode = 'RowExclusiveLock'
  AND granted;
]);
is($other_gate, 't', 'other database DML writer holds its own DB Branch writer gate');
$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_other_writer_target FROM DATABASE dbbranch_dml_gate_source]);
my $other_writer_branch_rows = $node->safe_psql(
	'dbbranch_other_writer_target',
	q[SELECT string_agg(id::text, ',' ORDER BY id) FROM dml_gate_rows;]);
is($other_writer_branch_rows, '1', 'db branch ignores other database active DML writer');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_other_writer_target;]);
$other_writer->query_safe(q[COMMIT;]);
$other_writer->quit;

my $subxact_writer = $node->background_psql('dbbranch_dml_gate_source', on_error_stop => 1);
$subxact_writer->query_safe(q[
BEGIN;
SAVEPOINT dbbranch_subxact_writer;
INSERT INTO dml_gate_rows VALUES (3);
]);
my $subxact_gate = $node->safe_psql(
	'postgres',
	q[
SELECT count(*) > 0
FROM pg_locks
WHERE locktype = 'object'
  AND classid = 'pg_dbbranch'::regclass
  AND objid = ] . $source_oid . q[
  AND objsubid = 0
  AND mode = 'RowExclusiveLock'
  AND granted;
]);
is($subxact_gate, 't', 'source subtransaction DML writer holds DB Branch writer gate');
$subxact_writer->query_safe(q[COMMIT;]);
$subxact_writer->quit;
$node->safe_psql('dbbranch_dml_gate_source', q[DELETE FROM dml_gate_rows WHERE id = 3;]);

my $writer = $node->background_psql('dbbranch_dml_gate_source', on_error_stop => 1);
$writer->query_safe(q[BEGIN; INSERT INTO dml_gate_rows VALUES (2);]);

my $gate_lock = $node->safe_psql(
	'postgres',
	q[
SELECT count(*) > 0
FROM pg_locks
WHERE locktype = 'object'
  AND classid = 'pg_dbbranch'::regclass
  AND objid = ] . $source_oid . q[
  AND objsubid = 0
  AND mode = 'RowExclusiveLock'
  AND granted;
]);
is($gate_lock, 't', 'active source DML writer holds DB Branch writer gate');

my @metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
my $metadata_file_count = scalar @metadata_files;
my $stderr = '';
my $result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_dml_gate_target FROM DATABASE dbbranch_dml_gate_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source DML writer');
like($stderr, qr/canceling statement due to lock timeout/,
	'CREATE BRANCH uses standard lock timeout while waiting for source DML writer');

my $target_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_dml_gate_target';]);
is($target_count, '0', 'active source DML writer creates no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, $metadata_file_count + 1,
	'active source DML writer rejection writes metadata file');
my $metadata = '';
for my $metadata_file (@metadata_files)
{
	open my $metadata_fh, '<', $metadata_file
	  or die "could not open $metadata_file: $!";
	my $contents = do { local $/; <$metadata_fh> };
	close $metadata_fh;
	if ($contents =~ /^branch_name=dbbranch_dml_gate_target$/m)
	{
		$metadata = $contents;
		last;
	}
}
like($metadata, qr/^status=FAILED$/m,
	'active source DML writer metadata final state is FAILED');
like($metadata, qr/^failure=canceling statement due to lock timeout$/m,
	'freeze-gate timeout metadata records the original failure');
my ($metadata_source_blocking_ms) = $metadata =~ /^source_blocking_ms=([0-9]+(?:\.[0-9]+)?)$/m;
ok(defined $metadata_source_blocking_ms && $metadata_source_blocking_ms > 0,
	'active source DML writer metadata records source blocking time');
like($metadata, qr/^wal_pin=not_started$/m,
	'active source DML writer metadata records WAL pin not started');
like($metadata, qr/^clone_result=not_started$/m,
	'active source DML writer metadata records clone not started');
like($metadata, qr/^cleanup=not_started$/m,
	'active source DML writer metadata records cleanup not started');
like($metadata, qr/^replay_method=not_started$/m,
	'active source DML writer metadata records replay not started');

my $cancel_branch =
  $node->background_psql('postgres', on_error_stop => 0);
$cancel_branch->query_safe(q[SET lock_timeout = 0;]);
my $cancel_branch_pid =
  $cancel_branch->query_safe(q[SELECT pg_backend_pid();]);
$cancel_branch->query_until(
	qr/start_cancel_gate_branch/,
	q(\echo start_cancel_gate_branch
CREATE BRANCH dbbranch_dml_gate_cancel_target FROM DATABASE dbbranch_dml_gate_source;
\echo finish_cancel_gate_branch
));
ok($node->poll_query_until(
	'postgres',
	q[
SELECT count(*) = 1
FROM pg_locks
WHERE pid = ] . $cancel_branch_pid . q[
  AND locktype = 'object'
  AND classid = 'pg_dbbranch'::regclass
  AND objid = ] . $source_oid . q[
  AND objsubid = 0
  AND mode = 'ShareLock'
  AND NOT granted;
]), 'cancelable CREATE BRANCH is waiting in the source freeze-gate queue');
is(
	$node->safe_psql(
		'postgres',
		q[SELECT pg_cancel_backend(] . $cancel_branch_pid . q[);]),
	't',
	'cancel the CREATE BRANCH freeze-gate wait');
$cancel_branch->query_until(qr/finish_cancel_gate_branch/, '');
like($cancel_branch->{stderr}, qr/canceling statement due to user request/,
	'CREATE BRANCH reports user cancellation while waiting for freeze gate');
$cancel_branch->quit;

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, $metadata_file_count + 2,
	'canceled freeze-gate wait writes a separate metadata file');
my $cancel_metadata = '';
for my $metadata_file (@metadata_files)
{
	open my $metadata_fh, '<', $metadata_file
	  or die "could not open $metadata_file: $!";
	my $contents = do { local $/; <$metadata_fh> };
	close $metadata_fh;
	if ($contents =~ /^branch_name=dbbranch_dml_gate_cancel_target$/m)
	{
		$cancel_metadata = $contents;
		last;
	}
}
like($cancel_metadata, qr/^status=FAILED$/m,
	'canceled freeze-gate wait metadata final state is FAILED');
like($cancel_metadata, qr/^failure=canceling statement due to user request$/m,
	'canceled freeze-gate wait metadata preserves the original failure');
like($cancel_metadata, qr/^wal_pin=not_started$/m,
	'canceled freeze-gate wait never starts WAL pinning');
my $cancel_target_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_dml_gate_cancel_target';]);
is($cancel_target_count, '0',
	'canceled freeze-gate wait creates no branch database');

my $deadlock_branch =
  $node->background_psql('dbbranch_dml_gate_source', on_error_stop => 0);
$deadlock_branch->query_safe(q[
SET lock_timeout = 0;
SET deadlock_timeout = '100ms';
SELECT pg_advisory_lock(505050);
]);
my $writer_pid = $writer->query_safe(q[SELECT pg_backend_pid();]);
$writer->query_until(
	qr/start_gate_deadlock_writer/,
	q(\echo start_gate_deadlock_writer
SELECT pg_advisory_xact_lock(505050);
\echo finish_gate_deadlock_writer
));
ok($node->poll_query_until(
	'postgres',
	q[
SELECT count(*) = 1
FROM pg_stat_activity
WHERE pid = ] . $writer_pid . q[
  AND wait_event_type = 'Lock'
  AND wait_event = 'advisory';
]), 'source writer waits on lock held by CREATE BRANCH session');

$deadlock_branch->query_until(
	qr/start_gate_deadlock_branch/,
	q(\echo start_gate_deadlock_branch
CREATE BRANCH dbbranch_dml_gate_deadlock_target FROM DATABASE dbbranch_dml_gate_source;
\echo finish_gate_deadlock_branch
));
$deadlock_branch->query_until(qr/finish_gate_deadlock_branch/, '');
like($deadlock_branch->{stderr}, qr/deadlock detected/,
	'CREATE BRANCH reports freeze-gate deadlock');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, $metadata_file_count + 3,
	'deadlocked freeze-gate wait writes a separate metadata file');
my $deadlock_metadata = '';
for my $metadata_file (@metadata_files)
{
	open my $metadata_fh, '<', $metadata_file
	  or die "could not open $metadata_file: $!";
	my $contents = do { local $/; <$metadata_fh> };
	close $metadata_fh;
	if ($contents =~ /^branch_name=dbbranch_dml_gate_deadlock_target$/m)
	{
		$deadlock_metadata = $contents;
		last;
	}
}
like($deadlock_metadata, qr/^status=FAILED$/m,
	'deadlocked freeze-gate wait metadata final state is FAILED');
like($deadlock_metadata, qr/^failure=deadlock detected$/m,
	'deadlocked freeze-gate wait metadata preserves the original failure');
like($deadlock_metadata, qr/^wal_pin=not_started$/m,
	'deadlocked freeze-gate wait never starts WAL pinning');
my $deadlock_target_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_dml_gate_deadlock_target';]);
is($deadlock_target_count, '0',
	'deadlocked freeze-gate wait creates no branch database');

$deadlock_branch->{stderr} = '';
$deadlock_branch->query_safe(q[SELECT pg_advisory_unlock(505050);]);
$deadlock_branch->quit;
$writer->query_until(qr/finish_gate_deadlock_writer/, '');

$writer->query_safe(q[COMMIT;]);
$writer->quit;

my $gate_released = $node->safe_psql(
	'postgres',
	q[
SELECT count(*)
FROM pg_locks
WHERE locktype = 'object'
  AND classid = 'pg_dbbranch'::regclass
  AND objid = ] . $source_oid . q[
  AND objsubid = 0
  AND mode = 'RowExclusiveLock';
]);
is($gate_released, '0', 'committed source DML writer releases DB Branch writer gate');

my $rows = $node->safe_psql(
	'dbbranch_dml_gate_source',
	q[SELECT string_agg(id::text, ',' ORDER BY id) FROM dml_gate_rows;]);
is($rows, '1,2', 'source DML writer commits normally');

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_dml_gate_drained_target FROM DATABASE dbbranch_dml_gate_source]);
my $branch_rows = $node->safe_psql(
	'dbbranch_dml_gate_drained_target',
	q[SELECT string_agg(id::text, ',' ORDER BY id) FROM dml_gate_rows;]);
is($branch_rows, '1,2', 'db branch succeeds after source DML writer drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_dml_gate_drained_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_dml_gate_source;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_dml_gate_other;]);
$node->stop;

done_testing();
