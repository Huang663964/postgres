# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(allows_streaming => 1);
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
like($stderr, qr/source database "dbbranch_dml_gate_source" has active write transactions/,
	'active source DML writer blocks CREATE BRANCH');

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
like($metadata, qr/^failure=source database has active write transactions$/m,
	'active source DML writer metadata records failure');
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
