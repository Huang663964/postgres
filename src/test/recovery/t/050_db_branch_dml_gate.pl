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

my $source_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_dml_gate_source';]);

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
$node->stop;

done_testing();
