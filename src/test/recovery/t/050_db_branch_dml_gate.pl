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

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_dml_gate_source;]);
$node->stop;

done_testing();
