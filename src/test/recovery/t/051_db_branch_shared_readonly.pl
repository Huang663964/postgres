# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(allows_streaming => 1);
$node->start;

sub fails_shared_readonly
{
	my ($database, $sql, $name) = @_;
	my $stderr = '';
	my $result = $node->psql($database, $sql, stderr => \$stderr);

	isnt($result, 0, $name);
	like($stderr, qr/database branch is shared read-only/,
		"$name reports the durable branch policy");
}

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_ro_source;]);
$node->safe_psql(
	'dbbranch_ro_source',
	q[
CREATE TABLE readonly_rows (id int PRIMARY KEY, payload text NOT NULL);
INSERT INTO readonly_rows VALUES (1, 'source-before-branch');
CREATE SEQUENCE readonly_seq;
CHECKPOINT;
]);

my $source_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_ro_source';]);

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_private FROM DATABASE dbbranch_ro_source;]);
my $private_contract = $node->safe_psql(
	'postgres',
	q[
SELECT buffer_mode::text || '|' || (family_root_db_oid = ] . $source_oid . q[)
FROM pg_dbbranch
WHERE branch_db_oid = (SELECT oid FROM pg_database WHERE datname = 'dbbranch_private');
]);
is($private_contract, 'p|true',
	'legacy CREATE BRANCH remains private writable and records its family');
$node->safe_psql(
	'dbbranch_private',
	q[INSERT INTO readonly_rows VALUES (2, 'private-write');]);

$node->safe_psql(
	'postgres',
	q[
CREATE BRANCH dbbranch_shared_ro
FROM DATABASE dbbranch_ro_source
BUFFER SHARED READ ONLY;
]);
my $shared_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_shared_ro';]);
my $shared_contract = $node->safe_psql(
	'postgres',
	q[
SELECT buffer_mode::text || '|' || (family_root_db_oid = ] . $source_oid . q[)
FROM pg_dbbranch WHERE branch_db_oid = ] . $shared_oid . q[;
]);
is($shared_contract, 's|true',
	'shared-readonly syntax persists mode and family root');

is(
	$node->safe_psql(
		'dbbranch_shared_ro',
		q[SELECT string_agg(id || ':' || payload, ',' ORDER BY id) FROM readonly_rows;]),
	'1:source-before-branch',
	'plain SQL reads work on a shared-readonly branch');

fails_shared_readonly(
	'dbbranch_shared_ro',
	q[INSERT INTO readonly_rows VALUES (3, 'blocked');],
	'DML is rejected');
fails_shared_readonly(
	'dbbranch_shared_ro',
	q[CREATE TABLE blocked_ddl (id int);],
	'DDL is rejected');
fails_shared_readonly(
	'dbbranch_shared_ro',
	q[CREATE TEMP TABLE blocked_temp (id int);],
	'temporary-table writes are rejected');
fails_shared_readonly(
	'dbbranch_shared_ro',
	q[SELECT nextval('readonly_seq');],
	'sequence writes are rejected');
fails_shared_readonly(
	'dbbranch_shared_ro',
	q[VACUUM readonly_rows;],
	'VACUUM is rejected');
fails_shared_readonly(
	'dbbranch_shared_ro',
	q[ANALYZE readonly_rows;],
	'ANALYZE is rejected');
fails_shared_readonly(
	'dbbranch_shared_ro',
	q[REINDEX TABLE readonly_rows;],
	'REINDEX is rejected');
fails_shared_readonly(
	'dbbranch_shared_ro',
	q[SET transaction_read_only = off; INSERT INTO readonly_rows VALUES (4, 'blocked');],
	'superuser cannot bypass the catalog policy with a GUC');

$node->safe_psql(
	'dbbranch_ro_source',
	q[INSERT INTO readonly_rows VALUES (5, 'source-remains-writable');]);
is(
	$node->safe_psql('dbbranch_ro_source', q[SELECT count(*) FROM readonly_rows;]),
	'2',
	'the private source remains writable');
is(
	$node->safe_psql('dbbranch_shared_ro', q[SELECT count(*) FROM readonly_rows;]),
	'1',
	'source writes remain isolated from the shared-readonly branch');

$node->safe_psql(
	'postgres',
	q[
CREATE BRANCH dbbranch_shared_child
FROM DATABASE dbbranch_shared_ro
BUFFER SHARED READ ONLY;
]);
my $child_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_shared_child';]);
is(
	$node->safe_psql(
		'postgres',
		q[SELECT family_root_db_oid = ] . $source_oid .
		q[ FROM pg_dbbranch WHERE branch_db_oid = ] . $child_oid . q[;]),
	't',
	'nested shared-readonly branches inherit the family root');

$node->restart;
is(
	$node->safe_psql(
		'postgres',
		q[SELECT buffer_mode::text FROM pg_dbbranch WHERE branch_db_oid = ] .
		$shared_oid . q[;]),
	's',
	'shared-readonly mode survives restart');
fails_shared_readonly(
	'dbbranch_shared_ro',
	q[DELETE FROM readonly_rows;],
	'the write policy survives restart');

my $drop_stderr = '';
my $drop_result = $node->psql(
	'postgres',
	q[DROP DATABASE dbbranch_ro_source;],
	stderr => \$drop_stderr);
isnt($drop_result, 0,
	'family root cannot be dropped while shared-readonly branches depend on it');
like($drop_stderr, qr/shared-readonly branches depend on it/,
	'drop failure explains the family dependency');

$drop_stderr = '';
$drop_result = $node->psql(
	'postgres',
	q[DROP DATABASE dbbranch_shared_ro;],
	stderr => \$drop_stderr);
isnt($drop_result, 0,
	'intermediate shared-readonly source cannot be dropped before its child');
like($drop_stderr, qr/shared-readonly branches depend on it/,
	'intermediate drop failure explains the direct dependency');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_shared_child;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_shared_ro;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_private;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ro_source;]);

is(
	$node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_dbbranch WHERE branch_db_oid IN (] .
		$shared_oid . q[,] . $child_oid . q[);]),
	'0',
	'dropping shared-readonly branches removes their catalog rows');

$node->stop;
done_testing();
