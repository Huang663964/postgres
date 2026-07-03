# Copyright (c) 2026, PostgreSQL Global Development Group

# Prototype harness for DB Branch.  This starts with the smallest real PG
# surface: a callable internal entry that validates names, then fails before
# branch creation until the freeze/clone/replay steps are implemented.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init;
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_source;');
$node->safe_psql(
	'dbbranch_source',
	qq(
CREATE TABLE users (id int PRIMARY KEY, name text NOT NULL);
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
CHECKPOINT;
));

my $source_rows = $node->safe_psql(
	'dbbranch_source',
	'SELECT count(*) FROM users;');
is($source_rows, '2', 'source database baseline is ready for DB Branch');

my $stderr = '';
my $result = $node->psql(
	'postgres',
	q[SELECT pg_create_database_branch('dbbranch_source', 'dbbranch_target');],
	stderr => \$stderr);

is($result, 3, 'db branch internal entry rejects unfinished implementation');
like(
	$stderr,
	qr/db_branch internal create entry not implemented yet/,
	'internal entry reaches the DB Branch implementation boundary');

done_testing();
