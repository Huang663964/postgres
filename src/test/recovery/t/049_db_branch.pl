# Copyright (c) 2026, PostgreSQL Global Development Group

# Prototype harness for DB Branch.  v0.1 only proves that the recovery TAP
# entry exists and can prepare a source database; the branch implementation
# starts in later v0 milestones.

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

SKIP:
{
	skip 'db_branch internal create entry not implemented yet', 1;

	# v0.2 will replace this skip with a call to the internal DB Branch entry.
	# Expected final shape: create dbbranch_target from dbbranch_source and
	# verify target rows match source at branch_lsn.
	my $branch_rows = $node->safe_psql(
		'dbbranch_target',
		'SELECT count(*) FROM users;');
	is($branch_rows, '2', 'branch matches source at branch_lsn');
}

done_testing();
