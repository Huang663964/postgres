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

my @metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 1, 'failed branch creation writes one metadata file');

open my $metadata_fh, '<', $metadata_files[0]
  or die "could not open $metadata_files[0]: $!";
my $metadata = do { local $/; <$metadata_fh> };
close $metadata_fh;

like($metadata, qr/^source_db_name=dbbranch_source$/m, 'metadata records source database');
like($metadata, qr/^branch_name=dbbranch_target$/m, 'metadata records branch database');
like($metadata, qr/^status_history=CREATING,FAILED$/m, 'metadata records CREATING to FAILED transition');
like($metadata, qr/^status=FAILED$/m, 'metadata final state is FAILED');

my $branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_target';]);
is($branch_count, '0', 'failed branch is not connectable');

done_testing();
