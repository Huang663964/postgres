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

is($result, 3, 'db branch internal entry rejects before replay is implemented');
like(
	$stderr,
	qr/(db_branch replay not implemented yet|db_branch storage clone requires FICLONE)/,
	'internal entry reaches the DB Branch storage or replay boundary');

my @metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 1, 'failed branch creation writes one metadata file');

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
like($metadata, qr/^clone_path=base\/pg_dbbranch_[0-9]+_[0-9a-f]+$/m, 'metadata records clone staging path');
like($metadata, qr/^status_history=CREATING,COPYING,FAILED$/m, 'metadata records CREATING to COPY failed transition');
like($metadata, qr/^status=FAILED$/m, 'metadata final state is FAILED');
like(
	$metadata,
	qr/^failure=(db_branch replay not implemented yet|db_branch storage clone requires FICLONE.*)$/m,
	'metadata records storage or replay failure');

my ($clone_path) = $metadata =~ /^clone_path=(.+)$/m;
my ($failure) = $metadata =~ /^failure=(.+)$/m;
if ($failure eq 'db_branch replay not implemented yet')
{
	ok(-d $node->data_dir . '/' . $clone_path, 'FICLONE clone staging directory exists');
	ok(-f $node->data_dir . '/' . $clone_path . '/PG_VERSION', 'FICLONE clone copied PG_VERSION');
}
else
{
	pass('FICLONE is unavailable on this filesystem and branch creation failed explicitly');
	pass('skipped clone file checks because FICLONE is unavailable');
}

my $branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_target';]);
is($branch_count, '0', 'failed branch is not connectable');

my $writer = $node->background_psql('dbbranch_source', on_error_stop => 1);
$writer->query_safe(q[BEGIN; INSERT INTO users VALUES (3, 'carol');]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[SELECT pg_create_database_branch('dbbranch_source', 'dbbranch_busy_target');],
	stderr => \$stderr);

is($result, 3, 'db branch internal entry rejects busy source database');
like(
	$stderr,
	qr/source database "dbbranch_source" is being accessed by other users/,
	'conservative source drain blocks active writer');

$writer->query_safe('ROLLBACK;');
$writer->quit;

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 2, 'busy branch attempt writes separate metadata file');

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

like($busy_metadata, qr/^status=FAILED$/m, 'busy source metadata final state is FAILED');
like(
	$busy_metadata,
	qr/^failure=source database is being accessed by other users$/m,
	'busy source metadata records drain failure');

done_testing();
