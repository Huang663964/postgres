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
like($metadata, qr/^clone_path=base\/pg_dbbranch_[0-9]+_[0-9a-f]+$/m, 'metadata records clone staging path');

my ($clone_path) = $metadata =~ /^clone_path=(.+)$/m;
if ($result == 0)
{
	like($metadata, qr/^clone_result=(done|copy_fallback)$/m, 'metadata records storage clone success');
	like($metadata, qr/^cleanup=not_needed$/m, 'metadata records no failed clone cleanup needed');
	like($metadata, qr/^status_history=CREATING,COPYING,READY$/m, 'metadata records READY transition');
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
	is($branch_rows, '1:alice,2:bob', 'ready branch can read cloned source rows');

	$node->safe_psql('dbbranch_target', q[INSERT INTO users VALUES (3, 'dora');]);
	my $source_after_branch_write = $node->safe_psql(
		'dbbranch_source',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is($source_after_branch_write, '1:alice,2:bob', 'branch writes do not affect source');
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

like($tablespace_metadata, qr/^clone_result=not_started$/m, 'tablespace metadata records clone not started');
like($tablespace_metadata, qr/^cleanup=not_started$/m, 'tablespace metadata records cleanup not started');
like($tablespace_metadata, qr/^status=FAILED$/m, 'tablespace metadata final state is FAILED');
like(
	$tablespace_metadata,
	qr/^failure=db_branch currently supports only pg_default tablespace$/m,
	'tablespace metadata records limitation');
my ($tablespace_clone_path) = $tablespace_metadata =~ /^clone_path=(.+)$/m;
ok(!-e $node->data_dir . '/' . $tablespace_clone_path, 'tablespace rejection does not create clone staging path');

my $writer = $node->background_psql('dbbranch_source', on_error_stop => 1);
$writer->query_safe(q[BEGIN; INSERT INTO users VALUES (3, 'carol');]);

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
is(scalar @metadata_files, 3, 'busy branch attempt writes separate metadata file');

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

like($busy_metadata, qr/^clone_result=not_started$/m, 'busy source metadata records clone not started');
like($busy_metadata, qr/^cleanup=not_started$/m, 'busy source metadata records cleanup not started');
like($busy_metadata, qr/^status=FAILED$/m, 'busy source metadata final state is FAILED');
like(
	$busy_metadata,
	qr/^failure=source database is being accessed by other users$/m,
	'busy source metadata records drain failure');

done_testing();
