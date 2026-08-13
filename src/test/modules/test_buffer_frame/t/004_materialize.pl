# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('materialize_frame');
$node->init(allows_streaming => 'physical');
$node->append_conf('postgresql.conf', "shared_buffers = '8MB'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_buffer_frame');
$node->safe_psql('template1', 'CREATE EXTENSION test_buffer_frame');

sub branch_mode
{
	my ($branch) = @_;

	return $node->safe_psql(
		'postgres',
		"SELECT buffer_mode::text FROM pg_dbbranch "
		  . "WHERE branch_db_oid = (SELECT oid FROM pg_database WHERE datname = '$branch')");
}

sub buffer_id
{
	my ($database) = @_;

	return $node->safe_psql(
		$database,
		q[SELECT test_buffer_frame_buffer_id('materialize_data'::regclass, 0)]);
}

sub mapping
{
	my ($buffer) = @_;

	return $node->safe_psql(
		'postgres', "SELECT test_buffer_frame_mapping_state($buffer)");
}

sub mapping_is_private
{
	my ($buffer) = @_;
	my ($frame, $attachments, $home) = split(/:/, mapping($buffer));

	return $frame == $buffer - 1 && $attachments == 1 && $home == 1;
}

$node->safe_psql('postgres', 'CREATE DATABASE materialize_frame_source');
$node->safe_psql(
	'materialize_frame_source', q[
CREATE TABLE materialize_data(id int PRIMARY KEY, payload text NOT NULL)
	WITH (autovacuum_enabled = false);
INSERT INTO materialize_data VALUES (1, repeat('source', 256));
CHECKPOINT;
]);
$node->safe_psql(
	'postgres', q[
CREATE BRANCH materialize_frame_a
FROM DATABASE materialize_frame_source
BUFFER SHARED READ ONLY;
CREATE BRANCH materialize_frame_b
FROM DATABASE materialize_frame_source
BUFFER SHARED READ ONLY;
]);

is($node->safe_psql('materialize_frame_a',
	q[SELECT count(*) FROM materialize_data;]), '1',
	'first branch loads an immutable heap page');
is($node->safe_psql('materialize_frame_b',
	q[SELECT count(*) FROM materialize_data;]), '1',
	'second branch reads the same bytes');

my $a_heap = buffer_id('materialize_frame_a');
my $b_heap = buffer_id('materialize_frame_b');
my ($b_frame) = split(/:/, mapping($b_heap));
is($b_frame, $a_heap - 1,
	'materialization scenario starts with a real cross-database alias');

if ($ENV{enable_injection_points} eq 'yes')
{
	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
	$node->safe_psql(
		'postgres',
		q[SELECT injection_points_attach(
			'db-branch-materialize-after-buffer-drop', 'error')]);

	my $stderr = '';
	my $result = $node->psql(
		'postgres',
		q[ALTER BRANCH materialize_frame_b MATERIALIZE WRITABLE;],
		stderr => \$stderr);
	isnt($result, 0, 'injected failure stops publication after cleanup');
	like($stderr, qr/injection point.*error|error.*injection point/i,
		'injected failure is reported');
	is(branch_mode('materialize_frame_b'), 'm',
		'private mode is not published after cleanup failure');
	ok(mapping_is_private($b_heap),
		'buffer cleanup detaches the old branch alias without copying it');

	$stderr = '';
	$result = $node->psql(
		'materialize_frame_b', 'SELECT 1;', stderr => \$stderr);
	isnt($result, 0, 'materializing branch remains fail closed');
	like($stderr, qr/database branch is not available for transactions/,
		'materializing state rejects target transactions');

	$node->safe_psql(
		'postgres',
		q[SELECT injection_points_detach(
			'db-branch-materialize-after-buffer-drop')]);
}

$node->safe_psql(
	'postgres',
	q[ALTER BRANCH materialize_frame_b MATERIALIZE WRITABLE;]);
is(branch_mode('materialize_frame_b'), 'p',
	'private writable is published only after buffer cleanup succeeds');

$node->safe_psql(
	'materialize_frame_b',
	q[INSERT INTO materialize_data VALUES (2, repeat('branch', 256));]);
is($node->safe_psql('materialize_frame_b',
	q[SELECT count(*) FROM materialize_data;]), '2',
	'materialized branch uses native writable buffers');
my $private_heap = buffer_id('materialize_frame_b');
ok(mapping_is_private($private_heap),
	'materialized branch reloads its page on a private identity frame');

is($node->safe_psql('materialize_frame_a',
	q[SELECT count(*) FROM materialize_data;]), '1',
	'shared-readonly sibling remains isolated');
my $stderr = '';
my $result = $node->psql(
	'materialize_frame_a',
	q[INSERT INTO materialize_data VALUES (3, 'blocked');],
	stderr => \$stderr);
isnt($result, 0, 'shared-readonly sibling still rejects writes');
like($stderr, qr/database branch is shared read-only/,
	'sibling retains the shared-readonly write gate');
is($node->safe_psql('materialize_frame_source',
	q[SELECT count(*) FROM materialize_data;]), '1',
	'materialized writes remain isolated from source storage');

$node->safe_psql('postgres', 'DROP DATABASE materialize_frame_b;');
$node->safe_psql('postgres', 'DROP DATABASE materialize_frame_a;');
$node->safe_psql('postgres', 'DROP DATABASE materialize_frame_source;');

$node->stop;
done_testing();
