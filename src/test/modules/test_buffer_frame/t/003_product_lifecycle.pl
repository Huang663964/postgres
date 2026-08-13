# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('product_lifecycle');
$node->init(allows_streaming => 'physical');
$node->append_conf('postgresql.conf', "shared_buffers = '8MB'\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_buffer_frame');
$node->safe_psql('template1', 'CREATE EXTENSION test_buffer_frame');

sub create_branch
{
	my ($source, $branch) = @_;

	$node->safe_psql(
		'postgres',
		"CREATE BRANCH $branch FROM DATABASE $source "
		  . 'BUFFER SHARED READ ONLY');
}

sub buffer_id
{
	my ($database, $relation, $block) = @_;

	return $node->safe_psql(
		$database,
		"SELECT test_buffer_frame_buffer_id('$relation'::regclass, $block)");
}

sub mapping
{
	my ($buffer) = @_;

	return $node->safe_psql(
		'postgres', "SELECT test_buffer_frame_mapping_state($buffer)");
}

sub aliases
{
	my ($target_db, $source_db, $relation, $block) = @_;
	my $source_buffer = buffer_id($source_db, $relation, $block);
	my $target_buffer = buffer_id($target_db, $relation, $block);
	my ($target_frame) = split(/:/, mapping($target_buffer));

	return $target_frame == $source_buffer - 1;
}

sub relation_is_dirty
{
	my ($database, $relation, $block) = @_;

	return $node->safe_psql(
		$database,
		"SELECT test_buffer_frame_is_dirty('$relation'::regclass, $block)");
}

sub mapping_is_private
{
	my ($buffer) = @_;
	my ($frame, $attachments, $home) = split(/:/, mapping($buffer));

	return $frame == $buffer - 1 && $attachments == 1 && $home == 1;
}

$node->safe_psql('postgres', 'CREATE DATABASE lifecycle_source');
$node->safe_psql(
	'lifecycle_source', q[
CREATE TABLE lifecycle_data(id int PRIMARY KEY, payload text NOT NULL)
	WITH (autovacuum_enabled = false);
INSERT INTO lifecycle_data
SELECT g, repeat(md5(g::text), 8) FROM generate_series(1, 200) AS g;
DELETE FROM lifecycle_data WHERE id = 1;

CREATE TABLE hash_data(id int, payload text)
	WITH (autovacuum_enabled = false);
CREATE INDEX hash_data_idx ON hash_data USING hash(id);
INSERT INTO hash_data VALUES (1, 'dead'), (2, 'live');
DELETE FROM hash_data WHERE id = 1;

CREATE TABLE gist_data(p point, payload text)
	WITH (autovacuum_enabled = false);
CREATE INDEX gist_data_idx ON gist_data USING gist(p);
INSERT INTO gist_data VALUES (point(1, 1), 'dead'), (point(2, 2), 'live');
DELETE FROM gist_data WHERE p ~= point(1, 1);

CREATE TABLE fork_data(id int, payload text)
	WITH (autovacuum_enabled = false);
INSERT INTO fork_data
SELECT g, repeat(md5(g::text), 32) FROM generate_series(1, 1000) AS g;
VACUUM fork_data;
CHECKPOINT;
]);
create_branch('lifecycle_source', 'lifecycle_a');
create_branch('lifecycle_source', 'lifecycle_b');

my $dead_lookup = q[
SET enable_seqscan = off;
SELECT count(*) FROM lifecycle_data WHERE id = 1;
];
is($node->safe_psql('lifecycle_a', $dead_lookup), '0',
	'first ordinary SQL read registers heap and btree candidates');
is($node->safe_psql('lifecycle_b', $dead_lookup), '0',
	'second ordinary SQL read sees the same result');
ok(aliases('lifecycle_b', 'lifecycle_a', 'lifecycle_data', 0),
	'ordinary heap reads share one immutable frame');
ok(aliases('lifecycle_b', 'lifecycle_a', 'lifecycle_data_pkey', 1),
	'ordinary btree reads share one immutable frame');
is(relation_is_dirty('lifecycle_a', 'lifecycle_data', 0), 'f',
	'candidate heap visibility hints are suppressed before the first alias');
is(relation_is_dirty('lifecycle_b', 'lifecycle_data_pkey', 1), 'f',
	'aliased btree LP_DEAD hints leave shared bytes clean');

my $hash_lookup = q[
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM hash_data WHERE id = 1;
];
is($node->safe_psql('lifecycle_a', $hash_lookup), '0',
	'first ordinary hash-index lookup sees the deleted tuple as absent');
is($node->safe_psql('lifecycle_b', $hash_lookup), '0',
	'second ordinary hash-index lookup sees the same result');
my $hash_nblocks = $node->safe_psql(
	'lifecycle_a',
	q[SELECT pg_relation_size('hash_data_idx') /
		current_setting('block_size')::int]);
my @shared_hash_blocks = grep {
	aliases('lifecycle_b', 'lifecycle_a', 'hash_data_idx', $_)
} (1 .. $hash_nblocks - 1);
ok(@shared_hash_blocks > 0,
	'hash bucket pages share after LP_DEAD hint suppression');
my $shared_hash_block = $shared_hash_blocks[0] // 1;
is(relation_is_dirty('lifecycle_a', 'hash_data_idx', $shared_hash_block), 'f',
	'hash candidate remains clean');

my $gist_lookup = q[
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM gist_data WHERE p ~= point(1, 1);
];
is($node->safe_psql('lifecycle_a', $gist_lookup), '0',
	'first ordinary GiST lookup sees the deleted tuple as absent');
is($node->safe_psql('lifecycle_b', $gist_lookup), '0',
	'second ordinary GiST lookup sees the same result');
ok(aliases('lifecycle_b', 'lifecycle_a', 'gist_data_idx', 0),
	'GiST leaf pages share after garbage-hint suppression');
is(relation_is_dirty('lifecycle_a', 'gist_data_idx', 0), 'f',
	'GiST candidate remains clean');

for my $fork (1, 2)
{
	my $a_fork = $node->safe_psql(
		'lifecycle_a',
		"SELECT test_buffer_frame_read_fork('fork_data'::regclass, $fork, 0)");
	my $b_fork = $node->safe_psql(
		'lifecycle_b',
		"SELECT test_buffer_frame_read_fork('fork_data'::regclass, $fork, 0)");

	ok(mapping_is_private($a_fork) && mapping_is_private($b_fork),
		$fork == 1 ? 'FSM pages remain private' : 'VM pages remain private');
}

$node->safe_psql('postgres', 'CHECKPOINT');
ok(aliases('lifecycle_b', 'lifecycle_a', 'lifecycle_data', 0),
	'checkpoint preserves a clean shared mapping');

my $a_heap = buffer_id('lifecycle_a', 'lifecycle_data', 0);
my $b_heap = buffer_id('lifecycle_b', 'lifecycle_data', 0);
is(
	$node->safe_psql('postgres', "SELECT test_buffer_frame_evict($b_heap)"),
	't', 'clock eviction detaches and invalidates a non-identity target');
like(mapping($a_heap), qr/^\d+:1:1:\d+:\d+$/,
	'last target eviction returns the source frame to private ownership');
is($node->safe_psql('lifecycle_b', 'SELECT count(*) FROM lifecycle_data'),
	'199', 'evicted branch page reloads from its own storage');
ok(aliases('lifecycle_b', 'lifecycle_a', 'lifecycle_data', 0),
	'reloaded equal page can be promoted again');

is(
	$node->safe_psql(
		'lifecycle_b',
		q[SELECT test_buffer_frame_drop_buffers('lifecycle_data'::regclass)]),
	't', 'relation invalidation automatically detaches a target alias');
is($node->safe_psql('lifecycle_b', 'SELECT count(*) FROM lifecycle_data'),
	'199', 'relation invalidation is followed by a correct private reload');

$node->safe_psql('postgres', 'CREATE DATABASE drop_source');
$node->safe_psql(
	'drop_source', q[
CREATE TABLE drop_data(id int PRIMARY KEY, payload text NOT NULL)
	WITH (autovacuum_enabled = false);
INSERT INTO drop_data VALUES (1, repeat('survives-owner-drop', 16));
CHECKPOINT;
]);
create_branch('drop_source', 'drop_owner');
create_branch('drop_source', 'drop_survivor');
is($node->safe_psql('drop_owner', 'SELECT count(*) FROM drop_data'), '1',
	'owner branch registers the source frame');
is($node->safe_psql('drop_survivor', 'SELECT count(*) FROM drop_data'), '1',
	'survivor branch promotes onto the owner frame');
ok(aliases('drop_survivor', 'drop_owner', 'drop_data', 0),
	'drop scenario starts with a real cross-database alias');

$node->safe_psql('postgres', 'DROP DATABASE drop_owner');
is(
	$node->safe_psql(
		'drop_survivor',
		q[SELECT id || ':' || length(payload) FROM drop_data]),
	'1:304',
	'survivor keeps reading the orphaned immutable frame after owner DROP');
is(relation_is_dirty('drop_survivor', 'drop_data', 0), 'f',
	'orphaned source bytes remain clean');

$node->safe_psql('postgres', 'DROP DATABASE drop_survivor');
$node->safe_psql('postgres', 'DROP DATABASE drop_source');
$node->safe_psql('postgres', 'DROP DATABASE lifecycle_b');
$node->safe_psql('postgres', 'DROP DATABASE lifecycle_a');
$node->safe_psql('postgres', 'DROP DATABASE lifecycle_source');

$node->stop;
done_testing();
