# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('promotion');
$node->init(allows_streaming => 'physical');
$node->append_conf(
	'postgresql.conf',
	"shared_preload_libraries = 'injection_points'\n"
	  . "shared_buffers = '32MB'\n"
	  . "io_method = worker\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$node->safe_psql('postgres', 'CREATE EXTENSION test_buffer_frame');
$node->safe_psql('template1', 'CREATE EXTENSION injection_points');
$node->safe_psql('template1', 'CREATE EXTENSION test_buffer_frame');
is($node->safe_psql('postgres', 'SHOW io_method'), 'worker',
	'promotion test runs with worker AIO enabled');

sub create_branch
{
	my ($source, $branch) = @_;

	$node->safe_psql(
		'postgres',
		"CREATE BRANCH $branch FROM DATABASE $source "
		  . 'BUFFER SHARED READ ONLY');
}

sub promote
{
	my ($database, $family, $hash, $async) = @_;
	my $hash_sql = defined($hash) ? $hash : 'NULL';

	return $node->safe_psql(
		$database,
		"SELECT test_buffer_frame_promote('frame_data'::regclass, 0, "
		  . "$family, $hash_sql, " . ($async ? 'true' : 'false') . ')');
}

sub mapping
{
	my ($database, $buffer) = @_;

	return $node->safe_psql(
		$database,
		"SELECT test_buffer_frame_mapping_state($buffer)");
}

sub aliases
{
	my ($target_db, $source_db) = @_;
	my $source_buffer = $node->safe_psql(
		$source_db,
		q[SELECT test_buffer_frame_buffer_id('frame_data'::regclass, 0)]);
	my $target_buffer = $node->safe_psql(
		$target_db,
		q[SELECT test_buffer_frame_buffer_id('frame_data'::regclass, 0)]);
	my $target_mapping = mapping($target_db, $target_buffer);
	my ($target_frame) = split(/:/, $target_mapping);

	return $target_frame == $source_buffer - 1 ? 't' : 'f';
}

sub stats
{
	return $node->safe_psql(
		'postgres', 'SELECT test_buffer_frame_promotion_stats()');
}

sub normalize_page
{
	my ($database) = @_;

	$node->safe_psql(
		$database,
		q[SELECT test_buffer_frame_overwrite_clean_page(
			'frame_data'::regclass, 0, 90)]);
}

$node->safe_psql('postgres', 'CREATE DATABASE frame_source');
$node->safe_psql(
	'frame_source', q[
CREATE TABLE frame_data(id int PRIMARY KEY, payload text)
	WITH (autovacuum_enabled = false);
INSERT INTO frame_data VALUES (1, repeat('same', 200));
CHECKPOINT;
]);
create_branch('frame_source', 'frame_a');
create_branch('frame_source', 'frame_b');

$node->safe_psql('postgres', 'SELECT test_buffer_frame_reset_promotion_stats()');
promote('frame_source', 'NULL', undef, 0);
is(stats(), '0:0:0:0:0:0',
	'ordinary source database is excluded before candidate lookup');

$node->safe_psql(
	'postgres', q[
CREATE UNLOGGED TABLE frame_unlogged(id int);
INSERT INTO frame_unlogged VALUES (1);
]);
$node->safe_psql('postgres', 'SELECT test_buffer_frame_reset_promotion_stats()');
my $unlogged_buffer = $node->safe_psql(
	'postgres',
	q[SELECT test_buffer_frame_promote('frame_unlogged'::regclass, 0, 1)]);
isnt($unlogged_buffer, '', 'unlogged relation read still returns a buffer');
is(stats(), '0:0:0:0:0:0',
	'unlogged relation is excluded before candidate lookup');

my $temporary = $node->background_psql('postgres');
$temporary->query_safe('CREATE TEMP TABLE frame_temporary(id int)');
$temporary->query_safe('INSERT INTO frame_temporary VALUES (1)');
$node->safe_psql('postgres', 'SELECT test_buffer_frame_reset_promotion_stats()');
my $temporary_buffer = $temporary->query_safe(
	q[SELECT test_buffer_frame_promote('frame_temporary'::regclass, 0, 1)]);
isnt($temporary_buffer, '', 'temporary relation read still returns a buffer');
is(stats(), '0:0:0:0:0:0',
	'temporary relation is excluded before candidate lookup');
$temporary->quit;

$node->safe_psql('postgres', 'SELECT test_buffer_frame_reset_promotion_stats()');
my $a_buffer = promote('frame_a', 'NULL', undef, 0);
like(stats(), qr/^1:1:0:0:0:0$/,
	'first eligible branch read registers one weak candidate');
my $cold_b = $node->safe_psql(
	'frame_b',
	q[SELECT test_buffer_frame_buffer_id('frame_data'::regclass, 0)]);
is(
	$node->safe_psql(
		'postgres', "SELECT test_buffer_frame_evict($cold_b)"),
	't', 'second branch page is cold before the AIO promotion read');
my $b_buffer = promote('frame_b', 'NULL', undef, 1);
like(stats(), qr/^2:1:1:0:1:0$/,
	'worker-AIO completion promotes an equal same-family page');
is(aliases('frame_b', 'frame_a'), 't',
	'same-family branch pages resolve to one frame');
like(mapping('frame_b', $b_buffer), qr/^\d+:2:0:1:1$/,
	'published target has one shared mapping generation');

create_branch('frame_source', 'frame_family_probe');
$node->safe_psql('postgres', 'SELECT test_buffer_frame_reset_promotion_stats()');
normalize_page('frame_family_probe');
promote('frame_family_probe', 'NULL', 31337, 0);
is(stats(), '1:1:0:0:0:0',
	'first family registers the forced cross-family probe key');

$node->safe_psql('postgres', 'CREATE DATABASE other_source TEMPLATE frame_source');
create_branch('other_source', 'other_a');
create_branch('other_source', 'other_b');
normalize_page('other_a');
normalize_page('other_b');
my $other_a_buffer = promote('other_a', 'NULL', 31337, 0);
like(mapping('other_a', $other_a_buffer), qr/^\d+:1:1:0:1$/,
	'cross-family page remains private and registers separately');
is(stats(), '2:2:0:0:0:0',
	'second family owns a distinct candidate key');
my $other_b_buffer = promote('other_b', 'NULL', 31337, 0);
is(stats(), '3:2:1:0:1:0',
	'second family records its own successful promotion');
is(aliases('other_b', 'other_a'), 't',
	'equal pages can still promote inside the second family');

$node->safe_psql('postgres', 'CREATE DATABASE collision_source');
$node->safe_psql(
	'collision_source', q[
CREATE TABLE frame_data(id int PRIMARY KEY, payload text)
	WITH (autovacuum_enabled = false);
INSERT INTO frame_data VALUES (1, repeat('old', 200));
CHECKPOINT;
]);
create_branch('collision_source', 'collision_old');
$node->safe_psql(
	'collision_source', q[
UPDATE frame_data SET payload = repeat('new', 200);
CHECKPOINT;
]);
create_branch('collision_source', 'collision_new');
$node->safe_psql('postgres', 'SELECT test_buffer_frame_reset_promotion_stats()');
promote('collision_old', 'NULL', 424242, 0);
my $new_buffer = promote('collision_new', 'NULL', 424242, 0);
is(aliases('collision_new', 'collision_old'), 'f',
	'forced digest collision cannot bypass the full 8KiB comparison');
like(mapping('collision_new', $new_buffer), qr/^\d+:1:1:0:\d+$/,
	'collision failure leaves the target on its private frame');
like(stats(), qr/^2:2:1:1:0:1$/,
	'collision is measured and its replacement candidate is registered');

my $ordinary_buffer = $node->safe_psql(
	'frame_a',
	q[SELECT test_buffer_frame_buffer_id('frame_data'::regclass, 0)]);
is(mapping('frame_a', $ordinary_buffer), mapping('frame_a', $a_buffer),
	'ordinary reads do not enable or change promotion state');
my $feature_off_stats = stats();
create_branch('frame_source', 'frame_feature_off');
is($node->safe_psql('frame_feature_off', 'SELECT count(*) FROM frame_data'),
	'1', 'ordinary feature-off SQL remains usable on a private branch page');
is(stats(), $feature_off_stats,
	'ordinary feature-off SQL does not touch promotion metrics');
my $feature_off_buffer = $node->safe_psql(
	'frame_feature_off',
	q[SELECT test_buffer_frame_buffer_id('frame_data'::regclass, 0)]);
like(mapping('frame_feature_off', $feature_off_buffer),
	qr/^\d+:1:1:0:\d+$/,
	'ordinary feature-off SQL leaves the branch page private');

SKIP:
{
	skip 'Injection points not supported by this build', 16
	  unless $ENV{enable_injection_points} eq 'yes';

	create_branch('frame_source', 'frame_concurrent_a');
	create_branch('frame_source', 'frame_concurrent_b');
	my $concurrent_point = 'dbbranch-frame-promotion-before-content-lock';
	$node->safe_psql(
		'frame_a',
		"SELECT injection_points_attach('$concurrent_point', 'wait')");
	$node->safe_psql('postgres', 'SELECT test_buffer_frame_reset_promotion_stats()');
	my $concurrent_a = $node->background_psql('frame_concurrent_a');
	my $concurrent_b = $node->background_psql('frame_concurrent_b');
	$concurrent_a->query_until(
		qr/concurrent_a_started/,
		"\\echo concurrent_a_started\n"
		  . "SELECT test_buffer_frame_promote('frame_data'::regclass, 0);\n"
		  . "\\echo concurrent_a_done\n");
	$concurrent_b->query_until(
		qr/concurrent_b_started/,
		"\\echo concurrent_b_started\n"
		  . "SELECT test_buffer_frame_promote('frame_data'::regclass, 0);\n"
		  . "\\echo concurrent_b_done\n");
	ok(
		$node->poll_query_until(
			'postgres',
			"SELECT count(*) = 2 FROM pg_stat_activity "
			  . "WHERE wait_event = '$concurrent_point'",
			't'),
		'both concurrent reads reach the pre-publication point');
	is(
		$node->safe_psql(
			'frame_a',
			"SELECT injection_points_wakeup('$concurrent_point')"),
		'', 'both concurrent reads are released');
	# A waiter can publish its old counter after the first broadcast.  A
	# second counter bump closes that test-only wakeup race deterministically.
	$node->safe_psql(
		'frame_a',
		"SELECT injection_points_wakeup('$concurrent_point')");
	$concurrent_a->query_until(qr/concurrent_a_done/, '');
	$concurrent_b->query_until(qr/concurrent_b_done/, '');
	$concurrent_a->quit;
	$concurrent_b->quit;
	$node->safe_psql(
		'frame_a',
		"SELECT injection_points_detach('$concurrent_point')");
	is(aliases('frame_concurrent_a', 'frame_a'), 't',
		'first concurrent target resolves to the family candidate');
	is(aliases('frame_concurrent_b', 'frame_a'), 't',
		'second concurrent target resolves to the family candidate');
	is(stats(), '2:0:2:0:2:0',
		'concurrent publication records two clean promotions');

	my $point = 'dbbranch_promotion_contention';
	create_branch('frame_source', 'frame_c');
	$node->safe_psql(
		'frame_a', "SELECT injection_points_attach('$point', 'wait')");
	my $locker = $node->background_psql('frame_a');
	$locker->query_until(
		qr/locker_started/,
		"\\echo locker_started\n"
		  . "SELECT test_buffer_frame_hold_page_lock('frame_data'::regclass, 0, '$point');\n"
		  . "\\echo locker_done\n");

	my $start = time();
	my $contended = promote('frame_c', 'NULL', undef, 0);
	ok(time() - $start < 5,
		'content-lock contention skips promotion without blocking');
	isnt($contended, '', 'contended read still returns a valid buffer');
	like(mapping('frame_c', $contended), qr/^\d+:1:1:0:\d+$/,
		'contention failure keeps the target private');

	is(
		$node->safe_psql('frame_a', "SELECT injection_points_wakeup('$point')"),
		'', 'blocked locker is released');
	$locker->query_until(qr/locker_done/, '');
	$locker->quit;
	$node->safe_psql(
		'frame_a', "SELECT injection_points_detach('$point')");
	pass('contention helper exits cleanly');

	create_branch('frame_source', 'frame_cancel');
	my $cancel_point = 'dbbranch-frame-promotion-before-content-lock';
	$node->safe_psql(
		'frame_cancel',
		"SELECT injection_points_attach('$cancel_point', 'wait')");
	my $cancel = $node->background_psql(
		'frame_cancel', on_error_stop => 0,
		timeout => 4 * $PostgreSQL::Test::Utils::timeout_default);
	my $cancel_pid = $cancel->query_safe('SELECT pg_backend_pid()');
	$cancel->{stderr} = '';
	$cancel->query_until(
		qr/cancel_started/,
		"\\echo cancel_started\n"
		  . "SELECT test_buffer_frame_promote('frame_data'::regclass, 0);\n"
		  . "\\echo cancel_done\n");
	ok(
		$node->poll_query_until(
			'postgres',
			"SELECT count(*) = 1 FROM pg_stat_activity "
			  . "WHERE pid = $cancel_pid AND wait_event = '$cancel_point'",
			't'),
		'promotion reaches the final pre-publication point');
	is(
		$node->safe_psql(
			'postgres', "SELECT pg_cancel_backend($cancel_pid)"),
		't', 'promotion query receives cancellation');
	is(
		$node->safe_psql(
			'frame_cancel',
			"SELECT injection_points_wakeup('$cancel_point')"),
		'', 'pre-publication wait is woken to process cancellation');
	$cancel->query_until(qr/cancel_done/, '');
	like($cancel->{stderr}, qr/canceling statement due to user request/,
		'canceled promotion reports the query interruption');
	$cancel->quit;
	$node->safe_psql(
		'frame_cancel',
		"SELECT injection_points_detach('$cancel_point')");
	my $cancel_buffer = $node->safe_psql(
		'frame_cancel',
		q[SELECT test_buffer_frame_buffer_id('frame_data'::regclass, 0)]);
	like(mapping('frame_cancel', $cancel_buffer), qr/^\d+:1:1:0:\d+$/,
		'cancellation before publication leaves a private frame');
	$cancel_buffer = promote('frame_cancel', 'NULL', undef, 0);
	is(aliases('frame_cancel', 'frame_a'), 't',
		'promotion succeeds after cancellation cleanup');
}

my $legacy_stderr = '';
my $legacy_result = $node->psql(
	'postgres',
	q[LOAD 'test_buffer_frame_legacy';],
	stderr => \$legacy_stderr);
isnt($legacy_result, 0,
	'extension compiled for the pre-frame ABI is rejected');
like($legacy_stderr, qr/ABI mismatch/,
	'legacy extension failure identifies the ABI boundary');

$node->backup('promotion_backup');
my $standby = PostgreSQL::Test::Cluster->new('promotion_standby');
$standby->init_from_backup(
	$node, 'promotion_backup', has_streaming => 1);
$standby->start;
$standby->safe_psql(
	'postgres', 'SELECT test_buffer_frame_reset_promotion_stats()');
my $standby_buffer = $standby->safe_psql(
	'frame_a',
	q[SELECT test_buffer_frame_promote('frame_data'::regclass, 0, 1)]);
is(
	$standby->safe_psql(
		'postgres', 'SELECT test_buffer_frame_promotion_stats()'),
	'0:0:0:0:0:0', 'recovery disables promotion before candidate lookup');
like(
	$standby->safe_psql(
		'frame_a',
		"SELECT test_buffer_frame_mapping_state($standby_buffer)"),
	qr/^\d+:1:1:0:0$/,
	'recovery read remains on its private frame');
$standby->stop;

$node->stop;
done_testing();
