# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('materialize');
$node->init(allows_streaming => 1);
$node->start;

sub branch_mode
{
	my ($branch) = @_;

	return $node->safe_psql(
		'postgres',
		"SELECT buffer_mode::text FROM pg_dbbranch "
		  . "WHERE branch_db_oid = (SELECT oid FROM pg_database WHERE datname = '$branch')");
}

sub wait_for_lock
{
	my ($pid, $name) = @_;

	$node->poll_query_until(
		'postgres',
		"SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = $pid;",
		't') or die "timed out waiting for $name";
}

$node->safe_psql('postgres', 'CREATE DATABASE materialize_source');
$node->safe_psql(
	'materialize_source', q[
CREATE TABLE materialize_rows(id int PRIMARY KEY, payload text NOT NULL);
INSERT INTO materialize_rows VALUES (1, 'source');
CHECKPOINT;
]);
$node->safe_psql(
	'postgres', q[
CREATE BRANCH materialize_target
FROM DATABASE materialize_source
BUFFER SHARED READ ONLY;
CREATE BRANCH materialize_sibling
FROM DATABASE materialize_source
BUFFER SHARED READ ONLY;
CREATE DATABASE materialize_ordinary;
CREATE ROLE materialize_nonowner;
]);

is(branch_mode('materialize_target'), 's',
	'materialize target starts shared read-only');

my $stderr = '';
my $result = $node->psql(
	'postgres',
	q[BEGIN; ALTER BRANCH materialize_target MATERIALIZE WRITABLE;],
	stderr => \$stderr);
isnt($result, 0, 'ALTER BRANCH is rejected inside a transaction block');
like($stderr, qr/ALTER BRANCH cannot run inside a transaction block/,
	'transaction-block rejection names ALTER BRANCH');

$stderr = '';
$result = $node->psql(
	'materialize_target',
	q[ALTER BRANCH materialize_target MATERIALIZE WRITABLE;],
	stderr => \$stderr);
isnt($result, 0, 'a target connection cannot materialize itself');
like($stderr, qr/cannot materialize the currently open database branch/,
	'self-materialization rejection explains the control-database rule');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[ALTER BRANCH materialize_ordinary MATERIALIZE WRITABLE;],
	stderr => \$stderr);
isnt($result, 0, 'a normal database cannot be materialized as a branch');
like($stderr, qr/is not a database branch/,
	'normal-database rejection identifies the missing branch contract');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[SET ROLE materialize_nonowner;
ALTER BRANCH materialize_target MATERIALIZE WRITABLE;],
	stderr => \$stderr);
isnt($result, 0, 'a non-owner cannot materialize another role\'s branch');
like($stderr, qr/must be owner of database materialize_target/,
	'ownership rejection identifies the target database');

my $old_reader =
  $node->background_psql('materialize_target', on_error_stop => 1);
my $new_reader =
  $node->background_psql('materialize_target', on_error_stop => 1);
my $new_reader_pid = $new_reader->query_safe('SELECT pg_backend_pid();');
$old_reader->query_safe(
	q[BEGIN; SELECT count(*) FROM materialize_rows;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[SET lock_timeout = '100ms';
ALTER BRANCH materialize_target MATERIALIZE WRITABLE;],
	stderr => \$stderr);
isnt($result, 0, 'materialization honors lock_timeout while an old transaction is active');
like($stderr, qr/canceling statement due to lock timeout/,
	'lock timeout is reported by the transaction gate');
is(branch_mode('materialize_target'), 's',
	'timeout before drain leaves the durable mode shared read-only');

my $cancel_materialize =
  $node->background_psql('postgres', on_error_stop => 0);
my $cancel_pid = $cancel_materialize->query_safe('SELECT pg_backend_pid();');
$cancel_materialize->query_until(
	qr/start_cancel_materialize/,
	q(\echo start_cancel_materialize
ALTER BRANCH materialize_target MATERIALIZE WRITABLE;
\echo finish_cancel_materialize
));
wait_for_lock($cancel_pid, 'cancelable materializer');
is($node->safe_psql('postgres', "SELECT pg_cancel_backend($cancel_pid);"),
	't', 'waiting materialization can be canceled');
$cancel_materialize->query_until(qr/finish_cancel_materialize/, '');
like($cancel_materialize->{stderr}, qr/canceling statement due to user request/,
	'canceled materialization reports the original cancellation');
$cancel_materialize->quit;
is(branch_mode('materialize_target'), 's',
	'cancel before drain leaves the durable mode shared read-only');

my $materialize = $node->background_psql('postgres', on_error_stop => 1);
my $materialize_pid = $materialize->query_safe('SELECT pg_backend_pid();');
$materialize->query_until(
	qr/start_materialize/,
	q(\echo start_materialize
ALTER BRANCH materialize_target MATERIALIZE WRITABLE;
\echo finish_materialize
));
wait_for_lock($materialize_pid, 'materializer');
is(branch_mode('materialize_target'), 's',
	'mode remains shared read-only while old transactions drain');

$new_reader->query_until(
	qr/start_new_reader/,
	q(\echo start_new_reader
SELECT count(*) FROM materialize_rows;
\echo finish_new_reader
));
wait_for_lock($new_reader_pid, 'new target transaction');

$old_reader->query_safe('COMMIT;');
$materialize->query_until(qr/finish_materialize/, '');
$new_reader->query_until(qr/finish_new_reader/, '');
$materialize->quit;
$new_reader->quit;
$old_reader->quit;

is(branch_mode('materialize_target'), 'p',
	'materialization publishes private writable after the drain');
is($node->safe_psql('materialize_target',
	q[SELECT count(*) FROM materialize_rows;]), '1',
	'a transaction queued behind materialization reads correct branch storage');

$node->safe_psql('materialize_target',
	q[INSERT INTO materialize_rows VALUES (2, 'target');]);
$node->safe_psql('materialize_target',
	q[ALTER TABLE materialize_rows ALTER COLUMN id TYPE bigint;]);
$node->safe_psql('materialize_target', 'VACUUM FULL materialize_rows;');
$node->safe_psql('materialize_target', 'REINDEX TABLE materialize_rows;');
$node->safe_psql('postgres', 'CHECKPOINT;');
is($node->safe_psql('materialize_target',
	q[SELECT string_agg(id || ':' || payload, ',' ORDER BY id) FROM materialize_rows;]),
	'1:source,2:target',
	'native DML and rewrite maintenance work after materialization');

is($node->safe_psql('materialize_source',
	q[SELECT count(*) FROM materialize_rows;]), '1',
	'target writes remain isolated from the source');
is($node->safe_psql('materialize_sibling',
	q[SELECT count(*) FROM materialize_rows;]), '1',
	'target writes remain isolated from the shared-readonly sibling');
$stderr = '';
$result = $node->psql(
	'materialize_sibling',
	q[INSERT INTO materialize_rows VALUES (3, 'blocked');],
	stderr => \$stderr);
isnt($result, 0, 'the sibling remains shared read-only');
like($stderr, qr/database branch is shared read-only/,
	'sibling write rejection retains the shared-readonly policy');

$node->safe_psql(
	'postgres',
	q[ALTER BRANCH materialize_target MATERIALIZE WRITABLE;]);
is(branch_mode('materialize_target'), 'p',
	'repeating materialization on a private branch is idempotent');

if ($ENV{enable_injection_points} eq 'yes')
{
	$node->safe_psql('postgres', 'CREATE EXTENSION injection_points;');
	$node->safe_psql(
		'postgres', q[
CREATE BRANCH materialize_crash
FROM DATABASE materialize_source
BUFFER SHARED READ ONLY;
SELECT injection_points_attach('db-branch-materialize-after-state', 'wait');
]);

	my $crash_materialize =
	  $node->background_psql('postgres', on_error_stop => 0);
	$crash_materialize->query_until(
		qr/start_crash_materialize/,
		q(\echo start_crash_materialize
ALTER BRANCH materialize_crash MATERIALIZE WRITABLE;
\echo finish_crash_materialize
));
	$node->wait_for_event('client backend',
		'db-branch-materialize-after-state');
	is(branch_mode('materialize_crash'), 'm',
		'materializing mode is durable before buffer cleanup');

	$node->stop('immediate');
	$crash_materialize->{run}->finish;
	$node->start;

	is(branch_mode('materialize_crash'), 'm',
		'materializing mode survives an immediate server restart');
	$stderr = '';
	$result = $node->psql(
		'materialize_crash', 'SELECT 1;', stderr => \$stderr);
	isnt($result, 0, 'materializing branch fails closed after restart');
	like($stderr, qr/database branch is not available for transactions/,
		'fail-closed connection reports materializing state');
	$node->safe_psql(
		'postgres',
		q[ALTER BRANCH materialize_crash MATERIALIZE WRITABLE;]);
	is(branch_mode('materialize_crash'), 'p',
		'explicit retry completes materialization after restart');
	$node->safe_psql('materialize_crash',
		q[INSERT INTO materialize_rows VALUES (4, 'retry');]);
	is($node->safe_psql('materialize_crash',
		q[SELECT count(*) FROM materialize_rows;]), '2',
		'retried branch is native writable');
}

$node->restart;
is(branch_mode('materialize_target'), 'p',
	'private writable mode survives restart');
$node->safe_psql('materialize_target',
	q[INSERT INTO materialize_rows VALUES (5, 'after-restart');]);
is($node->safe_psql('materialize_target',
	q[SELECT count(*) FROM materialize_rows;]), '3',
	'materialized branch remains writable after restart');

$node->safe_psql('postgres', 'DROP DATABASE materialize_target;');
if ($ENV{enable_injection_points} eq 'yes')
{
	$node->safe_psql('postgres', 'DROP DATABASE materialize_crash;');
}
$node->safe_psql('postgres', 'DROP DATABASE materialize_sibling;');
$node->safe_psql('postgres', 'DROP DATABASE materialize_ordinary;');
$node->safe_psql('postgres', 'DROP DATABASE materialize_source;');
$node->safe_psql('postgres', 'DROP ROLE materialize_nonowner;');

$node->stop;
done_testing();
