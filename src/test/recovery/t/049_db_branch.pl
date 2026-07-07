# Copyright (c) 2026, PostgreSQL Global Development Group

# Prototype harness for DB Branch.  The primary path creates a real,
# connectable branch when the filesystem supports FICLONE.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', 'max_prepared_transactions = 10');
$node->start;

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_source;');
$node->safe_psql(
	'dbbranch_source',
	qq(
CREATE TABLE users (id int PRIMARY KEY, name text NOT NULL);
INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
CHECKPOINT;
INSERT INTO users VALUES (3, 'carol');
));

my $source_rows = $node->safe_psql(
	'dbbranch_source',
	'SELECT count(*) FROM users;');
is($source_rows, '3', 'source database baseline is ready for DB Branch');

my $source_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_source';]);
my $source_dbpath = 'base/' . $source_oid;
$node->safe_psql('dbbranch_source', q[SELECT 1;]);
ok(-e $node->data_dir . '/' . $source_dbpath . '/pg_internal.init',
	'source relcache init file exists before DB Branch clone');

my $source_temp = $node->background_psql('dbbranch_source', on_error_stop => 1);
$source_temp->query_safe(q[CREATE TEMP TABLE temp_branch_private (id int);]);
$source_temp->query_safe(q[INSERT INTO temp_branch_private VALUES (1);]);
my $source_temp_relpath =
  $source_temp->query_safe(q[SELECT pg_relation_filepath('temp_branch_private');]);
my ($source_temp_file) = $source_temp_relpath =~ m{([^/]+)$};
ok(-e $node->data_dir . '/' . $source_temp_relpath,
	'live source temp relation file exists before DB Branch clone');

my $wal_start = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn();');
my $stderr = '';
my $result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_target FROM DATABASE dbbranch_source],
	stderr => \$stderr);

my @metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, 1, 'branch creation writes one metadata file');
my $metadata_file_count = scalar @metadata_files;

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
like($metadata, qr/^wal_range_bytes=[0-9]+$/m, 'metadata records WAL range size');
like($metadata, qr/^wal_records_scanned=[0-9]+$/m, 'metadata records scanned WAL count');
like($metadata, qr/^wal_source_records=[0-9]+$/m, 'metadata records source WAL count');
like($metadata, qr/^wal_other_db_records=[0-9]+$/m, 'metadata records other DB WAL count');
like($metadata, qr/^wal_mixed_records=[0-9]+$/m, 'metadata records mixed WAL count');
like($metadata, qr/^wal_global_records=[0-9]+$/m, 'metadata records global WAL count');
like($metadata, qr/^wal_source_fpi_blocks=[0-9]+$/m, 'metadata records source FPI block count');
like($metadata, qr/^wal_source_non_fpi_records=[0-9]+$/m, 'metadata records source non-FPI record count');
like($metadata, qr/^wal_replayed_records=[0-9]+$/m, 'metadata records replayed WAL count');
like($metadata, qr/^source_blocking_ms=[0-9]+(\.[0-9]+)?$/m, 'metadata records source blocking time');
like($metadata, qr/^clone_elapsed_ms=[0-9]+(\.[0-9]+)?$/m, 'metadata records clone elapsed time');
like($metadata, qr/^replay_elapsed_ms=[0-9]+(\.[0-9]+)?$/m, 'metadata records replay elapsed time');
my ($wal_records_scanned) = $metadata =~ /^wal_records_scanned=([0-9]+)$/m;
my ($wal_source_records) = $metadata =~ /^wal_source_records=([0-9]+)$/m;
my ($wal_other_db_records) = $metadata =~ /^wal_other_db_records=([0-9]+)$/m;
my ($wal_mixed_records) = $metadata =~ /^wal_mixed_records=([0-9]+)$/m;
my ($wal_global_records) = $metadata =~ /^wal_global_records=([0-9]+)$/m;
my ($wal_source_fpi_blocks) = $metadata =~ /^wal_source_fpi_blocks=([0-9]+)$/m;
my ($wal_source_non_fpi_records) = $metadata =~ /^wal_source_non_fpi_records=([0-9]+)$/m;
my ($wal_replayed_records) = $metadata =~ /^wal_replayed_records=([0-9]+)$/m;
ok(
	$wal_records_scanned >= $wal_source_records,
	'metadata WAL scan count covers source WAL count');
ok($wal_source_records > 0, 'metadata records source WAL after last checkpoint');
ok($wal_replayed_records > 0, 'metadata records replayed source WAL');
ok($wal_replayed_records <= $wal_source_records, 'metadata replayed WAL count is bounded by source WAL count');
ok($wal_source_fpi_blocks > 0, 'metadata records restored source full-page images');
ok($wal_mixed_records <= $wal_source_records, 'metadata mixed WAL count is bounded by source WAL count');
ok(
	$wal_records_scanned >= $wal_source_records + $wal_other_db_records + $wal_global_records,
	'metadata WAL scan count covers classified WAL counts');
like($metadata, qr/^clone_path=base\/[0-9]+$/m, 'metadata records branch storage path');
like($metadata, qr/^wal_pin=released$/m, 'metadata records released WAL pin');
like($metadata, qr/^replay_method=rmgr_redo$/m, 'metadata records rmgr redo replay method');

my ($clone_path) = $metadata =~ /^clone_path=(.+)$/m;
if ($result == 0)
{
	like($metadata, qr/^clone_result=(done|copy_fallback)$/m, 'metadata records storage clone success');
	like($metadata, qr/^cleanup=not_needed$/m, 'metadata records no failed clone cleanup needed');
	like($metadata, qr/^status_history=CREATING,COPYING,REPLAYING,READY$/m, 'metadata records READY transition');
	like($metadata, qr/^status=READY$/m, 'metadata final state is READY');
	like($metadata, qr/^failure=$/m, 'metadata records no failure');
	ok(-d $node->data_dir . '/' . $clone_path, 'branch storage path is installed');

	my $branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_target' AND datallowconn;]);
	is($branch_count, '1', 'ready branch is connectable');

	my $branch_oid = $node->safe_psql(
		'postgres',
		q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_target';]);
	is($clone_path, 'base/' . $branch_oid, 'branch storage path uses branch database OID');
	ok(!-e $node->data_dir . '/' . $clone_path . '/' . $source_temp_file,
		'db branch clone skips live source temp relation files');
	ok(!-e $node->data_dir . '/' . $clone_path . '/pg_internal.init',
		'db branch clone skips source relcache init file');

	my $branch_rows = $node->safe_psql(
		'dbbranch_target',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is($branch_rows, '1:alice,2:bob,3:carol', 'ready branch can read cloned source rows');

	my $source_locator = $node->safe_psql(
		'postgres',
		q[SELECT dattablespace || '/' || oid FROM pg_database WHERE datname = 'dbbranch_source';]);
	my $branch_locator = $node->safe_psql(
		'postgres',
		q[SELECT dattablespace || '/' || oid FROM pg_database WHERE datname = 'dbbranch_target';]);
	my $wal_end = $node->safe_psql('postgres', 'SELECT pg_current_wal_lsn();');
	my $waldump = '';
	my $catalog_state = $node->safe_psql(
		'postgres',
		q[SELECT status || '|' || replay_method || '|' || source_db_oid || '|' || branch_db_oid || '|' || (redo_ptr IS NOT NULL) || '|' || (branch_lsn IS NOT NULL) || '|' || (redo_ptr <= branch_lsn) || '|' || (wal_records_scanned >= 0) || '|' || (wal_source_records >= 0) || '|' || (wal_records_scanned >= wal_source_records) || '|' || (source_blocking_ms >= 0) || '|' || (clone_elapsed_ms >= 0) || '|' || (replay_elapsed_ms >= 0) || '|' || failure FROM pg_dbbranch WHERE branch_db_oid = (SELECT oid FROM pg_database WHERE datname = 'dbbranch_target');]);
	is(
		$catalog_state,
		'READY|rmgr_redo|' . $source_oid . '|' . $branch_oid . '|true|true|true|true|true|true|true|true|true|',
		'pg_dbbranch records READY metadata with replay method, WAL scan counts, and timings');

	my $catalog_clone_result = $node->safe_psql(
		'postgres',
		q[SELECT clone_result IN ('done', 'copy_fallback') FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_clone_result,
		't',
		'pg_dbbranch records storage clone result');

	my $catalog_ready_times = $node->safe_psql(
		'postgres',
		q[SELECT created_at IS NOT NULL AND ready_at IS NOT NULL AND created_at <= ready_at FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_ready_times,
		't',
		'pg_dbbranch records creation and ready timestamps');

	my $catalog_wal_range = $node->safe_psql(
		'postgres',
		q[SELECT wal_range_bytes >= 0 AND wal_range_bytes::numeric = branch_lsn - redo_ptr FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_wal_range,
		't',
		'pg_dbbranch records WAL range bytes');

	my $catalog_wal_replay = $node->safe_psql(
		'postgres',
		q[SELECT wal_replayed_records > 0 AND wal_replayed_records <= wal_source_records FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_wal_replay,
		't',
		'pg_dbbranch records replayed WAL count');

	my $catalog_wal_classification = $node->safe_psql(
		'postgres',
		q[SELECT (wal_other_db_records >= 0) || '|' || (wal_mixed_records >= 0) || '|' || (wal_global_records >= 0) || '|' || (wal_source_fpi_blocks >= 0) || '|' || (wal_source_non_fpi_records >= 0) || '|' || (wal_mixed_records <= wal_source_records) || '|' || (wal_records_scanned >= wal_source_records + wal_other_db_records + wal_global_records) FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_wal_classification,
		'true|true|true|true|true|true|true',
		'pg_dbbranch exposes WAL classification counts');

	ok(
		PostgreSQL::Test::Utils::run_log(
			[ 'pg_waldump', '-p', $node->data_dir, '-r', 'Database', '-s', $wal_start, '-e', $wal_end ],
			'>' => \$waldump),
		'db branch WAL can be dumped');
	like(
		$waldump,
		qr/CREATE_FILE_COPY.*copy dir \Q$source_locator\E to \Q$branch_locator\E/,
		'db branch creation records database file-copy WAL');

	$node->restart;
	my $branch_rows_after_restart = $node->safe_psql(
		'dbbranch_target',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is($branch_rows_after_restart, '1:alice,2:bob,3:carol', 'ready branch survives restart');
	my $catalog_state_after_restart = $node->safe_psql(
		'postgres',
		q[SELECT status || '|' || replay_method || '|' || source_db_oid || '|' || branch_db_oid || '|' || (redo_ptr IS NOT NULL) || '|' || (branch_lsn IS NOT NULL) || '|' || (redo_ptr <= branch_lsn) || '|' || (wal_records_scanned >= 0) || '|' || (wal_source_records >= 0) || '|' || (wal_records_scanned >= wal_source_records) || '|' || (source_blocking_ms >= 0) || '|' || (clone_elapsed_ms >= 0) || '|' || (replay_elapsed_ms >= 0) || '|' || failure FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is(
		$catalog_state_after_restart,
		'READY|rmgr_redo|' . $source_oid . '|' . $branch_oid . '|true|true|true|true|true|true|true|true|true|',
		'pg_dbbranch READY metadata with WAL scan counts and timings survives restart');

	$node->safe_psql('dbbranch_target', q[INSERT INTO users VALUES (4, 'dora');]);
	my $source_after_branch_write = $node->safe_psql(
		'dbbranch_source',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is($source_after_branch_write, '1:alice,2:bob,3:carol', 'branch writes do not affect source');

	$node->safe_psql('dbbranch_source', q[INSERT INTO users VALUES (6, 'frank');]);
	my $branch_after_source_write = $node->safe_psql(
		'dbbranch_target',
		q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM users;]);
	is(
		$branch_after_source_write,
		'1:alice,2:bob,3:carol,4:dora',
		'source writes after branch creation do not affect branch');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_target;]);
	my $catalog_after_drop = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_dbbranch WHERE branch_db_oid = ]
		  . $branch_oid
		  . q[ OR source_db_oid = ]
		  . $branch_oid
		  . q[;]);
	is($catalog_after_drop, '0', 'dropping branch removes pg_dbbranch metadata');
	ok(!-e $node->data_dir . '/' . $clone_path, 'dropping branch removes branch storage path');
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
	ok(!-e $node->data_dir . '/' . $clone_path, 'failed branch cleanup removes branch storage path');

	my $branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_target';]);
	is($branch_count, '0', 'failed branch is not connectable');
}

$source_temp->quit;

my $slot_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%']);
is($slot_count, '0', 'db branch releases WAL pin slot');

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_trigger_drain_source;]);
$node->safe_psql(
	'dbbranch_trigger_drain_source',
	q[
CREATE TABLE trigger_rows (id int PRIMARY KEY, note text);
CREATE FUNCTION trigger_touch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	NEW.note := coalesce(NEW.note, 'seen');
	RETURN NEW;
END
$$;
INSERT INTO trigger_rows VALUES (1, 'old');
CHECKPOINT;
]);

my $trigger_locker = $node->background_psql('dbbranch_trigger_drain_source', on_error_stop => 1);
$trigger_locker->query_safe(q[BEGIN; LOCK TABLE trigger_rows IN ROW EXCLUSIVE MODE;]);
my $trigger_writer = $node->background_psql('dbbranch_trigger_drain_source', on_error_stop => 1);
$trigger_writer->query_until(
	qr/start_trigger_drain_trigger/,
	q(\echo start_trigger_drain_trigger
CREATE TRIGGER trigger_rows_bi BEFORE INSERT ON trigger_rows FOR EACH ROW EXECUTE FUNCTION trigger_touch();
\echo finish_trigger_drain_trigger
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_trigger_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE TRIGGER trigger_rows_bi%';
]), 'active source create trigger waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_trigger_drain_target FROM DATABASE dbbranch_trigger_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create trigger');
like($stderr, qr/source database "dbbranch_trigger_drain_source" has active write transactions/,
	'active source create trigger holds db branch writer gate');

$trigger_locker->query_safe(q[COMMIT;]);
$trigger_locker->quit;
$trigger_writer->query_until(qr/finish_trigger_drain_trigger/, '');
$trigger_writer->quit;

my $trigger_exists = $node->safe_psql(
	'dbbranch_trigger_drain_source',
	q[SELECT count(*) FROM pg_trigger WHERE tgname = 'trigger_rows_bi';]);
is($trigger_exists, '1', 'source create trigger finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_trigger_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_language_drain_source;]);
$node->safe_psql('dbbranch_language_drain_source', q[CHECKPOINT;]);

my $language_writer = $node->background_psql('dbbranch_language_drain_source', on_error_stop => 1);
my $language_locker = $node->background_psql('dbbranch_language_drain_source', on_error_stop => 1);
$language_locker->query_safe(q[BEGIN; LOCK TABLE pg_language IN ACCESS EXCLUSIVE MODE;]);
$language_writer->query_until(
	qr/start_language_drain_language/,
	q(\echo start_language_drain_language
CREATE LANGUAGE dbbranch_lang HANDLER plpgsql_call_handler;
\echo finish_language_drain_language
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_language_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE LANGUAGE dbbranch_lang%';
]), 'active source create language waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_language_drain_target FROM DATABASE dbbranch_language_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create language');
like($stderr, qr/source database "dbbranch_language_drain_source" has active write transactions/,
	'active source create language holds db branch writer gate');

$language_locker->query_safe(q[COMMIT;]);
$language_locker->quit;
$language_writer->query_until(qr/finish_language_drain_language/, '');
$language_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_language_drain_target FROM DATABASE dbbranch_language_drain_source]);
my $language_exists = $node->safe_psql(
	'dbbranch_language_drain_target',
	q[SELECT count(*) FROM pg_language WHERE lanname = 'dbbranch_lang';]);
is($language_exists, '1', 'branch succeeds after source create language drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_language_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_language_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_am_drain_source;]);
$node->safe_psql('dbbranch_am_drain_source', q[CHECKPOINT;]);

my $am_writer = $node->background_psql('dbbranch_am_drain_source', on_error_stop => 1);
my $am_locker = $node->background_psql('dbbranch_am_drain_source', on_error_stop => 1);
$am_locker->query_safe(q[BEGIN; LOCK TABLE pg_am IN ACCESS EXCLUSIVE MODE;]);
$am_writer->query_until(
	qr/start_am_drain_am/,
	q(\echo start_am_drain_am
CREATE ACCESS METHOD dbbranch_am TYPE INDEX HANDLER bthandler;
\echo finish_am_drain_am
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_am_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE ACCESS METHOD dbbranch_am%';
]), 'active source create access method waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_am_drain_target FROM DATABASE dbbranch_am_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create access method');
like($stderr, qr/source database "dbbranch_am_drain_source" has active write transactions/,
	'active source create access method holds db branch writer gate');

$am_locker->query_safe(q[COMMIT;]);
$am_locker->quit;
$am_writer->query_until(qr/finish_am_drain_am/, '');
$am_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_am_drain_target FROM DATABASE dbbranch_am_drain_source]);
my $access_method_exists = $node->safe_psql(
	'dbbranch_am_drain_target',
	q[SELECT count(*) FROM pg_am WHERE amname = 'dbbranch_am';]);
is($access_method_exists, '1', 'branch succeeds after source create access method drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_am_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_am_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_transform_drain_source;]);
$node->safe_psql('dbbranch_transform_drain_source', q[CHECKPOINT;]);

my $transform_writer = $node->background_psql('dbbranch_transform_drain_source', on_error_stop => 1);
my $transform_locker = $node->background_psql('dbbranch_transform_drain_source', on_error_stop => 1);
$transform_locker->query_safe(q[BEGIN; LOCK TABLE pg_transform IN ACCESS EXCLUSIVE MODE;]);
$transform_writer->query_until(
	qr/start_transform_drain_transform/,
	q(\echo start_transform_drain_transform
CREATE TRANSFORM FOR int LANGUAGE SQL (
    FROM SQL WITH FUNCTION prsd_lextype(internal),
    TO SQL WITH FUNCTION int4recv(internal));
\echo finish_transform_drain_transform
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_transform_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE TRANSFORM FOR int LANGUAGE SQL%';
]), 'active source create transform waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_transform_drain_target FROM DATABASE dbbranch_transform_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create transform');
like($stderr, qr/source database "dbbranch_transform_drain_source" has active write transactions/,
	'active source create transform holds db branch writer gate');

$transform_locker->query_safe(q[COMMIT;]);
$transform_locker->quit;
$transform_writer->query_until(qr/finish_transform_drain_transform/, '');
$transform_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_transform_drain_target FROM DATABASE dbbranch_transform_drain_source]);
my $transform_exists = $node->safe_psql(
	'dbbranch_transform_drain_target',
	q[
SELECT count(*)
FROM pg_transform tr
JOIN pg_type ty ON ty.oid = tr.trftype
JOIN pg_language la ON la.oid = tr.trflang
WHERE ty.typname = 'int4'
  AND la.lanname = 'sql';
]);
is($transform_exists, '1', 'branch succeeds after source create transform drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_transform_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_transform_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_rule_drain_source;]);
$node->safe_psql(
	'dbbranch_rule_drain_source',
	q[
CREATE TABLE rule_rows (id int PRIMARY KEY);
INSERT INTO rule_rows VALUES (1);
CHECKPOINT;
]);

my $rule_locker = $node->background_psql('dbbranch_rule_drain_source', on_error_stop => 1);
$rule_locker->query_safe(q[BEGIN; LOCK TABLE rule_rows IN ACCESS SHARE MODE;]);
my $rule_writer = $node->background_psql('dbbranch_rule_drain_source', on_error_stop => 1);
$rule_writer->query_until(
	qr/start_rule_drain_rule/,
	q(\echo start_rule_drain_rule
CREATE RULE rule_rows_insert_ignore AS ON INSERT TO rule_rows DO INSTEAD NOTHING;
\echo finish_rule_drain_rule
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_rule_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE RULE rule_rows_insert_ignore%';
]), 'active source create rule waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_rule_drain_target FROM DATABASE dbbranch_rule_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create rule');
like($stderr, qr/source database "dbbranch_rule_drain_source" has active write transactions/,
	'active source create rule holds db branch writer gate');

$rule_locker->query_safe(q[COMMIT;]);
$rule_locker->quit;
$rule_writer->query_until(qr/finish_rule_drain_rule/, '');
$rule_writer->quit;

my $rule_exists = $node->safe_psql(
	'dbbranch_rule_drain_source',
	q[SELECT count(*) FROM pg_rewrite WHERE rulename = 'rule_rows_insert_ignore';]);
is($rule_exists, '1', 'source create rule finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_rule_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_view_drain_source;]);
$node->safe_psql(
	'dbbranch_view_drain_source',
	q[
CREATE TABLE view_rows (id int PRIMARY KEY);
INSERT INTO view_rows VALUES (1);
CHECKPOINT;
]);

my $view_locker = $node->background_psql('dbbranch_view_drain_source', on_error_stop => 1);
$view_locker->query_safe(q[BEGIN; LOCK TABLE view_rows IN ACCESS EXCLUSIVE MODE;]);
my $view_writer = $node->background_psql('dbbranch_view_drain_source', on_error_stop => 1);
$view_writer->query_until(
	qr/start_view_drain_view/,
	q(\echo start_view_drain_view
CREATE VIEW view_rows_v AS SELECT id FROM view_rows;
\echo finish_view_drain_view
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_view_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE VIEW view_rows_v%';
]), 'active source create view waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_view_drain_target FROM DATABASE dbbranch_view_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create view');
like($stderr, qr/source database "dbbranch_view_drain_source" has active write transactions/,
	'active source create view holds db branch writer gate');

$view_locker->query_safe(q[COMMIT;]);
$view_locker->quit;
$view_writer->query_until(qr/finish_view_drain_view/, '');
$view_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_view_drain_target FROM DATABASE dbbranch_view_drain_source]);
my $view_rows = $node->safe_psql(
	'dbbranch_view_drain_target',
	q[SELECT count(*) FROM view_rows_v;]);
is($view_rows, '1', 'branch succeeds after source create view drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_view_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_view_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_function_drain_source;]);
$node->safe_psql(
	'dbbranch_function_drain_source',
	q[
CREATE TABLE function_rows (id int PRIMARY KEY);
INSERT INTO function_rows VALUES (1);
CHECKPOINT;
]);

my $function_locker = $node->background_psql('dbbranch_function_drain_source', on_error_stop => 1);
$function_locker->query_safe(q[BEGIN; LOCK TABLE function_rows IN ACCESS EXCLUSIVE MODE;]);
my $function_writer = $node->background_psql('dbbranch_function_drain_source', on_error_stop => 1);
$function_writer->query_until(
	qr/start_function_drain_function/,
	q(\echo start_function_drain_function
CREATE FUNCTION function_rows_count() RETURNS int LANGUAGE SQL RETURN (SELECT count(*)::int FROM function_rows);
\echo finish_function_drain_function
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_function_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE FUNCTION function_rows_count%';
]), 'active source create function waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_function_drain_target FROM DATABASE dbbranch_function_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create function');
like($stderr, qr/source database "dbbranch_function_drain_source" has active write transactions/,
	'active source create function holds db branch writer gate');

$function_locker->query_safe(q[COMMIT;]);
$function_locker->quit;
$function_writer->query_until(qr/finish_function_drain_function/, '');
$function_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_function_drain_target FROM DATABASE dbbranch_function_drain_source]);
my $function_rows = $node->safe_psql(
	'dbbranch_function_drain_target',
	q[SELECT function_rows_count();]);
is($function_rows, '1', 'branch succeeds after source create function drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_function_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_function_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_function_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_function_drain_source',
	q[
CREATE FUNCTION alter_function_rows_count() RETURNS int LANGUAGE SQL RETURN 1;
CHECKPOINT;
]);

my $alter_function_locker = $node->background_psql('dbbranch_alter_function_drain_source', on_error_stop => 1);
$alter_function_locker->query_safe(q[BEGIN; LOCK TABLE pg_proc IN ACCESS EXCLUSIVE MODE;]);
my $alter_function_writer = $node->background_psql('dbbranch_alter_function_drain_source', on_error_stop => 1);
$alter_function_writer->query_until(
	qr/start_alter_function_drain_function/,
	q(\echo start_alter_function_drain_function
ALTER FUNCTION alter_function_rows_count() COST 2;
\echo finish_alter_function_drain_function
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_function_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER FUNCTION alter_function_rows_count%';
]), 'active source alter function waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_function_drain_target FROM DATABASE dbbranch_alter_function_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter function');
like($stderr, qr/source database "dbbranch_alter_function_drain_source" has active write transactions/,
	'active source alter function holds db branch writer gate');

$alter_function_locker->query_safe(q[COMMIT;]);
$alter_function_locker->quit;
$alter_function_writer->query_until(qr/finish_alter_function_drain_function/, '');
$alter_function_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_function_drain_target FROM DATABASE dbbranch_alter_function_drain_source]);
my $function_cost = $node->safe_psql(
	'dbbranch_alter_function_drain_target',
	q[SELECT procost::int FROM pg_proc WHERE proname = 'alter_function_rows_count';]);
is($function_cost, '2', 'branch succeeds after source alter function drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_function_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_function_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_domain_drain_source;]);
$node->safe_psql('dbbranch_domain_drain_source', q[CHECKPOINT;]);

my $domain_locker = $node->background_psql('dbbranch_domain_drain_source', on_error_stop => 1);
$domain_locker->query_safe(q[BEGIN; LOCK TABLE pg_type IN ACCESS EXCLUSIVE MODE;]);
my $domain_writer = $node->background_psql('dbbranch_domain_drain_source', on_error_stop => 1);
$domain_writer->query_until(
	qr/start_domain_drain_domain/,
	q(\echo start_domain_drain_domain
CREATE DOMAIN positive_int AS int CHECK (VALUE > 0);
\echo finish_domain_drain_domain
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_domain_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE DOMAIN positive_int%';
]), 'active source create domain waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_domain_drain_target FROM DATABASE dbbranch_domain_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create domain');
like($stderr, qr/source database "dbbranch_domain_drain_source" has active write transactions/,
	'active source create domain holds db branch writer gate');

$domain_locker->query_safe(q[COMMIT;]);
$domain_locker->quit;
$domain_writer->query_until(qr/finish_domain_drain_domain/, '');
$domain_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_domain_drain_target FROM DATABASE dbbranch_domain_drain_source]);
my $domain_exists = $node->safe_psql(
	'dbbranch_domain_drain_target',
	q[SELECT count(*) FROM pg_type WHERE typname = 'positive_int';]);
is($domain_exists, '1', 'branch succeeds after source create domain drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_domain_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_domain_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_domain_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_domain_drain_source',
	q[
CREATE DOMAIN dbbranch_domain AS int;
CHECKPOINT;
]);

my $alter_domain_writer = $node->background_psql('dbbranch_alter_domain_drain_source', on_error_stop => 1);
my $alter_domain_locker = $node->background_psql('dbbranch_alter_domain_drain_source', on_error_stop => 1);
$alter_domain_locker->query_safe(q[BEGIN; LOCK TABLE pg_type IN ACCESS EXCLUSIVE MODE;]);
$alter_domain_writer->query_until(
	qr/start_alter_domain_drain_domain/,
	q(\echo start_alter_domain_drain_domain
ALTER DOMAIN dbbranch_domain SET DEFAULT 7;
\echo finish_alter_domain_drain_domain
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_domain_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER DOMAIN dbbranch_domain%';
]), 'active source alter domain waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_domain_drain_target FROM DATABASE dbbranch_alter_domain_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter domain');
like($stderr, qr/source database "dbbranch_alter_domain_drain_source" has active write transactions/,
	'active source alter domain holds db branch writer gate');

$alter_domain_locker->query_safe(q[COMMIT;]);
$alter_domain_locker->quit;
$alter_domain_writer->query_until(qr/finish_alter_domain_drain_domain/, '');
$alter_domain_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_domain_drain_target FROM DATABASE dbbranch_alter_domain_drain_source]);
my $domain_default = $node->safe_psql(
	'dbbranch_alter_domain_drain_target',
	q[SELECT typdefault FROM pg_type WHERE typname = 'dbbranch_domain';]);
is($domain_default, '7', 'branch succeeds after source alter domain drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_domain_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_domain_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_enum_drain_source;]);
$node->safe_psql('dbbranch_enum_drain_source', q[CHECKPOINT;]);

my $enum_locker = $node->background_psql('dbbranch_enum_drain_source', on_error_stop => 1);
$enum_locker->query_safe(q[BEGIN; LOCK TABLE pg_type IN ACCESS EXCLUSIVE MODE;]);
my $enum_writer = $node->background_psql('dbbranch_enum_drain_source', on_error_stop => 1);
$enum_writer->query_until(
	qr/start_enum_drain_enum/,
	q(\echo start_enum_drain_enum
CREATE TYPE dbbranch_enum AS ENUM ('ready', 'failed');
\echo finish_enum_drain_enum
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_enum_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE TYPE dbbranch_enum%';
]), 'active source create enum waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_enum_drain_target FROM DATABASE dbbranch_enum_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create enum');
like($stderr, qr/source database "dbbranch_enum_drain_source" has active write transactions/,
	'active source create enum holds db branch writer gate');

$enum_locker->query_safe(q[COMMIT;]);
$enum_locker->quit;
$enum_writer->query_until(qr/finish_enum_drain_enum/, '');
$enum_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_enum_drain_target FROM DATABASE dbbranch_enum_drain_source]);
my $enum_label_count = $node->safe_psql(
	'dbbranch_enum_drain_target',
	q[
SELECT count(*)
FROM pg_type t
JOIN pg_enum e ON e.enumtypid = t.oid
WHERE t.typname = 'dbbranch_enum';
]);
is($enum_label_count, '2', 'branch succeeds after source create enum drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_enum_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_enum_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_composite_type_drain_source;]);
$node->safe_psql('dbbranch_composite_type_drain_source', q[CHECKPOINT;]);

my $composite_type_locker = $node->background_psql('dbbranch_composite_type_drain_source', on_error_stop => 1);
$composite_type_locker->query_safe(q[BEGIN; LOCK TABLE pg_type IN ACCESS EXCLUSIVE MODE;]);
my $composite_type_writer = $node->background_psql('dbbranch_composite_type_drain_source', on_error_stop => 1);
$composite_type_writer->query_until(
	qr/start_composite_type_drain_type/,
	q(\echo start_composite_type_drain_type
CREATE TYPE dbbranch_composite_type AS (id int, label text);
\echo finish_composite_type_drain_type
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_composite_type_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE TYPE dbbranch_composite_type%';
]), 'active source create composite type waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_composite_type_drain_target FROM DATABASE dbbranch_composite_type_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create composite type');
like($stderr, qr/source database "dbbranch_composite_type_drain_source" has active write transactions/,
	'active source create composite type holds db branch writer gate');

$composite_type_locker->query_safe(q[COMMIT;]);
$composite_type_locker->quit;
$composite_type_writer->query_until(qr/finish_composite_type_drain_type/, '');
$composite_type_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_composite_type_drain_target FROM DATABASE dbbranch_composite_type_drain_source]);
my $composite_type_exists = $node->safe_psql(
	'dbbranch_composite_type_drain_target',
	q[SELECT count(*) FROM pg_type WHERE typname = 'dbbranch_composite_type';]);
is($composite_type_exists, '1', 'branch succeeds after source create composite type drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_composite_type_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_composite_type_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_range_type_drain_source;]);
$node->safe_psql('dbbranch_range_type_drain_source', q[CHECKPOINT;]);

my $range_type_locker = $node->background_psql('dbbranch_range_type_drain_source', on_error_stop => 1);
$range_type_locker->query_safe(q[BEGIN; LOCK TABLE pg_type IN ACCESS EXCLUSIVE MODE;]);
my $range_type_writer = $node->background_psql('dbbranch_range_type_drain_source', on_error_stop => 1);
$range_type_writer->query_until(
	qr/start_range_type_drain_type/,
	q(\echo start_range_type_drain_type
CREATE TYPE dbbranch_int4_range AS RANGE (subtype = int4);
\echo finish_range_type_drain_type
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_range_type_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE TYPE dbbranch_int4_range%';
]), 'active source create range type waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_range_type_drain_target FROM DATABASE dbbranch_range_type_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create range type');
like($stderr, qr/source database "dbbranch_range_type_drain_source" has active write transactions/,
	'active source create range type holds db branch writer gate');

$range_type_locker->query_safe(q[COMMIT;]);
$range_type_locker->quit;
$range_type_writer->query_until(qr/finish_range_type_drain_type/, '');
$range_type_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_range_type_drain_target FROM DATABASE dbbranch_range_type_drain_source]);
my $range_type_exists = $node->safe_psql(
	'dbbranch_range_type_drain_target',
	q[
SELECT count(*)
FROM pg_range r
JOIN pg_type t ON t.oid = r.rngtypid
WHERE t.typname = 'dbbranch_int4_range';
]);
is($range_type_exists, '1', 'branch succeeds after source create range type drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_range_type_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_range_type_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_type_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_type_drain_source',
	q[
CREATE TYPE dbbranch_varchar_type;
CREATE FUNCTION dbbranch_varchar_in(cstring, oid, integer) RETURNS dbbranch_varchar_type
LANGUAGE internal IMMUTABLE PARALLEL SAFE STRICT AS 'varcharin';
CREATE FUNCTION dbbranch_varchar_out(dbbranch_varchar_type) RETURNS cstring
LANGUAGE internal IMMUTABLE PARALLEL SAFE STRICT AS 'varcharout';
CREATE TYPE dbbranch_varchar_type (
	input = dbbranch_varchar_in,
	output = dbbranch_varchar_out,
	alignment = integer,
	storage = main
);
CHECKPOINT;
]);

my $alter_type_locker = $node->background_psql('dbbranch_alter_type_drain_source', on_error_stop => 1);
$alter_type_locker->query_safe(q[BEGIN; LOCK TABLE pg_type IN ACCESS EXCLUSIVE MODE;]);
my $alter_type_writer = $node->background_psql('dbbranch_alter_type_drain_source', on_error_stop => 1);
$alter_type_writer->query_until(
	qr/start_alter_type_drain_type/,
	q(\echo start_alter_type_drain_type
ALTER TYPE dbbranch_varchar_type SET (storage = extended);
\echo finish_alter_type_drain_type
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_type_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER TYPE dbbranch_varchar_type%';
]), 'active source alter type waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_type_drain_target FROM DATABASE dbbranch_alter_type_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter type');
like($stderr, qr/source database "dbbranch_alter_type_drain_source" has active write transactions/,
	'active source alter type holds db branch writer gate');

$alter_type_locker->query_safe(q[COMMIT;]);
$alter_type_locker->quit;
$alter_type_writer->query_until(qr/finish_alter_type_drain_type/, '');
$alter_type_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_type_drain_target FROM DATABASE dbbranch_alter_type_drain_source]);
my $altered_type_storage = $node->safe_psql(
	'dbbranch_alter_type_drain_target',
	q[SELECT typstorage FROM pg_type WHERE typname = 'dbbranch_varchar_type';]);
is($altered_type_storage, 'x', 'branch succeeds after source alter type drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_type_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_type_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_enum_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_enum_drain_source',
	q[
CREATE TYPE dbbranch_alter_enum AS ENUM ('ready', 'failed');
CHECKPOINT;
]);

my $alter_enum_locker = $node->background_psql('dbbranch_alter_enum_drain_source', on_error_stop => 1);
$alter_enum_locker->query_safe(q[BEGIN; LOCK TABLE pg_enum IN ACCESS EXCLUSIVE MODE;]);
my $alter_enum_writer = $node->background_psql('dbbranch_alter_enum_drain_source', on_error_stop => 1);
$alter_enum_writer->query_until(
	qr/start_alter_enum_drain_enum/,
	q(\echo start_alter_enum_drain_enum
ALTER TYPE dbbranch_alter_enum ADD VALUE 'replayed';
\echo finish_alter_enum_drain_enum
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_enum_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER TYPE dbbranch_alter_enum%';
]), 'active source alter enum waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_enum_drain_target FROM DATABASE dbbranch_alter_enum_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter enum');
like($stderr, qr/source database "dbbranch_alter_enum_drain_source" has active write transactions/,
	'active source alter enum holds db branch writer gate');

$alter_enum_locker->query_safe(q[COMMIT;]);
$alter_enum_locker->quit;
$alter_enum_writer->query_until(qr/finish_alter_enum_drain_enum/, '');
$alter_enum_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_enum_drain_target FROM DATABASE dbbranch_alter_enum_drain_source]);
my $altered_enum_label_count = $node->safe_psql(
	'dbbranch_alter_enum_drain_target',
	q[
SELECT count(*)
FROM pg_type t
JOIN pg_enum e ON e.enumtypid = t.oid
WHERE t.typname = 'dbbranch_alter_enum';
]);
is($altered_enum_label_count, '3', 'branch succeeds after source alter enum drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_enum_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_enum_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_define_drain_source;]);
$node->safe_psql('dbbranch_define_drain_source', q[CHECKPOINT;]);

my $define_locker = $node->background_psql('dbbranch_define_drain_source', on_error_stop => 1);
$define_locker->query_safe(q[BEGIN; LOCK TABLE pg_collation IN ACCESS EXCLUSIVE MODE;]);
my $define_writer = $node->background_psql('dbbranch_define_drain_source', on_error_stop => 1);
$define_writer->query_until(
	qr/start_define_drain_collation/,
	q(\echo start_define_drain_collation
CREATE COLLATION dbbranch_collation FROM "C";
\echo finish_define_drain_collation
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_define_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE COLLATION dbbranch_collation%';
]), 'active source create collation waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_define_drain_target FROM DATABASE dbbranch_define_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create collation');
like($stderr, qr/source database "dbbranch_define_drain_source" has active write transactions/,
	'active source create collation holds db branch writer gate');

$define_locker->query_safe(q[COMMIT;]);
$define_locker->quit;
$define_writer->query_until(qr/finish_define_drain_collation/, '');
$define_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_define_drain_target FROM DATABASE dbbranch_define_drain_source]);
my $collation_exists = $node->safe_psql(
	'dbbranch_define_drain_target',
	q[SELECT count(*) FROM pg_collation WHERE collname = 'dbbranch_collation';]);
is($collation_exists, '1', 'branch succeeds after source create collation drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_define_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_define_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_collation_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_collation_drain_source',
	q[
CREATE COLLATION dbbranch_alter_collation FROM "C";
CHECKPOINT;
]);

my $alter_collation_locker = $node->background_psql('dbbranch_alter_collation_drain_source', on_error_stop => 1);
$alter_collation_locker->query_safe(q[BEGIN; LOCK TABLE pg_collation IN ACCESS EXCLUSIVE MODE;]);
my $alter_collation_writer = $node->background_psql('dbbranch_alter_collation_drain_source', on_error_stop => 1);
$alter_collation_writer->query_until(
	qr/start_alter_collation_drain_collation/,
	q(\echo start_alter_collation_drain_collation
ALTER COLLATION dbbranch_alter_collation REFRESH VERSION;
\echo finish_alter_collation_drain_collation
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_collation_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER COLLATION dbbranch_alter_collation%';
]), 'active source alter collation waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_collation_drain_target FROM DATABASE dbbranch_alter_collation_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter collation');
like($stderr, qr/source database "dbbranch_alter_collation_drain_source" has active write transactions/,
	'active source alter collation holds db branch writer gate');

$alter_collation_locker->query_safe(q[COMMIT;]);
$alter_collation_locker->quit;
$alter_collation_writer->query_until(qr/finish_alter_collation_drain_collation/, '');
$alter_collation_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_collation_drain_target FROM DATABASE dbbranch_alter_collation_drain_source]);
my $altered_collation_exists = $node->safe_psql(
	'dbbranch_alter_collation_drain_target',
	q[SELECT count(*) FROM pg_collation WHERE collname = 'dbbranch_alter_collation';]);
is($altered_collation_exists, '1', 'branch succeeds after source alter collation drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_collation_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_collation_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_create_sequence_drain_source;]);
$node->safe_psql('dbbranch_create_sequence_drain_source', q[CHECKPOINT;]);

my $create_sequence_locker = $node->background_psql('dbbranch_create_sequence_drain_source', on_error_stop => 1);
$create_sequence_locker->query_safe(q[BEGIN; LOCK TABLE pg_sequence IN ACCESS EXCLUSIVE MODE;]);
my $create_sequence_writer = $node->background_psql('dbbranch_create_sequence_drain_source', on_error_stop => 1);
$create_sequence_writer->query_until(
	qr/start_create_sequence_drain_sequence/,
	q(\echo start_create_sequence_drain_sequence
CREATE SEQUENCE dbbranch_created_sequence CACHE 1;
\echo finish_create_sequence_drain_sequence
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_create_sequence_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE SEQUENCE dbbranch_created_sequence%';
]), 'active source create sequence waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_create_sequence_drain_target FROM DATABASE dbbranch_create_sequence_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create sequence');
like($stderr, qr/source database "dbbranch_create_sequence_drain_source" has active write transactions/,
	'active source create sequence holds db branch writer gate');

$create_sequence_locker->query_safe(q[COMMIT;]);
$create_sequence_locker->quit;
$create_sequence_writer->query_until(qr/finish_create_sequence_drain_sequence/, '');
$create_sequence_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_create_sequence_drain_target FROM DATABASE dbbranch_create_sequence_drain_source]);
my $created_sequence_exists = $node->safe_psql(
	'dbbranch_create_sequence_drain_target',
	q[SELECT count(*) FROM pg_class WHERE relkind = 'S' AND relname = 'dbbranch_created_sequence';]);
is($created_sequence_exists, '1', 'branch succeeds after source create sequence drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_sequence_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_sequence_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_sequence_drain_source;]);
$node->safe_psql(
	'dbbranch_sequence_drain_source',
	q[
CREATE SEQUENCE dbbranch_sequence CACHE 1;
CHECKPOINT;
]);

my $sequence_locker = $node->background_psql('dbbranch_sequence_drain_source', on_error_stop => 1);
$sequence_locker->query_safe(q[BEGIN; LOCK TABLE pg_sequence IN ACCESS EXCLUSIVE MODE;]);
my $sequence_writer = $node->background_psql('dbbranch_sequence_drain_source', on_error_stop => 1);
$sequence_writer->query_until(
	qr/start_sequence_drain_alter/,
	q(\echo start_sequence_drain_alter
ALTER SEQUENCE dbbranch_sequence INCREMENT BY 2;
\echo finish_sequence_drain_alter
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_sequence_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER SEQUENCE dbbranch_sequence%';
]), 'active source alter sequence waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_sequence_drain_target FROM DATABASE dbbranch_sequence_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter sequence');
like($stderr, qr/source database "dbbranch_sequence_drain_source" has active write transactions/,
	'active source alter sequence holds db branch writer gate');

$sequence_locker->query_safe(q[COMMIT;]);
$sequence_locker->quit;
$sequence_writer->query_until(qr/finish_sequence_drain_alter/, '');
$sequence_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_sequence_drain_target FROM DATABASE dbbranch_sequence_drain_source]);
my $sequence_increment = $node->safe_psql(
	'dbbranch_sequence_drain_target',
	q[SELECT seqincrement FROM pg_sequence s JOIN pg_class c ON s.seqrelid = c.oid WHERE c.relname = 'dbbranch_sequence';]);
is($sequence_increment, '2', 'branch succeeds after source alter sequence drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_sequence_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_sequence_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_ctas_drain_source;]);
$node->safe_psql(
	'dbbranch_ctas_drain_source',
	q[
CREATE TABLE ctas_rows (id int);
INSERT INTO ctas_rows VALUES (1), (2);
CHECKPOINT;
]);

my $ctas_locker = $node->background_psql('dbbranch_ctas_drain_source', on_error_stop => 1);
$ctas_locker->query_safe(q[BEGIN; LOCK TABLE ctas_rows IN ACCESS EXCLUSIVE MODE;]);
my $ctas_writer = $node->background_psql('dbbranch_ctas_drain_source', on_error_stop => 1);
$ctas_writer->query_until(
	qr/start_ctas_drain_table/,
	q(\echo start_ctas_drain_table
CREATE TABLE ctas_result AS SELECT * FROM ctas_rows;
\echo finish_ctas_drain_table
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_ctas_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE TABLE ctas_result%';
]), 'active source create table as waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_ctas_drain_target FROM DATABASE dbbranch_ctas_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create table as');
like($stderr, qr/source database "dbbranch_ctas_drain_source" has active write transactions/,
	'active source create table as holds db branch writer gate');

$ctas_locker->query_safe(q[COMMIT;]);
$ctas_locker->quit;
$ctas_writer->query_until(qr/finish_ctas_drain_table/, '');
$ctas_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_ctas_drain_target FROM DATABASE dbbranch_ctas_drain_source]);
my $ctas_row_count = $node->safe_psql(
	'dbbranch_ctas_drain_target',
	q[SELECT count(*) FROM ctas_result;]);
is($ctas_row_count, '2', 'branch succeeds after source create table as drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ctas_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ctas_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_refresh_matview_drain_source;]);
$node->safe_psql(
	'dbbranch_refresh_matview_drain_source',
	q[
CREATE TABLE matview_rows (id int);
INSERT INTO matview_rows VALUES (1);
CREATE MATERIALIZED VIEW dbbranch_matview AS SELECT * FROM matview_rows;
INSERT INTO matview_rows VALUES (2);
CHECKPOINT;
]);

my $refresh_matview_locker = $node->background_psql('dbbranch_refresh_matview_drain_source', on_error_stop => 1);
$refresh_matview_locker->query_safe(q[BEGIN; LOCK TABLE matview_rows IN ACCESS EXCLUSIVE MODE;]);
my $refresh_matview_writer = $node->background_psql('dbbranch_refresh_matview_drain_source', on_error_stop => 1);
$refresh_matview_writer->query_until(
	qr/start_refresh_matview_drain_matview/,
	q(\echo start_refresh_matview_drain_matview
REFRESH MATERIALIZED VIEW dbbranch_matview;
\echo finish_refresh_matview_drain_matview
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_refresh_matview_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'REFRESH MATERIALIZED VIEW dbbranch_matview%';
]), 'active source refresh materialized view waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_refresh_matview_drain_target FROM DATABASE dbbranch_refresh_matview_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source refresh materialized view');
like($stderr, qr/source database "dbbranch_refresh_matview_drain_source" has active write transactions/,
	'active source refresh materialized view holds db branch writer gate');

$refresh_matview_locker->query_safe(q[COMMIT;]);
$refresh_matview_locker->quit;
$refresh_matview_writer->query_until(qr/finish_refresh_matview_drain_matview/, '');
$refresh_matview_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_refresh_matview_drain_target FROM DATABASE dbbranch_refresh_matview_drain_source]);
my $matview_row_count = $node->safe_psql(
	'dbbranch_refresh_matview_drain_target',
	q[SELECT count(*) FROM dbbranch_matview;]);
is($matview_row_count, '2', 'branch succeeds after source refresh materialized view drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_refresh_matview_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_refresh_matview_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_conversion_drain_source;]);
$node->safe_psql('dbbranch_conversion_drain_source', q[CHECKPOINT;]);

my $conversion_locker = $node->background_psql('dbbranch_conversion_drain_source', on_error_stop => 1);
$conversion_locker->query_safe(q[BEGIN; LOCK TABLE pg_conversion IN ACCESS EXCLUSIVE MODE;]);
my $conversion_writer = $node->background_psql('dbbranch_conversion_drain_source', on_error_stop => 1);
$conversion_writer->query_until(
	qr/start_conversion_drain_conversion/,
	q(\echo start_conversion_drain_conversion
CREATE CONVERSION dbbranch_conv FOR 'LATIN1' TO 'UTF8' FROM iso8859_1_to_utf8;
\echo finish_conversion_drain_conversion
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_conversion_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE CONVERSION dbbranch_conv%';
]), 'active source create conversion waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_conversion_drain_target FROM DATABASE dbbranch_conversion_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create conversion');
like($stderr, qr/source database "dbbranch_conversion_drain_source" has active write transactions/,
	'active source create conversion holds db branch writer gate');

$conversion_locker->query_safe(q[COMMIT;]);
$conversion_locker->quit;
$conversion_writer->query_until(qr/finish_conversion_drain_conversion/, '');
$conversion_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_conversion_drain_target FROM DATABASE dbbranch_conversion_drain_source]);
my $conversion_count = $node->safe_psql(
	'dbbranch_conversion_drain_target',
	q[SELECT count(*) FROM pg_conversion WHERE conname = 'dbbranch_conv';]);
is($conversion_count, '1', 'branch succeeds after source create conversion drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_conversion_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_conversion_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_cast_drain_source;]);
$node->safe_psql(
	'dbbranch_cast_drain_source',
	q[
CREATE TYPE dbbranch_cast_type AS (id int);
CREATE FUNCTION dbbranch_cast_to_text(dbbranch_cast_type) RETURNS text
LANGUAGE SQL IMMUTABLE AS $$ SELECT ($1).id::text $$;
CHECKPOINT;
]);

my $cast_locker = $node->background_psql('dbbranch_cast_drain_source', on_error_stop => 1);
$cast_locker->query_safe(q[BEGIN; LOCK TABLE pg_cast IN ACCESS EXCLUSIVE MODE;]);
my $cast_writer = $node->background_psql('dbbranch_cast_drain_source', on_error_stop => 1);
$cast_writer->query_until(
	qr/start_cast_drain_cast/,
	q(\echo start_cast_drain_cast
CREATE CAST (dbbranch_cast_type AS text) WITH FUNCTION dbbranch_cast_to_text(dbbranch_cast_type);
\echo finish_cast_drain_cast
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_cast_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE CAST (dbbranch_cast_type AS text)%';
]), 'active source create cast waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_cast_drain_target FROM DATABASE dbbranch_cast_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create cast');
like($stderr, qr/source database "dbbranch_cast_drain_source" has active write transactions/,
	'active source create cast holds db branch writer gate');

$cast_locker->query_safe(q[COMMIT;]);
$cast_locker->quit;
$cast_writer->query_until(qr/finish_cast_drain_cast/, '');
$cast_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_cast_drain_target FROM DATABASE dbbranch_cast_drain_source]);
my $cast_count = $node->safe_psql(
	'dbbranch_cast_drain_target',
	q[
SELECT count(*)
FROM pg_cast
WHERE castsource = 'dbbranch_cast_type'::regtype
  AND casttarget = 'text'::regtype;
]);
is($cast_count, '1', 'branch succeeds after source create cast drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_cast_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_cast_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_opclass_drain_source;]);
$node->safe_psql('dbbranch_opclass_drain_source', q[CHECKPOINT;]);

my $opclass_writer = $node->background_psql('dbbranch_opclass_drain_source', on_error_stop => 1);
my $opclass_locker = $node->background_psql('dbbranch_opclass_drain_source', on_error_stop => 1);
$opclass_locker->query_safe(q[BEGIN; LOCK TABLE pg_opclass IN ACCESS EXCLUSIVE MODE;]);
$opclass_writer->query_until(
	qr/start_opclass_drain_opclass/,
	q(\echo start_opclass_drain_opclass
CREATE OPERATOR CLASS dbbranch_opclass FOR TYPE int USING btree AS STORAGE int;
\echo finish_opclass_drain_opclass
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_opclass_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE OPERATOR CLASS dbbranch_opclass%';
]), 'active source create operator class waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_opclass_drain_target FROM DATABASE dbbranch_opclass_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create operator class');
like($stderr, qr/source database "dbbranch_opclass_drain_source" has active write transactions/,
	'active source create operator class holds db branch writer gate');

$opclass_locker->query_safe(q[COMMIT;]);
$opclass_locker->quit;
$opclass_writer->query_until(qr/finish_opclass_drain_opclass/, '');
$opclass_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_opclass_drain_target FROM DATABASE dbbranch_opclass_drain_source]);
my $opclass_count = $node->safe_psql(
	'dbbranch_opclass_drain_target',
	q[SELECT count(*) FROM pg_opclass WHERE opcname = 'dbbranch_opclass';]);
is($opclass_count, '1', 'branch succeeds after source create operator class drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_opclass_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_opclass_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_operator_drain_source;]);
$node->safe_psql(
	'dbbranch_operator_drain_source',
	q[
CREATE FUNCTION dbbranch_operator_eq(boolean, boolean) RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$ SELECT NULL::boolean; $$;
CREATE OPERATOR === (
	LEFTARG = boolean,
	RIGHTARG = boolean,
	PROCEDURE = dbbranch_operator_eq,
	RESTRICT = contsel
);
CHECKPOINT;
]);

my $operator_locker = $node->background_psql('dbbranch_operator_drain_source', on_error_stop => 1);
$operator_locker->query_safe(q[BEGIN; LOCK TABLE pg_operator IN ACCESS EXCLUSIVE MODE;]);
my $operator_writer = $node->background_psql('dbbranch_operator_drain_source', on_error_stop => 1);
$operator_writer->query_until(
	qr/start_operator_drain_operator/,
	q(\echo start_operator_drain_operator
ALTER OPERATOR === (boolean, boolean) SET (RESTRICT = NONE);
\echo finish_operator_drain_operator
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_operator_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER OPERATOR ===%';
]), 'active source alter operator waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_operator_drain_target FROM DATABASE dbbranch_operator_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter operator');
like($stderr, qr/source database "dbbranch_operator_drain_source" has active write transactions/,
	'active source alter operator holds db branch writer gate');

$operator_locker->query_safe(q[COMMIT;]);
$operator_locker->quit;
$operator_writer->query_until(qr/finish_operator_drain_operator/, '');
$operator_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_operator_drain_target FROM DATABASE dbbranch_operator_drain_source]);
my $operator_restrict_removed = $node->safe_psql(
	'dbbranch_operator_drain_target',
	q[
SELECT oprrest = 0::oid
FROM pg_operator
WHERE oprname = '==='
  AND oprleft = 'boolean'::regtype
  AND oprright = 'boolean'::regtype;
]);
is($operator_restrict_removed, 't', 'branch succeeds after source alter operator drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_operator_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_operator_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_opfamily_drain_source;]);
$node->safe_psql('dbbranch_opfamily_drain_source', q[CHECKPOINT;]);

my $opfamily_locker = $node->background_psql('dbbranch_opfamily_drain_source', on_error_stop => 1);
$opfamily_locker->query_safe(q[BEGIN; LOCK TABLE pg_opfamily IN ACCESS EXCLUSIVE MODE;]);
my $opfamily_writer = $node->background_psql('dbbranch_opfamily_drain_source', on_error_stop => 1);
$opfamily_writer->query_until(
	qr/start_opfamily_drain_opfamily/,
	q(\echo start_opfamily_drain_opfamily
CREATE OPERATOR FAMILY dbbranch_opfamily USING btree;
\echo finish_opfamily_drain_opfamily
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_opfamily_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE OPERATOR FAMILY dbbranch_opfamily%';
]), 'active source create operator family waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_opfamily_drain_target FROM DATABASE dbbranch_opfamily_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create operator family');
like($stderr, qr/source database "dbbranch_opfamily_drain_source" has active write transactions/,
	'active source create operator family holds db branch writer gate');

$opfamily_locker->query_safe(q[COMMIT;]);
$opfamily_locker->quit;
$opfamily_writer->query_until(qr/finish_opfamily_drain_opfamily/, '');
$opfamily_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_opfamily_drain_target FROM DATABASE dbbranch_opfamily_drain_source]);
my $opfamily_count = $node->safe_psql(
	'dbbranch_opfamily_drain_target',
	q[SELECT count(*) FROM pg_opfamily WHERE opfname = 'dbbranch_opfamily';]);
is($opfamily_count, '1', 'branch succeeds after source create operator family drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_opfamily_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_opfamily_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_opfamily_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_opfamily_drain_source',
	q[
CREATE OPERATOR FAMILY dbbranch_alter_opfamily USING btree;
CHECKPOINT;
]);

my $alter_opfamily_writer = $node->background_psql('dbbranch_alter_opfamily_drain_source', on_error_stop => 1);
my $alter_opfamily_locker = $node->background_psql('dbbranch_alter_opfamily_drain_source', on_error_stop => 1);
$alter_opfamily_locker->query_safe(q[BEGIN; LOCK TABLE pg_amop IN ACCESS EXCLUSIVE MODE;]);
$alter_opfamily_writer->query_until(
	qr/start_alter_opfamily_drain_opfamily/,
	q(\echo start_alter_opfamily_drain_opfamily
ALTER OPERATOR FAMILY dbbranch_alter_opfamily USING btree ADD OPERATOR 1 < (int4, int4);
\echo finish_alter_opfamily_drain_opfamily
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_opfamily_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER OPERATOR FAMILY dbbranch_alter_opfamily%';
]), 'active source alter operator family waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_opfamily_drain_target FROM DATABASE dbbranch_alter_opfamily_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter operator family');
like($stderr, qr/source database "dbbranch_alter_opfamily_drain_source" has active write transactions/,
	'active source alter operator family holds db branch writer gate');

$alter_opfamily_locker->query_safe(q[COMMIT;]);
$alter_opfamily_locker->quit;
$alter_opfamily_writer->query_until(qr/finish_alter_opfamily_drain_opfamily/, '');
$alter_opfamily_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_opfamily_drain_target FROM DATABASE dbbranch_alter_opfamily_drain_source]);
my $opfamily_operator_count = $node->safe_psql(
	'dbbranch_alter_opfamily_drain_target',
	q[
SELECT count(*)
FROM pg_amop ao
JOIN pg_opfamily ofam ON ofam.oid = ao.amopfamily
WHERE ofam.opfname = 'dbbranch_alter_opfamily';
]);
is($opfamily_operator_count, '1', 'branch succeeds after source alter operator family drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_opfamily_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_opfamily_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_tsdict_drain_source;]);
$node->safe_psql(
	'dbbranch_tsdict_drain_source',
	q[
CREATE TEXT SEARCH DICTIONARY dbbranch_tsdict (template=simple);
CHECKPOINT;
]);

my $tsdict_locker = $node->background_psql('dbbranch_tsdict_drain_source', on_error_stop => 1);
$tsdict_locker->query_safe(q[BEGIN; LOCK TABLE pg_ts_dict IN ACCESS EXCLUSIVE MODE;]);
my $tsdict_writer = $node->background_psql('dbbranch_tsdict_drain_source', on_error_stop => 1);
$tsdict_writer->query_until(
	qr/start_tsdict_drain_tsdict/,
	q(\echo start_tsdict_drain_tsdict
ALTER TEXT SEARCH DICTIONARY dbbranch_tsdict (Accept = false);
\echo finish_tsdict_drain_tsdict
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_tsdict_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER TEXT SEARCH DICTIONARY dbbranch_tsdict%';
]), 'active source alter text search dictionary waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_tsdict_drain_target FROM DATABASE dbbranch_tsdict_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter text search dictionary');
like($stderr, qr/source database "dbbranch_tsdict_drain_source" has active write transactions/,
	'active source alter text search dictionary holds db branch writer gate');

$tsdict_locker->query_safe(q[COMMIT;]);
$tsdict_locker->quit;
$tsdict_writer->query_until(qr/finish_tsdict_drain_tsdict/, '');
$tsdict_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_tsdict_drain_target FROM DATABASE dbbranch_tsdict_drain_source]);
my $tsdict_option = $node->safe_psql(
	'dbbranch_tsdict_drain_target',
	q[SELECT dictinitoption FROM pg_ts_dict WHERE dictname = 'dbbranch_tsdict';]);
is($tsdict_option, q[accept = 'false'], 'branch succeeds after source alter text search dictionary drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_tsdict_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_tsdict_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_tsconfig_drain_source;]);
$node->safe_psql(
	'dbbranch_tsconfig_drain_source',
	q[
CREATE TEXT SEARCH CONFIGURATION dbbranch_tsconfig (parser = default);
CHECKPOINT;
]);

my $tsconfig_locker = $node->background_psql('dbbranch_tsconfig_drain_source', on_error_stop => 1);
$tsconfig_locker->query_safe(q[BEGIN; LOCK TABLE pg_ts_config_map IN ACCESS EXCLUSIVE MODE;]);
my $tsconfig_writer = $node->background_psql('dbbranch_tsconfig_drain_source', on_error_stop => 1);
$tsconfig_writer->query_until(
	qr/start_tsconfig_drain_tsconfig/,
	q(\echo start_tsconfig_drain_tsconfig
ALTER TEXT SEARCH CONFIGURATION dbbranch_tsconfig ALTER MAPPING FOR asciiword WITH simple;
\echo finish_tsconfig_drain_tsconfig
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_tsconfig_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER TEXT SEARCH CONFIGURATION dbbranch_tsconfig%';
]), 'active source alter text search configuration waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_tsconfig_drain_target FROM DATABASE dbbranch_tsconfig_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter text search configuration');
like($stderr, qr/source database "dbbranch_tsconfig_drain_source" has active write transactions/,
	'active source alter text search configuration holds db branch writer gate');

$tsconfig_locker->query_safe(q[COMMIT;]);
$tsconfig_locker->quit;
$tsconfig_writer->query_until(qr/finish_tsconfig_drain_tsconfig/, '');
$tsconfig_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_tsconfig_drain_target FROM DATABASE dbbranch_tsconfig_drain_source]);
my $tsconfig_mapping_count = $node->safe_psql(
	'dbbranch_tsconfig_drain_target',
	q[
SELECT count(*)
FROM pg_ts_config_map m
JOIN pg_ts_config c ON c.oid = m.mapcfg
JOIN pg_ts_dict d ON d.oid = m.mapdict
WHERE c.cfgname = 'dbbranch_tsconfig'
  AND d.dictname = 'simple';
]);
is($tsconfig_mapping_count, '1', 'branch succeeds after source alter text search configuration drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_tsconfig_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_tsconfig_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_fdw_drain_source;]);
$node->safe_psql('dbbranch_fdw_drain_source', q[CHECKPOINT;]);

my $fdw_locker = $node->background_psql('dbbranch_fdw_drain_source', on_error_stop => 1);
$fdw_locker->query_safe(q[BEGIN; LOCK TABLE pg_foreign_data_wrapper IN ACCESS EXCLUSIVE MODE;]);
my $fdw_writer = $node->background_psql('dbbranch_fdw_drain_source', on_error_stop => 1);
$fdw_writer->query_until(
	qr/start_fdw_drain_fdw/,
	q(\echo start_fdw_drain_fdw
CREATE FOREIGN DATA WRAPPER dbbranch_fdw;
\echo finish_fdw_drain_fdw
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_fdw_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE FOREIGN DATA WRAPPER dbbranch_fdw%';
]), 'active source create foreign data wrapper waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_fdw_drain_target FROM DATABASE dbbranch_fdw_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create foreign data wrapper');
like($stderr, qr/source database "dbbranch_fdw_drain_source" has active write transactions/,
	'active source create foreign data wrapper holds db branch writer gate');

$fdw_locker->query_safe(q[COMMIT;]);
$fdw_locker->quit;
$fdw_writer->query_until(qr/finish_fdw_drain_fdw/, '');
$fdw_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_fdw_drain_target FROM DATABASE dbbranch_fdw_drain_source]);
my $fdw_exists = $node->safe_psql(
	'dbbranch_fdw_drain_target',
	q[SELECT count(*) FROM pg_foreign_data_wrapper WHERE fdwname = 'dbbranch_fdw';]);
is($fdw_exists, '1', 'branch succeeds after source create foreign data wrapper drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_fdw_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_fdw_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_fdw_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_fdw_drain_source',
	q[
CREATE FOREIGN DATA WRAPPER dbbranch_fdw;
CHECKPOINT;
]);

my $alter_fdw_locker = $node->background_psql('dbbranch_alter_fdw_drain_source', on_error_stop => 1);
$alter_fdw_locker->query_safe(q[BEGIN; LOCK TABLE pg_foreign_data_wrapper IN ACCESS EXCLUSIVE MODE;]);
my $alter_fdw_writer = $node->background_psql('dbbranch_alter_fdw_drain_source', on_error_stop => 1);
$alter_fdw_writer->query_until(
	qr/start_alter_fdw_drain_fdw/,
	q(\echo start_alter_fdw_drain_fdw
ALTER FOREIGN DATA WRAPPER dbbranch_fdw OPTIONS (ADD host 'localhost');
\echo finish_alter_fdw_drain_fdw
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_fdw_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER FOREIGN DATA WRAPPER dbbranch_fdw%';
]), 'active source alter foreign data wrapper waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_fdw_drain_target FROM DATABASE dbbranch_alter_fdw_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter foreign data wrapper');
like($stderr, qr/source database "dbbranch_alter_fdw_drain_source" has active write transactions/,
	'active source alter foreign data wrapper holds db branch writer gate');

$alter_fdw_locker->query_safe(q[COMMIT;]);
$alter_fdw_locker->quit;
$alter_fdw_writer->query_until(qr/finish_alter_fdw_drain_fdw/, '');
$alter_fdw_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_fdw_drain_target FROM DATABASE dbbranch_alter_fdw_drain_source]);
my $fdw_options = $node->safe_psql(
	'dbbranch_alter_fdw_drain_target',
	q[SELECT fdwoptions @> ARRAY['host=localhost'] FROM pg_foreign_data_wrapper WHERE fdwname = 'dbbranch_fdw';]);
is($fdw_options, 't', 'branch succeeds after source alter foreign data wrapper drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_fdw_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_fdw_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_server_drain_source;]);
$node->safe_psql(
	'dbbranch_server_drain_source',
	q[
CREATE FOREIGN DATA WRAPPER dbbranch_fdw;
CHECKPOINT;
]);

my $server_locker = $node->background_psql('dbbranch_server_drain_source', on_error_stop => 1);
$server_locker->query_safe(q[BEGIN; LOCK TABLE pg_foreign_server IN ACCESS EXCLUSIVE MODE;]);
my $server_writer = $node->background_psql('dbbranch_server_drain_source', on_error_stop => 1);
$server_writer->query_until(
	qr/start_server_drain_server/,
	q(\echo start_server_drain_server
CREATE SERVER dbbranch_server FOREIGN DATA WRAPPER dbbranch_fdw;
\echo finish_server_drain_server
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_server_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE SERVER dbbranch_server%';
]), 'active source create foreign server waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_server_drain_target FROM DATABASE dbbranch_server_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create foreign server');
like($stderr, qr/source database "dbbranch_server_drain_source" has active write transactions/,
	'active source create foreign server holds db branch writer gate');

$server_locker->query_safe(q[COMMIT;]);
$server_locker->quit;
$server_writer->query_until(qr/finish_server_drain_server/, '');
$server_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_server_drain_target FROM DATABASE dbbranch_server_drain_source]);
my $server_exists = $node->safe_psql(
	'dbbranch_server_drain_target',
	q[SELECT count(*) FROM pg_foreign_server WHERE srvname = 'dbbranch_server';]);
is($server_exists, '1', 'branch succeeds after source create foreign server drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_server_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_server_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_server_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_server_drain_source',
	q[
CREATE FOREIGN DATA WRAPPER dbbranch_fdw;
CREATE SERVER dbbranch_server FOREIGN DATA WRAPPER dbbranch_fdw;
CHECKPOINT;
]);

my $alter_server_locker = $node->background_psql('dbbranch_alter_server_drain_source', on_error_stop => 1);
$alter_server_locker->query_safe(q[BEGIN; LOCK TABLE pg_foreign_server IN ACCESS EXCLUSIVE MODE;]);
my $alter_server_writer = $node->background_psql('dbbranch_alter_server_drain_source', on_error_stop => 1);
$alter_server_writer->query_until(
	qr/start_alter_server_drain_server/,
	q(\echo start_alter_server_drain_server
ALTER SERVER dbbranch_server OPTIONS (ADD host 'localhost');
\echo finish_alter_server_drain_server
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_server_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER SERVER dbbranch_server%';
]), 'active source alter foreign server waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_server_drain_target FROM DATABASE dbbranch_alter_server_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter foreign server');
like($stderr, qr/source database "dbbranch_alter_server_drain_source" has active write transactions/,
	'active source alter foreign server holds db branch writer gate');

$alter_server_locker->query_safe(q[COMMIT;]);
$alter_server_locker->quit;
$alter_server_writer->query_until(qr/finish_alter_server_drain_server/, '');
$alter_server_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_server_drain_target FROM DATABASE dbbranch_alter_server_drain_source]);
my $server_options = $node->safe_psql(
	'dbbranch_alter_server_drain_target',
	q[SELECT srvoptions @> ARRAY['host=localhost'] FROM pg_foreign_server WHERE srvname = 'dbbranch_server';]);
is($server_options, 't', 'branch succeeds after source alter foreign server drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_server_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_server_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_user_mapping_drain_source;]);
$node->safe_psql(
	'dbbranch_user_mapping_drain_source',
	q[
CREATE FOREIGN DATA WRAPPER dbbranch_fdw;
CREATE SERVER dbbranch_server FOREIGN DATA WRAPPER dbbranch_fdw;
CHECKPOINT;
]);

my $user_mapping_locker = $node->background_psql('dbbranch_user_mapping_drain_source', on_error_stop => 1);
$user_mapping_locker->query_safe(q[BEGIN; LOCK TABLE pg_user_mapping IN ACCESS EXCLUSIVE MODE;]);
my $user_mapping_writer = $node->background_psql('dbbranch_user_mapping_drain_source', on_error_stop => 1);
$user_mapping_writer->query_until(
	qr/start_user_mapping_drain_mapping/,
	q(\echo start_user_mapping_drain_mapping
CREATE USER MAPPING FOR CURRENT_USER SERVER dbbranch_server OPTIONS (user 'branch_user');
\echo finish_user_mapping_drain_mapping
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_user_mapping_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE USER MAPPING%';
]), 'active source create user mapping waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_user_mapping_drain_target FROM DATABASE dbbranch_user_mapping_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create user mapping');
like($stderr, qr/source database "dbbranch_user_mapping_drain_source" has active write transactions/,
	'active source create user mapping holds db branch writer gate');

$user_mapping_locker->query_safe(q[COMMIT;]);
$user_mapping_locker->quit;
$user_mapping_writer->query_until(qr/finish_user_mapping_drain_mapping/, '');
$user_mapping_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_user_mapping_drain_target FROM DATABASE dbbranch_user_mapping_drain_source]);
my $user_mapping_exists = $node->safe_psql(
	'dbbranch_user_mapping_drain_target',
	q[
SELECT count(*)
FROM pg_user_mapping m
JOIN pg_foreign_server s ON s.oid = m.umserver
WHERE s.srvname = 'dbbranch_server';
]);
is($user_mapping_exists, '1', 'branch succeeds after source create user mapping drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_user_mapping_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_user_mapping_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_mapping_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_mapping_drain_source',
	q[
CREATE FOREIGN DATA WRAPPER dbbranch_fdw;
CREATE SERVER dbbranch_server FOREIGN DATA WRAPPER dbbranch_fdw;
CREATE USER MAPPING FOR CURRENT_USER SERVER dbbranch_server OPTIONS (user 'branch_user');
CHECKPOINT;
]);

my $alter_mapping_locker = $node->background_psql('dbbranch_alter_mapping_drain_source', on_error_stop => 1);
$alter_mapping_locker->query_safe(q[BEGIN; LOCK TABLE pg_user_mapping IN ACCESS EXCLUSIVE MODE;]);
my $alter_mapping_writer = $node->background_psql('dbbranch_alter_mapping_drain_source', on_error_stop => 1);
$alter_mapping_writer->query_until(
	qr/start_alter_mapping_drain_mapping/,
	q(\echo start_alter_mapping_drain_mapping
ALTER USER MAPPING FOR CURRENT_USER SERVER dbbranch_server OPTIONS (SET user 'branch_user2');
\echo finish_alter_mapping_drain_mapping
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_mapping_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER USER MAPPING%';
]), 'active source alter user mapping waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_mapping_drain_target FROM DATABASE dbbranch_alter_mapping_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter user mapping');
like($stderr, qr/source database "dbbranch_alter_mapping_drain_source" has active write transactions/,
	'active source alter user mapping holds db branch writer gate');

$alter_mapping_locker->query_safe(q[COMMIT;]);
$alter_mapping_locker->quit;
$alter_mapping_writer->query_until(qr/finish_alter_mapping_drain_mapping/, '');
$alter_mapping_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_mapping_drain_target FROM DATABASE dbbranch_alter_mapping_drain_source]);
my $alter_mapping_options = $node->safe_psql(
	'dbbranch_alter_mapping_drain_target',
	q[
SELECT m.umoptions @> ARRAY['user=branch_user2']
FROM pg_user_mapping m
JOIN pg_foreign_server s ON s.oid = m.umserver
WHERE s.srvname = 'dbbranch_server';
]);
is($alter_mapping_options, 't', 'branch succeeds after source alter user mapping drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_mapping_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_mapping_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_drop_mapping_drain_source;]);
$node->safe_psql(
	'dbbranch_drop_mapping_drain_source',
	q[
CREATE FOREIGN DATA WRAPPER dbbranch_fdw;
CREATE SERVER dbbranch_server FOREIGN DATA WRAPPER dbbranch_fdw;
CREATE USER MAPPING FOR CURRENT_USER SERVER dbbranch_server OPTIONS (user 'branch_user');
CHECKPOINT;
]);

my $drop_mapping_locker = $node->background_psql('dbbranch_drop_mapping_drain_source', on_error_stop => 1);
$drop_mapping_locker->query_safe(q[BEGIN; LOCK TABLE pg_user_mapping IN ACCESS EXCLUSIVE MODE;]);
my $drop_mapping_writer = $node->background_psql('dbbranch_drop_mapping_drain_source', on_error_stop => 1);
$drop_mapping_writer->query_until(
	qr/start_drop_mapping_drain_mapping/,
	q(\echo start_drop_mapping_drain_mapping
DROP USER MAPPING FOR CURRENT_USER SERVER dbbranch_server;
\echo finish_drop_mapping_drain_mapping
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_drop_mapping_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'DROP USER MAPPING%';
]), 'active source drop user mapping waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_drop_mapping_drain_target FROM DATABASE dbbranch_drop_mapping_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source drop user mapping');
like($stderr, qr/source database "dbbranch_drop_mapping_drain_source" has active write transactions/,
	'active source drop user mapping holds db branch writer gate');

$drop_mapping_locker->query_safe(q[COMMIT;]);
$drop_mapping_locker->quit;
$drop_mapping_writer->query_until(qr/finish_drop_mapping_drain_mapping/, '');
$drop_mapping_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_drop_mapping_drain_target FROM DATABASE dbbranch_drop_mapping_drain_source]);
my $drop_mapping_exists = $node->safe_psql(
	'dbbranch_drop_mapping_drain_target',
	q[
SELECT count(*)
FROM pg_user_mapping m
JOIN pg_foreign_server s ON s.oid = m.umserver
WHERE s.srvname = 'dbbranch_server';
]);
is($drop_mapping_exists, '0', 'branch succeeds after source drop user mapping drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_mapping_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_mapping_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_policy_drain_source;]);
$node->safe_psql(
	'dbbranch_policy_drain_source',
	q[
CREATE TABLE policy_rows (id int PRIMARY KEY);
INSERT INTO policy_rows VALUES (1);
CHECKPOINT;
]);

my $policy_locker = $node->background_psql('dbbranch_policy_drain_source', on_error_stop => 1);
$policy_locker->query_safe(q[BEGIN; LOCK TABLE policy_rows IN ACCESS SHARE MODE;]);
my $policy_writer = $node->background_psql('dbbranch_policy_drain_source', on_error_stop => 1);
$policy_writer->query_until(
	qr/start_policy_drain_policy/,
	q(\echo start_policy_drain_policy
CREATE POLICY policy_rows_select ON policy_rows FOR SELECT USING (id > 0);
\echo finish_policy_drain_policy
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_policy_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE POLICY policy_rows_select%';
]), 'active source create policy waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_policy_drain_target FROM DATABASE dbbranch_policy_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create policy');
like($stderr, qr/source database "dbbranch_policy_drain_source" has active write transactions/,
	'active source create policy holds db branch writer gate');

$policy_locker->query_safe(q[COMMIT;]);
$policy_locker->quit;
$policy_writer->query_until(qr/finish_policy_drain_policy/, '');
$policy_writer->quit;

my $policy_exists = $node->safe_psql(
	'dbbranch_policy_drain_source',
	q[SELECT count(*) FROM pg_policy WHERE polname = 'policy_rows_select';]);
is($policy_exists, '1', 'source create policy finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_policy_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_policy_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_policy_drain_source',
	q[
CREATE TABLE alter_policy_rows (id int PRIMARY KEY);
CREATE POLICY alter_policy_rows_select ON alter_policy_rows FOR SELECT USING (id > 0);
INSERT INTO alter_policy_rows VALUES (1);
CHECKPOINT;
]);

my $alter_policy_locker = $node->background_psql('dbbranch_alter_policy_drain_source', on_error_stop => 1);
$alter_policy_locker->query_safe(q[BEGIN; LOCK TABLE alter_policy_rows IN ACCESS SHARE MODE;]);
my $alter_policy_writer = $node->background_psql('dbbranch_alter_policy_drain_source', on_error_stop => 1);
$alter_policy_writer->query_until(
	qr/start_alter_policy_drain_policy/,
	q(\echo start_alter_policy_drain_policy
ALTER POLICY alter_policy_rows_select ON alter_policy_rows USING (id > 1);
\echo finish_alter_policy_drain_policy
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_policy_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER POLICY alter_policy_rows_select%';
]), 'active source alter policy waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_policy_drain_target FROM DATABASE dbbranch_alter_policy_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter policy');
like($stderr, qr/source database "dbbranch_alter_policy_drain_source" has active write transactions/,
	'active source alter policy holds db branch writer gate');

$alter_policy_locker->query_safe(q[COMMIT;]);
$alter_policy_locker->quit;
$alter_policy_writer->query_until(qr/finish_alter_policy_drain_policy/, '');
$alter_policy_writer->quit;

my $alter_policy_expr = $node->safe_psql(
	'dbbranch_alter_policy_drain_source',
	q[SELECT pg_get_expr(polqual, polrelid) FROM pg_policy WHERE polname = 'alter_policy_rows_select';]);
like($alter_policy_expr, qr/id > 1/, 'source alter policy finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_policy_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_rename_drain_source;]);
$node->safe_psql(
	'dbbranch_rename_drain_source',
	q[
CREATE TABLE rename_rows (id int PRIMARY KEY);
INSERT INTO rename_rows VALUES (1);
CHECKPOINT;
]);

my $rename_locker = $node->background_psql('dbbranch_rename_drain_source', on_error_stop => 1);
$rename_locker->query_safe(q[BEGIN; LOCK TABLE rename_rows IN ACCESS SHARE MODE;]);
my $rename_writer = $node->background_psql('dbbranch_rename_drain_source', on_error_stop => 1);
$rename_writer->query_until(
	qr/start_rename_drain_rename/,
	q(\echo start_rename_drain_rename
ALTER TABLE rename_rows RENAME TO renamed_rows;
\echo finish_rename_drain_rename
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_rename_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER TABLE rename_rows RENAME%';
]), 'active source rename waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_rename_drain_target FROM DATABASE dbbranch_rename_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source rename');
like($stderr, qr/source database "dbbranch_rename_drain_source" has active write transactions/,
	'active source rename holds db branch writer gate');

$rename_locker->query_safe(q[COMMIT;]);
$rename_locker->quit;
$rename_writer->query_until(qr/finish_rename_drain_rename/, '');
$rename_writer->quit;

my $renamed_table_exists = $node->safe_psql(
	'dbbranch_rename_drain_source',
	q[SELECT to_regclass('public.renamed_rows') IS NOT NULL;]);
is($renamed_table_exists, 't', 'source rename finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_rename_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_create_schema_drain_source;]);
$node->safe_psql('dbbranch_create_schema_drain_source', q[CHECKPOINT;]);

my $create_schema_locker = $node->background_psql('dbbranch_create_schema_drain_source', on_error_stop => 1);
$create_schema_locker->query_safe(q[BEGIN; LOCK TABLE pg_namespace IN ACCESS EXCLUSIVE MODE;]);
my $create_schema_writer = $node->background_psql('dbbranch_create_schema_drain_source', on_error_stop => 1);
$create_schema_writer->query_until(
	qr/start_create_schema_drain_schema/,
	q(\echo start_create_schema_drain_schema
CREATE SCHEMA dbbranch_created_schema;
\echo finish_create_schema_drain_schema
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_create_schema_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE SCHEMA dbbranch_created_schema%';
]), 'active source create schema waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_create_schema_drain_target FROM DATABASE dbbranch_create_schema_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create schema');
like($stderr, qr/source database "dbbranch_create_schema_drain_source" has active write transactions/,
	'active source create schema holds db branch writer gate');

$create_schema_locker->query_safe(q[COMMIT;]);
$create_schema_locker->quit;
$create_schema_writer->query_until(qr/finish_create_schema_drain_schema/, '');
$create_schema_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_create_schema_drain_target FROM DATABASE dbbranch_create_schema_drain_source]);
my $created_schema_exists = $node->safe_psql(
	'dbbranch_create_schema_drain_target',
	q[SELECT count(*) FROM pg_namespace WHERE nspname = 'dbbranch_created_schema';]);
is($created_schema_exists, '1', 'branch succeeds after source create schema drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_schema_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_schema_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_schema_drain_source;]);
$node->safe_psql(
	'dbbranch_schema_drain_source',
	q[
CREATE SCHEMA branch_dst;
CREATE TABLE schema_rows (id int PRIMARY KEY);
INSERT INTO schema_rows VALUES (1);
CHECKPOINT;
]);

my $schema_locker = $node->background_psql('dbbranch_schema_drain_source', on_error_stop => 1);
$schema_locker->query_safe(q[BEGIN; LOCK TABLE schema_rows IN ACCESS SHARE MODE;]);
my $schema_writer = $node->background_psql('dbbranch_schema_drain_source', on_error_stop => 1);
$schema_writer->query_until(
	qr/start_schema_drain_schema/,
	q(\echo start_schema_drain_schema
ALTER TABLE schema_rows SET SCHEMA branch_dst;
\echo finish_schema_drain_schema
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_schema_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER TABLE schema_rows SET SCHEMA%';
]), 'active source set schema waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_schema_drain_target FROM DATABASE dbbranch_schema_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source set schema');
like($stderr, qr/source database "dbbranch_schema_drain_source" has active write transactions/,
	'active source set schema holds db branch writer gate');

$schema_locker->query_safe(q[COMMIT;]);
$schema_locker->quit;
$schema_writer->query_until(qr/finish_schema_drain_schema/, '');
$schema_writer->quit;

my $schema_table_moved = $node->safe_psql(
	'dbbranch_schema_drain_source',
	q[SELECT to_regclass('branch_dst.schema_rows') IS NOT NULL AND to_regclass('public.schema_rows') IS NULL;]);
is($schema_table_moved, 't', 'source set schema finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_schema_drain_source;]);

$node->safe_psql('postgres', q[CREATE ROLE dbbranch_schema_owner;]);
$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_owner_drain_source;]);
$node->safe_psql(
	'dbbranch_owner_drain_source',
	q[
CREATE SCHEMA owner_schema;
CHECKPOINT;
]);

my $owner_locker = $node->background_psql('dbbranch_owner_drain_source', on_error_stop => 1);
$owner_locker->query_safe(q[BEGIN; LOCK TABLE pg_namespace IN ACCESS EXCLUSIVE MODE;]);
my $owner_writer = $node->background_psql('dbbranch_owner_drain_source', on_error_stop => 1);
$owner_writer->query_until(
	qr/start_owner_drain_owner/,
	q(\echo start_owner_drain_owner
ALTER SCHEMA owner_schema OWNER TO dbbranch_schema_owner;
\echo finish_owner_drain_owner
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_owner_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER SCHEMA owner_schema OWNER%';
]), 'active source alter schema owner waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_owner_drain_target FROM DATABASE dbbranch_owner_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter schema owner');
like($stderr, qr/source database "dbbranch_owner_drain_source" has active write transactions/,
	'active source alter schema owner holds db branch writer gate');

$owner_locker->query_safe(q[COMMIT;]);
$owner_locker->quit;
$owner_writer->query_until(qr/finish_owner_drain_owner/, '');
$owner_writer->quit;

my $owner_changed = $node->safe_psql(
	'dbbranch_owner_drain_source',
	q[SELECT nspowner = 'dbbranch_schema_owner'::regrole FROM pg_namespace WHERE nspname = 'owner_schema';]);
is($owner_changed, 't', 'source alter schema owner finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_owner_drain_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_schema_owner;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_extension_drain_source;]);
$node->safe_psql('dbbranch_extension_drain_source', q[CHECKPOINT;]);

my $extension_locker = $node->background_psql('dbbranch_extension_drain_source', on_error_stop => 1);
$extension_locker->query_safe(q[BEGIN; LOCK TABLE pg_extension IN ACCESS EXCLUSIVE MODE;]);
my $extension_writer = $node->background_psql('dbbranch_extension_drain_source', on_error_stop => 1);
$extension_writer->query_until(
	qr/start_extension_drain_extension/,
	q(\echo start_extension_drain_extension
CREATE EXTENSION amcheck;
\echo finish_extension_drain_extension
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_extension_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE EXTENSION amcheck%';
]), 'active source create extension waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_extension_drain_target FROM DATABASE dbbranch_extension_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create extension');
like($stderr, qr/source database "dbbranch_extension_drain_source" has active write transactions/,
	'active source create extension holds db branch writer gate');

$extension_locker->query_safe(q[COMMIT;]);
$extension_locker->quit;
$extension_writer->query_until(qr/finish_extension_drain_extension/, '');
$extension_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_extension_drain_target FROM DATABASE dbbranch_extension_drain_source]);
my $extension_exists = $node->safe_psql(
	'dbbranch_extension_drain_target',
	q[SELECT count(*) FROM pg_extension WHERE extname = 'amcheck';]);
is($extension_exists, '1', 'branch succeeds after source create extension drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_extension_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_extension_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_extension_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_extension_drain_source',
	q[
CREATE EXTENSION amcheck;
CREATE SCHEMA ext_schema;
CHECKPOINT;
]);

my $alter_extension_locker = $node->background_psql('dbbranch_alter_extension_drain_source', on_error_stop => 1);
$alter_extension_locker->query_safe(q[BEGIN; LOCK TABLE pg_extension IN ACCESS EXCLUSIVE MODE;]);
my $alter_extension_writer = $node->background_psql('dbbranch_alter_extension_drain_source', on_error_stop => 1);
$alter_extension_writer->query_until(
	qr/start_alter_extension_drain_extension/,
	q(\echo start_alter_extension_drain_extension
ALTER EXTENSION amcheck SET SCHEMA ext_schema;
\echo finish_alter_extension_drain_extension
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_extension_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER EXTENSION amcheck%';
]), 'active source alter extension waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_extension_drain_target FROM DATABASE dbbranch_alter_extension_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter extension');
like($stderr, qr/source database "dbbranch_alter_extension_drain_source" has active write transactions/,
	'active source alter extension holds db branch writer gate');

$alter_extension_locker->query_safe(q[COMMIT;]);
$alter_extension_locker->quit;
$alter_extension_writer->query_until(qr/finish_alter_extension_drain_extension/, '');
$alter_extension_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_extension_drain_target FROM DATABASE dbbranch_alter_extension_drain_source]);
my $extension_schema = $node->safe_psql(
	'dbbranch_alter_extension_drain_target',
	q[
SELECT n.nspname
FROM pg_extension e
JOIN pg_namespace n ON n.oid = e.extnamespace
WHERE e.extname = 'amcheck';
]);
is($extension_schema, 'ext_schema', 'branch succeeds after source alter extension drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_extension_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_extension_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_extension_contents_drain_source;]);
$node->safe_psql(
	'dbbranch_alter_extension_contents_drain_source',
	q[
CREATE EXTENSION amcheck;
CREATE TABLE ext_member (id int);
CHECKPOINT;
]);

my $alter_extension_contents_locker = $node->background_psql('dbbranch_alter_extension_contents_drain_source', on_error_stop => 1);
$alter_extension_contents_locker->query_safe(q[BEGIN; LOCK TABLE pg_extension IN ACCESS EXCLUSIVE MODE;]);
my $alter_extension_contents_writer = $node->background_psql('dbbranch_alter_extension_contents_drain_source', on_error_stop => 1);
$alter_extension_contents_writer->query_until(
	qr/start_alter_extension_contents_drain_extension/,
	q(\echo start_alter_extension_contents_drain_extension
ALTER EXTENSION amcheck ADD TABLE ext_member;
\echo finish_alter_extension_contents_drain_extension
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_extension_contents_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER EXTENSION amcheck ADD TABLE ext_member%';
]), 'active source alter extension contents waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_extension_contents_drain_target FROM DATABASE dbbranch_alter_extension_contents_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter extension contents');
like($stderr, qr/source database "dbbranch_alter_extension_contents_drain_source" has active write transactions/,
	'active source alter extension contents holds db branch writer gate');

$alter_extension_contents_locker->query_safe(q[COMMIT;]);
$alter_extension_contents_locker->quit;
$alter_extension_contents_writer->query_until(qr/finish_alter_extension_contents_drain_extension/, '');
$alter_extension_contents_writer->quit;

$node->safe_psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_extension_contents_drain_target FROM DATABASE dbbranch_alter_extension_contents_drain_source]);
my $extension_member_count = $node->safe_psql(
	'dbbranch_alter_extension_contents_drain_target',
	q[
SELECT count(*)
FROM pg_depend d
JOIN pg_extension e ON e.oid = d.refobjid
JOIN pg_class c ON c.oid = d.objid
WHERE d.refclassid = 'pg_extension'::regclass
  AND d.classid = 'pg_class'::regclass
  AND d.deptype = 'e'
  AND e.extname = 'amcheck'
  AND c.relname = 'ext_member';
]);
is($extension_member_count, '1', 'branch succeeds after source alter extension contents drains');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_extension_contents_drain_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_extension_contents_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_event_trigger_drain_source;]);
$node->safe_psql(
	'dbbranch_event_trigger_drain_source',
	q[
CREATE FUNCTION dbbranch_event_trigger_proc() RETURNS event_trigger AS $$
BEGIN
  NULL;
END;
$$ LANGUAGE plpgsql;
CHECKPOINT;
]);

my $event_trigger_writer =
  $node->background_psql('dbbranch_event_trigger_drain_source', on_error_stop => 1);
$event_trigger_writer->query_safe(q[
BEGIN;
CREATE EVENT TRIGGER dbbranch_event_trigger_waiter
	ON ddl_command_start EXECUTE PROCEDURE dbbranch_event_trigger_proc();
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_event_trigger_drain_target FROM DATABASE dbbranch_event_trigger_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create event trigger');
like($stderr, qr/source database "dbbranch_event_trigger_drain_source" has active write transactions/,
	'active source create event trigger holds db branch writer gate');

$event_trigger_writer->query_safe(q[COMMIT;]);
$event_trigger_writer->quit;

my $event_trigger_exists = $node->safe_psql(
	'dbbranch_event_trigger_drain_source',
	q[SELECT count(*) FROM pg_event_trigger WHERE evtname = 'dbbranch_event_trigger_waiter';]);
is($event_trigger_exists, '1', 'source create event trigger finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_event_trigger_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_depends_drain_source;]);
$node->safe_psql(
	'dbbranch_depends_drain_source',
	q[
CREATE MATERIALIZED VIEW depends_mv AS SELECT 1 AS id;
CHECKPOINT;
]);

my $depends_locker = $node->background_psql('dbbranch_depends_drain_source', on_error_stop => 1);
$depends_locker->query_safe(q[BEGIN; SELECT count(*) FROM depends_mv;]);
my $depends_writer = $node->background_psql('dbbranch_depends_drain_source', on_error_stop => 1);
$depends_writer->query_until(
	qr/start_depends_drain_depends/,
	q(\echo start_depends_drain_depends
ALTER MATERIALIZED VIEW depends_mv DEPENDS ON EXTENSION plpgsql;
\echo finish_depends_drain_depends
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_depends_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER MATERIALIZED VIEW depends_mv DEPENDS%';
]), 'active source depends on extension waits on source matview lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_depends_drain_target FROM DATABASE dbbranch_depends_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source depends on extension');
like($stderr, qr/source database "dbbranch_depends_drain_source" has active write transactions/,
	'active source depends on extension holds db branch writer gate');

$depends_locker->query_safe(q[COMMIT;]);
$depends_locker->quit;
$depends_writer->query_until(qr/finish_depends_drain_depends/, '');
$depends_writer->quit;

my $depends_recorded = $node->safe_psql(
	'dbbranch_depends_drain_source',
	q[SELECT count(*) FROM pg_depend WHERE classid = 'pg_class'::regclass AND objid = 'depends_mv'::regclass AND refclassid = 'pg_extension'::regclass AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'plpgsql') AND deptype = 'x';]);
is($depends_recorded, '1', 'source depends on extension finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_depends_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_create_role_drain_source;]);
$node->safe_psql('dbbranch_create_role_drain_source', q[CHECKPOINT;]);

my $create_role_writer =
  $node->background_psql('dbbranch_create_role_drain_source', on_error_stop => 1);
$create_role_writer->query_safe(q[BEGIN; CREATE ROLE dbbranch_create_role_waiter;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_create_role_drain_target FROM DATABASE dbbranch_create_role_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create role');
like($stderr, qr/source database "dbbranch_create_role_drain_source" has active write transactions/,
	'active source create role holds db branch writer gate');

$create_role_writer->query_safe(q[COMMIT;]);
$create_role_writer->quit;

my $create_role_exists = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_roles WHERE rolname = 'dbbranch_create_role_waiter';]);
is($create_role_exists, '1', 'source create role finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP ROLE dbbranch_create_role_waiter;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_role_drain_source;]);

$node->safe_psql(
	'postgres',
	q[
CREATE ROLE dbbranch_grant_role_parent;
CREATE ROLE dbbranch_grant_role_member;
CREATE DATABASE dbbranch_grant_role_drain_source;
]);
$node->safe_psql('dbbranch_grant_role_drain_source', q[CHECKPOINT;]);

my $grant_role_writer =
  $node->background_psql('dbbranch_grant_role_drain_source', on_error_stop => 1);
$grant_role_writer->query_safe(
	q[BEGIN; GRANT dbbranch_grant_role_parent TO dbbranch_grant_role_member;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_grant_role_drain_target FROM DATABASE dbbranch_grant_role_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source grant role');
like($stderr, qr/source database "dbbranch_grant_role_drain_source" has active write transactions/,
	'active source grant role holds db branch writer gate');

$grant_role_writer->query_safe(q[COMMIT;]);
$grant_role_writer->quit;

my $grant_role_applied = $node->safe_psql(
	'postgres',
	q[SELECT pg_has_role('dbbranch_grant_role_member', 'dbbranch_grant_role_parent', 'member');]);
is($grant_role_applied, 't', 'source grant role finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_grant_role_drain_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_grant_role_member;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_grant_role_parent;]);

$node->safe_psql('postgres', q[CREATE ROLE dbbranch_default_reader;]);
$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_default_drain_source;]);
$node->safe_psql('dbbranch_default_drain_source', q[CHECKPOINT;]);

my $default_locker = $node->background_psql('dbbranch_default_drain_source', on_error_stop => 1);
$default_locker->query_safe(q[BEGIN; LOCK TABLE pg_default_acl IN ACCESS EXCLUSIVE MODE;]);
my $default_writer = $node->background_psql('dbbranch_default_drain_source', on_error_stop => 1);
$default_writer->query_until(
	qr/start_default_drain_default/,
	q(\echo start_default_drain_default
ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO dbbranch_default_reader;
\echo finish_default_drain_default
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_default_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER DEFAULT PRIVILEGES%';
]), 'active source alter default privileges waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_default_drain_target FROM DATABASE dbbranch_default_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter default privileges');
like($stderr, qr/source database "dbbranch_default_drain_source" has active write transactions/,
	'active source alter default privileges holds db branch writer gate');

$default_locker->query_safe(q[COMMIT;]);
$default_locker->quit;
$default_writer->query_until(qr/finish_default_drain_default/, '');
$default_writer->quit;

$node->safe_psql('dbbranch_default_drain_source', q[CREATE TABLE default_priv_rows (id int);]);
my $default_priv_applied = $node->safe_psql(
	'dbbranch_default_drain_source',
	q[SELECT has_table_privilege('dbbranch_default_reader', 'default_priv_rows', 'select');]);
is($default_priv_applied, 't', 'source alter default privileges finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_default_drain_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_default_reader;]);

$node->safe_psql('postgres', q[CREATE ROLE dbbranch_grant_reader;]);
$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_grant_drain_source;]);
$node->safe_psql(
	'dbbranch_grant_drain_source',
	q[
CREATE TABLE grant_rows (id int PRIMARY KEY);
INSERT INTO grant_rows VALUES (1);
CHECKPOINT;
]);

my $grant_locker = $node->background_psql('dbbranch_grant_drain_source', on_error_stop => 1);
$grant_locker->query_safe(q[BEGIN; LOCK TABLE grant_rows IN ACCESS EXCLUSIVE MODE;]);
my $grant_writer = $node->background_psql('dbbranch_grant_drain_source', on_error_stop => 1);
$grant_writer->query_until(
	qr/start_grant_drain_grant/,
	q(\echo start_grant_drain_grant
GRANT SELECT ON grant_rows TO dbbranch_grant_reader;
\echo finish_grant_drain_grant
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_grant_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'GRANT SELECT ON grant_rows%';
]), 'active source grant waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_grant_drain_target FROM DATABASE dbbranch_grant_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source grant');
like($stderr, qr/source database "dbbranch_grant_drain_source" has active write transactions/,
	'active source grant holds db branch writer gate');

$grant_locker->query_safe(q[COMMIT;]);
$grant_locker->quit;
$grant_writer->query_until(qr/finish_grant_drain_grant/, '');
$grant_writer->quit;

my $grant_applied = $node->safe_psql(
	'dbbranch_grant_drain_source',
	q[SELECT has_table_privilege('dbbranch_grant_reader', 'grant_rows', 'select');]);
is($grant_applied, 't', 'source grant finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_grant_drain_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_grant_reader;]);

$node->safe_psql('postgres', q[CREATE ROLE dbbranch_drop_owned_owner;]);
$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_drop_owned_drain_source;]);
$node->safe_psql(
	'dbbranch_drop_owned_drain_source',
	q[
CREATE TABLE drop_owned_rows (id int PRIMARY KEY);
ALTER TABLE drop_owned_rows OWNER TO dbbranch_drop_owned_owner;
CHECKPOINT;
]);

my $drop_owned_locker =
  $node->background_psql('dbbranch_drop_owned_drain_source', on_error_stop => 1);
$drop_owned_locker->query_safe(q[BEGIN; LOCK TABLE drop_owned_rows IN ACCESS SHARE MODE;]);
my $drop_owned_writer =
  $node->background_psql('dbbranch_drop_owned_drain_source', on_error_stop => 1);
$drop_owned_writer->query_until(
	qr/start_drop_owned_drain_drop_owned/,
	q(\echo start_drop_owned_drain_drop_owned
DROP OWNED BY dbbranch_drop_owned_owner;
\echo finish_drop_owned_drain_drop_owned
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_drop_owned_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'DROP OWNED BY dbbranch_drop_owned_owner%';
]), 'active source drop owned waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_drop_owned_drain_target FROM DATABASE dbbranch_drop_owned_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source drop owned');
like($stderr, qr/source database "dbbranch_drop_owned_drain_source" has active write transactions/,
	'active source drop owned holds db branch writer gate');

$drop_owned_locker->query_safe(q[COMMIT;]);
$drop_owned_locker->quit;
$drop_owned_writer->query_until(qr/finish_drop_owned_drain_drop_owned/, '');
$drop_owned_writer->quit;

my $drop_owned_removed = $node->safe_psql(
	'dbbranch_drop_owned_drain_source',
	q[SELECT to_regclass('drop_owned_rows') IS NULL;]);
is($drop_owned_removed, 't', 'source drop owned finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_owned_drain_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_drop_owned_owner;]);

$node->safe_psql('postgres', q[CREATE ROLE dbbranch_reassign_owned_owner;]);
$node->safe_psql('postgres', q[CREATE ROLE dbbranch_reassign_owned_new_owner;]);
$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_reassign_owned_drain_source;]);
$node->safe_psql(
	'dbbranch_reassign_owned_drain_source',
	q[
CREATE TABLE reassign_owned_rows (id int PRIMARY KEY);
ALTER TABLE reassign_owned_rows OWNER TO dbbranch_reassign_owned_owner;
CHECKPOINT;
]);

my $reassign_owned_locker =
  $node->background_psql('dbbranch_reassign_owned_drain_source', on_error_stop => 1);
$reassign_owned_locker->query_safe(q[BEGIN; LOCK TABLE reassign_owned_rows IN ACCESS SHARE MODE;]);
my $reassign_owned_writer =
  $node->background_psql('dbbranch_reassign_owned_drain_source', on_error_stop => 1);
$reassign_owned_writer->query_until(
	qr/start_reassign_owned_drain_reassign/,
	q(\echo start_reassign_owned_drain_reassign
REASSIGN OWNED BY dbbranch_reassign_owned_owner TO dbbranch_reassign_owned_new_owner;
\echo finish_reassign_owned_drain_reassign
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_reassign_owned_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'REASSIGN OWNED BY dbbranch_reassign_owned_owner%';
]), 'active source reassign owned waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_reassign_owned_drain_target FROM DATABASE dbbranch_reassign_owned_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source reassign owned');
like($stderr, qr/source database "dbbranch_reassign_owned_drain_source" has active write transactions/,
	'active source reassign owned holds db branch writer gate');

$reassign_owned_locker->query_safe(q[COMMIT;]);
$reassign_owned_locker->quit;
$reassign_owned_writer->query_until(qr/finish_reassign_owned_drain_reassign/, '');
$reassign_owned_writer->quit;

my $reassign_owned_changed = $node->safe_psql(
	'dbbranch_reassign_owned_drain_source',
	q[SELECT relowner = 'dbbranch_reassign_owned_new_owner'::regrole FROM pg_class WHERE relname = 'reassign_owned_rows';]);
is($reassign_owned_changed, 't', 'source reassign owned finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_reassign_owned_drain_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_reassign_owned_owner;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_reassign_owned_new_owner;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_comment_drain_source;]);
$node->safe_psql(
	'dbbranch_comment_drain_source',
	q[
CREATE TABLE comment_rows (id int PRIMARY KEY);
INSERT INTO comment_rows VALUES (1);
CHECKPOINT;
]);

my $comment_locker = $node->background_psql('dbbranch_comment_drain_source', on_error_stop => 1);
$comment_locker->query_safe(q[BEGIN; LOCK TABLE comment_rows IN ACCESS EXCLUSIVE MODE;]);
my $comment_writer = $node->background_psql('dbbranch_comment_drain_source', on_error_stop => 1);
$comment_writer->query_until(
	qr/start_comment_drain_comment/,
	q(\echo start_comment_drain_comment
COMMENT ON TABLE comment_rows IS 'branch comment';
\echo finish_comment_drain_comment
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_comment_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'COMMENT ON TABLE comment_rows%';
]), 'active source comment waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_comment_drain_target FROM DATABASE dbbranch_comment_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source comment');
like($stderr, qr/source database "dbbranch_comment_drain_source" has active write transactions/,
	'active source comment holds db branch writer gate');

$comment_locker->query_safe(q[COMMIT;]);
$comment_locker->quit;
$comment_writer->query_until(qr/finish_comment_drain_comment/, '');
$comment_writer->quit;

my $comment_applied = $node->safe_psql(
	'dbbranch_comment_drain_source',
	q[SELECT obj_description('comment_rows'::regclass);]);
is($comment_applied, 'branch comment', 'source comment finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_comment_drain_source;]);

$node->safe_psql(
	'postgres',
	q[
CREATE ROLE dbbranch_comment_role_waiter;
CREATE DATABASE dbbranch_comment_role_drain_source;
]);
$node->safe_psql('dbbranch_comment_role_drain_source', q[CHECKPOINT;]);

my $comment_role_writer =
  $node->background_psql('dbbranch_comment_role_drain_source', on_error_stop => 1);
$comment_role_writer->query_safe(
	q[BEGIN; COMMENT ON ROLE dbbranch_comment_role_waiter IS 'branch role comment';]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_comment_role_drain_target FROM DATABASE dbbranch_comment_role_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source comment role');
like($stderr, qr/source database "dbbranch_comment_role_drain_source" has active write transactions/,
	'active source comment role holds db branch writer gate');

$comment_role_writer->query_safe(q[COMMIT;]);
$comment_role_writer->quit;

my $comment_role_applied = $node->safe_psql(
	'postgres',
	q[SELECT shobj_description('dbbranch_comment_role_waiter'::regrole, 'pg_authid');]);
is($comment_role_applied, 'branch role comment',
	'source comment role finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_comment_role_drain_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_comment_role_waiter;]);

$node->safe_psql(
	'postgres',
	q[
CREATE ROLE dbbranch_seclabel_role_waiter;
CREATE DATABASE dbbranch_seclabel_role_drain_source;
]);
$node->safe_psql('dbbranch_seclabel_role_drain_source', q[CHECKPOINT;]);

my $seclabel_role_writer =
  $node->background_psql('dbbranch_seclabel_role_drain_source', on_error_stop => 1);
$seclabel_role_writer->query_safe(
	q[LOAD 'dummy_seclabel';]);
$seclabel_role_writer->query_safe(
	q[BEGIN; SECURITY LABEL FOR 'dummy' ON ROLE dbbranch_seclabel_role_waiter IS 'classified';]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_seclabel_role_drain_target FROM DATABASE dbbranch_seclabel_role_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source security label role');
like($stderr, qr/source database "dbbranch_seclabel_role_drain_source" has active write transactions/,
	'active source security label role holds db branch writer gate');

$seclabel_role_writer->query_safe(q[COMMIT;]);
$seclabel_role_writer->quit;

my $seclabel_role_applied = $node->safe_psql(
	'postgres',
	q[SELECT label FROM pg_shseclabel WHERE objoid = 'dbbranch_seclabel_role_waiter'::regrole AND provider = 'dummy';]);
is($seclabel_role_applied, 'classified',
	'source security label role finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_seclabel_role_drain_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_seclabel_role_waiter;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_copy_drain_source;]);
$node->safe_psql(
	'dbbranch_copy_drain_source',
	q[
CREATE TABLE copy_rows (id int PRIMARY KEY);
INSERT INTO copy_rows VALUES (1);
CHECKPOINT;
]);

my $copy_locker = $node->background_psql('dbbranch_copy_drain_source', on_error_stop => 1);
$copy_locker->query_safe(q[BEGIN; LOCK TABLE copy_rows IN ACCESS EXCLUSIVE MODE;]);
my $copy_writer = $node->background_psql('dbbranch_copy_drain_source', on_error_stop => 1);
$copy_writer->query_until(
	qr/start_copy_drain_copy/,
	q(\echo start_copy_drain_copy
COPY copy_rows FROM STDIN;
2
\.
\echo finish_copy_drain_copy
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_copy_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'COPY copy_rows%';
]), 'active source copy from waits on source table lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_copy_drain_target FROM DATABASE dbbranch_copy_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source copy from');
like($stderr, qr/source database "dbbranch_copy_drain_source" has active write transactions/,
	'active source copy from holds db branch writer gate');

$copy_locker->query_safe(q[COMMIT;]);
$copy_locker->quit;
$copy_writer->query_until(qr/finish_copy_drain_copy/, '');
$copy_writer->quit;

my $copy_rows = $node->safe_psql(
	'dbbranch_copy_drain_source',
	q[SELECT count(*) FROM copy_rows;]);
is($copy_rows, '2', 'source copy from finishes after db branch rejects');
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_copy_drain_source;]);

SKIP:
{
	skip 'Injection points not supported by this build', 151
	  if ($ENV{enable_injection_points} // '') ne 'yes'
	  || !$node->check_extension('injection_points');

	$node->safe_psql('postgres', q[CREATE EXTENSION IF NOT EXISTS injection_points;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_createdb_drain_source;]);
	$node->safe_psql('dbbranch_createdb_drain_source', q[CHECKPOINT;]);
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-create-database', 'wait');]);
	my $createdb_writer =
	  $node->background_psql('dbbranch_createdb_drain_source', on_error_stop => 1);
	$createdb_writer->query_until(
		qr/start_createdb_drain_createdb/,
		q(\echo start_createdb_drain_createdb
CREATE DATABASE dbbranch_createdb_created;
\echo finish_createdb_drain_createdb
));
	$node->wait_for_event('client backend', 'db-branch-create-database');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_createdb_drain_target FROM DATABASE dbbranch_createdb_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source create database');
	like($stderr, qr/source database "dbbranch_createdb_drain_source" has active write transactions/,
		'active source create database holds db branch writer gate');

	$node->safe_psql('postgres',
		q[SELECT injection_points_wakeup('db-branch-create-database');]);
	$createdb_writer->query_until(qr/finish_createdb_drain_createdb/, '');
	$createdb_writer->quit;
	my $created_db_exists = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_createdb_created';]);
	is($created_db_exists, '1',
		'source create database finishes after db branch rejects');
	$node->safe_psql('postgres',
		q[SELECT injection_points_detach('db-branch-create-database');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_createdb_created;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_createdb_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_dropdb_drain_source;]);
	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_dropdb_dropped;]);
	$node->safe_psql('dbbranch_dropdb_drain_source', q[CHECKPOINT;]);
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-drop-database', 'wait');]);
	my $dropdb_writer =
	  $node->background_psql('dbbranch_dropdb_drain_source', on_error_stop => 1);
	$dropdb_writer->query_until(
		qr/start_dropdb_drain_dropdb/,
		q(\echo start_dropdb_drain_dropdb
DROP DATABASE dbbranch_dropdb_dropped;
\echo finish_dropdb_drain_dropdb
));
	$node->wait_for_event('client backend', 'db-branch-drop-database');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_dropdb_drain_target FROM DATABASE dbbranch_dropdb_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source drop database');
	like($stderr, qr/source database "dbbranch_dropdb_drain_source" has active write transactions/,
		'active source drop database holds db branch writer gate');

	$node->safe_psql('postgres',
		q[SELECT injection_points_wakeup('db-branch-drop-database');]);
	$dropdb_writer->query_until(qr/finish_dropdb_drain_dropdb/, '');
	$dropdb_writer->quit;
	my $dropped_db_exists = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_dropdb_dropped';]);
	is($dropped_db_exists, '0',
		'source drop database finishes after db branch rejects');
	$node->safe_psql('postgres',
		q[SELECT injection_points_detach('db-branch-drop-database');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_dropdb_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_create_tspc_drain_source;]);
	$node->safe_psql('dbbranch_create_tspc_drain_source', q[CHECKPOINT;]);
	my $create_tspc_dir = $node->basedir . '/dbbranch_create_tspc';
	mkdir($create_tspc_dir) or die "could not create $create_tspc_dir: $!";
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-create-tablespace', 'wait');]);
	my $create_tspc_writer =
	  $node->background_psql('dbbranch_create_tspc_drain_source', on_error_stop => 1);
	$create_tspc_writer->query_until(
		qr/start_create_tspc_drain_tspc/,
		qq(\\echo start_create_tspc_drain_tspc
CREATE TABLESPACE dbbranch_create_tspc LOCATION '$create_tspc_dir';
\\echo finish_create_tspc_drain_tspc
));
	$node->wait_for_event('client backend', 'db-branch-create-tablespace');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_create_tspc_drain_target FROM DATABASE dbbranch_create_tspc_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source create tablespace');
	like($stderr, qr/source database "dbbranch_create_tspc_drain_source" has active write transactions/,
		'active source create tablespace holds db branch writer gate');

	$node->safe_psql('postgres',
		q[SELECT injection_points_wakeup('db-branch-create-tablespace');]);
	$create_tspc_writer->query_until(qr/finish_create_tspc_drain_tspc/, '');
	$create_tspc_writer->quit;
	my $created_tspc_exists = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_tablespace WHERE spcname = 'dbbranch_create_tspc';]);
	is($created_tspc_exists, '1',
		'source create tablespace finishes after db branch rejects');
	$node->safe_psql('postgres',
		q[SELECT injection_points_detach('db-branch-create-tablespace');]);
	$node->safe_psql('postgres', q[DROP TABLESPACE dbbranch_create_tspc;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_tspc_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_drop_tspc_drain_source;]);
	my $drop_tspc_dir = $node->basedir . '/dbbranch_drop_tspc';
	mkdir($drop_tspc_dir) or die "could not create $drop_tspc_dir: $!";
	$node->safe_psql(
		'dbbranch_drop_tspc_drain_source',
		"CREATE TABLESPACE dbbranch_drop_tspc LOCATION '$drop_tspc_dir';");
	$node->safe_psql('dbbranch_drop_tspc_drain_source', q[CHECKPOINT;]);
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-drop-tablespace', 'wait');]);
	my $drop_tspc_writer =
	  $node->background_psql('dbbranch_drop_tspc_drain_source', on_error_stop => 1);
	$drop_tspc_writer->query_until(
		qr/start_drop_tspc_drain_tspc/,
		q(\echo start_drop_tspc_drain_tspc
DROP TABLESPACE dbbranch_drop_tspc;
\echo finish_drop_tspc_drain_tspc
));
	$node->wait_for_event('client backend', 'db-branch-drop-tablespace');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_drop_tspc_drain_target FROM DATABASE dbbranch_drop_tspc_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source drop tablespace');
	like($stderr, qr/source database "dbbranch_drop_tspc_drain_source" has active write transactions/,
		'active source drop tablespace holds db branch writer gate');

	$node->safe_psql('postgres',
		q[SELECT injection_points_wakeup('db-branch-drop-tablespace');]);
	$drop_tspc_writer->query_until(qr/finish_drop_tspc_drain_tspc/, '');
	$drop_tspc_writer->quit;
	my $dropped_tspc_exists = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_tablespace WHERE spcname = 'dbbranch_drop_tspc';]);
	is($dropped_tspc_exists, '0',
		'source drop tablespace finishes after db branch rejects');
	$node->safe_psql('postgres',
		q[SELECT injection_points_detach('db-branch-drop-tablespace');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_tspc_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_import_schema_drain_source;]);
	my $node_port = $node->port;
	$node->safe_psql(
		'dbbranch_import_schema_drain_source',
		qq[
CREATE EXTENSION postgres_fdw;
CREATE SCHEMA import_src;
CREATE TABLE import_src.import_rows (id int PRIMARY KEY);
CREATE SCHEMA import_dest;
CREATE SERVER dbbranch_import_server FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'dbbranch_import_schema_drain_source', port '$node_port');
CREATE USER MAPPING FOR CURRENT_USER SERVER dbbranch_import_server;
CHECKPOINT;
]);

	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-import-foreign-schema', 'wait');]);
	my $import_schema_writer =
	  $node->background_psql('dbbranch_import_schema_drain_source', on_error_stop => 1);
	$import_schema_writer->query_until(
		qr/start_import_schema_drain_import/,
		q(\echo start_import_schema_drain_import
IMPORT FOREIGN SCHEMA import_src LIMIT TO (import_rows) FROM SERVER dbbranch_import_server INTO import_dest;
\echo finish_import_schema_drain_import
));
	$node->wait_for_event('client backend', 'db-branch-import-foreign-schema');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_import_schema_drain_target FROM DATABASE dbbranch_import_schema_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source import foreign schema');
	like($stderr, qr/source database "dbbranch_import_schema_drain_source" has active write transactions/,
		'active source import foreign schema holds db branch writer gate');

	$node->safe_psql('postgres',
		q[SELECT injection_points_wakeup('db-branch-import-foreign-schema');]);
	$import_schema_writer->query_until(qr/finish_import_schema_drain_import/, '');
	$import_schema_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_import_schema_drain_target FROM DATABASE dbbranch_import_schema_drain_source]);
	my $import_schema_table_exists = $node->safe_psql(
		'dbbranch_import_schema_drain_target',
		q[
SELECT count(*)
FROM pg_foreign_table ft
JOIN pg_class c ON c.oid = ft.ftrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'import_dest'
  AND c.relname = 'import_rows';
]);
	is($import_schema_table_exists, '1',
		'branch succeeds after source import foreign schema drains');

	$node->safe_psql('postgres',
		q[SELECT injection_points_detach('db-branch-import-foreign-schema');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_import_schema_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_import_schema_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_idle_drain_source;]);
	$node->safe_psql(
		'dbbranch_idle_drain_source',
		q[
CREATE TABLE idle_rows (id int PRIMARY KEY);
CREATE SEQUENCE idle_seq CACHE 1;
CREATE UNLOGGED SEQUENCE idle_unlogged_seq CACHE 1;
INSERT INTO idle_rows VALUES (1);
CHECKPOINT;
]);

	my $idle_reader = $node->background_psql('dbbranch_idle_drain_source', on_error_stop => 0);
	$idle_reader->query_safe(q[SELECT 1;]);
	$idle_reader->query_safe(q[SELECT nextval('idle_seq');]);
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-before-drain', 'wait');]);

	my $idle_branch = $node->background_psql('postgres', on_error_stop => 1);
	$idle_branch->query_until(
		qr/start_idle_drain_branch/,
		q(\echo start_idle_drain_branch
CREATE BRANCH dbbranch_idle_drain_target FROM DATABASE dbbranch_idle_drain_source;
\echo finish_idle_drain_branch
));
	$node->wait_for_event('client backend', 'db-branch-before-drain');
	my (undef, $idle_seq_ret) = $idle_reader->query(q[
SET statement_timeout = '500ms';
SELECT nextval('idle_seq');
]);
	is($idle_seq_ret, 1, 'db branch writer gate blocks existing source sequence writes');
	like($idle_reader->{stderr}, qr/canceling statement due to statement timeout/,
		'existing source sequence write waits on db branch writer gate');
	my (undef, $idle_setval_ret) = $idle_reader->query(q[
SELECT setval('idle_unlogged_seq', 7);
]);
	is($idle_setval_ret, 1, 'db branch writer gate blocks existing source sequence setval');
	like($idle_reader->{stderr}, qr/canceling statement due to statement timeout/,
		'existing source sequence setval waits on db branch writer gate');

	$node->safe_psql('postgres', q[SELECT injection_points_wakeup('db-branch-before-drain');]);
	$idle_branch->query_until(qr/finish_idle_drain_branch/, '');
	$idle_branch->quit;

	my $idle_branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_idle_drain_target';]);
	is($idle_branch_count, '1', 'db branch does not wait for idle source backend to leave');

	my $idle_rows = $node->safe_psql(
		'dbbranch_idle_drain_target',
		q[SELECT count(*) FROM idle_rows;]);
	is($idle_rows, '1', 'idle-drained branch reads source rows');

	$idle_reader->quit;
	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-before-drain');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_idle_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_idle_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_create_drain_source;]);
	$node->safe_psql(
		'dbbranch_create_drain_source',
		q[
CREATE TABLE create_base_rows (id int PRIMARY KEY);
INSERT INTO create_base_rows VALUES (1);
CHECKPOINT;
]);
	my $create_writer = $node->background_psql('dbbranch_create_drain_source', on_error_stop => 1);
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-before-clone', 'wait');]);

	my $create_branch = $node->background_psql('postgres', on_error_stop => 1);
	$create_branch->query_until(
		qr/start_create_drain_branch/,
		q(\echo start_create_drain_branch
CREATE BRANCH dbbranch_create_drain_target FROM DATABASE dbbranch_create_drain_source;
\echo finish_create_drain_branch
	));
	$node->wait_for_event('client backend', 'db-branch-before-clone');

	$create_writer->query_until(
		qr/start_create_drain_create/,
		q(\echo start_create_drain_create
CREATE TABLE created_during_branch (id int PRIMARY KEY);
\echo finish_create_drain_create
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_create_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE TABLE created_during_branch%';
]), 'source create table waits on db branch writer gate');

	$node->safe_psql('postgres', q[SELECT injection_points_wakeup('db-branch-before-clone');]);
	$create_branch->query_until(qr/finish_create_drain_branch/, '');
	$create_branch->quit;
	$create_writer->query_until(qr/finish_create_drain_create/, '');
	$create_writer->quit;

	my $source_created_table = $node->safe_psql(
		'dbbranch_create_drain_source',
		q[SELECT to_regclass('public.created_during_branch') IS NOT NULL;]);
	is($source_created_table, 't', 'source create table finishes after db branch releases gate');
	my $branch_created_table = $node->safe_psql(
		'dbbranch_create_drain_target',
		q[SELECT to_regclass('public.created_during_branch') IS NULL;]);
	is($branch_created_table, 't', 'branch excludes source table created after clone started');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-before-clone');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_refresh_drain_source;]);
	$node->safe_psql(
		'dbbranch_refresh_drain_source',
		q[
CREATE TABLE refresh_rows (id int PRIMARY KEY, note text);
INSERT INTO refresh_rows VALUES (1, 'old');
CREATE MATERIALIZED VIEW refresh_mv AS SELECT * FROM refresh_rows;
INSERT INTO refresh_rows VALUES (2, 'new');
CHECKPOINT;
]);

	my $refresh_locker = $node->background_psql('dbbranch_refresh_drain_source', on_error_stop => 1);
	$refresh_locker->query_safe(q[BEGIN; SELECT count(*) FROM refresh_mv;]);
	my $refresh_writer = $node->background_psql('dbbranch_refresh_drain_source', on_error_stop => 1);
	$refresh_writer->query_until(
		qr/start_refresh_drain_refresh/,
		q(\echo start_refresh_drain_refresh
REFRESH MATERIALIZED VIEW refresh_mv;
\echo finish_refresh_drain_refresh
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_refresh_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'REFRESH MATERIALIZED VIEW%';
]), 'active source refresh materialized view waits on source matview lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_refresh_drain_target FROM DATABASE dbbranch_refresh_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source refresh materialized view');
	like($stderr, qr/source database "dbbranch_refresh_drain_source" has active write transactions/,
		'active source refresh materialized view holds db branch writer gate');

	$refresh_locker->query_safe(q[COMMIT;]);
	$refresh_locker->quit;
	$refresh_writer->query_until(qr/finish_refresh_drain_refresh/, '');
	$refresh_writer->quit;
	$node->safe_psql('dbbranch_refresh_drain_source', q[CHECKPOINT;]);

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_refresh_drain_target FROM DATABASE dbbranch_refresh_drain_source]);
	my $refresh_rows = $node->safe_psql(
		'dbbranch_refresh_drain_target',
		q[SELECT count(*) FROM refresh_mv;]);
	is($refresh_rows, '2', 'branch succeeds after source refresh materialized view drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_refresh_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_refresh_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_drain_source;]);
	$node->safe_psql(
		'dbbranch_drain_source',
		q[
CREATE TABLE drain_rows (id int PRIMARY KEY);
INSERT INTO drain_rows VALUES (1);
CHECKPOINT;
]);

	my $drain_writer = $node->background_psql('dbbranch_drain_source', on_error_stop => 1);
	$drain_writer->query_safe(q[BEGIN; INSERT INTO drain_rows VALUES (2);]);
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-before-drain', 'wait');]);

	my $drain_branch = $node->background_psql('postgres', on_error_stop => 1);
	$drain_branch->query_until(
		qr/start_drain_branch/,
		q(\echo start_drain_branch
CREATE BRANCH dbbranch_drain_target FROM DATABASE dbbranch_drain_source;
\echo finish_drain_branch
));
	usleep(200_000);
	my $drain_branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_drain_target';]);
	is($drain_branch_count, '0', 'db branch waits for active source writer');

	$drain_writer->query_safe('COMMIT;');
	$drain_writer->quit;
	$node->wait_for_event('client backend', 'db-branch-before-drain');
	my $freeze_timed_out = 0;
	$node->psql(
		'dbbranch_drain_source',
		q[SELECT 1],
		timeout => 1,
		timed_out => \$freeze_timed_out);
	ok($freeze_timed_out, 'db branch freeze gate blocks new source connections');

	$node->safe_psql('postgres', q[SELECT injection_points_wakeup('db-branch-before-drain');]);
	$drain_branch->query_until(qr/finish_drain_branch/, '');
	$drain_branch->quit;

	my $drain_rows = $node->safe_psql(
		'dbbranch_drain_target',
		q[SELECT string_agg(id::text, ',' ORDER BY id) FROM drain_rows;]);
	is($drain_rows, '1,2', 'db branch waits for a short source writer to drain');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-before-drain');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_vacuum_drain_source;]);
	$node->safe_psql(
		'dbbranch_vacuum_drain_source',
		q[
CREATE TABLE vacuum_rows (id int PRIMARY KEY, note text) WITH (autovacuum_enabled = false);
INSERT INTO vacuum_rows SELECT g, repeat('x', 100) FROM generate_series(1, 50) g;
DELETE FROM vacuum_rows WHERE id <= 25;
CHECKPOINT;
]);

	my $vacuum_locker = $node->background_psql('dbbranch_vacuum_drain_source', on_error_stop => 1);
	$vacuum_locker->query_safe(q[BEGIN; LOCK TABLE vacuum_rows IN ACCESS EXCLUSIVE MODE;]);
	my $vacuum_writer = $node->background_psql('dbbranch_vacuum_drain_source', on_error_stop => 1);
	$vacuum_writer->query_until(
		qr/start_vacuum_drain_vacuum/,
		q(\echo start_vacuum_drain_vacuum
VACUUM (INDEX_CLEANUP ON) vacuum_rows;
\echo finish_vacuum_drain_vacuum
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_vacuum_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'VACUUM%';
]), 'active source vacuum waits on source table lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_vacuum_drain_target FROM DATABASE dbbranch_vacuum_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source vacuum');
	like($stderr, qr/source database "dbbranch_vacuum_drain_source" has active write transactions/,
		'active source vacuum holds db branch writer gate');

	$vacuum_locker->query_safe(q[COMMIT;]);
	$vacuum_locker->quit;
	$vacuum_writer->query_until(qr/finish_vacuum_drain_vacuum/, '');
	$vacuum_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_vacuum_drain_target FROM DATABASE dbbranch_vacuum_drain_source]);
	my $vacuum_rows = $node->safe_psql(
		'dbbranch_vacuum_drain_target',
		q[SELECT count(*) FROM vacuum_rows;]);
	is($vacuum_rows, '25', 'branch succeeds after source vacuum drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_vacuum_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_vacuum_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_cluster_drain_source;]);
	$node->safe_psql(
		'dbbranch_cluster_drain_source',
		q[
CREATE TABLE cluster_rows (id int PRIMARY KEY, note text);
INSERT INTO cluster_rows SELECT g, repeat('x', 100) FROM generate_series(1, 25) g;
CHECKPOINT;
]);

	my $cluster_locker = $node->background_psql('dbbranch_cluster_drain_source', on_error_stop => 1);
	$cluster_locker->query_safe(q[BEGIN; LOCK TABLE cluster_rows IN ACCESS EXCLUSIVE MODE;]);
	my $cluster_writer = $node->background_psql('dbbranch_cluster_drain_source', on_error_stop => 1);
	$cluster_writer->query_until(
		qr/start_cluster_drain_cluster/,
		q(\echo start_cluster_drain_cluster
CLUSTER cluster_rows USING cluster_rows_pkey;
\echo finish_cluster_drain_cluster
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_cluster_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CLUSTER%';
]), 'active source cluster waits on source table lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_cluster_drain_target FROM DATABASE dbbranch_cluster_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source cluster');
	like($stderr, qr/source database "dbbranch_cluster_drain_source" has active write transactions/,
		'active source cluster holds db branch writer gate');

	$cluster_locker->query_safe(q[COMMIT;]);
	$cluster_locker->quit;
	$cluster_writer->query_until(qr/finish_cluster_drain_cluster/, '');
	$cluster_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_cluster_drain_target FROM DATABASE dbbranch_cluster_drain_source]);
	my $cluster_rows = $node->safe_psql(
		'dbbranch_cluster_drain_target',
		q[SELECT count(*) FROM cluster_rows;]);
	is($cluster_rows, '25', 'branch succeeds after source cluster drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_cluster_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_cluster_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_reindex_drain_source;]);
	$node->safe_psql(
		'dbbranch_reindex_drain_source',
		q[
CREATE TABLE reindex_rows (id int PRIMARY KEY, note text);
INSERT INTO reindex_rows SELECT g, repeat('x', 100) FROM generate_series(1, 25) g;
CHECKPOINT;
]);

	my $reindex_locker = $node->background_psql('dbbranch_reindex_drain_source', on_error_stop => 1);
	$reindex_locker->query_safe(q[BEGIN; LOCK TABLE reindex_rows IN ACCESS EXCLUSIVE MODE;]);
	my $reindex_writer = $node->background_psql('dbbranch_reindex_drain_source', on_error_stop => 1);
	$reindex_writer->query_until(
		qr/start_reindex_drain_reindex/,
		q(\echo start_reindex_drain_reindex
REINDEX TABLE reindex_rows;
\echo finish_reindex_drain_reindex
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_reindex_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'REINDEX%';
]), 'active source reindex waits on source table lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_reindex_drain_target FROM DATABASE dbbranch_reindex_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source reindex');
	like($stderr, qr/source database "dbbranch_reindex_drain_source" has active write transactions/,
		'active source reindex holds db branch writer gate');

	$reindex_locker->query_safe(q[COMMIT;]);
	$reindex_locker->quit;
	$reindex_writer->query_until(qr/finish_reindex_drain_reindex/, '');
	$reindex_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_reindex_drain_target FROM DATABASE dbbranch_reindex_drain_source]);
	my $reindex_rows = $node->safe_psql(
		'dbbranch_reindex_drain_target',
		q[SELECT count(*) FROM reindex_rows;]);
	is($reindex_rows, '25', 'branch succeeds after source reindex drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_reindex_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_reindex_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_truncate_drain_source;]);
	$node->safe_psql(
		'dbbranch_truncate_drain_source',
		q[
CREATE TABLE truncate_rows (id int PRIMARY KEY, note text);
INSERT INTO truncate_rows SELECT g, repeat('x', 100) FROM generate_series(1, 25) g;
CHECKPOINT;
]);

	my $truncate_locker = $node->background_psql('dbbranch_truncate_drain_source', on_error_stop => 1);
	$truncate_locker->query_safe(q[BEGIN; LOCK TABLE truncate_rows IN ACCESS EXCLUSIVE MODE;]);
	my $truncate_writer = $node->background_psql('dbbranch_truncate_drain_source', on_error_stop => 1);
	$truncate_writer->query_until(
		qr/start_truncate_drain_truncate/,
		q(\echo start_truncate_drain_truncate
TRUNCATE truncate_rows;
\echo finish_truncate_drain_truncate
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_truncate_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'TRUNCATE%';
]), 'active source truncate waits on source table lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_truncate_drain_target FROM DATABASE dbbranch_truncate_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source truncate');
	like($stderr, qr/source database "dbbranch_truncate_drain_source" has active write transactions/,
		'active source truncate holds db branch writer gate');

	$truncate_locker->query_safe(q[COMMIT;]);
	$truncate_locker->quit;
	$truncate_writer->query_until(qr/finish_truncate_drain_truncate/, '');
	$truncate_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_truncate_drain_target FROM DATABASE dbbranch_truncate_drain_source]);
	my $truncate_rows = $node->safe_psql(
		'dbbranch_truncate_drain_target',
		q[SELECT count(*) FROM truncate_rows;]);
	is($truncate_rows, '0', 'branch succeeds after source truncate drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_truncate_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_truncate_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_drain_source;]);
	$node->safe_psql(
		'dbbranch_alter_drain_source',
		q[
CREATE TABLE alter_rows (id int PRIMARY KEY, note text);
INSERT INTO alter_rows SELECT g, repeat('x', 100) FROM generate_series(1, 25) g;
CHECKPOINT;
]);

	my $alter_locker = $node->background_psql('dbbranch_alter_drain_source', on_error_stop => 1);
	$alter_locker->query_safe(q[BEGIN; LOCK TABLE alter_rows IN ACCESS SHARE MODE;]);
	my $alter_writer = $node->background_psql('dbbranch_alter_drain_source', on_error_stop => 1);
	$alter_writer->query_until(
		qr/start_alter_drain_alter/,
		q(\echo start_alter_drain_alter
ALTER TABLE alter_rows ADD COLUMN added text;
\echo finish_alter_drain_alter
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER TABLE%';
]), 'active source alter table waits on source table lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_alter_drain_target FROM DATABASE dbbranch_alter_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source alter table');
	like($stderr, qr/source database "dbbranch_alter_drain_source" has active write transactions/,
		'active source alter table holds db branch writer gate');

	$alter_locker->query_safe(q[COMMIT;]);
	$alter_locker->quit;
	$alter_writer->query_until(qr/finish_alter_drain_alter/, '');
	$alter_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_alter_drain_target FROM DATABASE dbbranch_alter_drain_source]);
	my $alter_column_count = $node->safe_psql(
		'dbbranch_alter_drain_target',
		q[SELECT count(*) FROM information_schema.columns WHERE table_name = 'alter_rows' AND column_name = 'added';]);
	is($alter_column_count, '1', 'branch succeeds after source alter table drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_index_drain_source;]);
	$node->safe_psql(
		'dbbranch_index_drain_source',
		q[
CREATE TABLE index_rows (id int PRIMARY KEY, note text);
INSERT INTO index_rows SELECT g, repeat('x', 100) FROM generate_series(1, 25) g;
CHECKPOINT;
]);

	my $index_locker = $node->background_psql('dbbranch_index_drain_source', on_error_stop => 1);
	$index_locker->query_safe(q[BEGIN; LOCK TABLE index_rows IN ROW EXCLUSIVE MODE;]);
	my $index_writer = $node->background_psql('dbbranch_index_drain_source', on_error_stop => 1);
	$index_writer->query_until(
		qr/start_index_drain_index/,
		q(\echo start_index_drain_index
CREATE INDEX index_rows_note_idx ON index_rows (note);
\echo finish_index_drain_index
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_index_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE INDEX%';
]), 'active source create index waits on source table lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_index_drain_target FROM DATABASE dbbranch_index_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source create index');
	like($stderr, qr/source database "dbbranch_index_drain_source" has active write transactions/,
		'active source create index holds db branch writer gate');

	$index_locker->query_safe(q[COMMIT;]);
	$index_locker->quit;
	$index_writer->query_until(qr/finish_index_drain_index/, '');
	$index_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_index_drain_target FROM DATABASE dbbranch_index_drain_source]);
	my $index_exists = $node->safe_psql(
		'dbbranch_index_drain_target',
		q[SELECT to_regclass('public.index_rows_note_idx') IS NOT NULL;]);
	is($index_exists, 't', 'branch succeeds after source create index drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_index_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_index_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_stats_drain_source;]);
	$node->safe_psql(
		'dbbranch_stats_drain_source',
		q[
CREATE TABLE stats_rows (id int, grp int);
INSERT INTO stats_rows SELECT g, g % 3 FROM generate_series(1, 25) g;
CHECKPOINT;
]);

	my $stats_locker = $node->background_psql('dbbranch_stats_drain_source', on_error_stop => 1);
	$stats_locker->query_safe(q[BEGIN; LOCK TABLE stats_rows IN SHARE UPDATE EXCLUSIVE MODE;]);
	my $stats_writer = $node->background_psql('dbbranch_stats_drain_source', on_error_stop => 1);
	$stats_writer->query_until(
		qr/start_stats_drain_stats/,
		q(\echo start_stats_drain_stats
CREATE STATISTICS stats_rows_id_grp ON id, grp FROM stats_rows;
\echo finish_stats_drain_stats
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_stats_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE STATISTICS stats_rows_id_grp%';
]), 'active source create statistics waits on source table lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_stats_drain_target FROM DATABASE dbbranch_stats_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source create statistics');
	like($stderr, qr/source database "dbbranch_stats_drain_source" has active write transactions/,
		'active source create statistics holds db branch writer gate');

	$stats_locker->query_safe(q[COMMIT;]);
	$stats_locker->quit;
	$stats_writer->query_until(qr/finish_stats_drain_stats/, '');
	$stats_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_stats_drain_target FROM DATABASE dbbranch_stats_drain_source]);
	my $stats_exists = $node->safe_psql(
		'dbbranch_stats_drain_target',
		q[SELECT count(*) FROM pg_statistic_ext WHERE stxname = 'stats_rows_id_grp';]);
	is($stats_exists, '1', 'branch succeeds after source create statistics drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_stats_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_stats_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_stats_drain_source;]);
	$node->safe_psql(
		'dbbranch_alter_stats_drain_source',
		q[
CREATE TABLE alter_stats_rows (id int, grp int);
INSERT INTO alter_stats_rows SELECT g, g % 3 FROM generate_series(1, 25) g;
CREATE STATISTICS alter_stats_rows_id_grp ON id, grp FROM alter_stats_rows;
CHECKPOINT;
]);

	my $alter_stats_locker = $node->background_psql('dbbranch_alter_stats_drain_source', on_error_stop => 1);
	$alter_stats_locker->query_safe(q[BEGIN; LOCK TABLE pg_statistic_ext IN ACCESS EXCLUSIVE MODE;]);
	my $alter_stats_writer = $node->background_psql('dbbranch_alter_stats_drain_source', on_error_stop => 1);
	$alter_stats_writer->query_until(
		qr/start_alter_stats_drain_stats/,
		q(\echo start_alter_stats_drain_stats
ALTER STATISTICS alter_stats_rows_id_grp SET STATISTICS 2;
\echo finish_alter_stats_drain_stats
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_stats_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER STATISTICS alter_stats_rows_id_grp%';
]), 'active source alter statistics waits on source catalog lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_alter_stats_drain_target FROM DATABASE dbbranch_alter_stats_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source alter statistics');
	like($stderr, qr/source database "dbbranch_alter_stats_drain_source" has active write transactions/,
		'active source alter statistics holds db branch writer gate');

	$alter_stats_locker->query_safe(q[COMMIT;]);
	$alter_stats_locker->quit;
	$alter_stats_writer->query_until(qr/finish_alter_stats_drain_stats/, '');
	$alter_stats_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_alter_stats_drain_target FROM DATABASE dbbranch_alter_stats_drain_source]);
	my $alter_stats_target = $node->safe_psql(
		'dbbranch_alter_stats_drain_target',
		q[SELECT stxstattarget FROM pg_statistic_ext WHERE stxname = 'alter_stats_rows_id_grp';]);
	is($alter_stats_target, '2', 'branch succeeds after source alter statistics drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_stats_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_stats_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_create_publication_drain_source;]);
	$node->safe_psql('dbbranch_create_publication_drain_source', q[CHECKPOINT;]);

	my $create_publication_writer = $node->background_psql('dbbranch_create_publication_drain_source', on_error_stop => 1);
	my $create_publication_locker = $node->background_psql('dbbranch_create_publication_drain_source', on_error_stop => 1);
	$create_publication_locker->query_safe(q[BEGIN; LOCK TABLE pg_publication IN ACCESS EXCLUSIVE MODE;]);
	$create_publication_writer->query_until(
		qr/start_create_publication_drain_publication/,
		q(\echo start_create_publication_drain_publication
CREATE PUBLICATION dbbranch_create_pub;
\echo finish_create_publication_drain_publication
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_create_publication_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE PUBLICATION dbbranch_create_pub%';
]), 'active source create publication waits on source catalog lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_create_publication_drain_target FROM DATABASE dbbranch_create_publication_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source create publication');
	like($stderr, qr/source database "dbbranch_create_publication_drain_source" has active write transactions/,
		'active source create publication holds db branch writer gate');

	$create_publication_locker->query_safe(q[COMMIT;]);
	$create_publication_locker->quit;
	$create_publication_writer->query_until(qr/finish_create_publication_drain_publication/, '');
	$create_publication_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_create_publication_drain_target FROM DATABASE dbbranch_create_publication_drain_source]);
	my $create_publication_exists = $node->safe_psql(
		'dbbranch_create_publication_drain_target',
		q[SELECT count(*) FROM pg_publication WHERE pubname = 'dbbranch_create_pub';]);
	is($create_publication_exists, '1', 'branch succeeds after source create publication drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_publication_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_publication_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_publication_drain_source;]);
	$node->safe_psql(
		'dbbranch_publication_drain_source',
		q[
CREATE TABLE publication_rows (id int PRIMARY KEY);
CREATE PUBLICATION publication_rows_pub;
INSERT INTO publication_rows VALUES (1);
CHECKPOINT;
]);

	my $publication_locker = $node->background_psql('dbbranch_publication_drain_source', on_error_stop => 1);
	$publication_locker->query_safe(q[BEGIN; LOCK TABLE publication_rows IN SHARE UPDATE EXCLUSIVE MODE;]);
	my $publication_writer = $node->background_psql('dbbranch_publication_drain_source', on_error_stop => 1);
	$publication_writer->query_until(
		qr/start_publication_drain_publication/,
		q(\echo start_publication_drain_publication
ALTER PUBLICATION publication_rows_pub ADD TABLE publication_rows;
\echo finish_publication_drain_publication
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_publication_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER PUBLICATION publication_rows_pub%';
]), 'active source alter publication waits on source table lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_publication_drain_target FROM DATABASE dbbranch_publication_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source alter publication');
	like($stderr, qr/source database "dbbranch_publication_drain_source" has active write transactions/,
		'active source alter publication holds db branch writer gate');

	$publication_locker->query_safe(q[COMMIT;]);
	$publication_locker->quit;
	$publication_writer->query_until(qr/finish_publication_drain_publication/, '');
	$publication_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_publication_drain_target FROM DATABASE dbbranch_publication_drain_source]);
	my $publication_rel_exists = $node->safe_psql(
		'dbbranch_publication_drain_target',
		q[SELECT count(*) FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid WHERE p.pubname = 'publication_rows_pub';]);
	is($publication_rel_exists, '1', 'branch succeeds after source alter publication drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_publication_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_publication_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_drop_drain_source;]);
	$node->safe_psql(
		'dbbranch_drop_drain_source',
		q[
CREATE TABLE drop_rows (id int PRIMARY KEY, note text);
INSERT INTO drop_rows SELECT g, repeat('x', 100) FROM generate_series(1, 25) g;
CHECKPOINT;
]);

	my $drop_locker = $node->background_psql('dbbranch_drop_drain_source', on_error_stop => 1);
	$drop_locker->query_safe(q[BEGIN; LOCK TABLE drop_rows IN ACCESS SHARE MODE;]);
	my $drop_writer = $node->background_psql('dbbranch_drop_drain_source', on_error_stop => 1);
	$drop_writer->query_until(
		qr/start_drop_drain_drop/,
		q(\echo start_drop_drain_drop
DROP TABLE drop_rows;
\echo finish_drop_drain_drop
));
	ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_drop_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'DROP TABLE%';
]), 'active source drop table waits on source table lock');

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_drop_drain_target FROM DATABASE dbbranch_drop_drain_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports active source drop table');
	like($stderr, qr/source database "dbbranch_drop_drain_source" has active write transactions/,
		'active source drop table holds db branch writer gate');

	$drop_locker->query_safe(q[COMMIT;]);
	$drop_locker->quit;
	$drop_writer->query_until(qr/finish_drop_drain_drop/, '');
	$drop_writer->quit;

	$node->safe_psql(
		'postgres',
		q[CREATE BRANCH dbbranch_drop_drain_target FROM DATABASE dbbranch_drop_drain_source]);
	my $drop_table_missing = $node->safe_psql(
		'dbbranch_drop_drain_target',
		q[SELECT to_regclass('public.drop_rows') IS NULL;]);
	is($drop_table_missing, 't', 'branch succeeds after source drop table drains');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_drain_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_drain_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_clone_fail_source;]);
	$node->safe_psql(
		'dbbranch_clone_fail_source',
		q[
CREATE TABLE clone_fail_rows (id int PRIMARY KEY);
INSERT INTO clone_fail_rows VALUES (1);
CHECKPOINT;
UPDATE clone_fail_rows SET id = 1 WHERE id = 1;
]);

	my %clone_base_before = map { $_ => 1 } glob $node->data_dir . '/base/*';
	my @clone_metadata_before = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-before-clone', 'error');]);

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_clone_fail_target FROM DATABASE dbbranch_clone_fail_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports injected clone setup failure');
	like($stderr, qr/db-branch-before-clone/,
		'db branch surfaces clone setup injection failure');

	my $clone_fail_branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_clone_fail_target';]);
	is($clone_fail_branch_count, '0', 'clone setup failure creates no branch database');

	my $clone_fail_slot_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($clone_fail_slot_count, '0', 'clone setup failure releases DB Branch WAL pin');

	my @clone_paths_left = grep { !$clone_base_before{$_} } glob $node->data_dir . '/base/*';
	is(scalar @clone_paths_left, 0, 'clone setup failure creates no branch storage path');

	my $clone_source_rows = $node->safe_psql(
		'dbbranch_clone_fail_source',
		q[
INSERT INTO clone_fail_rows VALUES (2);
SELECT count(*) FROM clone_fail_rows;
]);
	is($clone_source_rows, '2', 'source accepts writes after clone setup failure');

	my @clone_metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	is(scalar @clone_metadata_files, scalar(@clone_metadata_before) + 1,
		'clone setup failure writes separate metadata file');

	my $clone_metadata = '';
	for my $path (@clone_metadata_files)
	{
		open my $fh, '<', $path or die "could not open $path: $!";
		my $contents = do { local $/; <$fh> };
		close $fh;
		if ($contents =~ /^branch_name=dbbranch_clone_fail_target$/m)
		{
			$clone_metadata = $contents;
			last;
		}
	}

	like($clone_metadata, qr/^wal_pin=released$/m,
		'clone setup failure metadata records released WAL pin');
	like($clone_metadata, qr/^redo_ptr=[0-9A-F]+\/[0-9A-F]+$/m,
		'clone setup failure metadata records redo pointer');
	unlike($clone_metadata, qr/^redo_ptr=0\/0$/m,
		'clone setup failure redo pointer is valid');
	like($clone_metadata, qr/^branch_lsn=[0-9A-F]+\/[0-9A-F]+$/m,
		'clone setup failure metadata records branch LSN');
	unlike($clone_metadata, qr/^branch_lsn=0\/0$/m,
		'clone setup failure branch LSN is valid');
	like($clone_metadata, qr/^clone_result=not_started$/m,
		'clone setup failure metadata records clone not started');
	like($clone_metadata, qr/^cleanup=not_started$/m,
		'clone setup failure metadata records cleanup not started');
	like($clone_metadata, qr/^replay_method=not_started$/m,
		'clone setup failure metadata records replay not started');
	like($clone_metadata, qr/^status_history=CREATING,FAILED$/m,
		'clone setup failure metadata records failed transition');
	like($clone_metadata, qr/^status=FAILED$/m,
		'clone setup failure metadata final state is FAILED');
	like($clone_metadata, qr/^failure=.+db-branch-before-clone.*$/m,
		'clone setup failure metadata records failure reason');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-before-clone');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_clone_fail_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_mixed_wal_source;]);
	$node->safe_psql(
		'dbbranch_mixed_wal_source',
		q[
CREATE TABLE mixed_wal_rows (id int PRIMARY KEY);
INSERT INTO mixed_wal_rows VALUES (1);
CHECKPOINT;
UPDATE mixed_wal_rows SET id = 1 WHERE id = 1;
]);

	my %mixed_base_before = map { $_ => 1 } glob $node->data_dir . '/base/*';
	my @mixed_metadata_before = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-after-wal-scan', 'increment-uint64');]);

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_mixed_wal_target FROM DATABASE dbbranch_mixed_wal_source],
		stderr => \$stderr);
	is($result, 3, 'db branch rejects mixed WAL before clone');
	like($stderr, qr/DB Branch rmgr replay does not support mixed WAL records yet/,
		'db branch surfaces mixed WAL rejection');

	my $mixed_branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_mixed_wal_target';]);
	is($mixed_branch_count, '0', 'mixed WAL rejection creates no branch database');

	my $mixed_slot_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($mixed_slot_count, '0', 'mixed WAL rejection releases DB Branch WAL pin');

	my @mixed_paths_left = grep { !$mixed_base_before{$_} } glob $node->data_dir . '/base/*';
	is(scalar @mixed_paths_left, 0, 'mixed WAL rejection creates no branch storage path');

	my $mixed_source_rows = $node->safe_psql(
		'dbbranch_mixed_wal_source',
		q[
INSERT INTO mixed_wal_rows VALUES (2);
SELECT count(*) FROM mixed_wal_rows;
]);
	is($mixed_source_rows, '2', 'source accepts writes after mixed WAL rejection');

	my @mixed_metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	is(scalar @mixed_metadata_files, scalar(@mixed_metadata_before) + 1,
		'mixed WAL rejection writes separate metadata file');

	my $mixed_metadata = '';
	for my $path (@mixed_metadata_files)
	{
		open my $fh, '<', $path or die "could not open $path: $!";
		my $contents = do { local $/; <$fh> };
		close $fh;
		if ($contents =~ /^branch_name=dbbranch_mixed_wal_target$/m)
		{
			$mixed_metadata = $contents;
			last;
		}
	}

	like($mixed_metadata, qr/^clone_result=not_started$/m,
		'mixed WAL rejection metadata records clone not started');
	like($mixed_metadata, qr/^cleanup=not_started$/m,
		'mixed WAL rejection metadata records cleanup not started');
	like($mixed_metadata, qr/^replay_method=not_started$/m,
		'mixed WAL rejection metadata records replay not started');
	like($mixed_metadata, qr/^wal_mixed_records=[1-9][0-9]*$/m,
		'mixed WAL rejection metadata records mixed WAL');
	like($mixed_metadata, qr/^failure=DB Branch rmgr replay does not support mixed WAL records yet$/m,
		'mixed WAL rejection metadata records failure reason');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-after-wal-scan');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_mixed_wal_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_clone_mid_fail_source;]);
	$node->safe_psql(
		'dbbranch_clone_mid_fail_source',
		q[
CREATE TABLE clone_mid_fail_rows (id int PRIMARY KEY);
INSERT INTO clone_mid_fail_rows VALUES (1);
CHECKPOINT;
UPDATE clone_mid_fail_rows SET id = 1 WHERE id = 1;
]);

	my %clone_mid_base_before = map { $_ => 1 } glob $node->data_dir . '/base/*';
	my @clone_mid_metadata_before = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-during-clone', 'error');]);

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_clone_mid_fail_target FROM DATABASE dbbranch_clone_mid_fail_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports injected clone failure');
	like($stderr, qr/db-branch-during-clone/,
		'db branch surfaces clone injection failure');

	my $clone_mid_branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_clone_mid_fail_target';]);
	is($clone_mid_branch_count, '0', 'clone failure creates no branch database');

	my $clone_mid_slot_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($clone_mid_slot_count, '0', 'clone failure releases DB Branch WAL pin');

	my @clone_mid_paths_left = grep { !$clone_mid_base_before{$_} } glob $node->data_dir . '/base/*';
	is(scalar @clone_mid_paths_left, 0, 'clone failure removes branch storage path');

	my $clone_mid_source_rows = $node->safe_psql(
		'dbbranch_clone_mid_fail_source',
		q[
INSERT INTO clone_mid_fail_rows VALUES (2);
SELECT count(*) FROM clone_mid_fail_rows;
]);
	is($clone_mid_source_rows, '2', 'source accepts writes after clone failure');

	my @clone_mid_metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	is(scalar @clone_mid_metadata_files, scalar(@clone_mid_metadata_before) + 1,
		'clone failure writes separate metadata file');

	my $clone_mid_metadata = '';
	for my $path (@clone_mid_metadata_files)
	{
		open my $fh, '<', $path or die "could not open $path: $!";
		my $contents = do { local $/; <$fh> };
		close $fh;
		if ($contents =~ /^branch_name=dbbranch_clone_mid_fail_target$/m)
		{
			$clone_mid_metadata = $contents;
			last;
		}
	}

	like($clone_mid_metadata, qr/^wal_pin=released$/m,
		'clone failure metadata records released WAL pin');
	like($clone_mid_metadata, qr/^clone_result=failed$/m,
		'clone failure metadata records clone failure');
	like($clone_mid_metadata, qr/^cleanup=done$/m,
		'clone failure metadata records clone cleanup');
	like($clone_mid_metadata, qr/^replay_method=not_started$/m,
		'clone failure metadata records replay not started');
	like($clone_mid_metadata, qr/^status_history=CREATING,COPYING,FAILED$/m,
		'clone failure metadata records failed transition');
	like($clone_mid_metadata, qr/^status=FAILED$/m,
		'clone failure metadata final state is FAILED');
	like($clone_mid_metadata, qr/^failure=.+db-branch-during-clone.*$/m,
		'clone failure metadata records failure reason');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-during-clone');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_clone_mid_fail_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_replay_gate_source;]);
	$node->safe_psql(
		'dbbranch_replay_gate_source',
		q[
CREATE TABLE gate_rows (id int PRIMARY KEY);
INSERT INTO gate_rows VALUES (1);
CHECKPOINT;
UPDATE gate_rows SET id = 1 WHERE id = 1;
]);
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-before-replay', 'wait');]);

	my $gate_branch = $node->background_psql('postgres', on_error_stop => 1);
	$gate_branch->query_until(
		qr/start_gate_branch/,
		q(\echo start_gate_branch
CREATE BRANCH dbbranch_replay_gate_target FROM DATABASE dbbranch_replay_gate_source;
\echo finish_gate_branch
));
	$node->wait_for_event('client backend', 'db-branch-before-replay');

	my $pin_active = $node->safe_psql(
		'postgres',
		q[SELECT count(*) = 1 AND bool_and(active) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($pin_active, 't', 'db branch WAL pin is active before replay');

	$node->safe_psql('dbbranch_replay_gate_source', q[INSERT INTO gate_rows VALUES (2);]);
	my $gate_source_rows = $node->safe_psql(
		'dbbranch_replay_gate_source',
		q[SELECT count(*) FROM gate_rows;]);
	is($gate_source_rows, '2', 'source accepts writes after clone before replay');

	$node->safe_psql('postgres', q[SELECT injection_points_wakeup('db-branch-before-replay');]);
	$gate_branch->query_until(qr/finish_gate_branch/, '');
	$gate_branch->quit;

	my $gate_branch_rows = $node->safe_psql(
		'dbbranch_replay_gate_target',
		q[SELECT count(*) FROM gate_rows;]);
	is($gate_branch_rows, '1', 'branch excludes source writes after branch LSN');

	my $gate_slot_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($gate_slot_count, '0', 'db branch releases WAL pin after replay resumes');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-before-replay');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_replay_gate_target;]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_replay_gate_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_drop_during_source;]);
	$node->safe_psql(
		'dbbranch_drop_during_source',
		q[
CREATE TABLE drop_during_rows (id int PRIMARY KEY);
INSERT INTO drop_during_rows VALUES (1);
CHECKPOINT;
UPDATE drop_during_rows SET id = 1 WHERE id = 1;
]);

	my %drop_during_base_before = map { $_ => 1 } glob $node->data_dir . '/base/*';
	my @drop_during_metadata_before = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-before-replay', 'wait');]);

	my $drop_during_branch = $node->background_psql('postgres', on_error_stop => 0);
	$drop_during_branch->query_until(
		qr/start_drop_during_branch/,
		q(\echo start_drop_during_branch
CREATE BRANCH dbbranch_drop_during_target FROM DATABASE dbbranch_drop_during_source;
\echo finish_drop_during_branch
));
	$node->wait_for_event('client backend', 'db-branch-before-replay');

	my @drop_during_clone_paths = grep { !$drop_during_base_before{$_} } glob $node->data_dir . '/base/*';
	is(scalar @drop_during_clone_paths, 1,
		'source-drop race has cloned branch storage before replay');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_during_source;]);
	$node->safe_psql('postgres', q[SELECT injection_points_wakeup('db-branch-before-replay');]);
	$drop_during_branch->query_until(qr/finish_drop_during_branch/, '');
	like(
		$drop_during_branch->{stderr},
		qr/source database "dbbranch_drop_during_source" was dropped while creating branch/,
		'db branch reports source drop during replay window');
	$drop_during_branch->quit;

	my $drop_during_branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_drop_during_target';]);
	is($drop_during_branch_count, '0', 'source-drop race creates no branch database');

	my $drop_during_slot_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($drop_during_slot_count, '0', 'source-drop race releases DB Branch WAL pin');

	my @drop_during_clone_left = grep { -e $_ } @drop_during_clone_paths;
	is(scalar @drop_during_clone_left, 0,
		'source-drop race removes cloned branch storage path');

	my $drop_during_catalog_rows = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_dbbranch WHERE branch_db_oid NOT IN (SELECT oid FROM pg_database) OR source_db_oid NOT IN (SELECT oid FROM pg_database);]);
	is($drop_during_catalog_rows, '0',
		'source-drop race leaves no orphan pg_dbbranch rows');

	my @drop_during_metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	is(scalar @drop_during_metadata_files, scalar(@drop_during_metadata_before) + 1,
		'source-drop race writes separate metadata file');

	my $drop_during_metadata = '';
	for my $path (@drop_during_metadata_files)
	{
		open my $fh, '<', $path or die "could not open $path: $!";
		my $contents = do { local $/; <$fh> };
		close $fh;
		if ($contents =~ /^branch_name=dbbranch_drop_during_target$/m)
		{
			$drop_during_metadata = $contents;
			last;
		}
	}

	like($drop_during_metadata, qr/^wal_pin=released$/m,
		'source-drop race metadata records released WAL pin');
	like($drop_during_metadata, qr/^clone_result=(done|copy_fallback)$/m,
		'source-drop race metadata records clone result');
	like($drop_during_metadata, qr/^cleanup=done$/m,
		'source-drop race metadata records clone cleanup');
	like($drop_during_metadata, qr/^replay_method=rmgr_redo$/m,
		'source-drop race metadata records replay method');
	like($drop_during_metadata, qr/^status_history=CREATING,COPYING,REPLAYING,FAILED$/m,
		'source-drop race metadata records failed transition');
	like($drop_during_metadata, qr/^status=FAILED$/m,
		'source-drop race metadata final state is FAILED');
	like($drop_during_metadata,
		qr/^failure=source database "dbbranch_drop_during_source" was dropped while creating branch$/m,
		'source-drop race metadata records failure reason');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-before-replay');]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_replay_fail_source;]);
	$node->safe_psql(
		'dbbranch_replay_fail_source',
		q[
CREATE TABLE replay_fail_rows (id int PRIMARY KEY);
INSERT INTO replay_fail_rows VALUES (1);
CHECKPOINT;
UPDATE replay_fail_rows SET id = 1 WHERE id = 1;
]);

	my %replay_base_before = map { $_ => 1 } glob $node->data_dir . '/base/*';
	my @replay_metadata_before = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-before-replay', 'error');]);

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_replay_fail_target FROM DATABASE dbbranch_replay_fail_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports injected replay failure');
	like($stderr, qr/db-branch-before-replay/,
		'db branch surfaces replay injection failure');

	my $replay_fail_branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_replay_fail_target';]);
	is($replay_fail_branch_count, '0', 'replay failure creates no branch database');

	my $replay_fail_slot_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($replay_fail_slot_count, '0', 'replay failure releases DB Branch WAL pin');

	my @replay_clone_left = grep { !$replay_base_before{$_} } glob $node->data_dir . '/base/*';
	is(scalar @replay_clone_left, 0, 'replay failure removes cloned branch storage path');

	my $replay_source_rows = $node->safe_psql(
		'dbbranch_replay_fail_source',
		q[
INSERT INTO replay_fail_rows VALUES (2);
SELECT count(*) FROM replay_fail_rows;
]);
	is($replay_source_rows, '2', 'source accepts writes after replay failure');

	my @replay_metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	is(scalar @replay_metadata_files, scalar(@replay_metadata_before) + 1,
		'replay failure writes separate metadata file');

	my $replay_metadata = '';
	for my $path (@replay_metadata_files)
	{
		open my $fh, '<', $path or die "could not open $path: $!";
		my $contents = do { local $/; <$fh> };
		close $fh;
		if ($contents =~ /^branch_name=dbbranch_replay_fail_target$/m)
		{
			$replay_metadata = $contents;
			last;
		}
	}

	like($replay_metadata, qr/^wal_pin=released$/m,
		'replay failure metadata records released WAL pin');
	like($replay_metadata, qr/^redo_ptr=[0-9A-F]+\/[0-9A-F]+$/m,
		'replay failure metadata records redo pointer');
	unlike($replay_metadata, qr/^redo_ptr=0\/0$/m,
		'replay failure redo pointer is valid');
	like($replay_metadata, qr/^branch_lsn=[0-9A-F]+\/[0-9A-F]+$/m,
		'replay failure metadata records branch LSN');
	unlike($replay_metadata, qr/^branch_lsn=0\/0$/m,
		'replay failure branch LSN is valid');
	like($replay_metadata, qr/^clone_result=(done|copy_fallback)$/m,
		'replay failure metadata records clone result');
	like($replay_metadata, qr/^cleanup=done$/m,
		'replay failure metadata records clone cleanup');
	like($replay_metadata, qr/^status_history=CREATING,COPYING,REPLAYING,FAILED$/m,
		'replay failure metadata records failed transition');
	like($replay_metadata, qr/^status=FAILED$/m,
		'replay failure metadata final state is FAILED');
	like($replay_metadata, qr/^failure=.+db-branch-before-replay.*$/m,
		'replay failure metadata records failure reason');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-before-replay');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_replay_fail_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_replay_redo_fail_source;]);
	$node->safe_psql(
		'dbbranch_replay_redo_fail_source',
		q[
CREATE TABLE replay_redo_fail_rows (id int PRIMARY KEY);
INSERT INTO replay_redo_fail_rows VALUES (1);
CHECKPOINT;
UPDATE replay_redo_fail_rows SET id = 1 WHERE id = 1;
]);

	my %replay_redo_base_before = map { $_ => 1 } glob $node->data_dir . '/base/*';
	my @replay_redo_metadata_before = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-during-replay', 'error');]);

	$stderr = '';
	$result = $node->psql(
		'postgres',
		q[CREATE BRANCH dbbranch_replay_redo_fail_target FROM DATABASE dbbranch_replay_redo_fail_source],
		stderr => \$stderr);
	is($result, 3, 'db branch reports injected rmgr replay failure');
	like($stderr, qr/db-branch-during-replay/,
		'db branch surfaces rmgr replay injection failure');

	my $replay_redo_branch_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_replay_redo_fail_target';]);
	is($replay_redo_branch_count, '0', 'rmgr replay failure creates no branch database');

	my $replay_redo_slot_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($replay_redo_slot_count, '0', 'rmgr replay failure releases DB Branch WAL pin');

	my @replay_redo_clone_left = grep { !$replay_redo_base_before{$_} } glob $node->data_dir . '/base/*';
	is(scalar @replay_redo_clone_left, 0, 'rmgr replay failure removes cloned branch storage path');

	my $replay_redo_source_rows = $node->safe_psql(
		'dbbranch_replay_redo_fail_source',
		q[
INSERT INTO replay_redo_fail_rows VALUES (2);
SELECT count(*) FROM replay_redo_fail_rows;
]);
	is($replay_redo_source_rows, '2', 'source accepts writes after rmgr replay failure');

	my @replay_redo_metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	is(scalar @replay_redo_metadata_files, scalar(@replay_redo_metadata_before) + 1,
		'rmgr replay failure writes separate metadata file');

	my $replay_redo_metadata = '';
	for my $path (@replay_redo_metadata_files)
	{
		open my $fh, '<', $path or die "could not open $path: $!";
		my $contents = do { local $/; <$fh> };
		close $fh;
		if ($contents =~ /^branch_name=dbbranch_replay_redo_fail_target$/m)
		{
			$replay_redo_metadata = $contents;
			last;
		}
	}

	like($replay_redo_metadata, qr/^wal_pin=released$/m,
		'rmgr replay failure metadata records released WAL pin');
	like($replay_redo_metadata, qr/^clone_result=(done|copy_fallback)$/m,
		'rmgr replay failure metadata records clone result');
	like($replay_redo_metadata, qr/^cleanup=done$/m,
		'rmgr replay failure metadata records clone cleanup');
	like($replay_redo_metadata, qr/^replay_method=rmgr_redo$/m,
		'rmgr replay failure metadata records replay method');
	like($replay_redo_metadata, qr/^wal_replayed_records=0$/m,
		'rmgr replay failure metadata records no replayed records');
	like($replay_redo_metadata, qr/^status_history=CREATING,COPYING,REPLAYING,FAILED$/m,
		'rmgr replay failure metadata records failed transition');
	like($replay_redo_metadata, qr/^status=FAILED$/m,
		'rmgr replay failure metadata final state is FAILED');
	like($replay_redo_metadata, qr/^failure=.+db-branch-during-replay.*$/m,
		'rmgr replay failure metadata records failure reason');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-during-replay');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_replay_redo_fail_source;]);

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_install_fail_source;]);
	$node->safe_psql(
		'dbbranch_install_fail_source',
		q[
CREATE TABLE install_fail_rows (id int PRIMARY KEY);
INSERT INTO install_fail_rows VALUES (1);
CHECKPOINT;
INSERT INTO install_fail_rows VALUES (2);
]);

	my %base_before = map { $_ => 1 } glob $node->data_dir . '/base/*';
	my @install_metadata_before = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	$node->safe_psql('postgres',
		q[SELECT injection_points_attach('db-branch-before-install', 'wait');]);

	my $install_branch = $node->background_psql('postgres', on_error_stop => 0);
	$install_branch->query_until(
		qr/start_install_branch/,
		q(\echo start_install_branch
CREATE BRANCH dbbranch_install_fail_target FROM DATABASE dbbranch_install_fail_source;
\echo finish_install_branch
));
	$node->wait_for_event('client backend', 'db-branch-before-install');
	my @install_clone_paths = grep { !$base_before{$_} } glob $node->data_dir . '/base/*';
	is(scalar @install_clone_paths, 1, 'install failure creates one branch storage path before catalog install');

	$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_install_fail_target;]);
	$node->safe_psql('postgres', q[SELECT injection_points_wakeup('db-branch-before-install');]);
	$install_branch->query_until(qr/finish_install_branch/, '');
	like(
		$install_branch->{stderr},
		qr/(duplicate key value violates unique constraint|already exists)/,
		'db branch install failure is reported to the client');
	$install_branch->quit;

	my $install_slot_count = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'dbbranch_%';]);
	is($install_slot_count, '0', 'install failure releases DB Branch WAL pin');

	$node->safe_psql('postgres', q[SELECT injection_points_detach('db-branch-before-install');]);
	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_install_fail_target;]);

	my @install_clone_left = grep { -e $_ } @install_clone_paths;
	is(scalar @install_clone_left, 0, 'install failure removes cloned branch storage path');

	my $install_catalog_rows = $node->safe_psql(
		'postgres',
		q[SELECT count(*) FROM pg_dbbranch WHERE branch_db_oid NOT IN (SELECT oid FROM pg_database);]);
	is($install_catalog_rows, '0', 'install failure leaves no orphan pg_dbbranch rows');

	my $install_source_rows = $node->safe_psql(
		'dbbranch_install_fail_source',
		q[
INSERT INTO install_fail_rows VALUES (3);
SELECT count(*) FROM install_fail_rows;
]);
	is($install_source_rows, '3', 'source accepts writes after install failure');

	my @install_metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
	is(scalar @install_metadata_files, scalar(@install_metadata_before) + 1,
		'install failure writes separate metadata file');

	my $install_metadata = '';
	for my $path (@install_metadata_files)
	{
		open my $fh, '<', $path or die "could not open $path: $!";
		my $contents = do { local $/; <$fh> };
		close $fh;
		if ($contents =~ /^branch_name=dbbranch_install_fail_target$/m)
		{
			$install_metadata = $contents;
			last;
		}
	}

	like($install_metadata, qr/^wal_pin=released$/m,
		'install failure metadata records released WAL pin');
	like($install_metadata, qr/^cleanup=done$/m,
		'install failure metadata records clone cleanup');
	like($install_metadata, qr/^status_history=CREATING,COPYING,REPLAYING,FAILED$/m,
		'install failure metadata records failed transition');
	like($install_metadata, qr/^status=FAILED$/m,
		'install failure metadata final state is FAILED');
	like($install_metadata, qr/^failure=.+$/m,
		'install failure metadata records failure reason');

	$node->safe_psql('postgres', q[DROP DATABASE dbbranch_install_fail_source;]);
}

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count = scalar @metadata_files;

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
is(scalar @metadata_files, $metadata_file_count, 'transaction-block rejection writes no metadata file');

$node->safe_psql('postgres', q[
CREATE ROLE dbbranch_no_createdb LOGIN;
CREATE ROLE dbbranch_createdb LOGIN CREATEDB;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_no_createdb_target FROM DATABASE dbbranch_source],
	stderr => \$stderr,
	extra_params => [ '--username' => 'dbbranch_no_createdb' ]);

is($result, 3, 'db branch requires CREATEDB privilege');
like(
	$stderr,
	qr/permission denied to create database/,
	'db branch reports missing CREATEDB privilege');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_not_owner_target FROM DATABASE dbbranch_source],
	stderr => \$stderr,
	extra_params => [ '--username' => 'dbbranch_createdb' ]);

is($result, 3, 'db branch requires source database ownership');
like(
	$stderr,
	qr/permission denied to copy database "dbbranch_source"/,
	'db branch reports missing source ownership');

my $permission_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname IN ('dbbranch_no_createdb_target', 'dbbranch_not_owner_target');]);
is($permission_branch_count, '0', 'permission failures create no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, $metadata_file_count, 'permission failures write no metadata file');

$node->safe_psql('postgres', q[DROP ROLE dbbranch_no_createdb, dbbranch_createdb;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_existing_target;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_existing_target FROM DATABASE dbbranch_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects an existing target database name');
like(
	$stderr,
	qr/database "dbbranch_existing_target" already exists/,
	'db branch reports existing target database name');

$node->safe_psql('postgres', q[
CREATE DATABASE dbbranch_no_conn_source;
ALTER DATABASE dbbranch_no_conn_source WITH ALLOW_CONNECTIONS false;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_no_conn_target FROM DATABASE dbbranch_no_conn_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects source database with disabled connections');
like(
	$stderr,
	qr/source database "dbbranch_no_conn_source" is not accepting connections/,
	'db branch reports source allow-connection restriction');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_missing_target FROM DATABASE dbbranch_missing_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects missing source database');
like(
	$stderr,
	qr/source database "dbbranch_missing_source" does not exist/,
	'db branch reports missing source database');

my $control_reject_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname IN ('dbbranch_no_conn_target', 'dbbranch_missing_target')]);
is($control_reject_branch_count, '0', 'control validation failures create no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, $metadata_file_count, 'control validation failures write no metadata file');

$node->safe_psql('postgres', q[
DROP DATABASE dbbranch_existing_target;
ALTER DATABASE dbbranch_no_conn_source WITH ALLOW_CONNECTIONS true;
DROP DATABASE dbbranch_no_conn_source;
]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_collversion_source;]);
$node->safe_psql(
	'postgres',
	q[UPDATE pg_database SET datcollversion = 'dbbranch-bogus-version' WHERE datname = 'dbbranch_collversion_source';]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_collversion_target FROM DATABASE dbbranch_collversion_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects source database with collation version mismatch');
like(
	$stderr,
	qr/source database "dbbranch_collversion_source" has a collation version/,
	'db branch reports source collation version mismatch');

my $collversion_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_collversion_target';]);
is($collversion_branch_count, '0', 'collation version rejection creates no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, $metadata_file_count, 'collation version rejection writes no metadata file');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_collversion_source;]);

my $func_lookup = $node->safe_psql(
	'postgres',
	q[SELECT to_regprocedure('pg_create_database_branch(name,name)') IS NULL;]);
is($func_lookup, 't', 'CREATE BRANCH has no SQL wrapper function');

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_alter_database_drain_source;]);
$node->safe_psql('dbbranch_alter_database_drain_source', q[CHECKPOINT;]);

my $alter_database_set_writer =
  $node->background_psql('postgres', on_error_stop => 1);
$alter_database_set_writer->query_safe(
	q[BEGIN; ALTER DATABASE dbbranch_alter_database_drain_source SET work_mem = '65MB';]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_database_set_drain_target FROM DATABASE dbbranch_alter_database_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter database set');
like($stderr, qr/source database "dbbranch_alter_database_drain_source" has active write transactions/,
	'active source alter database set holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source alter database set writes separate metadata file');

$alter_database_set_writer->query_safe(q[COMMIT;]);
$alter_database_set_writer->quit;

my $alter_database_work_mem = $node->safe_psql(
	'dbbranch_alter_database_drain_source',
	q[SHOW work_mem;]);
is($alter_database_work_mem, '65MB',
	'source alter database set finishes after db branch rejects');

my $alter_database_writer =
  $node->background_psql('postgres', on_error_stop => 1);
$alter_database_writer->query_safe(
	q[BEGIN; ALTER DATABASE dbbranch_alter_database_drain_source CONNECTION LIMIT 8;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_database_drain_target FROM DATABASE dbbranch_alter_database_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter database');
like($stderr, qr/source database "dbbranch_alter_database_drain_source" has active write transactions/,
	'active source alter database holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source alter database writes separate metadata file');

$alter_database_writer->query_safe(q[COMMIT;]);
$alter_database_writer->quit;

my $alter_database_connlimit = $node->safe_psql(
	'postgres',
	q[SELECT datconnlimit FROM pg_database WHERE datname = 'dbbranch_alter_database_drain_source';]);
is($alter_database_connlimit, '8',
	'source alter database finishes after db branch rejects');

$node->safe_psql(
	'postgres',
	q[
ALTER DATABASE dbbranch_alter_database_drain_source RESET work_mem;
ALTER DATABASE dbbranch_alter_database_drain_source CONNECTION LIMIT -1;
DROP DATABASE dbbranch_alter_database_drain_source;
]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_database_rename_drain_source;]);
$node->safe_psql('dbbranch_database_rename_drain_source', q[CHECKPOINT;]);

my $database_rename_writer =
  $node->background_psql('postgres', on_error_stop => 1);
$database_rename_writer->query_safe(
	q[BEGIN; ALTER DATABASE dbbranch_database_rename_drain_source RENAME TO dbbranch_database_rename_drain_source_renamed;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_database_rename_drain_target FROM DATABASE dbbranch_database_rename_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source database rename');
like($stderr, qr/source database "dbbranch_database_rename_drain_source" has active write transactions/,
	'active source database rename holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source database rename writes separate metadata file');

$database_rename_writer->query_safe(q[COMMIT;]);
$database_rename_writer->quit;

my $database_rename_applied = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_database_rename_drain_source_renamed';]);
is($database_rename_applied, '1',
	'source database rename finishes after db branch rejects');

$node->safe_psql(
	'postgres',
	q[
ALTER DATABASE dbbranch_database_rename_drain_source_renamed
	RENAME TO dbbranch_database_rename_drain_source;
DROP DATABASE dbbranch_database_rename_drain_source;
]);

$node->safe_psql('postgres', 'CREATE ROLE dbbranch_database_grant_drain_role LOGIN;');
$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_database_grant_drain_source;]);
$node->safe_psql('dbbranch_database_grant_drain_source', q[CHECKPOINT;]);

my $database_grant_writer =
  $node->background_psql('postgres', on_error_stop => 1);
$database_grant_writer->query_safe(
	q[BEGIN; GRANT CONNECT ON DATABASE dbbranch_database_grant_drain_source TO dbbranch_database_grant_drain_role;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_database_grant_drain_target FROM DATABASE dbbranch_database_grant_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source database grant');
like($stderr, qr/source database "dbbranch_database_grant_drain_source" has active write transactions/,
	'active source database grant holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source database grant writes separate metadata file');

$database_grant_writer->query_safe(q[COMMIT;]);
$database_grant_writer->quit;

my $database_grant_applied = $node->safe_psql(
	'postgres',
	q[SELECT has_database_privilege('dbbranch_database_grant_drain_role', 'dbbranch_database_grant_drain_source', 'connect');]);
is($database_grant_applied, 't',
	'source database grant finishes after db branch rejects');

$node->safe_psql(
	'postgres',
	q[
REVOKE CONNECT ON DATABASE dbbranch_database_grant_drain_source FROM dbbranch_database_grant_drain_role;
DROP DATABASE dbbranch_database_grant_drain_source;
DROP ROLE dbbranch_database_grant_drain_role;
]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_database_comment_drain_source;]);
$node->safe_psql('dbbranch_database_comment_drain_source', q[CHECKPOINT;]);

my $database_comment_writer =
  $node->background_psql('postgres', on_error_stop => 1);
$database_comment_writer->query_safe(
	q[BEGIN; COMMENT ON DATABASE dbbranch_database_comment_drain_source IS 'branch database comment';]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_database_comment_drain_target FROM DATABASE dbbranch_database_comment_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source database comment');
like($stderr, qr/source database "dbbranch_database_comment_drain_source" has active write transactions/,
	'active source database comment holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source database comment writes separate metadata file');

$database_comment_writer->query_safe(q[COMMIT;]);
$database_comment_writer->quit;

my $database_comment_applied = $node->safe_psql(
	'postgres',
	q[SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = 'dbbranch_database_comment_drain_source';]);
is($database_comment_applied, 'branch database comment',
	'source database comment finishes after db branch rejects');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_database_comment_drain_source;]);

$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_database_seclabel_drain_source;]);
$node->safe_psql('dbbranch_database_seclabel_drain_source', q[CHECKPOINT;]);

my $database_seclabel_writer =
  $node->background_psql('postgres', on_error_stop => 1);
$database_seclabel_writer->query_safe(q[LOAD 'dummy_seclabel';]);
$database_seclabel_writer->query_safe(
	q[BEGIN; SECURITY LABEL FOR 'dummy' ON DATABASE dbbranch_database_seclabel_drain_source IS 'classified';]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_database_seclabel_drain_target FROM DATABASE dbbranch_database_seclabel_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source database security label');
like($stderr, qr/source database "dbbranch_database_seclabel_drain_source" has active write transactions/,
	'active source database security label holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source database security label writes separate metadata file');

$database_seclabel_writer->query_safe(q[COMMIT;]);
$database_seclabel_writer->quit;

my $database_seclabel_applied = $node->safe_psql(
	'postgres',
	q[SELECT label FROM pg_shseclabel WHERE classoid = 'pg_database'::regclass AND objoid = (SELECT oid FROM pg_database WHERE datname = 'dbbranch_database_seclabel_drain_source') AND provider = 'dummy';]);
is($database_seclabel_applied, 'classified',
	'source database security label finishes after db branch rejects');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_database_seclabel_drain_source;]);

$node->safe_psql('postgres', 'CREATE ROLE dbbranch_role_setting_drain_role LOGIN;');
$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_role_setting_drain_source;]);
$node->safe_psql('dbbranch_role_setting_drain_source', q[CHECKPOINT;]);

my $role_setting_writer =
  $node->background_psql('postgres', on_error_stop => 1);
$role_setting_writer->query_safe(
	q[BEGIN; ALTER ROLE dbbranch_role_setting_drain_role IN DATABASE dbbranch_role_setting_drain_source SET maintenance_work_mem = '33MB';]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_role_setting_drain_target FROM DATABASE dbbranch_role_setting_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source role-in-database setting');
like($stderr, qr/source database "dbbranch_role_setting_drain_source" has active write transactions/,
	'active source role-in-database setting holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source role-in-database setting writes separate metadata file');

$role_setting_writer->query_safe(q[COMMIT;]);
$role_setting_writer->quit;

my $role_setting_work_mem = $node->safe_psql(
	'dbbranch_role_setting_drain_source',
	q[SHOW maintenance_work_mem;],
	extra_params => [ '--username' => 'dbbranch_role_setting_drain_role' ]);
is($role_setting_work_mem, '33MB',
	'source role-in-database setting finishes after db branch rejects');

$node->safe_psql(
	'postgres',
	q[
ALTER ROLE dbbranch_role_setting_drain_role IN DATABASE dbbranch_role_setting_drain_source RESET maintenance_work_mem;
DROP DATABASE dbbranch_role_setting_drain_source;
DROP ROLE dbbranch_role_setting_drain_role;
]);

$node->safe_psql('postgres', 'CREATE ROLE dbbranch_setting_role LOGIN;');
$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_setting_source;');
$node->safe_psql(
	'postgres',
	q[
ALTER DATABASE dbbranch_setting_source SET work_mem = '64MB';
ALTER DATABASE dbbranch_setting_source CONNECTION LIMIT 7;
REVOKE CONNECT ON DATABASE dbbranch_setting_source FROM PUBLIC;
GRANT CONNECT ON DATABASE dbbranch_setting_source TO dbbranch_setting_role;
ALTER ROLE dbbranch_setting_role IN DATABASE dbbranch_setting_source
	SET maintenance_work_mem = '32MB';
]);
$node->safe_psql(
	'dbbranch_setting_source',
	q[
CREATE TABLE setting_rows (id int PRIMARY KEY);
INSERT INTO setting_rows VALUES (1);
CREATE FUNCTION branch_login_proc() RETURNS event_trigger AS $$
BEGIN
  NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
CREATE EVENT TRIGGER branch_login_trigger
	ON login EXECUTE PROCEDURE branch_login_proc();
ALTER EVENT TRIGGER branch_login_trigger ENABLE ALWAYS;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_setting_target FROM DATABASE dbbranch_setting_source],
	stderr => \$stderr);

is($result, 0, 'db branch copies database-level settings');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count, 'settings branch writes separate metadata file');

my $setting_login_flag = $node->safe_psql(
	'postgres',
	q[
SELECT s.dathasloginevt::text || '|' || t.dathasloginevt::text
FROM pg_database s, pg_database t
WHERE s.datname = 'dbbranch_setting_source'
  AND t.datname = 'dbbranch_setting_target';
]);
is($setting_login_flag, 'true|true', 'branch copies source login event flag');

my $setting_login_trigger = $node->safe_psql(
	'dbbranch_setting_target',
	q[
SELECT count(*)
FROM pg_event_trigger
WHERE evtname = 'branch_login_trigger'
  AND evtevent = 'login'
  AND evtenabled = 'A';
]);
is($setting_login_trigger, '1', 'branch copies source login event trigger');

my $setting_work_mem = $node->safe_psql(
	'dbbranch_setting_target',
	q[SHOW work_mem;]);
is($setting_work_mem, '64MB', 'branch applies copied database-level setting');

my $setting_branch_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_setting_target';]);
my $setting_connlimit = $node->safe_psql(
	'postgres',
	q[SELECT datconnlimit FROM pg_database WHERE datname = 'dbbranch_setting_target';]);
is($setting_connlimit, '7', 'branch copies source connection limit');

my $setting_acl = $node->safe_psql(
	'postgres',
	q[SELECT (s.datacl IS NOT NULL) || '|' || (t.datacl IS NULL)
FROM pg_database s, pg_database t
WHERE s.datname = 'dbbranch_setting_source'
  AND t.datname = 'dbbranch_setting_target';]);
is($setting_acl, 'true|true', 'branch keeps default database ACL instead of copying source ACL');

my $setting_role_work_mem = $node->safe_psql(
	'dbbranch_setting_target',
	q[SHOW maintenance_work_mem;],
	extra_params => [ '--username' => 'dbbranch_setting_role' ]);
is($setting_role_work_mem, '32MB', 'branch applies copied role-in-database setting');

my $setting_rows = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_db_role_setting WHERE setdatabase = ]
	  . $setting_branch_oid
	  . q[;]);
is($setting_rows, '2', 'branch has copied database and role-in-database settings');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_setting_target;]);
my $setting_rows_after_drop = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_db_role_setting WHERE setdatabase = ]
	  . $setting_branch_oid
	  . q[;]);
is($setting_rows_after_drop, '0', 'dropping branch removes copied database settings');

$node->safe_psql(
	'postgres',
	q[ALTER DATABASE dbbranch_setting_source RESET work_mem;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_setting_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_setting_role;]);

my $tablespace_options_dir = $node->basedir . '/dbbranch_tablespace_options';
mkdir($tablespace_options_dir) or die "could not create $tablespace_options_dir: $!";
$node->safe_psql('postgres', "CREATE TABLESPACE dbbranch_options_ts LOCATION '$tablespace_options_dir';");
$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_tablespace_options_drain_source;]);
$node->safe_psql('dbbranch_tablespace_options_drain_source', q[CHECKPOINT;]);

my $tablespace_options_writer =
  $node->background_psql('dbbranch_tablespace_options_drain_source', on_error_stop => 1);
$tablespace_options_writer->query_safe(
	q[BEGIN; ALTER TABLESPACE dbbranch_options_ts SET (random_page_cost = 1.7);]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_tablespace_options_drain_target FROM DATABASE dbbranch_tablespace_options_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter tablespace options');
like($stderr, qr/source database "dbbranch_tablespace_options_drain_source" has active write transactions/,
	'active source alter tablespace options holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source alter tablespace options writes separate metadata file');

$tablespace_options_writer->query_safe(q[COMMIT;]);
$tablespace_options_writer->quit;

my $tablespace_option_applied = $node->safe_psql(
	'postgres',
	q[SELECT spcoptions @> ARRAY['random_page_cost=1.7'] FROM pg_tablespace WHERE spcname = 'dbbranch_options_ts';]);
is($tablespace_option_applied, 't',
	'source alter tablespace options finishes after db branch rejects');

$node->safe_psql('postgres', q[ALTER TABLESPACE dbbranch_options_ts RESET (random_page_cost);]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_tablespace_options_drain_source;]);
$node->safe_psql('postgres', q[DROP TABLESPACE dbbranch_options_ts;]);

my $tablespace_dir = $node->basedir . '/dbbranch_ts';
mkdir($tablespace_dir) or die "could not create $tablespace_dir: $!";
$node->safe_psql('postgres', "CREATE TABLESPACE dbbranch_ts LOCATION '$tablespace_dir';");
$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_ts_source TABLESPACE dbbranch_ts;');
$node->safe_psql(
	'dbbranch_ts_source',
	q[
CREATE TABLE ts_rows (id int PRIMARY KEY, name text NOT NULL);
INSERT INTO ts_rows VALUES (1, 'ts');
CHECKPOINT;
INSERT INTO ts_rows VALUES (2, 'redo');
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_ts_target FROM DATABASE dbbranch_ts_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports non-default source tablespace');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count, 'tablespace branch writes separate metadata file');

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

like($tablespace_metadata, qr/^wal_range_bytes=[1-9][0-9]*$/m, 'tablespace metadata records WAL range');
like($tablespace_metadata, qr/^wal_records_scanned=[1-9][0-9]*$/m, 'tablespace metadata records scanned WAL count');
like($tablespace_metadata, qr/^wal_source_records=[1-9][0-9]*$/m, 'tablespace metadata records source WAL count');
like($tablespace_metadata, qr/^clone_path=pg_tblspc\/[0-9]+\/[^\/]+\/[0-9]+$/m,
	'tablespace metadata records tablespace branch storage path');
like($tablespace_metadata, qr/^wal_pin=released$/m, 'tablespace metadata records released WAL pin');
like($tablespace_metadata, qr/^clone_result=(done|copy_fallback)$/m, 'tablespace metadata records clone success');
like($tablespace_metadata, qr/^cleanup=not_needed$/m, 'tablespace metadata records no cleanup');
like($tablespace_metadata, qr/^replay_method=rmgr_redo$/m, 'tablespace metadata records rmgr replay');
like($tablespace_metadata, qr/^status=READY$/m, 'tablespace metadata final state is READY');
like($tablespace_metadata, qr/^failure=$/m, 'tablespace metadata records no failure');
my ($tablespace_clone_path) = $tablespace_metadata =~ /^clone_path=(.+)$/m;
ok(-d $node->data_dir . '/' . $tablespace_clone_path, 'tablespace branch storage path is installed');

my $tablespace_rows = $node->safe_psql(
	'dbbranch_ts_target',
	q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM ts_rows;]);
is($tablespace_rows, '1:ts,2:redo', 'tablespace branch reads cloned and replayed rows');

my $same_tablespace = $node->safe_psql(
	'postgres',
	q[SELECT s.dattablespace = t.dattablespace FROM pg_database s, pg_database t WHERE s.datname = 'dbbranch_ts_source' AND t.datname = 'dbbranch_ts_target';]);
is($same_tablespace, 't', 'tablespace branch keeps source default tablespace');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ts_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ts_source;]);
$node->safe_psql('postgres', q[DROP TABLESPACE dbbranch_ts;]);


$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_unlogged_source;');
$node->safe_psql(
	'dbbranch_unlogged_source',
	q[
CREATE UNLOGGED TABLE cache_entries (id int PRIMARY KEY);
INSERT INTO cache_entries VALUES (1);
CHECKPOINT;
INSERT INTO cache_entries VALUES (2);
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_unlogged_target FROM DATABASE dbbranch_unlogged_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports unlogged source relations');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count, 'unlogged branch writes separate metadata file');

my $unlogged_metadata = '';

for my $path (@metadata_files)
{
	open my $fh, '<', $path or die "could not open $path: $!";
	my $contents = do { local $/; <$fh> };
	close $fh;
	if ($contents =~ /^branch_name=dbbranch_unlogged_target$/m)
	{
		$unlogged_metadata = $contents;
		last;
	}
}

like($unlogged_metadata, qr/^wal_pin=released$/m, 'unlogged metadata records released WAL pin');
like($unlogged_metadata, qr/^clone_result=(done|copy_fallback)$/m, 'unlogged metadata records clone success');
like($unlogged_metadata, qr/^cleanup=not_needed$/m, 'unlogged metadata records no cleanup');
like($unlogged_metadata, qr/^replay_method=rmgr_redo$/m, 'unlogged metadata records rmgr replay');
like($unlogged_metadata, qr/^status=READY$/m, 'unlogged metadata final state is READY');
like($unlogged_metadata, qr/^failure=$/m, 'unlogged metadata records no failure');
my ($unlogged_clone_path) = $unlogged_metadata =~ /^clone_path=(.+)$/m;
ok(-d $node->data_dir . '/' . $unlogged_clone_path, 'unlogged branch storage path is installed');

my $unlogged_rows = $node->safe_psql(
	'dbbranch_unlogged_target',
	q[SELECT string_agg(id::text, ',' ORDER BY id) FROM cache_entries;]);
is($unlogged_rows, '1,2', 'unlogged branch reads flushed unlogged rows');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_unlogged_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_unlogged_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_prepared_source;');
$node->safe_psql(
	'dbbranch_prepared_source',
	q[
CREATE TABLE prepared_rows (id int PRIMARY KEY);
BEGIN;
INSERT INTO prepared_rows VALUES (1);
PREPARE TRANSACTION 'dbbranch_prepared_xact';
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_prepared_target FROM DATABASE dbbranch_prepared_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects source database with prepared transaction');
like(
	$stderr,
	qr/source database "dbbranch_prepared_source" has prepared transactions/,
	'db branch reports prepared transaction limitation');

$node->safe_psql('dbbranch_prepared_source', q[ROLLBACK PREPARED 'dbbranch_prepared_xact';]);

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count, 'prepared transaction rejection writes separate metadata file');

my $prepared_metadata = '';
for my $path (@metadata_files)
{
	open my $fh, '<', $path or die "could not open $path: $!";
	my $contents = do { local $/; <$fh> };
	close $fh;
	if ($contents =~ /^branch_name=dbbranch_prepared_target$/m)
	{
		$prepared_metadata = $contents;
		last;
	}
}

like($prepared_metadata, qr/^wal_pin=not_started$/m, 'prepared metadata records WAL pin not started');
like($prepared_metadata, qr/^clone_result=not_started$/m, 'prepared metadata records clone not started');
like($prepared_metadata, qr/^cleanup=not_started$/m, 'prepared metadata records cleanup not started');
like($prepared_metadata, qr/^replay_method=not_started$/m, 'prepared metadata records replay not started');
like($prepared_metadata, qr/^status=FAILED$/m, 'prepared metadata final state is FAILED');
like(
	$prepared_metadata,
	qr/^failure=source database has prepared transactions$/m,
	'prepared metadata records drain failure');
my ($prepared_clone_path) = $prepared_metadata =~ /^clone_path=(.*)$/m;
ok($prepared_clone_path eq '' || !-e $node->data_dir . '/' . $prepared_clone_path,
	'prepared rejection does not create clone staging path');

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_create_subscription_drain_source;');
$node->safe_psql('dbbranch_create_subscription_drain_source', q[CHECKPOINT;]);

my $create_subscription_locker =
  $node->background_psql('dbbranch_create_subscription_drain_source', on_error_stop => 1);
my $create_subscription_writer =
  $node->background_psql('dbbranch_create_subscription_drain_source', on_error_stop => 1);
$create_subscription_locker->query_safe(q[BEGIN; LOCK TABLE pg_subscription IN ACCESS EXCLUSIVE MODE;]);
$create_subscription_writer->query_until(
	qr/start_create_subscription_drain_subscription/,
	q(\echo start_create_subscription_drain_subscription
CREATE SUBSCRIPTION dbbranch_create_subscription_sub
CONNECTION 'dbname=dbbranch_subscription_missing'
PUBLICATION dbbranch_subscription_pub
WITH (slot_name = NONE, connect = false);
\echo finish_create_subscription_drain_subscription
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_create_subscription_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'CREATE SUBSCRIPTION dbbranch_create_subscription_sub%';
]), 'active source create subscription waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_create_subscription_drain_target FROM DATABASE dbbranch_create_subscription_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source create subscription');
like($stderr, qr/source database "dbbranch_create_subscription_drain_source" has active write transactions/,
	'active source create subscription holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source create subscription writes separate metadata file');

$create_subscription_locker->query_safe(q[COMMIT;]);
$create_subscription_locker->quit;
$create_subscription_writer->query_until(qr/finish_create_subscription_drain_subscription/, '');
$create_subscription_writer->quit;

my $create_subscription_exists = $node->safe_psql(
	'dbbranch_create_subscription_drain_source',
	q[SELECT count(*) FROM pg_subscription WHERE subname = 'dbbranch_create_subscription_sub';]);
is($create_subscription_exists, '1', 'source create subscription completes after drain test');

$node->safe_psql(
	'dbbranch_create_subscription_drain_source',
	q[DROP SUBSCRIPTION dbbranch_create_subscription_sub;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_create_subscription_drain_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_alter_subscription_drain_source;');
$node->safe_psql(
	'dbbranch_alter_subscription_drain_source',
	q[
CREATE SUBSCRIPTION dbbranch_alter_subscription_sub
CONNECTION 'dbname=dbbranch_subscription_missing'
PUBLICATION dbbranch_subscription_pub
WITH (slot_name = NONE, connect = false);
CHECKPOINT;
]);

my $alter_subscription_locker =
  $node->background_psql('dbbranch_alter_subscription_drain_source', on_error_stop => 1);
my $alter_subscription_writer =
  $node->background_psql('dbbranch_alter_subscription_drain_source', on_error_stop => 1);
$alter_subscription_locker->query_safe(q[BEGIN; LOCK TABLE pg_subscription IN ACCESS EXCLUSIVE MODE;]);
$alter_subscription_writer->query_until(
	qr/start_alter_subscription_drain_subscription/,
	q(\echo start_alter_subscription_drain_subscription
ALTER SUBSCRIPTION dbbranch_alter_subscription_sub DISABLE;
\echo finish_alter_subscription_drain_subscription
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_alter_subscription_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'ALTER SUBSCRIPTION dbbranch_alter_subscription_sub%';
]), 'active source alter subscription waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_alter_subscription_drain_target FROM DATABASE dbbranch_alter_subscription_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source alter subscription');
like($stderr, qr/source database "dbbranch_alter_subscription_drain_source" has active write transactions/,
	'active source alter subscription holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source alter subscription writes separate metadata file');

$alter_subscription_locker->query_safe(q[COMMIT;]);
$alter_subscription_locker->quit;
$alter_subscription_writer->query_until(qr/finish_alter_subscription_drain_subscription/, '');
$alter_subscription_writer->quit;

my $alter_subscription_disabled = $node->safe_psql(
	'dbbranch_alter_subscription_drain_source',
	q[SELECT NOT subenabled FROM pg_subscription WHERE subname = 'dbbranch_alter_subscription_sub';]);
is($alter_subscription_disabled, 't', 'source alter subscription completes after drain test');

$node->safe_psql(
	'dbbranch_alter_subscription_drain_source',
	q[DROP SUBSCRIPTION dbbranch_alter_subscription_sub;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_alter_subscription_drain_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_drop_subscription_drain_source;');
$node->safe_psql(
	'dbbranch_drop_subscription_drain_source',
	q[
CREATE SUBSCRIPTION dbbranch_drop_subscription_sub
CONNECTION 'dbname=dbbranch_subscription_missing'
PUBLICATION dbbranch_subscription_pub
WITH (slot_name = NONE, connect = false);
CHECKPOINT;
]);

my $drop_subscription_locker =
  $node->background_psql('dbbranch_drop_subscription_drain_source', on_error_stop => 1);
my $drop_subscription_writer =
  $node->background_psql('dbbranch_drop_subscription_drain_source', on_error_stop => 1);
$drop_subscription_locker->query_safe(q[BEGIN; LOCK TABLE pg_subscription IN ACCESS EXCLUSIVE MODE;]);
$drop_subscription_writer->query_until(
	qr/start_drop_subscription_drain_subscription/,
	q(\echo start_drop_subscription_drain_subscription
DROP SUBSCRIPTION dbbranch_drop_subscription_sub;
\echo finish_drop_subscription_drain_subscription
));
ok($node->poll_query_until('postgres', q[
SELECT count(*) > 0
FROM pg_stat_activity
WHERE datname = 'dbbranch_drop_subscription_drain_source'
  AND wait_event_type = 'Lock'
  AND query LIKE 'DROP SUBSCRIPTION dbbranch_drop_subscription_sub%';
]), 'active source drop subscription waits on source catalog lock');

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_drop_subscription_drain_target FROM DATABASE dbbranch_drop_subscription_drain_source],
	stderr => \$stderr);
is($result, 3, 'db branch reports active source drop subscription');
like($stderr, qr/source database "dbbranch_drop_subscription_drain_source" has active write transactions/,
	'active source drop subscription holds db branch writer gate');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'active source drop subscription writes separate metadata file');

$drop_subscription_locker->query_safe(q[COMMIT;]);
$drop_subscription_locker->quit;
$drop_subscription_writer->query_until(qr/finish_drop_subscription_drain_subscription/, '');
$drop_subscription_writer->quit;

my $drop_subscription_exists = $node->safe_psql(
	'dbbranch_drop_subscription_drain_source',
	q[SELECT count(*) FROM pg_subscription WHERE subname = 'dbbranch_drop_subscription_sub';]);
is($drop_subscription_exists, '0', 'source drop subscription completes after drain test');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_subscription_drain_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_subscription_source;');
$node->safe_psql(
	'dbbranch_subscription_source',
	q[
CREATE SUBSCRIPTION dbbranch_subscription_sub
CONNECTION 'dbname=dbbranch_subscription_missing'
PUBLICATION dbbranch_subscription_pub
WITH (slot_name = NONE, connect = false);
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_subscription_target FROM DATABASE dbbranch_subscription_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects source database with subscription');
like(
	$stderr,
	qr/source database "dbbranch_subscription_source" is being used by logical replication subscription/,
	'db branch reports subscription limitation');

my $subscription_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_subscription_target';]);
is($subscription_branch_count, '0', 'subscription source rejection creates no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
$metadata_file_count++;
is(scalar @metadata_files, $metadata_file_count,
	'subscription source rejection writes separate metadata file');

my $subscription_metadata = '';
for my $path (@metadata_files)
{
	open my $fh, '<', $path or die "could not open $path: $!";
	my $contents = do { local $/; <$fh> };
	close $fh;
	if ($contents =~ /^branch_name=dbbranch_subscription_target$/m)
	{
		$subscription_metadata = $contents;
		last;
	}
}

like($subscription_metadata, qr/^wal_pin=not_started$/m,
	'subscription metadata records WAL pin not started');
like($subscription_metadata, qr/^clone_result=not_started$/m,
	'subscription metadata records clone not started');
like($subscription_metadata, qr/^cleanup=not_started$/m,
	'subscription metadata records cleanup not started');
like($subscription_metadata, qr/^replay_method=not_started$/m,
	'subscription metadata records replay not started');
like($subscription_metadata, qr/^status=FAILED$/m,
	'subscription metadata final state is FAILED');
like(
	$subscription_metadata,
	qr/^failure=source database has logical replication subscriptions$/m,
	'subscription metadata records rejection reason');
my ($subscription_clone_path) = $subscription_metadata =~ /^clone_path=(.*)$/m;
ok($subscription_clone_path eq '' || !-e $node->data_dir . '/' . $subscription_clone_path,
	'subscription rejection does not create clone staging path');

$node->safe_psql(
	'dbbranch_subscription_source',
	q[DROP SUBSCRIPTION dbbranch_subscription_sub;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_subscription_source;]);

$node->safe_psql(
	'postgres',
	q[
CREATE DATABASE dbbranch_invalid_source;
UPDATE pg_database SET datconnlimit = -2 WHERE datname = 'dbbranch_invalid_source';
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_invalid_target FROM DATABASE dbbranch_invalid_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects invalid source database');
like(
	$stderr,
	qr/cannot use invalid database "dbbranch_invalid_source" as source/,
	'db branch reports invalid source database');

my $invalid_branch_count = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_invalid_target';]);
is($invalid_branch_count, '0', 'invalid source rejection creates no branch database');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @metadata_files, $metadata_file_count, 'invalid source rejection writes no metadata file');

$node->safe_psql('postgres', 'DROP DATABASE dbbranch_invalid_source;');

my $rel_tablespace_dir = $node->basedir . '/dbbranch_rel_ts';
mkdir($rel_tablespace_dir) or die "could not create $rel_tablespace_dir: $!";
$node->safe_psql('postgres', "CREATE TABLESPACE dbbranch_rel_ts LOCATION '$rel_tablespace_dir';");
$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_rel_ts_source;');
$node->safe_psql(
	'dbbranch_rel_ts_source',
	q[
CREATE TABLE rel_ts_rows (id int PRIMARY KEY, name text NOT NULL) TABLESPACE dbbranch_rel_ts;
INSERT INTO rel_ts_rows VALUES (1, 'rel-ts');
CHECKPOINT;
INSERT INTO rel_ts_rows VALUES (2, 'redo');
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_rel_ts_target FROM DATABASE dbbranch_rel_ts_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports relations in non-default tablespaces');

my $rel_tablespace_rows = $node->safe_psql(
	'dbbranch_rel_ts_target',
	q[SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM rel_ts_rows;]);
is($rel_tablespace_rows, '1:rel-ts,2:redo',
	'relation tablespace branch reads cloned and replayed rows');

my $rel_tablespace_path = $node->safe_psql(
	'dbbranch_rel_ts_target',
	q[SELECT pg_relation_filepath('rel_ts_rows');]);
like($rel_tablespace_path, qr/^pg_tblspc\/[0-9]+\/[^\/]+\/[0-9]+\/[0-9]+$/,
	'relation tablespace file stays under pg_tblspc');
ok(-f $node->data_dir . '/' . $rel_tablespace_path,
	'relation tablespace branch file exists');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_rel_ts_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_rel_ts_source;]);
$node->safe_psql('postgres', q[DROP TABLESPACE dbbranch_rel_ts;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_wal_source;');
$node->safe_psql(
	'dbbranch_wal_source',
	q[
CREATE TABLE wal_rows (id int PRIMARY KEY, payload text NOT NULL);
CREATE TABLE gin_rows (id int PRIMARY KEY, tags text[] NOT NULL);
CREATE INDEX gin_rows_tags_idx ON gin_rows USING gin (tags);
CREATE SEQUENCE wal_seq CACHE 1;
INSERT INTO wal_rows VALUES (1, repeat('a', 9000)), (2, repeat('b', 9000));
SELECT nextval('wal_seq');
CHECKPOINT;
UPDATE wal_rows SET payload = repeat('c', 9000) WHERE id = 1;
DELETE FROM wal_rows WHERE id = 2;
INSERT INTO wal_rows VALUES (3, repeat('d', 9000));
INSERT INTO gin_rows VALUES (1, ARRAY['branch', 'gin']), (2, ARRAY['source']);
SELECT nextval('wal_seq');
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_wal_target FROM DATABASE dbbranch_wal_source],
	stderr => \$stderr);

is($result, 0, 'db branch handles heap, toast, btree, and sequence state');

my $wal_rows = $node->safe_psql(
	'dbbranch_wal_target',
	q[
SELECT string_agg(id || ':' || substr(payload, 1, 1) || ':' || length(payload), ',' ORDER BY id)
FROM wal_rows;
]);
is($wal_rows, '1:c:9000,3:d:9000',
	'branch sees replayed update, delete, insert, and TOAST values');

my $wal_index_lookup = $node->safe_psql(
	'dbbranch_wal_target',
	q[
SET enable_seqscan = off;
SELECT substr(payload, 1, 1) || ':' || length(payload) FROM wal_rows WHERE id = 3;
]);
is($wal_index_lookup, 'd:9000', 'branch btree index lookup sees replayed row');

$node->safe_psql('dbbranch_wal_target', q[CREATE EXTENSION amcheck;]);
is(
	$node->safe_psql(
		'dbbranch_wal_target',
		q[SELECT bt_index_check('wal_rows_pkey', true);]),
	'', 'branch btree passes amcheck after WAL replay');

my $gin_index_lookup = $node->safe_psql(
	'dbbranch_wal_target',
	q[
SET enable_seqscan = off;
SELECT string_agg(id::text, ',' ORDER BY id) FROM gin_rows WHERE tags @> ARRAY['branch'];
]);
is($gin_index_lookup, '1', 'branch GIN index lookup sees replayed row');

my $source_sequence = $node->safe_psql(
	'dbbranch_wal_source',
	q[SELECT last_value FROM wal_seq;]);
my $branch_sequence = $node->safe_psql(
	'dbbranch_wal_target',
	q[SELECT last_value FROM wal_seq;]);
is($branch_sequence, $source_sequence,
	'branch sequence durable state matches source');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_wal_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_wal_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_partition_source;');
$node->safe_psql(
	'dbbranch_partition_source',
	q[
CREATE TABLE part_rows (
	id int,
	bucket int,
	payload text NOT NULL,
	PRIMARY KEY (bucket, id)
) PARTITION BY RANGE (bucket);
CREATE TABLE part_rows_low PARTITION OF part_rows FOR VALUES FROM (0) TO (10);
CREATE TABLE part_rows_high PARTITION OF part_rows FOR VALUES FROM (10) TO (20);
INSERT INTO part_rows VALUES (1, 1, 'low-before'), (2, 11, 'high-before');
CHECKPOINT;
INSERT INTO part_rows VALUES (3, 2, 'low-redo'), (4, 12, 'high-redo');
UPDATE part_rows SET payload = 'high-updated' WHERE bucket = 11 AND id = 2;
DELETE FROM part_rows WHERE bucket = 1 AND id = 1;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_partition_target FROM DATABASE dbbranch_partition_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports partitioned table WAL replay');

my $partition_rows = $node->safe_psql(
	'dbbranch_partition_target',
	q[
SELECT string_agg(tableoid::regclass::text || ':' || id || ':' || payload, ',' ORDER BY id)
FROM part_rows;
]);
is($partition_rows, 'part_rows_high:2:high-updated,part_rows_low:3:low-redo,part_rows_high:4:high-redo',
	'branch reads replayed rows from both partitions');

my $partition_index_lookup = $node->safe_psql(
	'dbbranch_partition_target',
	q[
SET enable_seqscan = off;
SELECT payload FROM part_rows WHERE bucket = 12 AND id = 4;
]);
is($partition_index_lookup, 'high-redo',
	'branch partition index lookup sees replayed row');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_partition_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_partition_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_toast_source;');
$node->safe_psql(
	'dbbranch_toast_source',
	q[
CREATE TABLE toast_rows (id int PRIMARY KEY, payload text NOT NULL);
ALTER TABLE toast_rows ALTER COLUMN payload SET STORAGE EXTERNAL;
INSERT INTO toast_rows
SELECT 1, string_agg(md5(g::text), '') FROM generate_series(1, 256) g;
CHECKPOINT;
INSERT INTO toast_rows
SELECT 2, string_agg(md5(g::text), '') FROM generate_series(257, 512) g;
UPDATE toast_rows SET payload = payload || md5('dbbranch-toast-update') WHERE id = 1;
]);

my $toast_expected = $node->safe_psql(
	'dbbranch_toast_source',
	q[SELECT string_agg(id || ':' || length(payload) || ':' || md5(payload), ',' ORDER BY id) FROM toast_rows;]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_toast_target FROM DATABASE dbbranch_toast_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports TOAST WAL replay');

my $toast_rows = $node->safe_psql(
	'dbbranch_toast_target',
	q[SELECT string_agg(id || ':' || length(payload) || ':' || md5(payload), ',' ORDER BY id) FROM toast_rows;]);
is($toast_rows, $toast_expected, 'branch reads cloned and replayed TOAST values');

my $toast_index_lookup = $node->safe_psql(
	'dbbranch_toast_target',
	q[
SET enable_seqscan = off;
SELECT length(payload) > 8000 FROM toast_rows WHERE id = 2;
]);
is($toast_index_lookup, 't', 'branch index lookup fetches replayed TOAST value');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_toast_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_toast_source;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_drop_source;');
$node->safe_psql(
	'dbbranch_drop_source',
	q[
CREATE TABLE drop_rows (id int PRIMARY KEY, payload text NOT NULL);
INSERT INTO drop_rows VALUES (1, 'before');
CHECKPOINT;
INSERT INTO drop_rows VALUES (2, 'after');
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_drop_target FROM DATABASE dbbranch_drop_source],
	stderr => \$stderr);

is($result, 0, 'db branch can be created before dropping source database');

my $drop_source_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_drop_source';]);
my $drop_branch_oid = $node->safe_psql(
	'postgres',
	q[SELECT oid FROM pg_database WHERE datname = 'dbbranch_drop_target';]);

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_source;]);

my $drop_branch_rows = $node->safe_psql(
	'dbbranch_drop_target',
	q[SELECT string_agg(id || ':' || payload, ',' ORDER BY id) FROM drop_rows;]);
is($drop_branch_rows, '1:before,2:after',
	'branch remains readable after source database is dropped');

my $drop_catalog_rows = $node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_dbbranch WHERE source_db_oid = ]
	  . $drop_source_oid
	  . q[ OR branch_db_oid = ]
	  . $drop_branch_oid
	  . q[;]);
is($drop_catalog_rows, '0',
	'dropping source removes pg_dbbranch catalog metadata');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_drop_target;]);

$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_ddl_source;');
$node->safe_psql(
	'dbbranch_ddl_source',
	q[
CREATE TABLE ddl_base (id int PRIMARY KEY);
INSERT INTO ddl_base VALUES (1);
CHECKPOINT;
CREATE TABLE ddl_after (id int PRIMARY KEY, payload text NOT NULL);
INSERT INTO ddl_after VALUES (2, 'created-after-checkpoint');
CREATE INDEX ddl_after_payload_idx ON ddl_after(payload);
CREATE VIEW ddl_after_view AS SELECT id, payload FROM ddl_after;
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_ddl_target FROM DATABASE dbbranch_ddl_source],
	stderr => \$stderr);

is($result, 0, 'db branch supports post-checkpoint DDL');

my $ddl_rows = $node->safe_psql(
	'dbbranch_ddl_target',
	q[SELECT string_agg(id || ':' || payload, ',' ORDER BY id) FROM ddl_after;]);
is($ddl_rows, '2:created-after-checkpoint',
	'branch reads table created after checkpoint');

my $ddl_view_rows = $node->safe_psql(
	'dbbranch_ddl_target',
	q[SELECT string_agg(id || ':' || payload, ',' ORDER BY id) FROM ddl_after_view;]);
is($ddl_view_rows, '2:created-after-checkpoint',
	'branch reads view created after checkpoint');

my $ddl_index_lookup = $node->safe_psql(
	'dbbranch_ddl_target',
	q[
SET enable_seqscan = off;
SELECT id FROM ddl_after WHERE payload = 'created-after-checkpoint';
]);
is($ddl_index_lookup, '2',
	'branch index lookup works on table created after checkpoint');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ddl_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_ddl_source;]);

$node->append_conf('postgresql.conf', 'full_page_writes = off');
$node->reload;
$node->safe_psql('postgres', 'CREATE DATABASE dbbranch_nonfpi_source;');
$node->safe_psql(
	'dbbranch_nonfpi_source',
	q[
CREATE TABLE nonfpi_rows (id int PRIMARY KEY);
CHECKPOINT;
INSERT INTO nonfpi_rows VALUES (1);
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_nonfpi_target FROM DATABASE dbbranch_nonfpi_source],
	stderr => \$stderr);

is($result, 0, 'db branch replays source WAL records without full-page images');

@metadata_files = glob $node->data_dir . '/global/pg_dbbranch_*.state';
my $nonfpi_metadata = '';
for my $path (@metadata_files)
{
	open my $fh, '<', $path or die "could not open $path: $!";
	my $contents = do { local $/; <$fh> };
	close $fh;
	if ($contents =~ /^branch_name=dbbranch_nonfpi_target$/m)
	{
		$nonfpi_metadata = $contents;
		last;
	}
}

like(
	$nonfpi_metadata,
	qr/^wal_source_non_fpi_records=[1-9][0-9]*$/m,
	'non-FPI metadata records source WAL outside FPI replay');
like(
	$nonfpi_metadata,
	qr/^clone_result=(done|copy_fallback)$/m,
	'non-FPI metadata records storage clone success');
like(
	$nonfpi_metadata,
	qr/^replay_method=rmgr_redo$/m,
	'non-FPI metadata records rmgr redo replay method');
like(
	$nonfpi_metadata,
	qr/^status_history=CREATING,COPYING,REPLAYING,READY$/m,
	'non-FPI metadata records READY transition');
like(
	$nonfpi_metadata,
	qr/^failure=$/m,
	'non-FPI metadata records no failure');

my $nonfpi_rows = $node->safe_psql(
	'dbbranch_nonfpi_target',
	q[SELECT count(*) FROM nonfpi_rows;]);
is($nonfpi_rows, '1', 'non-FPI branch reads rmgr-replayed rows');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_nonfpi_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_nonfpi_source;]);
$node->append_conf('postgresql.conf', 'full_page_writes = on');
$node->reload;

$node->safe_psql('postgres', q[
CREATE ROLE dbbranch_template_createdb LOGIN CREATEDB;
CREATE DATABASE dbbranch_template_source;
ALTER DATABASE dbbranch_template_source IS_TEMPLATE true;
]);
$node->safe_psql(
	'dbbranch_template_source',
	q[
CREATE TABLE template_rows (id int PRIMARY KEY);
INSERT INTO template_rows VALUES (1);
]);

$stderr = '';
$result = $node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_template_target FROM DATABASE dbbranch_template_source],
	stderr => \$stderr,
	extra_params => [ '--username' => 'dbbranch_template_createdb' ]);

is($result, 0, 'db branch allows CREATEDB role to copy template source');

my $template_branch_owner = $node->safe_psql(
	'postgres',
	q[SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = 'dbbranch_template_target';]);
is($template_branch_owner, 'dbbranch_template_createdb', 'template branch is owned by creator');

my $template_rows = $node->safe_psql(
	'dbbranch_template_target',
	q[SELECT count(*) FROM template_rows;]);
is($template_rows, '1', 'template branch copies source data');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_template_target;]);
$node->safe_psql('postgres', q[ALTER DATABASE dbbranch_template_source IS_TEMPLATE false;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_template_source;]);
$node->safe_psql('postgres', q[DROP ROLE dbbranch_template_createdb;]);


$node->safe_psql('postgres', q[CREATE DATABASE dbbranch_cmdtag_source;]);
$node->safe_psql(
	'dbbranch_cmdtag_source',
	q[
CREATE TABLE cmdtag_rows (id int PRIMARY KEY);
INSERT INTO cmdtag_rows VALUES (1);
]);
command_like(
	[
		$node->installed_command('psql'),
		'--no-psqlrc',
		'--set' => 'ON_ERROR_STOP=1',
		'--dbname' => $node->connstr('postgres'),
		'--command' =>
		  'CREATE BRANCH dbbranch_cmdtag_target FROM DATABASE dbbranch_cmdtag_source'
	],
	qr/^CREATE BRANCH$/m,
	'CREATE BRANCH reports its own command tag');
my $cmdtag_rows = $node->safe_psql(
	'dbbranch_cmdtag_target',
	q[SELECT count(*) FROM cmdtag_rows;]);
is($cmdtag_rows, '1', 'command-tag branch copies source data');

$result = $node->psql(
	'postgres',
	q[SET debug_print_parse = on;
CREATE BRANCH dbbranch_debug_target FROM DATABASE dbbranch_cmdtag_source;]);
is($result, 0, 'CREATE BRANCH supports parse tree debug output');

$node->safe_psql('postgres', q[DROP DATABASE dbbranch_debug_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_cmdtag_target;]);
$node->safe_psql('postgres', q[DROP DATABASE dbbranch_cmdtag_source;]);

my $checksum_node = PostgreSQL::Test::Cluster->new('checksum');
$checksum_node->init(allows_streaming => 1, extra => ['--data-checksums']);
$checksum_node->start;
is($checksum_node->safe_psql('postgres', q[SHOW data_checksums;]),
	'on', 'checksum test node has data checksums enabled');
$checksum_node->safe_psql('postgres', q[CREATE DATABASE dbbranch_checksum_source;]);
$checksum_node->safe_psql(
	'dbbranch_checksum_source',
	q[
CREATE TABLE checksum_rows (id int PRIMARY KEY, payload text NOT NULL);
INSERT INTO checksum_rows VALUES (1, repeat('a', 9000));
CHECKPOINT;
UPDATE checksum_rows SET payload = repeat('b', 9000) WHERE id = 1;
INSERT INTO checksum_rows VALUES (2, repeat('c', 9000));
]);

$stderr = '';
$result = $checksum_node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_checksum_target FROM DATABASE dbbranch_checksum_source],
	stderr => \$stderr);
is($result, 0, 'db branch works with data checksums enabled');

my $checksum_rows = $checksum_node->safe_psql(
	'dbbranch_checksum_target',
	q[
SET enable_seqscan = off;
SELECT string_agg(id || ':' || substr(payload, 1, 1) || ':' || length(payload), ',' ORDER BY id)
FROM checksum_rows;
]);
is($checksum_rows, '1:b:9000,2:c:9000',
	'checksum branch reads cloned and replayed indexed rows');

$checksum_node->safe_psql('postgres', q[DROP DATABASE dbbranch_checksum_target;]);
$checksum_node->safe_psql('postgres', q[DROP DATABASE dbbranch_checksum_source;]);
$checksum_node->stop;

my $minimal_node = PostgreSQL::Test::Cluster->new('minimal');
$minimal_node->init;
$minimal_node->append_conf('postgresql.conf', qq[
wal_level = minimal
max_wal_senders = 0
]);
$minimal_node->start;
$minimal_node->safe_psql('postgres', q[CREATE DATABASE dbbranch_minimal_source;]);
$minimal_node->safe_psql(
	'dbbranch_minimal_source',
	q[
CREATE TABLE minimal_rows (id int PRIMARY KEY);
INSERT INTO minimal_rows VALUES (1);
]);

$stderr = '';
$result = $minimal_node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_minimal_target FROM DATABASE dbbranch_minimal_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects wal_level=minimal');
like(
	$stderr,
	qr/CREATE BRANCH can only be used if "wal_level" >= "replica"/,
	'db branch reports wal_level requirement');

my $minimal_branch_count = $minimal_node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_minimal_target';]);
is($minimal_branch_count, '0', 'wal_level rejection creates no branch database');

my @minimal_metadata_files = glob $minimal_node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @minimal_metadata_files, 0, 'wal_level rejection writes no metadata file');

$minimal_node->stop;

my $slotless_node = PostgreSQL::Test::Cluster->new('slotless');
$slotless_node->init;
$slotless_node->append_conf('postgresql.conf', qq[
wal_level = replica
max_replication_slots = 0
]);
$slotless_node->start;
$slotless_node->safe_psql('postgres', q[CREATE DATABASE dbbranch_slotless_source;]);
$slotless_node->safe_psql(
	'dbbranch_slotless_source',
	q[
CREATE TABLE slotless_rows (id int PRIMARY KEY);
INSERT INTO slotless_rows VALUES (1);
]);

$stderr = '';
$result = $slotless_node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_slotless_target FROM DATABASE dbbranch_slotless_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects max_replication_slots=0');
like(
	$stderr,
	qr/CREATE BRANCH can only be used if "max_replication_slots" > 0/,
	'db branch reports WAL pin replication slot requirement');

my $slotless_branch_count = $slotless_node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_slotless_target';]);
is($slotless_branch_count, '0', 'slotless WAL pin rejection creates no branch database');

my @slotless_metadata_files = glob $slotless_node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @slotless_metadata_files, 0, 'slotless WAL pin rejection writes no metadata file');

$slotless_node->stop;

my $slotfull_node = PostgreSQL::Test::Cluster->new('slotfull');
$slotfull_node->init;
$slotfull_node->append_conf('postgresql.conf', qq[
wal_level = replica
max_replication_slots = 1
]);
$slotfull_node->start;
$slotfull_node->safe_psql('postgres', q[CREATE DATABASE dbbranch_slotfull_source;]);
$slotfull_node->safe_psql(
	'dbbranch_slotfull_source',
	q[
CREATE TABLE slotfull_rows (id int PRIMARY KEY);
INSERT INTO slotfull_rows VALUES (1);
]);
$slotfull_node->safe_psql(
	'postgres',
	q[SELECT pg_create_physical_replication_slot('dbbranch_busy_slot');]);

$stderr = '';
$result = $slotfull_node->psql(
	'postgres',
	q[CREATE BRANCH dbbranch_slotfull_target FROM DATABASE dbbranch_slotfull_source],
	stderr => \$stderr);

is($result, 3, 'db branch rejects exhausted replication slots');
like(
	$stderr,
	qr/all replication slots are in use/,
	'db branch reports exhausted replication slots');

my $slotfull_branch_count = $slotfull_node->safe_psql(
	'postgres',
	q[SELECT count(*) FROM pg_database WHERE datname = 'dbbranch_slotfull_target';]);
is($slotfull_branch_count, '0', 'exhausted WAL pin creates no branch database');

my @slotfull_metadata_files = glob $slotfull_node->data_dir . '/global/pg_dbbranch_*.state';
is(scalar @slotfull_metadata_files, 1, 'exhausted WAL pin writes failure metadata');

open my $slotfull_metadata_fh, '<', $slotfull_metadata_files[0]
  or die "could not open $slotfull_metadata_files[0]: $!";
my $slotfull_metadata = do { local $/; <$slotfull_metadata_fh> };
close $slotfull_metadata_fh;

like($slotfull_metadata, qr/^branch_name=dbbranch_slotfull_target$/m,
	'exhausted WAL pin metadata records branch name');
like($slotfull_metadata, qr/^wal_pin=failed$/m,
	'exhausted WAL pin metadata records failed WAL pin');
like($slotfull_metadata, qr/^clone_result=not_started$/m,
	'exhausted WAL pin metadata records clone not started');
like($slotfull_metadata, qr/^status=FAILED$/m,
	'exhausted WAL pin metadata final state is FAILED');
like($slotfull_metadata, qr/^failure=all replication slots are in use$/m,
	'exhausted WAL pin metadata records failure reason');
my ($slotfull_clone_path) = $slotfull_metadata =~ /^clone_path=(.*)$/m;
ok(!-e $slotfull_node->data_dir . '/' . $slotfull_clone_path,
	'exhausted WAL pin creates no clone path');

$slotfull_node->safe_psql('postgres', q[SELECT pg_drop_replication_slot('dbbranch_busy_slot');]);
$slotfull_node->stop;

done_testing();
