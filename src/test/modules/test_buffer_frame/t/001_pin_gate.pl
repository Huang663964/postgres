# Copyright (c) 2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('postgresql.conf',
	"shared_preload_libraries = 'injection_points'");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
$node->safe_psql('postgres', 'CREATE EXTENSION test_buffer_frame');
$node->safe_psql(
	'postgres', q[
CREATE TABLE bf_data(id int, payload text) WITH (autovacuum_enabled = false);
INSERT INTO bf_data VALUES (1, repeat('x', 200));
CREATE TABLE bf_other(id int, payload text) WITH (autovacuum_enabled = false);
INSERT INTO bf_other VALUES (2, repeat('y', 200));
CHECKPOINT;
]);

my $buffer_id = $node->safe_psql(
	'postgres',
	q[SELECT test_buffer_frame_buffer_id('bf_data'::regclass, 0)]);
my $frame_id = $buffer_id - 1;
my $other_buffer_id = $node->safe_psql(
	'postgres',
	q[SELECT test_buffer_frame_buffer_id('bf_other'::regclass, 0)]);
my $other_frame_id = $other_buffer_id - 1;
my $point_number = 0;
my $bg_timeout = 4 * $PostgreSQL::Test::Utils::timeout_default;

sub new_point
{
	my ($suffix) = @_;

	return sprintf('bf_%02d_%s', ++$point_number, $suffix);
}

sub attach_point
{
	my ($name, $action) = @_;

	$node->safe_psql(
		'postgres',
		"SELECT injection_points_attach('$name', '$action')");
}

sub wake_point
{
	my ($name) = @_;

	$node->safe_psql('postgres',
		"SELECT injection_points_wakeup('$name')");
}

sub detach_point
{
	my ($name) = @_;

	$node->safe_psql('postgres',
		"SELECT injection_points_detach('$name')");
}

sub new_background_psql
{
	my $psql = $node->background_psql(
		'postgres',
		on_error_stop => 0,
		timeout => $bg_timeout);
	my $pid = $psql->query_safe('SELECT pg_backend_pid()');

	return ($psql, $pid);
}

sub start_background_query
{
	my ($psql, $tag, $sql) = @_;

	$psql->{stderr} = '';
	$psql->query_until(
		qr/start_$tag/,
		"\\echo start_$tag\n$sql;\n\\echo finish_$tag\n");
}

sub finish_background_query
{
	my ($psql, $tag) = @_;

	$psql->query_until(qr/finish_$tag/, '');
	my $stderr = $psql->{stderr};
	$psql->{stderr} = '';
	return $stderr;
}

sub wait_for_pid_event
{
	my ($pid, $event, $label) = @_;

	ok(
		$node->poll_query_until(
			'postgres',
			qq[
SELECT count(*) = 1
FROM pg_stat_activity
WHERE pid = $pid AND wait_event = '$event'
],
			't'),
		$label);
}

sub wait_for_backend_type_event
{
	my ($backend_type, $event, $label) = @_;

	ok(
		$node->poll_query_until(
			'postgres',
			qq[
SELECT count(*) >= 1
FROM pg_stat_activity
WHERE backend_type = '$backend_type' AND wait_event = '$event'
],
			't'),
		$label);
}

sub buffer_state
{
	return buffer_state_for($buffer_id);
}

sub buffer_state_for
{
	my ($id) = @_;

	return $node->safe_psql(
		'postgres',
		"SELECT test_buffer_frame_state($id)");
}

sub buffer_is_dirty
{
	return $node->safe_psql(
		'postgres',
		q[SELECT test_buffer_frame_is_dirty('bf_data'::regclass, 0)]);
}

sub assert_clean_and_repin
{
	my ($label) = @_;
	my $clean = "-1:f:0:$frame_id";

	is(buffer_state(), $clean, "$label: gate and pins are clear");
	is(
		$node->safe_psql(
			'postgres',
			q[SELECT test_buffer_frame_buffer_id('bf_data'::regclass, 0)]),
		$buffer_id,
		"$label: buffer can be pinned again");
	is(buffer_state(), $clean, "$label: frame mapping remains identity");
}

sub assert_other_clean_and_repin
{
	my ($label) = @_;
	my $clean = "-1:f:0:$other_frame_id";

	is(buffer_state_for($other_buffer_id), $clean,
		"$label: other buffer gate and pins are clear");
	is(
		$node->safe_psql(
			'postgres',
			q[SELECT test_buffer_frame_buffer_id('bf_other'::regclass, 0)]),
		$other_buffer_id,
		"$label: other buffer can be pinned again");
	is(buffer_state_for($other_buffer_id), $clean,
		"$label: other frame mapping remains identity");
}

sub run_owner_failure
{
	my ($action, $error_re, $label) = @_;
	my ($psql, $pid) = new_background_psql();
	my ($output, $error) = $psql->query(
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '', '$action')");

	ok($error, "$label: statement fails");
	like($psql->{stderr}, $error_re, "$label: expected failure");
	$psql->{stderr} = '';
	$psql->quit;
	assert_clean_and_repin($label);
}

sub run_two_buffer_failure
{
	my ($action, $error_re, $label) = @_;
	my ($psql, $pid) = new_background_psql();
	my ($output, $error) = $psql->query(
		"SELECT test_buffer_frame_two_buffers('bf_data'::regclass, 0, "
		  . "'bf_other'::regclass, 0, '$action')");

	ok($error, "$label: statement fails");
	like($psql->{stderr}, $error_re, "$label: expected failure");
	$psql->{stderr} = '';
	$psql->quit;
	assert_clean_and_repin($label);
	assert_other_clean_and_repin($label);
}

assert_clean_and_repin('initial state');
assert_other_clean_and_repin('initial state');

subtest 'old reader drains while new first pin remains gated' => sub
{
	my $reader_point = new_point('old_reader');
	my $writer_point = new_point('writer_after');
	my $new_reader_point = new_point('new_reader');
	attach_point($reader_point, 'wait');
	attach_point($writer_point, 'wait');
	attach_point($new_reader_point, 'wait');

	my ($reader, $reader_pid) = new_background_psql();
	my ($writer, $writer_pid) = new_background_psql();
	my ($new_reader, $new_reader_pid) = new_background_psql();

	start_background_query(
		$reader,
		'old_reader',
		"SELECT test_buffer_frame_reader('bf_data'::regclass, 0, '$reader_point', '', false)");
	wait_for_pid_event($reader_pid, $reader_point,
		'old reader holds a Page pointer and pin');

	start_background_query(
		$writer,
		'draining_writer',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '$writer_point', 'normal')");
	wait_for_pid_event($writer_pid, 'BufferPin',
		'writer waits for old reader pin');
	like(buffer_state(), qr/^\d+:t:2:$frame_id$/,
		'writer published gate while two backends are pinned');

	start_background_query(
		$new_reader,
		'gated_reader',
		"SELECT test_buffer_frame_reader('bf_data'::regclass, 0, '$new_reader_point', '', false)");
	wait_for_pid_event($new_reader_pid, 'BufferPin',
		'new backend first pin waits behind gate');

	my $foreign_wake_observer =
	  'buffer-write-intent-after-foreign-cv-wake';
	attach_point($foreign_wake_observer, 'wait');
	$node->safe_psql('postgres',
		"SELECT test_buffer_frame_broadcast($buffer_id)");
	wait_for_pid_event($new_reader_pid, $foreign_wake_observer,
		'foreign first-pin waiter actually wakes on a spurious broadcast');
	detach_point($foreign_wake_observer);
	wake_point($foreign_wake_observer);
	wait_for_pid_event($new_reader_pid, 'BufferPin',
		'foreign waiter revalidates mapping and waits again');

	wake_point($reader_point);
	is(finish_background_query($reader, 'old_reader'), '',
		'old reader validates its Page pointer and releases');
	detach_point($reader_point);
	wait_for_pid_event($writer_pid, $writer_point,
		'old reader release lets writer finish draining');
	wait_for_pid_event($new_reader_pid, 'BufferPin',
		'new reader remains gated after drain');
	like(buffer_state(), qr/^\d+:t:1:$frame_id$/,
		'writer is sole pin while gate remains published');

	wake_point($writer_point);
	is(finish_background_query($writer, 'draining_writer'), '',
		'writer ends intent normally');
	detach_point($writer_point);
	wait_for_pid_event($new_reader_pid, $new_reader_point,
		'End broadcasts and admits new reader');
	wake_point($new_reader_point);
	is(finish_background_query($new_reader, 'gated_reader'), '',
		'new reader completes after End');
	detach_point($new_reader_point);

	$reader->quit;
	$writer->quit;
	$new_reader->quit;
	assert_clean_and_repin('drain and first-pin gate');
};

subtest 'foreign existing nested pin does not advance drain' => sub
{
	my $nested_point = new_point('nested_before');
	my $partial_point = new_point('nested_partial');
	my $writer_point = new_point('nested_writer');
	attach_point($nested_point, 'wait');
	attach_point($partial_point, 'wait');
	attach_point($writer_point, 'wait');

	my ($reader, $reader_pid) = new_background_psql();
	my ($writer, $writer_pid) = new_background_psql();

	start_background_query(
		$reader,
		'nested_reader',
		"SELECT test_buffer_frame_reader('bf_data'::regclass, 0, '$nested_point', '$partial_point', true)");
	wait_for_pid_event($reader_pid, $nested_point,
		'foreign backend holds its original pin');

	start_background_query(
		$writer,
		'nested_drain',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '$writer_point', 'normal')");
	wait_for_pid_event($writer_pid, 'BufferPin',
		'writer drains foreign backend');

	wake_point($nested_point);
	wait_for_pid_event($reader_pid, $partial_point,
		'foreign nested pin is allowed');
	wait_for_pid_event($writer_pid, 'BufferPin',
		'releasing one local reference does not finish shared-pin drain');
	like(buffer_state(), qr/^\d+:t:2:$frame_id$/,
		'shared refcount remains unchanged by nested local pin');

	wake_point($partial_point);
	is(finish_background_query($reader, 'nested_reader'), '',
		'foreign backend releases its last local reference');
	wait_for_pid_event($writer_pid, $writer_point,
		'last local release wakes writer');
	wake_point($writer_point);
	is(finish_background_query($writer, 'nested_drain'), '',
		'writer completes after foreign nested pins drain');

	detach_point($nested_point);
	detach_point($partial_point);
	detach_point($writer_point);
	$reader->quit;
	$writer->quit;
	assert_clean_and_repin('foreign nested pin');
};

subtest 'write-intent owner ReadBuffer nested pin fails closed' => sub
{
	run_owner_failure(
		'read',
		qr/write-intent owner cannot pin buffer \d+ again/,
		'owner ReadBuffer nested pin');
};

subtest 'write-intent owner IncrBufferRefCount fails closed' => sub
{
	run_owner_failure(
		'incr',
		qr/write-intent owner cannot pin buffer \d+ again/,
		'owner IncrBufferRefCount');
};

subtest 'write-intent owner cannot release sole pin before End' => sub
{
	run_owner_failure(
		'release',
		qr/write-intent owner must end intent before releasing buffer \d+/,
		'owner early ReleaseBuffer');
};

subtest 'Begin fails while backend holds another shared pin' => sub
{
	run_two_buffer_failure(
		'begin_with_other',
		qr/write intent on buffer \d+ requires no other shared pin; buffer \d+ is pinned/,
		'Begin with another shared pin');
};

subtest 'active intent rejects first pin of another shared buffer' => sub
{
	run_two_buffer_failure(
		'pin_other_after_begin',
		qr/cannot pin shared buffer \d+ while write intent is active on buffer \d+/,
		'first pin after Begin');
};

subtest 'cross-buffer gated waits fail fast instead of deadlocking' => sub
{
	my $reader_x_point = new_point('cross_reader_x');
	my $reader_y_point = new_point('cross_reader_y');
	my $writer_x_point = new_point('cross_writer_x');
	my $writer_y_point = new_point('cross_writer_y');
	attach_point($reader_x_point, 'wait');
	attach_point($reader_y_point, 'wait');
	attach_point($writer_x_point, 'wait');
	attach_point($writer_y_point, 'wait');

	my ($reader_x, $reader_x_pid) = new_background_psql();
	my ($reader_y, $reader_y_pid) = new_background_psql();
	my ($writer_x, $writer_x_pid) = new_background_psql();
	my ($writer_y, $writer_y_pid) = new_background_psql();

	start_background_query(
		$reader_x,
		'cross_reader_x',
		"SELECT test_buffer_frame_cross_pin('bf_data'::regclass, 0, "
		  . "'bf_other'::regclass, 0, '$reader_x_point')");
	start_background_query(
		$reader_y,
		'cross_reader_y',
		"SELECT test_buffer_frame_cross_pin('bf_other'::regclass, 0, "
		  . "'bf_data'::regclass, 0, '$reader_y_point')");
	wait_for_pid_event($reader_x_pid, $reader_x_point,
		'reader X holds X before cross-pin');
	wait_for_pid_event($reader_y_pid, $reader_y_point,
		'reader Y holds Y before cross-pin');

	start_background_query(
		$writer_x,
		'cross_writer_x',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '$writer_x_point', 'normal')");
	start_background_query(
		$writer_y,
		'cross_writer_y',
		"SELECT test_buffer_frame_writer('bf_other'::regclass, 0, '', '$writer_y_point', 'normal')");
	wait_for_pid_event($writer_x_pid, 'BufferPin',
		'writer X gates X and drains reader X');
	wait_for_pid_event($writer_y_pid, 'BufferPin',
		'writer Y gates Y and drains reader Y');

	wake_point($reader_x_point);
	like(
		finish_background_query($reader_x, 'cross_reader_x'),
		qr/cannot wait for write intent on buffer \d+ while buffer \d+ is pinned/,
		'reader X fails instead of waiting for gated Y');
	wait_for_pid_event($writer_x_pid, $writer_x_point,
		'reader X failure releases X and advances writer X');

	wake_point($reader_y_point);
	like(
		finish_background_query($reader_y, 'cross_reader_y'),
		qr/cannot wait for write intent on buffer \d+ while buffer \d+ is pinned/,
		'reader Y fails instead of waiting for gated X');
	wait_for_pid_event($writer_y_pid, $writer_y_point,
		'reader Y failure releases Y and advances writer Y');

	wake_point($writer_x_point);
	wake_point($writer_y_point);
	is(finish_background_query($writer_x, 'cross_writer_x'), '',
		'writer X completes');
	is(finish_background_query($writer_y, 'cross_writer_y'), '',
		'writer Y completes');

	detach_point($reader_x_point);
	detach_point($reader_y_point);
	detach_point($writer_x_point);
	detach_point($writer_y_point);
	$reader_x->quit;
	$reader_y->quit;
	$writer_x->quit;
	$writer_y->quit;
	assert_clean_and_repin('cross-buffer wait cycle');
	assert_other_clean_and_repin('cross-buffer wait cycle');
};

subtest 'second pre-pinned writer fails fast and wakes first writer' => sub
{
	my $first_before = new_point('writer1_before');
	my $second_before = new_point('writer2_before');
	my $first_after = new_point('writer1_after');
	attach_point($first_before, 'wait');
	attach_point($second_before, 'wait');
	attach_point($first_after, 'wait');

	my ($first, $first_pid) = new_background_psql();
	my ($second, $second_pid) = new_background_psql();

	start_background_query(
		$first,
		'first_writer',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '$first_before', '$first_after', 'normal')");
	start_background_query(
		$second,
		'second_writer',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '$second_before', '', 'normal')");
	wait_for_pid_event($first_pid, $first_before,
		'first writer is pre-pinned');
	wait_for_pid_event($second_pid, $second_before,
		'second writer is pre-pinned');

	wake_point($first_before);
	wait_for_pid_event($first_pid, 'BufferPin',
		'first writer waits for second writer pin');
	wake_point($second_before);
	my $second_error = finish_background_query($second, 'second_writer');
	like($second_error, qr/buffer \d+ already has a write intent/,
		'second writer fails fast');
	wait_for_pid_event($first_pid, $first_after,
		'second writer error releases pin and wakes first writer');

	wake_point($first_after);
	is(finish_background_query($first, 'first_writer'), '',
		'first writer completes');

	detach_point($first_before);
	detach_point($second_before);
	detach_point($first_after);
	$first->quit;
	$second->quit;
	assert_clean_and_repin('two writers');
};

subtest 'query cancel while writer drains clears gate' => sub
{
	my $reader_point = new_point('cancel_reader');
	attach_point($reader_point, 'wait');
	my ($reader, $reader_pid) = new_background_psql();
	my ($writer, $writer_pid) = new_background_psql();

	start_background_query(
		$reader,
		'cancel_reader',
		"SELECT test_buffer_frame_reader('bf_data'::regclass, 0, '$reader_point', '', false)");
	wait_for_pid_event($reader_pid, $reader_point,
		'reader blocks cancel test writer drain');
	start_background_query(
		$writer,
		'cancel_writer',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '', 'normal')");
	wait_for_pid_event($writer_pid, 'BufferPin',
		'cancel target waits in BufferPin');

	is(
		$node->safe_psql('postgres',
			"SELECT pg_cancel_backend($writer_pid)"),
		't',
		'cancel signal sent');
	like(
		finish_background_query($writer, 'cancel_writer'),
		qr/canceling statement due to user request/,
		'writer reports query cancellation');
	is(buffer_state(), "-1:f:1:$frame_id",
		'cancel clears owner and gate before reader pin releases');

	wake_point($reader_point);
	is(finish_background_query($reader, 'cancel_reader'), '',
		'reader completes after canceled writer');
	detach_point($reader_point);
	$reader->quit;
	$writer->quit;
	assert_clean_and_repin('query cancel');
};

subtest 'injection ERROR after Begin clears gate' => sub
{
	my $error_point = new_point('after_begin_error');
	attach_point($error_point, 'error');
	my ($psql, $pid) = new_background_psql();
	my ($output, $error) = $psql->query(
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '$error_point', 'normal')");

	ok($error, 'injection point fails writer');
	like($psql->{stderr},
		qr/error triggered for injection point $error_point/,
		'injection error occurs after Begin');
	$psql->{stderr} = '';
	$psql->quit;
	detach_point($error_point);
	assert_clean_and_repin('injection error after Begin');
};

subtest 'explicit transaction error and rollback clear gate' => sub
{
	my ($psql, $pid) = new_background_psql();
	$psql->query_safe('BEGIN');
	my ($output, $error) = $psql->query(
		q[SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '', 'error')]);

	ok($error, 'explicit transaction statement fails');
	like($psql->{stderr}, qr/deliberate buffer write-intent test error/,
		'explicit transaction sees deliberate error');
	$psql->{stderr} = '';
	$psql->query_safe('ROLLBACK');
	is($psql->query_safe('SELECT 1'), '1',
		'session remains usable after explicit rollback');
	$psql->quit;
	assert_clean_and_repin('explicit transaction rollback');
};

subtest 'spurious BufferIOCV broadcast does not cross predicate' => sub
{
	my $reader_point = new_point('spurious_reader');
	my $writer_point = new_point('spurious_writer');
	my $wake_observer = 'buffer-write-intent-after-cv-wake';
	attach_point($reader_point, 'wait');
	attach_point($writer_point, 'wait');
	my ($reader, $reader_pid) = new_background_psql();
	my ($writer, $writer_pid) = new_background_psql();

	start_background_query(
		$reader,
		'spurious_reader',
		"SELECT test_buffer_frame_reader('bf_data'::regclass, 0, '$reader_point', '', false)");
	wait_for_pid_event($reader_pid, $reader_point,
		'reader holds pin for spurious wake');
	start_background_query(
		$writer,
		'spurious_writer',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '$writer_point', 'normal')");
	wait_for_pid_event($writer_pid, 'BufferPin',
		'writer waits before spurious broadcast');

	attach_point($wake_observer, 'wait');
	$node->safe_psql('postgres',
		"SELECT test_buffer_frame_broadcast($buffer_id)");
	wait_for_pid_event($writer_pid, $wake_observer,
		'writer actually returns from ConditionVariableSleep');
	detach_point($wake_observer);
	wake_point($wake_observer);
	wait_for_pid_event($writer_pid, 'BufferPin',
		'writer rechecks the predicate and returns to BufferPin');
	like(buffer_state(), qr/^\d+:t:2:$frame_id$/,
		'spurious broadcast does not change drain predicate');

	wake_point($reader_point);
	is(finish_background_query($reader, 'spurious_reader'), '',
		'reader releases after spurious broadcast');
	wait_for_pid_event($writer_pid, $writer_point,
		'real last-pin release completes drain');
	wake_point($writer_point);
	is(finish_background_query($writer, 'spurious_writer'), '',
		'writer completes after real predicate change');

	detach_point($reader_point);
	detach_point($writer_point);
	$reader->quit;
	$writer->quit;
	assert_clean_and_repin('spurious broadcast');
};

sub run_strict_flush_mode
{
	my ($mode) = @_;
	my $dirty_before = new_point("${mode}_dirty_before");
	my $dirty_after = new_point("${mode}_dirty_after");
	attach_point($dirty_before, 'wait');
	attach_point($dirty_after, 'wait');
	my ($reader, $reader_pid) = new_background_psql();
	my ($writer, $writer_pid) = new_background_psql();
	my ($flusher, $flusher_pid) = new_background_psql();

	start_background_query(
		$reader,
		"${mode}_dirty_reader",
		"SELECT test_buffer_frame_dirty_reader('bf_data'::regclass, 0, '$dirty_before', '$dirty_after')");
	wait_for_pid_event($reader_pid, $dirty_before,
		"$mode: dirtying reader holds the old pin before marking dirty");

	start_background_query(
		$writer,
		"${mode}_dirty_writer",
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '', 'normal')");
	wait_for_pid_event($writer_pid, 'BufferPin',
		"$mode: writer publishes gate while clean and drains old pin");

	wake_point($dirty_before);
	wait_for_pid_event($reader_pid, $dirty_after,
		"$mode: old pinned backend legally marks the gated buffer dirty");
	like(buffer_state(), qr/^\d+:t:2:$frame_id$/,
		"$mode: dirty buffer remains gated with writer and old reader pins");

	start_background_query(
		$flusher,
		"${mode}_strict_flush",
		"SELECT test_buffer_frame_flush('bf_data'::regclass, '$mode')");
	wait_for_pid_event($flusher_pid, 'BufferPin',
		"$mode: strict flush waits instead of skipping gated dirty buffer");

	wake_point($dirty_after);
	is(finish_background_query($reader, "${mode}_dirty_reader"), '',
		"$mode: dirtying reader releases old pin");
	like(
		finish_background_query($writer, "${mode}_dirty_writer"),
		qr/buffer \d+ changed state while draining pins/,
		"$mode: writer fails closed when drained buffer became dirty");
	is(finish_background_query($flusher, "${mode}_strict_flush"), '',
		"$mode: strict flush retries after gate cleanup");
	is(buffer_is_dirty(), 'f', "$mode: strict flush writes the buffer clean");

	detach_point($dirty_before);
	detach_point($dirty_after);
	$reader->quit;
	$writer->quit;
	$flusher->quit;
	assert_clean_and_repin("$mode strict gated dirty flush");
}

for my $mode (qw(relation relations database))
{
	subtest "strict $mode flush waits for a gated dirty buffer" => sub
	{
		run_strict_flush_mode($mode);
	};
}

subtest 'checkpointer waits for a gated dirty buffer' => sub
{
	my $dirty_before = new_point('checkpoint_dirty_before');
	my $dirty_after = new_point('checkpoint_dirty_after');
	attach_point($dirty_before, 'wait');
	attach_point($dirty_after, 'wait');
	my ($reader, $reader_pid) = new_background_psql();
	my ($writer, $writer_pid) = new_background_psql();
	my ($checkpoint, $checkpoint_pid) = new_background_psql();

	start_background_query(
		$reader,
		'checkpoint_dirty_reader',
		"SELECT test_buffer_frame_dirty_reader('bf_data'::regclass, 0, '$dirty_before', '$dirty_after')");
	wait_for_pid_event($reader_pid, $dirty_before,
		'checkpoint case reader holds the old pin');
	start_background_query(
		$writer,
		'checkpoint_dirty_writer',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '', 'normal')");
	wait_for_pid_event($writer_pid, 'BufferPin',
		'checkpoint case writer publishes gate while buffer is clean');

	wake_point($dirty_before);
	wait_for_pid_event($reader_pid, $dirty_after,
		'checkpoint case old reader dirties gated buffer');
	start_background_query($checkpoint, 'gated_checkpoint', 'CHECKPOINT');
	wait_for_backend_type_event('checkpointer', 'BufferPin',
		'checkpointer waits instead of completing past gated dirty buffer');

	wake_point($dirty_after);
	is(finish_background_query($reader, 'checkpoint_dirty_reader'), '',
		'checkpoint case reader releases old pin');
	like(
		finish_background_query($writer, 'checkpoint_dirty_writer'),
		qr/buffer \d+ changed state while draining pins/,
		'checkpoint case writer fails closed on dirty state');
	is(finish_background_query($checkpoint, 'gated_checkpoint'), '',
		'checkpoint completes after gate cleanup');
	is(buffer_is_dirty(), 'f', 'checkpoint writes the selected buffer clean');

	detach_point($dirty_before);
	detach_point($dirty_after);
	$reader->quit;
	$writer->quit;
	$checkpoint->quit;
	assert_clean_and_repin('gated dirty checkpoint');
};

subtest 'ordinary cleanup waiter bit still admits new pins' => sub
{
	my $old_point = new_point('cleanup_old');
	my $new_point = new_point('cleanup_new');
	my $cleanup_point = new_point('cleanup_after');
	attach_point($old_point, 'wait');
	attach_point($new_point, 'wait');
	attach_point($cleanup_point, 'wait');
	my ($old_reader, $old_pid) = new_background_psql();
	my ($new_reader, $new_pid) = new_background_psql();
	my ($cleanup, $cleanup_pid) = new_background_psql();

	start_background_query(
		$old_reader,
		'cleanup_old',
		"SELECT test_buffer_frame_reader('bf_data'::regclass, 0, '$old_point', '', false)");
	wait_for_pid_event($old_pid, $old_point,
		'old reader blocks cleanup lock');
	start_background_query(
		$cleanup,
		'cleanup_waiter',
		"SELECT test_buffer_frame_cleanup('bf_data'::regclass, 0, '', '$cleanup_point')");
	wait_for_pid_event($cleanup_pid, 'BufferPin',
		'cleanup backend waits for sole pin');
	is(buffer_state(), "-1:t:2:$frame_id",
		'cleanup bit has no write-intent owner');

	start_background_query(
		$new_reader,
		'cleanup_new',
		"SELECT test_buffer_frame_reader('bf_data'::regclass, 0, '$new_point', '', false)");
	wait_for_pid_event($new_pid, $new_point,
		'new first pin crosses ordinary cleanup bit');
	is(buffer_state(), "-1:t:3:$frame_id",
		'ordinary cleanup bit remains while new reader is pinned');

	wake_point($old_point);
	is(finish_background_query($old_reader, 'cleanup_old'), '',
		'old reader releases cleanup pin');
	wait_for_pid_event($cleanup_pid, 'BufferPin',
		'cleanup still waits for new reader');
	wake_point($new_point);
	is(finish_background_query($new_reader, 'cleanup_new'), '',
		'new reader releases cleanup pin');
	wait_for_pid_event($cleanup_pid, $cleanup_point,
		'last foreign release wakes cleanup backend');
	is(buffer_state(), "-1:f:1:$frame_id",
		'ordinary cleanup bit clears before cleanup proceeds');
	wake_point($cleanup_point);
	is(finish_background_query($cleanup, 'cleanup_waiter'), '',
		'cleanup backend completes');

	detach_point($old_point);
	detach_point($new_point);
	detach_point($cleanup_point);
	$old_reader->quit;
	$new_reader->quit;
	$cleanup->quit;
	assert_clean_and_repin('ordinary cleanup waiter');
};

subtest 'writer fails fast after crossing an ordinary cleanup waiter bit' => sub
{
	my $reader_point = new_point('reverse_cleanup_reader');
	my $cleanup_point = new_point('reverse_cleanup_after');
	attach_point($reader_point, 'wait');
	attach_point($cleanup_point, 'wait');
	my ($reader, $reader_pid) = new_background_psql();
	my ($cleanup, $cleanup_pid) = new_background_psql();
	my ($writer, $writer_pid) = new_background_psql();

	start_background_query(
		$reader,
		'reverse_cleanup_reader',
		"SELECT test_buffer_frame_reader('bf_data'::regclass, 0, '$reader_point', '', false)");
	wait_for_pid_event($reader_pid, $reader_point,
		'old reader holds pin before cleanup waiter');

	start_background_query(
		$cleanup,
		'reverse_cleanup',
		"SELECT test_buffer_frame_cleanup('bf_data'::regclass, 0, '', '$cleanup_point')");
	wait_for_pid_event($cleanup_pid, 'BufferPin',
		'cleanup waiter publishes ordinary waiter bit');
	is(buffer_state(), "-1:t:2:$frame_id",
		'ordinary waiter bit is published without an owner');

	start_background_query(
		$writer,
		'reverse_writer',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '', 'normal')");
	like(
		finish_background_query($writer, 'reverse_writer'),
		qr/buffer \d+ already has a pin-count waiter/,
		'writer first pin crosses ordinary bit but Begin fails fast');
	wait_for_pid_event($cleanup_pid, 'BufferPin',
		'writer cleanup leaves ordinary cleanup waiter draining old reader');

	wake_point($reader_point);
	is(finish_background_query($reader, 'reverse_cleanup_reader'), '',
		'old reader releases its pin');
	wait_for_pid_event($cleanup_pid, $cleanup_point,
		'cleanup waiter proceeds after writer failure and old-pin release');
	wake_point($cleanup_point);
	is(finish_background_query($cleanup, 'reverse_cleanup'), '',
		'cleanup waiter completes');

	detach_point($reader_point);
	detach_point($cleanup_point);
	$reader->quit;
	$cleanup->quit;
	$writer->quit;
	assert_clean_and_repin('cleanup waiter then writer');
};

subtest 'cleanup waiter fails fast against write-intent gate' => sub
{
	my $cleanup_before = new_point('interop_cleanup');
	my $writer_before = new_point('interop_writer_before');
	my $writer_after = new_point('interop_writer_after');
	attach_point($cleanup_before, 'wait');
	attach_point($writer_before, 'wait');
	attach_point($writer_after, 'wait');
	my ($cleanup, $cleanup_pid) = new_background_psql();
	my ($writer, $writer_pid) = new_background_psql();

	start_background_query(
		$cleanup,
		'interoperable_cleanup',
		"SELECT test_buffer_frame_cleanup('bf_data'::regclass, 0, '$cleanup_before', '')");
	start_background_query(
		$writer,
		'interoperable_writer',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '$writer_before', '$writer_after', 'normal')");
	wait_for_pid_event($cleanup_pid, $cleanup_before,
		'cleanup backend is pre-pinned');
	wait_for_pid_event($writer_pid, $writer_before,
		'writer backend is pre-pinned');

	wake_point($writer_before);
	wait_for_pid_event($writer_pid, 'BufferPin',
		'writer publishes gate and drains cleanup backend');
	wake_point($cleanup_before);
	like(
		finish_background_query($cleanup, 'interoperable_cleanup'),
		qr/buffer \d+ has a pending write intent/,
		'cleanup waiter fails fast against write gate');
	wait_for_pid_event($writer_pid, $writer_after,
		'cleanup failure releases pin and wakes writer');
	wake_point($writer_after);
	is(finish_background_query($writer, 'interoperable_writer'), '',
		'writer completes after cleanup conflict');

	detach_point($cleanup_before);
	detach_point($writer_before);
	detach_point($writer_after);
	$cleanup->quit;
	$writer->quit;
	assert_clean_and_repin('cleanup and write-intent interoperation');
};

subtest 'backend termination after Begin clears gate' => sub
{
	my $writer_point = new_point('terminate_writer');
	attach_point($writer_point, 'wait');
	my ($writer, $writer_pid) = new_background_psql();

	start_background_query(
		$writer,
		'terminate_writer',
		"SELECT test_buffer_frame_writer('bf_data'::regclass, 0, '', '$writer_point', 'normal')");
	wait_for_pid_event($writer_pid, $writer_point,
		'writer holds gate after Begin');
	like(buffer_state(), qr/^\d+:t:1:$frame_id$/,
		'terminating backend owns pending gate');
	is(
		$node->safe_psql('postgres',
			"SELECT pg_terminate_backend($writer_pid)"),
		't',
		'terminate signal sent');
	ok(
		$node->poll_query_until(
			'postgres',
			"SELECT count(*) = 0 FROM pg_stat_activity WHERE pid = $writer_pid",
			't'),
		'terminated backend exits');
	$writer->{run}->finish;
	detach_point($writer_point);
	assert_clean_and_repin('backend termination');
};

$node->stop;
done_testing();
