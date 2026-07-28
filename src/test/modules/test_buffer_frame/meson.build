# Copyright (c) 2025, PostgreSQL Global Development Group

test_buffer_frame_sources = files(
  'test_buffer_frame.c',
)

if host_system == 'windows'
  test_buffer_frame_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_buffer_frame',
    '--FILEDESC', 'test_buffer_frame - test shared buffer frame pin gates',])
endif

test_buffer_frame = shared_module('test_buffer_frame',
  test_buffer_frame_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_buffer_frame

test_install_data += files(
  'test_buffer_frame.control',
  'test_buffer_frame--1.0.sql',
)

tests += {
  'name': 'test_buffer_frame',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'env': {
       'enable_injection_points': get_option('injection_points') ? 'yes' : 'no',
    },
    'tests': [
      't/001_pin_gate.pl',
    ],
  },
}
