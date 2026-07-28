# src/test/modules/test_buffer_frame/Makefile

MODULE_big = test_buffer_frame
OBJS = \
	$(WIN32RES) \
	test_buffer_frame.o
PGFILEDESC = "test_buffer_frame - test shared buffer frame pin gates"

EXTENSION = test_buffer_frame
DATA = test_buffer_frame--1.0.sql

EXTRA_INSTALL = src/test/modules/injection_points
TAP_TESTS = 1

export enable_injection_points

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = src/test/modules/test_buffer_frame
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
