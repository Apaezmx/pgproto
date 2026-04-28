MODULE_big = pgproto
OBJS = src/pgproto.o src/io.o src/registry.o src/navigation.o src/gin.o src/json.o src/mutation.o
PG_CPPFLAGS = -std=c99 -I.

EXTENSION = pgproto
DATA = sql/pgproto--1.0.sql
REGRESS = pgproto_test conformance_test

PG_CONFIG ?= pg_config
PGXS := $(shell "$(PG_CONFIG)" --pgxs)
include $(PGXS)
