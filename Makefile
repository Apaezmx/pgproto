MODULE_big = pgproto
UPB_OBJS = $(patsubst %.c,%.o,$(shell find vendor/upb/upb -name "*.c" ! -path "*/conformance/*" ! -path "*/stage0/*" ! -path "*/cmake/*")) \
           vendor/upb/upb/reflection/stage0/google/protobuf/descriptor.upb.o

OBJS = src/pgproto.o src/io.o src/registry.o src/navigation.o src/gin.o src/json.o src/mutation.o $(UPB_OBJS) third_party/protobuf/third_party/utf8_range/utf8_range.o
PG_CPPFLAGS = -std=c99 -I. -Ivendor/upb -Ivendor/upb/upb/reflection/stage0 -Ithird_party/protobuf/third_party/utf8_range -DUPB_BOOTSTRAP_STAGE=0



EXTENSION = pgproto
DATA = sql/pgproto--1.0.sql
REGRESS = pgproto_test

PG_CONFIG ?= pg_config
PGXS := $(shell "$(PG_CONFIG)" --pgxs)
include $(PGXS)


