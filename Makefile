MODULE_big = pgproto
UPB_OBJS = $(patsubst %.c,%.o,$(shell find third_party/upb/archive/upb -name "*.c" ! -path "*/conformance/*" ! -path "*/stage0/*")) \
           third_party/upb/archive/upb/reflection/stage0/google/protobuf/descriptor.upb.o

OBJS = src/pgproto.o src/io.o src/registry.o src/navigation.o src/gin.o src/json.o $(UPB_OBJS) third_party/utf8_range/naive.o
PG_CPPFLAGS = -Ithird_party/upb/archive -Ithird_party/upb/archive/upb/reflection/stage0 -Ithird_party/utf8_range



EXTENSION = pgproto
DATA = sql/pgproto--1.0.sql
REGRESS = pgproto_test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
