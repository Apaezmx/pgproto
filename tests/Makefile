CC = gcc
CFLAGS = -I. -Isrc -DPGPROTO_UNIT_TEST -O2 -g -Wall

TESTS = common_test registry_test navigation_test mutation_test json_test io_test

all: $(TESTS)
	@for t in $(TESTS); do \
		echo "Running tests/$$t..."; \
		./tests/$$t || exit 1; \
	done
	@echo "All unit tests passed successfully!"

coverage: CFLAGS += -fprofile-arcs -ftest-coverage
coverage: all
	lcov --capture --directory tests --output-file unit_coverage.info --ignore-errors utility,inconsistent
	lcov --remove unit_coverage.info '/usr/*' --output-file unit_coverage_filtered.info --ignore-errors inconsistent
	lcov --list unit_coverage_filtered.info --ignore-errors inconsistent

common_test: tests/common_test.c
	$(CC) $(CFLAGS) $< -o tests/common_test

registry_test: tests/registry_test.c
	$(CC) $(CFLAGS) $< -o tests/registry_test

navigation_test: tests/navigation_test.c
	$(CC) $(CFLAGS) $< -o tests/navigation_test

mutation_test: tests/mutation_test.c
	$(CC) $(CFLAGS) $< -o tests/mutation_test

json_test: tests/json_test.c
	$(CC) $(CFLAGS) $< -o tests/json_test

io_test: tests/io_test.c
	$(CC) $(CFLAGS) $< -o tests/io_test

clean:
	rm -f $(addprefix tests/, $(TESTS))
	rm -f src/*.gcda src/*.gcno *.gcda *.gcno unit_coverage.info unit_coverage_filtered.info
