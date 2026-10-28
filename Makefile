# Common Kova commands.
#
#   make              show this help
#   make release      optimized build
#   make test         full test suite
#   make test-sql     query engine tests, output visible
#   make run-debug    database engine with debug logs
#   make run-dump     kovadump (ARGS="backup --db db db.kdmp")
#   make seed         load Oracle sample into database `db`
#   make dump         backup database `db` to db.kdmp
#   make restore      restore db.kdmp into database `db`
#   make site         static site on http://127.0.0.1:4173
#
# Config overrides (optional, only exported when set on the make command line):
#   make run KOVA_PORT=27018 KOVA_LOG_LEVEL=debug

CARGO ?= cargo

ifneq ($(KOVA_PORT),)
export KOVA_PORT
endif
ifneq ($(KOVA_LOG_LEVEL),)
export KOVA_LOG_LEVEL
endif
ifneq ($(KOVA_CONFIG),)
export KOVA_CONFIG
endif
ifneq ($(KOVA_WARN_NON_MONGO_QUERIES),)
export KOVA_WARN_NON_MONGO_QUERIES
endif
ifneq ($(BENCH_ROWS),)
export BENCH_ROWS
endif
ifneq ($(BENCH_LOOKUPS),)
export BENCH_LOOKUPS
endif

.DEFAULT_GOAL := help

.PHONY: help build release test test-sql test-one check fmt fmt-check clippy lint run run-release run-debug run-dump run-dump-debug seed indexes server-status views security demos dump restore benchmark doc site clean

help:
	@printf '%s\n' \
		'Targets:' \
		'  build        cargo build' \
		'  release      cargo build --release' \
		'  test         cargo test --all-targets' \
		'  test-sql     query_engine tests (--nocapture)' \
		'  test-one     one test: make test-one TEST=name' \
		'  check        cargo check --all-targets' \
		'  fmt          cargo fmt' \
		'  fmt-check    cargo fmt --check' \
		'  clippy       cargo clippy --all-targets -- -D warnings' \
		'  lint         fmt-check + clippy' \
		'  run          database engine (kova)' \
		'  run-release  database engine, release build' \
		'  run-debug    database engine with KOVA_LOG_LEVEL=debug' \
		'  run-dump     kovadump; ARGS="backup --db db db.kdmp"' \
		'  run-dump-debug  kovadump with KOVA_LOG_LEVEL=debug' \
		'  seed         Oracle sample, emp indexes, and views into database db' \
		'  indexes      index tour over the seeded emp collection' \
		'  server-status  serverStatus counters' \
		'  benchmark    time MQL and SQL five times on database benchmark, write the docs page' \
		'  views        checks the views seed left on emp' \
		'  security     users, roles, and authenticate' \
		'  demos        server-status, views, then security' \
		'  dump         backup database db to db.kdmp' \
		'  restore      restore DUMP into database db (default db.kdmp)' \
		'  doc          cargo doc --no-deps --open' \
		'  site         static site at http://127.0.0.1:4173' \
		'  clean        cargo clean' \
		'' \
		'The mongosh and kovadump targets talk to a server that is already running.' \
		'  make seed                 localhost:27018' \
		'  make views                localhost:27018' \
		'  make demos KOVA_PORT=27018' \
		'  make dump                 database db -> db.kdmp' \
		'  make restore DUMP=pg.sql  database db, port 27018' \
		'  make dump KOVA_DB=shop KOVA_PORT=27019 DUMP=shop.kdmp' \
		'  make benchmark            database benchmark, port 27018' \
		'  make benchmark KOVA_PORT=27019 BENCH_ROWS=1000 BENCH_LOOKUPS=100' \
		'  BENCH_MONGO=1 make benchmark   also time MongoDB on port 27017'

build:
	$(CARGO) build

release:
	$(CARGO) build --release

test:
	$(CARGO) test --all-targets

test-sql:
	$(CARGO) test --lib query_engine -- --nocapture

test-one:
	@test -n "$(TEST)" || { echo 'usage: make test-one TEST=<filter>'; exit 1; }
	$(CARGO) test --all-targets $(TEST) -- --nocapture

check:
	$(CARGO) check --all-targets

fmt:
	$(CARGO) fmt

fmt-check:
	$(CARGO) fmt --check

clippy:
	$(CARGO) clippy --all-targets -- -D warnings

lint: fmt-check clippy

run:
	$(CARGO) run --bin kova

run-release:
	$(CARGO) run --release --bin kova

run-debug:
	KOVA_LOG_LEVEL=debug $(CARGO) run --bin kova

run-dump:
	$(CARGO) run --bin kovadump -- $(if $(ARGS),$(ARGS),--help)

run-dump-debug:
	KOVA_LOG_LEVEL=debug $(CARGO) run --bin kovadump -- $(if $(ARGS),$(ARGS),--help)

# Reloads the Oracle Scott sample into database `db`, then creates the
# five emp indexes and the well_paid / emp_search views. Drops that
# database first. The server must already be listening.
seed:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/seed-oracle.js

# Walks emp indexes against the seeded sample. Leaves the five indexes.
indexes:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/index-demo.js

# serverStatus, views, and security. Each script prints ok/FAIL and exits
# non-zero on the first failure. They share the port with seed and indexes.
server-status:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/server-status.js

# Department pay rollup over the Scott sample. Requires `make seed`.
# Prints each department, then the unweighted and weighted company averages.
# Leaves the department rows in `deptPay`.
dept-pay:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/dept-pay.js

# Checks well_paid and emp_search. Requires `make seed`. Does not drop them.
views:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/views.js

security:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/security.js

# Times MQL and SQL on a server that is already listening. Drops and
# Runs the pass five times, recreating only the database `benchmark`, then writes the means.
# Asks whether to repeat the same steps on MongoDB (port 27017).
#   make benchmark
#   make benchmark KOVA_PORT=27019 BENCH_ROWS=1000 BENCH_LOOKUPS=100
#   BENCH_MONGO=1 make benchmark
benchmark:
	python3 scripts/benchmark.py

demos: server-status views security

# Backup and restore talk to a server that is already listening.
# Database defaults to db, port to 27018, file to db.kdmp.
#   make dump
#   make restore
#   make restore DUMP=pg.sql KOVA_DB=shop KOVA_PORT=27019
#   make restore YES=1
#   make restore YES=1 TABLES="proposta_venda venda"
KOVA_DB ?= db
DUMP ?= db.kdmp
YES ?=
TABLES ?=

dump:
	$(CARGO) run --quiet --bin kovadump -- backup --host 127.0.0.1 --port $(if $(KOVA_PORT),$(KOVA_PORT),27018) --db $(KOVA_DB) $(DUMP)

restore:
	$(CARGO) run --quiet --bin kovadump -- restore --host 127.0.0.1 --port $(if $(KOVA_PORT),$(KOVA_PORT),27018) --db $(KOVA_DB) $(if $(YES),--yes,) $(foreach table,$(TABLES),--table $(table)) $(DUMP)

doc:
	$(CARGO) doc --no-deps --open

# The published site drops .html from addresses. This server does the same.
#   make site
#   make site SITE_PORT=4180
SITE_PORT ?= 4173

site:
	python3 scripts/serve-site.py $(SITE_PORT)

clean:
	$(CARGO) clean
