# Common Kova commands.
#
#   make              show this help
#   make release      optimized build
#   make test         full test suite
#   make test-sql     query engine tests, output visible
#   make run-debug    server with debug logs
#   make seed         load Oracle sample into database `db`
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

.DEFAULT_GOAL := help

.PHONY: help build release test test-sql test-one check fmt fmt-check clippy lint run run-release run-debug seed indexes server-status views security demos doc clean

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
		'  run          cargo run' \
		'  run-release  cargo run --release' \
		'  run-debug    cargo run with KOVA_LOG_LEVEL=debug' \
		'  seed         Oracle emp/dept/salgrade/bonus into database db' \
		'  indexes      index tour over the seeded emp collection' \
		'  server-status  serverStatus counters' \
		'  views        views and the on-disk inverted index' \
		'  security     users, roles, and authenticate' \
		'  demos        server-status, views, then security' \
		'  doc          cargo doc --no-deps --open' \
		'  clean        cargo clean' \
		'' \
		'The mongosh targets talk to a server that is already running.' \
		'  make seed                 localhost:27018' \
		'  make views                localhost:27018' \
		'  make demos KOVA_PORT=27018'

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
	$(CARGO) run

run-release:
	$(CARGO) run --release

run-debug:
	KOVA_LOG_LEVEL=debug $(CARGO) run

# Reloads the Oracle Scott sample into database `db`. Drops that database first.
# The server must already be listening. Default port matches kova.toml (27018).
seed:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/seed-oracle.js

# Walks emp indexes against the seeded sample. Does not drop the database.
indexes:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/index-demo.js

# serverStatus, views, and security. Each script prints ok/FAIL and exits
# non-zero on the first failure. They share the port with seed and indexes.
server-status:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/server-status.js

views:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/views.js

security:
	mongosh --quiet "mongodb://127.0.0.1:$(if $(KOVA_PORT),$(KOVA_PORT),27018)/db" --file scripts/security.js

demos: server-status views security

doc:
	$(CARGO) doc --no-deps --open

clean:
	$(CARGO) clean
