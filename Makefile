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

.PHONY: help build release test test-sql test-one check fmt fmt-check clippy lint run run-release run-debug seed doc clean

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
		'  doc          cargo doc --no-deps --open' \
		'  clean        cargo clean' \
		'' \
		'seed talks to a server that is already running.' \
		'  make seed                 localhost:27018' \
		'  make seed KOVA_PORT=27018'

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

doc:
	$(CARGO) doc --no-deps --open

clean:
	$(CARGO) clean
