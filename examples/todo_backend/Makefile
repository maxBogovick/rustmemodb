SHELL := /bin/bash

.PHONY: run run-rustmemodb up down migrate seed test test-db

run:
	cargo run --bin todo_backend

run-rustmemodb:
	./scripts/run-rustmemodb.sh

up:
	./scripts/dev-up.sh

down:
	./scripts/dev-down.sh

migrate:
	./scripts/migrate-up.sh

seed:
	./scripts/seed.sh

test:
	./scripts/test.sh

test-db:
	./scripts/test-with-postgres.sh
