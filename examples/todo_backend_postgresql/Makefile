SHELL := /bin/bash

.PHONY: run up down migrate seed test test-db

run:
	cargo run

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
