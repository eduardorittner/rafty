.PHONY: up down build clean wasm test

# Support passing a number of tests as an argument (e.g. `make test 1000`)
ifeq ($(firstword $(MAKECMDGOALS)),test)
  # use the rest as arguments for the "test" target
  TEST_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets so make won't complain about missing targets
  $(eval $(TEST_ARGS):;@:)
endif

test:
	python3 scripts/run_tests.py $(TEST_ARGS)

up:
	docker compose -f docker-scenario/docker-compose.yml up --build -d

down:
	docker compose -f docker-scenario/docker-compose.yml down

build:
	docker compose -f docker-scenario/docker-compose.yml build

clean:
	docker compose -f docker-scenario/docker-compose.yml down -v --rmi local

wasm:
	cd harness && wasm-pack build --release --target web --out-dir pkg --out-name rafty_wasm
