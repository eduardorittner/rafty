.PHONY: up down build clean wasm

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
