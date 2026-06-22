.PHONY: up down build clean

up:
	docker compose -f docker-scenario/docker-compose.yml up --build -d

down:
	docker compose -f docker-scenario/docker-compose.yml down

build:
	docker compose -f docker-scenario/docker-compose.yml build

clean:
	docker compose -f docker-scenario/docker-compose.yml down -v --rmi local
