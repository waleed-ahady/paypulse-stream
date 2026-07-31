SHELL := /bin/bash

.PHONY: up down reset logs test lint format compose-check ps

up:
	docker compose up --build -d

down:
	docker compose down

reset:
	docker compose down --volumes --remove-orphans

logs:
	docker compose logs --follow --tail=200

ps:
	docker compose ps

test:
	python -m pytest

lint:
	python -m ruff check .

format:
	python -m ruff format .

compose-check:
	docker compose config --quiet

# Purpose: these short commands make common development operations memorable and consistent.
