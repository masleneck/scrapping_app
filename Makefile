PYTHON ?= python

.PHONY: install lint format test run up down logs db-upgrade db-downgrade db-history

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e .[dev]

lint:
	ruff check app tests

format:
	ruff check --fix app tests
	ruff format app tests

test:
	pytest -q

run:
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

up:
	docker compose up --build -d postgres mock-source app

down:
	docker compose down

logs:
	docker compose logs -f app

db-upgrade:
	alembic upgrade head

db-downgrade:
	alembic downgrade -1

db-history:
	alembic history --verbose
