PYTHON ?= python

.PHONY: install lint format test run up down logs

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
	docker compose up --build -d app mock-source

down:
	docker compose down

logs:
	docker compose logs -f app
