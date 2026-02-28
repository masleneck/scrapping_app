# scrapping_app

Минимальный стартовый scaffold проекта: FastAPI + Docker/devcontainer + базовые инструменты разработки.

## Что уже есть
- `app/main.py` с endpoint `GET /health`.
- `Dockerfile` и `docker-compose.yml` для dev-контейнера.
- `.devcontainer/devcontainer.json` для VS Code Dev Containers.
- `pyproject.toml` (Python 3.14, FastAPI/uvicorn, dev-зависимости `pytest`/`ruff`).
- `Makefile` с командами: `install`, `lint`, `format`, `test`, `run`.

## Быстрый старт
```bash
make install
make run
```

Проверка:
```bash
curl http://localhost:8000/health
```
