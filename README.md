# scrapping_app

Шаг 4: прикладной прототип мониторинга рейсов — скрапинг страниц, обработка данных в `FlightEvent`, хранение в PostgreSQL, аналитика и простой интерфейс.

## Что уже есть
- `GET /health` — проверка сервиса.
- `GET /dashboard` — простой веб-интерфейс (агрегаты + последние события).
- `GET /flights/scrape` — async сбор рейсов из одного источника (`SOURCE_URL`).
- `GET /flights/scrape-url?url=...` — скрапинг произвольной страницы (для реальных источников).
- `GET /flights/scrape-all` — конкурентный async сбор из нескольких источников.
- `GET /events` — фильтруемый список событий из PostgreSQL.
- `GET /events/stats` — аналитические агрегаты (`total`, `by_event_type`, `by_source`, `top_flights`).
- `GET /events/{id}` — получение события по ID.
- `POST /events` — создание события вручную.
- `PATCH /events/{id}` — обновление события.
- `DELETE /events/{id}` — удаление события.
- Alembic миграции для таблицы `flight_events`.
- Репозиторий на async SQLAlchemy + raw SQL (без ORM схем).

## Конфигурация
Используется `.env` (см. `.env.example`):
- `SOURCE_URL` — URL одного источника.
- `SOURCE_URLS` — список URL через запятую для batch-сбора.
- `DATABASE_URL` — URL PostgreSQL (`postgresql+asyncpg://...`).

## Быстрый старт (Docker)
```bash
make up
```

Проверка:
```bash
curl http://localhost:8000/health
curl http://localhost:8000/flights/scrape
curl "http://localhost:8000/flights/scrape-url?url=http://mock-source:8080/flights.html&source=demo_real"
curl "http://localhost:8000/events/stats"
```

Открыть интерфейс:
```bash
open http://localhost:8000/dashboard
```

## Поддерживаемые HTML-структуры
1. Таблица `table#flights` (текущий mock-источник).
2. Generic-таблица с заголовками `Flight/Direction/Status/...` (или русские аналоги `Рейс/Направление/Статус/...`).

## Alembic
```bash
make db-upgrade
make db-history
```

## Проверка в DBeaver
Параметры подключения к локальному контейнеру PostgreSQL:
- Host: `localhost`
- Port: `5432`
- Database: `scrapping_app`
- User: `scrapping`
- Password: `scrapping`

После `make up` таблица `flight_events` создается миграцией автоматически.

## Поведение при недоступном источнике
Если источник рейсов недоступен, API возвращает `502` c `detail.error=source_unavailable`.

Если страница загружена, но структура не распознана — `422` c `detail.error=source_parse_error`.

## Troubleshooting
1. `docker compose ps` — проверить, что `app`, `postgres`, `mock-source` в статусе `Up`.
2. `docker compose logs -f app` — посмотреть ошибки FastAPI/Alembic.
3. `docker compose logs -f postgres` — проверить готовность БД.
4. Убедиться, что порты `8000`, `8080`, `5432` не заняты.
