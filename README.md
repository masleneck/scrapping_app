# scrapping_app

Шаг 5: прикладной прототип мониторинга рейсов с real-source ingestion, дедупликацией событий, планировщиком и аналитикой по источникам.

## Что уже есть
- `GET /health` — проверка сервиса.
- `GET /dashboard` — простой веб-интерфейс (агрегаты + последние события).
- `GET /flights/scrape` — async сбор рейсов из одного источника (`SOURCE_URL`).
- `GET /flights/scrape-url?url=...` — скрапинг произвольной страницы (для реальных источников).
- `GET /flights/scrape-all` — конкурентный async сбор из нескольких источников.
- `GET /flights/scrape-real` — сбор из реальных источников (официальный `svo.aero` + агрегаторы) с дедупликацией и событиями `RMSEVENT_ADD/UPDATE/DELETE`.
- `GET /events` — фильтруемый список событий из PostgreSQL.
- `GET /events/stats` — аналитические агрегаты (`total`, `by_event_type`, `by_source`, `top_flights`).
- `GET /events/real-stats` — аналитика по real-source (`runs/status/timeline/add-upd-del/active statuses`).
- `GET /events/{id}` — получение события по ID.
- `POST /events` — создание события вручную.
- `PATCH /events/{id}` — обновление события.
- `DELETE /events/{id}` — удаление события.
- Alembic миграции для таблиц `flight_events`, `real_source_state`, `real_source_runs`.
- Репозиторий на async SQLAlchemy + raw SQL (без ORM схем).
- Фоновый scheduler для периодического real-source сбора и дедупликации.
- Расширенные логи `rms_ingest`/`rms_source` (success/fail + add/upd/del/unchanged).

## Конфигурация
Используется `.env` (см. `.env.example`):
- `SOURCE_URL` — URL одного источника.
- `SOURCE_URLS` — список URL через запятую для batch-сбора.
- `DATABASE_URL` — URL PostgreSQL (`postgresql+asyncpg://...`).
- `REAL_SCRAPE_SCHEDULER_ENABLED` — включение фонового scheduler (`true/false`).
- `REAL_SCRAPE_INTERVAL_SECONDS` — период планировщика в секундах.
- `REAL_SCRAPE_RETRY_ATTEMPTS` — число retry для HTTP-запросов к real-source.
- `REAL_SCRAPE_RETRY_BACKOFF_SECONDS` — базовый backoff между retry.

## Быстрый старт (Docker)
```bash
make build
make up
```

`make build` нужен при первом запуске и после изменений в `requirements.txt`.
`make up` теперь не делает принудительную пересборку образов.

Проверка:
```bash
curl http://localhost:8000/health
curl http://localhost:8000/flights/scrape
curl http://localhost:8000/flights/scrape-real
curl "http://localhost:8000/flights/scrape-url?url=http://mock-source:8080/flights.html&source=demo_real"
curl "http://localhost:8000/events/stats"
curl "http://localhost:8000/events/real-stats?hours=24"
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

## Реальные источники и ограничения
- Для `svo.aero` используется JSON endpoint `https://www.svo.aero/bitrix/timetable/`.
- Для агрегаторов (например, `kupibilet`) используется извлечение встроенных данных страницы.
- Для источников с anti-bot/challenge фиксируется диагностический статус (`blocked/error`) без попыток обхода защит.
- Добавлены легальные меры устойчивости: retry/backoff и совместимые header-профили.

## Troubleshooting
1. `docker compose ps` — проверить, что `app`, `postgres`, `mock-source` в статусе `Up`.
2. `docker compose logs -f app` — посмотреть ошибки FastAPI/Alembic.
3. `docker compose logs -f postgres` — проверить готовность БД.
4. Убедиться, что порты `8000`, `8080`, `5432` не заняты.
5. Проверить scheduler-логи по ключам `rms_ingest` и `rms_source`.
