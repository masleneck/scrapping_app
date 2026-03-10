# scrapping_app

Прототип резервного канала для УМС: real-source скрейпинг, нормализация рейсов, дедупликация, событийная модель `add/upd/del`, хранение в PostgreSQL и dashboard-аналитика.

## Что реализовано
- `GET /health` — статус сервиса.
- `GET /dashboard` — web-интерфейс:
  - поиск/фильтры по актуальным рейсам;
  - таблица parser-performance;
  - график распределения статусов;
  - последние события.
- `GET /flights/scrape-real` — сбор real-source и запись только `RMSEVENT_ADD/UPDATE/DELETE`.
- `GET /flights/current` — единый реестр актуальных рейсов (без дублей по источникам).
- `GET /events` / `GET /events/stats` / `GET /events/real-stats`.
- `POST /admin/cleanup-real-data` — очистка real-source состояния (`flight_current`, `real_source_state`, `real_source_runs`, RMSEVENT в `flight_events`).

## Нормализация и уникальность рейса
- Номер приводится к единому формату (`normalized_flight_number`), например:
  - `FV6519`, `SU6519Россия, Аэрофлот`, `SU 6519` -> единый нормализованный вид.
- Идентификатор экземпляра рейса (`flight_instance_key`) строится по:
  - `normalized_flight_number + direction + schedule-anchor`.
- Повтор одного и того же рейса из разных источников схлопывается в одну запись `flight_current`.
- При конфликте выбирается наиболее актуальный снапшот по времени/приоритету источника/полноте полей.

## Источники
- `svo_official_bitrix` (`https://www.svo.aero/bitrix/timetable/`).
- `kupibilet` (embedded + generic parser).
- `yandex_rasp` (bs4 + regex parser).
- `tripcom` (generic parser).
- `flightaware`:
  - `https://ru.flightaware.com/live/airport/UUEE` (bs4 + regex parser),
  - sample history page parser.

Это позволяет сравнивать разные стратегии на одном и том же источнике в `parser_performance`.

## Событийная модель
Используются только 3 типа изменений:
- `RMSEVENT_ADD` — рейс появился.
- `RMSEVENT_UPDATE` — изменились данные рейса.
- `RMSEVENT_DELETE` — рейс исчез из актуального состояния.

## Конфигурация `.env`
- `SOURCE_URL`
- `SOURCE_URLS`
- `DATABASE_URL`
- `REAL_SCRAPE_SCHEDULER_ENABLED`
- `REAL_SCRAPE_INTERVAL_SECONDS`
- `REAL_SCRAPE_RETRY_ATTEMPTS`
- `REAL_SCRAPE_RETRY_BACKOFF_SECONDS`
- `REAL_SCRAPE_ENRICH_SVO_INFO_ENABLED`
- `REAL_SCRAPE_ENRICH_SVO_INFO_LIMIT`

## Быстрый старт
```bash
make build
make up
```

Проверка:
```bash
curl http://localhost:8000/health
curl "http://localhost:8000/flights/scrape-real?persist=true"
curl "http://localhost:8000/flights/current?limit=20"
curl "http://localhost:8000/events/real-stats?hours=24"
```

Открыть dashboard:
```bash
open http://localhost:8000/dashboard
```

## Миграции
```bash
make db-upgrade
make db-history
```

## Ограничения и правовой режим
- Не используется обход CAPTCHA/защит.
- Используются легальные методы устойчивости:
  - retry/backoff,
  - стандартные header-профили,
  - диагностика `blocked/error`.
