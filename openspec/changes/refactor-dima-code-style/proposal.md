# Proposal: Рефакторинг payment-system под кодстайл Dima

**Дата:** 2026-10-15
**Ветка:** `refactor/dima-code-style`
**Тип:** Полный рефакторинг структуры и стиля, **без изменения поведения и API-контракта**.

## Зачем (Why)

Сервис полностью реализует асинхронное ТЗ процессинга платежей (FastAPI + SQLAlchemy
async + Alembic + FastStream/RabbitMQ + Outbox + retry/DLQ + HMAC webhook +
идемпотентность). Все 36 тестов зелёные. Однако текущая раскладка — плоский пакет
`app/` со `structlog` и `ruff` — не соответствует обязательному кодстайлу владельца
(`dima-code-style`). Нужен полный рефакторинг структуры, слоёв и конвенций без
регрессий функциональности.

## Что меняется (What)

1. **Структура:** плоский `app/` → каноническое дерево: корневые `main.py` + `config.py`,
   пакет `src/` (`database/`, `dependencies/`, `handlers/`, `routers/`, `schemas/`,
   `services/{queries/,uow/}`, `utils/`), пакет `telemetry/`.
2. **Архитектура:** внедряются слои Handler + Query + Unit of Work
   (`AbstractUnitOfWork`, `PaymentsUnitOfWork`, `PaymentQuery`/`OutboxQuery`,
   `PaymentHandler`/`ProcessingHandler`). Роутеры становятся тонкими (только
   `Depends` + вызов handler-а).
3. **Конфиг:** `pydantic-settings` с `frozen=True`, `case_sensitive=False`,
   UPPER_SNAKE-полями, module-level `settings` (без `lru_cache`/`get_settings`).
4. **БД:** `NullPool`, `engine` + `SessionLocal` + `Base` в `src/database/db_settings.py`;
   все ORM-модели в одном файле `src/database/models.py`.
5. **Логирование/телеметрия:** `structlog` удаляется, используется стандартный
   `logging` + OpenTelemetry (`telemetry/setup_telemetry.py`, OTLP gRPC insecure,
   включение в `lifespan` при не-debug режиме).
6. **Схемы:** базовый `CamelSchema` в `src/schemas/base.py`.
7. **Инструменты:** Python 3.13, `uv` (`pyproject.toml` + `uv.lock`) с закреплёнными
   диапазонами версий; Black (dev) вместо ruff; multi-stage `Dockerfile`
   (`python:3.13-slim`) + `gunicorn.conf.py`.
8. **Документация кода:** русские Sphinx-докстринги, inline-комментарии «зачем»,
   маркеры `# === ШАГ N: ... ===`.
9. **Messaging/worker/webhook:** не покрыты стилем — размещаются под `src/messaging/`
   и `src/workers/`, роутингуются через Handler/UoW/Query.

## Вне scope (Out of scope)

- Изменение API-контракта: маршруты, коды ответов, тела запросов/ответов, заголовки
  (`X-API-Key`, `Idempotency-Key`, `X-Signature`, `X-Timestamp`).
- Изменение схемы БД: таблицы `payments`/`outbox`, типы колонок, имена enum
  (`currency`, `payment_status`) и их значения, constraints и имена индексов.
- Изменение RabbitMQ-топологии, алгоритмов retry (3 попытки, backoff 2s/4s через
  TTL+DLX), DLQ, HMAC-подписи, идемпотентности (202 + существующий платёж).
- Изменение числа и ролей сервисов `docker-compose` (остаётся 5: `postgres`,
  `rabbitmq`, `api`, `outbox-worker`, `consumer`).
- Новые фичи и миграция данных.

## Критерии приёмки (Acceptance criteria)

- **AC-1.** WHEN все 36 существующих тестов запускаются против новой структуры
  THEN они проходят; меняются только импорты/имена, ассерты — без изменений.
- **AC-2.** WHEN `docker compose up --build` выполняется THEN стартуют 5 сервисов,
  `alembic upgrade head` проходит, `/health` отдаёт `{"status":"ok"}`, а создание и
  чтение платежа работают как раньше.
- **AC-3.** WHEN на пустой БД выполняется `alembic revision --autogenerate`
  THEN генерируется пустая миграция (схема идентична, нет diff).
- **AC-4.** WHEN POST `/api/v1/payments` повторяется с тем же `Idempotency-Key`
  THEN возвращается `202` с тем же `payment_id` (новый платёж не создаётся).
- **AC-5.** WHEN webhook отвечает `5xx`/`429` THEN сообщение уходит в retry-очередь
  с backoff 2s→4s, после 3 попыток — в DLQ с header `x-reason`.
- **AC-6.** WHEN внешний API сериализует ответ THEN ключи JSON остаются snake_case
  (`payment_id`, `created_at`, `webhook_url`) — фиксированное отступление от
  `CamelSchema`, одобренное владельцем.
- **AC-7.** WHEN код проверяется ревью THEN не встречается: `Optional[...]`/`Union[...]`,
  `structlog`, `ruff`, одинарные кавычки, SQL в роутерах/handler-ах, синхронный `def`
  вместо `async def`; `black --check .` проходит.
