# Асинхронная генерация recap

Асинхронный контур появился после хакатонного MVP. Публичный API остался совместимым с готовым recap, но тяжёлая работа выполняется отдельным worker.

## Публичный API

`POST /api/v1/recaps` быстро принимает запрос:

- готовый snapshot — `200 OK` и объект `Recap`;
- новая или выполняющаяся задача — `202 Accepted` и состояние `RecapRequestStatus`.

Состояние можно получить двумя способами:

- `GET /api/v1/recaps/{id}` — обычный polling;
- `GET /api/v1/recaps/{id}/stream` — SSE-события `status`, `ready`, `failed`.

Frontend сначала подключается к SSE. При ошибке соединения он продолжает polling по `links.self` с интервалом `poll_after_ms`.

## Путь команды

```text
Recap API
  -> PostgreSQL: recap_request + outbox_event
  -> Outbox Publisher
  -> Redpanda: recap.generation-commands.v1
  -> Worker
  -> consumed_events
  -> claim recap_request
```

Запрос и запись outbox создаются одной транзакцией. Если Redpanda временно недоступна, API продолжает принимать запросы, а publisher отправит накопленные команды после восстановления.

## Повторная доставка

Redpanda может доставить команду больше одного раза. Повторную обработку ограничивают:

- `consumed_events` с ключом `(consumer_name, event_id)`;
- уникальность `profile_id + year + algorithm_version`;
- проверка владельца задачи по `worker_id`;
- уникальность готового recap для профиля и года.

В Docker Compose по умолчанию используется режим `hybrid`: worker читает команды из Redpanda и одновременно проверяет очередь PostgreSQL. Для тестов доступны режимы `database` и `broker`.

## Claim и lease

Worker выбирает задачу через:

```sql
FOR UPDATE SKIP LOCKED
```

При получении задачи сохраняются `worker_id`, `locked_at`, `lease_expires_at`, а `attempt_count` увеличивается. Heartbeat продлевает lease. Если worker остановился, после истечения lease задачу может забрать другой процесс.

Все изменения выполняются с проверкой владельца. Старый worker не может сохранить результат после потери lease.

## Повторные попытки

Временные ошибки возвращают задачу в `queued` с увеличивающейся задержкой. После `max_attempts` задача становится `failed`, а событие отправляется в DLQ.

Без повторов завершаются бизнес-ошибки:

- недостаточно активности;
- профиль не найден;
- выбранный год недоступен.

Ошибка Mistral не останавливает генерацию: используется шаблонный текст.

## Завершение задачи

Snapshot, карточки, метрики, роль, достижения, объяснение, публичная проекция и статус `ready` сохраняются одной транзакцией PostgreSQL. Состояние `ready` без готового результата не возникает.

## SSE

SSE endpoint сразу отправляет текущее состояние и затем читает его из PostgreSQL. Поток закрывается после `ready` или `failed`. Nginx buffering для этого маршрута отключён.

## Запуск

```bash
docker compose up -d --build
```

Основные сервисы:

```text
frontend
backend
postgres
clickhouse
redpanda
outbox
worker
```

Несколько workers:

```bash
docker compose up -d --scale worker=3
```

## Проверки

```bash
make bench-smoke
make bench-faults
```

CI проверяет конкурентный claim, восстановление lease, дедупликацию inbox, outbox, SSE и обработку временных и постоянных ошибок.

## Переменные окружения

```env
WORKER_COMMAND_SOURCE=hybrid
WORKER_POLL_INTERVAL=500ms
WORKER_LEASE_DURATION=30s
WORKER_HEARTBEAT_INTERVAL=10s
WORKER_RETRY_BASE=2s
WORKER_RETRY_MAX=30s
WORKER_MAX_ATTEMPTS=3
WORKER_SHUTDOWN_TIMEOUT=15s
RECAP_POLL_AFTER=500ms

REDPANDA_HTTP_URL=http://localhost:18082
REDPANDA_COMMAND_TOPIC=recap.generation-commands.v1
REDPANDA_LIFECYCLE_TOPIC=recap.lifecycle.v1
REDPANDA_DLQ_TOPIC=recap.generation-commands.dlq.v1
REDPANDA_CONSUMER_GROUP=recap-workers-v1
OUTBOX_POLL_INTERVAL=250ms
OUTBOX_LEASE_DURATION=15s
OUTBOX_BATCH_SIZE=50

SSE_POLL_INTERVAL=250ms
SSE_HEARTBEAT_INTERVAL=10s
```

Интервал heartbeat должен быть меньше длительности lease.
