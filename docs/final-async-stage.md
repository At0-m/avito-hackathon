# Асинхронная доработка после хакатона

Этот контур был добавлен после завершения хакатонного MVP. Его задача — вынести генерацию из HTTP-запроса и проверить более надёжную схему обработки.

## Текущий путь

```text
POST /api/v1/recaps
  -> PostgreSQL: recap_request + outbox_event
  -> outbox publisher
  -> Redpanda
  -> worker + дедупликация inbox
  -> ClickHouse, персонализация и narrative fallback
  -> PostgreSQL: snapshot + status ready
```

Frontend подключается к `GET /api/v1/recaps/{id}/stream`. При разрыве SSE он продолжает получать состояние обычными `GET`-запросами.

## Что проверяют тесты

- конкурентный claim через `FOR UPDATE SKIP LOCKED`;
- восстановление после истечения lease;
- запрет старому worker завершать чужую задачу;
- откат транзакции при ошибке сохранения snapshot;
- конкурентную обработку outbox;
- дедупликацию команд через `consumed_events`;
- создание новой команды при retry;
- heartbeat worker и остановку обработки после потери lease;
- постоянные и временные ошибки;
- SSE-события `ready`/`failed` и переход на polling.

Docker smoke test дополнительно останавливает workers и Redpanda во время активных запросов и проверяет восстановление.

## Модель доставки

Команды доставляются как минимум один раз. Повторная доставка допустима, поэтому обработка защищена `command_id`, уникальностью request, lease и ограничениями snapshot. После исчерпания попыток задача получает статус `failed`, а диагностическое событие отправляется в DLQ.

## Нагрузочные тесты

```bash
make bench-smoke
make bench-paths
make bench-matrix
make bench-faults
```

Отчёты содержат задержку принятия запроса, время до `ready`, пропускную способность, размер очереди, время обработки backlog, ошибки, потерянные запросы и повторы. Сведения о машине сохраняются в `environment.json`.

Критерии и параметры описаны в [`../bench/README.md`](../bench/README.md).

## Режимы worker

- `WORKER_COMMAND_SOURCE=hybrid` — Redpanda и резервный polling PostgreSQL;
- `WORKER_COMMAND_SOURCE=broker` — только Redpanda;
- `WORKER_COMMAND_SOURCE=database` — очередь PostgreSQL для сравнения.

Масштабирование:

```bash
docker compose up -d --build --scale worker=3
```
