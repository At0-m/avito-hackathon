# События Redpanda

Redpanda используется только внутри backend-контура. Публичный клиент работает через REST API и OpenAPI-контракт.

## Топики

| Топик | Ключ | Отправитель | Получатель | Контракт |
|---|---|---|---|---|
| `recap.generation-commands.v1` | `recap_id` | outbox publisher | группа recap workers | `GenerateRecapCommandV1` |
| `recap.lifecycle.v1` | `recap_id` | recap workers | метрики и будущие проекторы | `RecapLifecycleV1` |
| `recap.generation-commands.dlq.v1` | `recap_id` | recap workers | ручная диагностика | `FailedCommandV1` |

Версия схемы входит в название топика. В `v1` можно добавлять необязательные поля без нарушения обратной совместимости. Несовместимые изменения требуют новой версии.

## GenerateRecapCommandV1

```json
{
  "command_id": "8bed08fc-9cbe-4c9a-8ea0-67480918d7b8",
  "recap_id": "6dfc1418-e106-49ab-b839-c9498418e8f6",
  "profile_id": "33333333-3333-3333-3333-333333333333",
  "year": 2026,
  "algorithm_version": "recap-rules-2026.08.2-spike",
  "requested_at": "2026-08-29T12:00:00Z",
  "force_regenerate": false
}
```

`command_id` относится к конкретной попытке доставки. При повторном запуске задачи создаётся новая команда, а `recap_id` остаётся прежним.

## RecapLifecycleV1

```json
{
  "event_id": "d550157f-9b44-4f08-b404-b20449147454",
  "recap_id": "6dfc1418-e106-49ab-b839-c9498418e8f6",
  "status": "processing",
  "stage": "computing_features",
  "progress_percent": 35,
  "attempt": 1,
  "occurred_at": "2026-08-29T12:00:00.500Z"
}
```

Состояние задачи хранится в PostgreSQL. События жизненного цикла нужны для метрик и дополнительных проекторов, но их потеря не меняет состояние запроса.

## FailedCommandV1

```json
{
  "event_id": "9ba5b859-acb7-458a-8802-c08210c88ebd",
  "command_id": "8bed08fc-9cbe-4c9a-8ea0-67480918d7b8",
  "recap_id": "6dfc1418-e106-49ab-b839-c9498418e8f6",
  "error_code": "dependency_unavailable",
  "error_message": "activity source is unavailable",
  "attempt_count": 3,
  "failed_at": "2026-08-29T12:00:30Z"
}
```

## Гарантии доставки

- `recap_requests` и `outbox_events` создаются одной транзакцией;
- outbox повторяет публикацию после недоступности брокера;
- одна команда может прийти повторно, если публикация прошла, а подтверждение в PostgreSQL не сохранилось;
- `consumed_events` устраняет повторную обработку одного `command_id`;
- уникальность запроса, lease и неизменяемый snapshot дают дополнительную защиту;
- в обычном Docker Compose используется режим `hybrid`: Redpanda остаётся основным транспортом, а PostgreSQL polling страхует временные проблемы consumer group;
- `database` используется как базовый вариант для сравнения, `broker` — как строгий режим только через Redpanda.
