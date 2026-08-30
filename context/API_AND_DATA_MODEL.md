# API и модель данных

Канонический контракт находится в `docs/openapi.yaml`.

## API

| Метод и путь | Назначение | Результат |
|---|---|---|
| `GET /profiles` | Список профилей и доступных годов | `Profile[]` |
| `GET /profiles/{id}` | Один профиль | `Profile` |
| `POST /recaps` | Создать задачу или вернуть готовый recap | `202 RecapRequestStatus` или `200 Recap` |
| `GET /recaps/{id}` | Состояние задачи или готовый recap | `RecapRequestStatus` или `Recap` |
| `GET /recaps/{id}/stream` | Прогресс через SSE | `status / ready / failed` |
| `GET /recaps/{id}/explanation` | Объяснение роли, стиля и достижений | `RecapExplanation` |
| `GET /recaps/{id}/share` | Публичная проекция | `ShareCard` |
| `POST /recaps/{id}/interactions` | Продуктовое событие | `InteractionResponse` |

Готовый recap неизменяем. Повторный запрос для той же пары профиля, года и версии алгоритма возвращает существующую задачу или сохранённый результат.

## Хранение

- PostgreSQL: профили, справочники, задачи, outbox/inbox, snapshots, объяснения и публичные карточки;
- ClickHouse: активность и interaction events;
- Redpanda: внутренние команды worker и события жизненного цикла.

Задача и команда outbox создаются одной транзакцией. Готовый snapshot и статус `ready` также сохраняются одной транзакцией.

## Карточки

`RecapCard` различается по полю `type`:

- `intro`;
- `metric`;
- `district`;
- `archetype`;
- `achievements`;
- `summary`;
- `final`.

## Ошибки

Единый формат ответа — `APIError`. Основные коды:

- `invalid_argument`;
- `profile_not_found`;
- `recap_not_found`;
- `insufficient_activity`;
- `rate_limit_exceeded`;
- `dependency_unavailable`;
- `internal_error`.
