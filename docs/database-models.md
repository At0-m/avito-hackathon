# Модели данных PostgreSQL и ClickHouse

Каноническая схема находится в SQL-миграциях `backend/migrations`.

## Разделение хранилищ

- PostgreSQL хранит профили, состояние генерации, outbox/inbox и готовые snapshots;
- ClickHouse хранит события активности и продуктовые interaction events;
- Redpanda передаёт команды и события, но не является источником состояния;
- связи между ClickHouse и PostgreSQL проверяет приложение.

ER-диаграмма основного MVP находится в `docs/database.dbml`. Асинхронные таблицы добавлены миграциями `003_recap_requests.sql` и `004_eventing.sql`.

## PostgreSQL

### Профили и справочники

- `profiles` — имя, описание, аватар и сценарий;
- `profile_available_years` — доступные годы;
- `verticals`, `categories`, `metric_definitions` — таксономия и метрики;
- `archetype_roles`, `archetype_styles`, `achievement_definitions` — варианты персонализации.

### Задачи генерации

`recap_requests` хранит:

- статус `queued / processing / ready / failed`;
- этап и процент выполнения;
- число попыток и время следующего запуска;
- владельца задачи и lease;
- ссылку на готовый recap;
- безопасный код ошибки и признак возможности повтора.

Уникальность `(profile_id, year, algorithm_version)` не позволяет создать две одинаковые задачи. Workers используют `FOR UPDATE SKIP LOCKED`. Изменять задачу в `processing` может только её текущий владелец.

### Outbox и inbox

`outbox_events` содержит команды, созданные в одной транзакции с `recap_request`. Publisher забирает записи по lease, повторяет отправку при ошибке и после успеха переводит запись в `published`.

`consumed_events` хранит обработанные `command_id`. Первичный ключ `(consumer_name, event_id)` защищает от повторной доставки внутри группы consumers.

Опубликованные записи outbox остаются в базе для диагностики. Их очистку можно добавить отдельной retention-задачей.

### Готовый recap

`recaps` — корневая таблица результата. Уникальность `(profile_id, year)` не допускает второй snapshot для той же пары. Одной транзакцией сохраняются:

- `recap_cards`;
- `recap_metrics`;
- `recap_archetypes`;
- `recap_achievements`;
- `recap_explanations` и `recap_rule_facts`;
- `share_cards`, `share_facts`, `share_achievements`;
- переход соответствующей задачи в `ready`.

`recap_cards.data` используется только для данных, зависящих от типа карточки. Поля для связей и ограничений вынесены в отдельные колонки и таблицы.

## ClickHouse

### `activity_events`

- движок: `ReplacingMergeTree(received_at)`;
- partition key: `toYear(occurred_at)`;
- sorting key: `(profile_id, event_id)`;
- более поздняя версия события заменяет предыдущую запись с тем же `profile_id + event_id`;
- worker выбирает события профиля в границах указанного года.

### `interactions`

- движок: `ReplacingMergeTree(received_at)`;
- partition key: `toYYYYMM(occurred_at)`;
- sorting key: `(recap_id, event_id)`;
- повторный `event_id` не создаёт второе логическое событие.

## Изменение схемы

1. Добавлять новую нумерованную миграцию, не переписывая уже применённые.
2. При изменении контракта обновлять DBML, OpenAPI и документацию.
3. Проверять ограничения состояния и владельца интеграционными тестами.
4. Не переносить состояние задачи и готовый recap в ClickHouse или Redpanda.
5. При росте объёма добавить правила очистки `outbox_events` и `consumed_events`.
