# Нагрузочные тесты асинхронной генерации

Тесты проверяют полный путь генерации:

```text
POST /recaps -> PostgreSQL + outbox -> Redpanda -> worker -> ClickHouse -> snapshot
```

Результаты сохраняются в `bench/results/<timestamp>/`. В репозитории нет заранее подготовленных цифр: показатели зависят от машины, Docker и выбранной нагрузки.

## Быстрая проверка

```bash
make bench-smoke
```

По умолчанию создаются 100 профилей, по 50 событий на профиль и 100 запросов с конкурентностью 20. Seed данных — `42`.

Параметры можно переопределить:

```bash
PROFILES=1000 EVENTS_PER_PROFILE=100 REQUESTS=1000 CONCURRENCY=50 make bench-smoke
```

## Сравнение PostgreSQL и Redpanda

```bash
make bench-paths
```

Сценарий дважды запускает один и тот же набор данных на чистых volumes:

- `WORKER_COMMAND_SOURCE=database` — очередь в PostgreSQL;
- `WORKER_COMMAND_SOURCE=broker` — доставка команд через Redpanda.

Итоги записываются в `summary.md`.

## Матрица нагрузки

```bash
make bench-matrix
```

Запускаются режимы 20, 50 и 100 запросов в секунду, а также burst из 1000 запросов.

## Проверка отказов

```bash
make bench-faults
```

Во время нагрузки по очереди останавливаются:

1. workers;
2. Redpanda;
3. ClickHouse.

Для каждого сценария используется отдельный seed, чтобы не переиспользовать готовые snapshots.

## Результаты

Каталог запуска содержит JSON-отчёты, краткие Markdown-сводки, сведения об окружении и логи сервисов. Основные метрики:

- p50/p95/p99 времени принятия `POST`;
- p50/p95/p99 времени до `ready`;
- пропускная способность;
- размер очереди и outbox;
- время обработки backlog;
- потерянные запросы;
- повторяющиеся request ID и snapshots.

Сводный отчёт можно собрать вручную:

```bash
cd backend
go run ./cmd/bench-report --output ../bench/results/summary.md ../bench/results/*-api.json
```

## Критерии

Для текущего стенда используются следующие ориентиры:

- p95 принятия `POST /recaps` — не более 150 мс;
- ошибки API — менее 1% в steady/ramp-сценариях;
- потерянные команды — 0;
- повторяющиеся snapshots — 0;
- backlog из 1000 команд обрабатывается не дольше 60 секунд на указанной машине.

Это проектные ориентиры, а не универсальные гарантии. Вместе с выводами нужно сохранять `environment.json`, исходные отчёты и ограничения ресурсов.

## Ручной запуск

Создание данных:

```bash
cd backend
go run ./cmd/bench-data --profiles 1000 --events-per-profile 100 --seed 42
```

Постоянная нагрузка:

```bash
go run ./cmd/bench-api \
  --scenario steady-20-rps \
  --requests 6000 \
  --concurrency 50 \
  --rate 20 \
  --seed 42 \
  --output ../bench/results/steady-20-rps.json
```

Перезапуск worker во время теста:

```bash
go run ./cmd/bench-fault \
  --service worker \
  --action kill-start \
  --delay 2s \
  --outage 5s \
  --compose-file ../docker-compose.yml
```
