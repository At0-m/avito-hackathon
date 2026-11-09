# Нагрузочный стенд Avito Recap

Стенд реализует MVP из [`docs/benchmark-observability.md`](../docs/benchmark-observability.md). В него входят изолированный Compose-проект, k6 с open-model подачей, single-node VictoriaMetrics, Grafana с двумя provisioned dashboards, PostgreSQL sampler, manifest запросов со сверкой в PostgreSQL и итоговый отчёт с exit code. Есть четыре MVP-сценария: smoke, steady, hot key + warm hit и backlog 1000. Отдельно запускаются только вручную burst и soak.

```text
k6 (bench-сеть) ──POST──▶ API ──tx──▶ PostgreSQL (recap_requests + outbox)
     │ remote write                     │
     ▼                                  ▼
VictoriaMetrics ◀─scrape─ API, outbox, каждая реплика worker, sampler, Redpanda
     ▲                                  outbox ──▶ Redpanda ──▶ worker ──▶ ClickHouse ──▶ snapshot в PG
   Grafana
```

> В репозитории нет измеренных значений. Сборка, тесты и прогоны в рамках этой правки не выполнялись. Все цифры и статус «проверено» появятся только после реального запуска на стенде.

## Изоляция от dev-стека

| Что | dev `docker-compose.yml` | bench `bench/compose.bench.yml` |
|---|---|---|
| Compose project | имя каталога | `avito-recap-bench` (флаг `-p` и `name:`) |
| `container_name` | `avito-recap-clickhouse`, `avito-recap-backend`, `avito-recap-frontend` | не задан |
| Порты на хосте | 8080, 5173, 8123, 9000, 19092, 18082, 19644 | только `127.0.0.1:13000` (Grafana) и `127.0.0.1:18428` (VictoriaMetrics), их можно переопределить через `BENCH_GRAFANA_PORT` и `BENCH_VM_PORT` |
| PostgreSQL | в сети Compose, на хост не опубликован | то же; seed, sampler и сверка работают в контейнере `tools` внутри bench-сети |
| Volumes | `<dev>_postgres_data`, … | `avito-recap-bench_*`; dev volumes не затрагиваются |
| Образы | собираются Compose | `avito-recap-bench-app:local`, `avito-recap-bench-tools:local` |

Описания сервисов повторяют `docker-compose.yml`: те же образы PG/CH/Redpanda, миграции и топики. При изменении dev-стека этот файл нужно синхронизировать вручную. Frontend в bench-стенд не входит.

## Требования к хосту

- Linux, Docker Engine с Compose v2 (`docker compose`), `bash`, GNU `date`, `awk`. Go и k6 на хосте не нужны.
- Доступ к Docker Hub и `docker.redpanda.com` для первой загрузки образов: `grafana/k6:0.54.0`, `victoriametrics/victoria-metrics:v1.106.1`, `grafana/grafana:11.3.1`, `postgres:15-alpine`, `clickhouse/clickhouse-server:24.8`, Redpanda `v26.2.2`.
- Памяти должно хватать на PG, CH, Redpanda, API, outbox, 3 worker, VM и Grafana. Ориентир — не меньше 8 ГБ свободной RAM. Лимиты CPU/RAM в Compose не заданы, фактические значения попадают в `host/container-env.txt` (`cpus=`, `memory=`).
- Версии образов закреплены, но в этой правке не скачивались и не проверялись. Имена рядов k6 remote write (`k6_*_total`, `k6_*_p95`) нужно проверить на первом smoke (см. «Что проверить первым запуском»).

## Последовательность команд

Все команды выполняются из корня репозитория.

```bash
# 0. (по желанию) поднять стенд заранее и проверить, что все сервисы живы
make bench-up            # = bash bench/run.sh up
make bench-status

# 1. MVP-сценарии
make bench-mvp-smoke     # smoke: 100 профилей x 50 событий, 5 POST/с
make bench-steady        # 20 POST/с: 1 мин прогрев + 5 мин измерение
make bench-hotkey        # 100 конкурентных POST одной пары, затем warm hit
make bench-backlog       # 1000 queued при остановленных workers, drain тремя workers
make bench-mvp           # все четыре подряд, остановка на первом провале

# 2. Только вручную, не входят в bench-mvp и CI
make bench-burst         # 10 → 100 → 10 POST/с
make bench-soak          # 5 POST/с, 60 минут

# 3. Grafana и VictoriaMetrics
make bench-grafana       # печатает адреса и команду ssh-туннеля
# с рабочей машины: ssh -L 13000:127.0.0.1:13000 -L 18428:127.0.0.1:18428 <bench-host>
# затем http://127.0.0.1:13000 → папка «Avito Recap bench»

# 4. Результат последнего прогона
ls -t bench/results | head
less bench/results/<run-id>/summary.md

# 5. Остановка и очистка
make bench-down          # остановить контейнеры стенда, данные сохраняются
make bench-clean         # удалить контейнеры, сеть, volumes и образы ТОЛЬКО проекта avito-recap-bench
```

Каждый сценарий сам собирает и поднимает стенд (`docker compose build` + `up -d --wait --scale worker=N`). Затем он ждёт, пока в VictoriaMetrics появятся живые targets `api=1, outbox=1, worker=N, sampler=1` и активные consumers Redpanda в каждом worker. После этого сценарий снимает окружение, генерирует свежий детерминированный набор данных с новым `SEED` (по умолчанию текущее время Unix) и выполняет preflight. Preflight проверяет, что каждый профиль на месте, у каждого ровно `EVENTS_PER_PROFILE` событий в ClickHouse, для когорты нет старых работ и snapshots, а системная очередь пуста.

Exit code: `0` — все проверки пройдены, иначе не ноль. Отчёт сохраняется в любом случае. Повторить прогон на тех же данных можно с прежним `SEED`, но тогда preflight укажет, что когорта уже тёплая. Чистое состояние перед прогоном даёт `BENCH_RESET=1`: стенд пересоздаёт только свои volumes.

## Сценарии

### Smoke — `make bench-mvp-smoke`

100 новых профилей × 50 событий, подача 5 POST/с (open model). Каждая 10-я итерация дополнительно опрашивает работу до ready с учётом `poll_after_ms`, это небольшая когорта user time to ready. После подачи стенд ждёт завершения всех принятых работ и сверяет все 100 с PostgreSQL.

Проверяется: 100 подтверждённых работ, 100 новых 202, 100 ready со связанным snapshot нужного профиля и года. Пропавших подтверждений, неожиданных failed, дублей и 200 у холодной когорты быть не должно. Manifest должен совпадать со счётчиком k6, targets всех worker и ряды k6 должны быть в VM, narrative — template при отключённом provider.

Параметры: `SMOKE_COUNT=100`, `SMOKE_RATE=5`, `SMOKE_POLL_EVERY_NTH=10`, `WORKERS=3`.

### Steady cold — `make bench-steady`

- Прогрев: 20 POST/с в течение 60 с на отдельных 1200 профилях. Затем ожидание пустой очереди и сверка прогрева.
- Измерение: 20 POST/с в течение 300 с на следующих 6000 профилях. Одна итерация k6 = один POST, capacity-поток не опрашивает статус. Если worker тормозит, VU не занимаются и входящий поток не падает.
- Пользовательская когорта: отдельный сценарий k6, 1 POST каждые 5 с на своих профилях (60 работ). Опрос `GET /recaps/{id}` идёт с `poll_after_ms`, около 2 GET/с при `RECAP_POLL_AFTER=500ms`. Этот дополнительный RPS входит в описание нагрузки.

Пороги: p95 приёма cold 202 ≤ 150 мс (клиент: от начала POST до получения и проверки тела), доля ошибок POST < 1%, `dropped_iterations = 0`, профили не исчерпаны, фактические POST/с ≥ 98% заданных. Корректность проверяется как в smoke.

Параметры: `STEADY_RATE=20`, `STEADY_WARMUP_S=60`, `STEADY_DURATION_S=300`, `STEADY_USER_EVERY_S=5` (0 отключает когорту), `POST_P95_MS=150`, `POST_ERROR_RATE=0.01`, `MIN_INGRESS_RATIO=0.98`, `POST_TIMEOUT_S=10`, `BENCH_MAX_VUS`, `BENCH_PREALLOCATED_VUS`.

### Hot key → warm hit — `make bench-hotkey`

1. `docker compose stop worker` останавливает все реплики.
2. Волна 1: 100 VU одновременно отправляют POST для одной свежей пары профиль/год. Ожидается, что все ответы 202 с одним request ID (1 новый и 99 повторных 202), в PG одна работа и одна команда в outbox. Сверка выполняется до возврата workers.
3. `docker compose start worker`, затем ожидание ready.
4. Волна 2: снова 100 POST. Ожидается, что все ответы 200 со snapshot, ID которого совпадает с request ID. После волны в PG по-прежнему одна работа и один snapshot.

Параметр: `HOTKEY_VUS=100`.

### Backlog 1000 — `make bench-backlog`

Режим broker и три worker фиксированы для этого сценария (`BACKLOG_COMMAND_SOURCE`, `BACKLOG_WORKERS`). Workers сначала останавливаются, k6 принимает 1000 уникальных работ (`shared-iterations`, 50 VU). Затем `wait-queued` подтверждает в PostgreSQL ровно 1000 queued, 0 processing и системный backlog = 1000. После этого фиксируется t0, сразу выполняется `docker compose start worker` и опрашивается PG каждые 250 мс до готовности всех 1000 snapshots.

В отчёте есть время до первой готовности, drain от t0 до последней готовности и момент опустошения outbox для когорты. Погрешность наблюдения — интервал опроса. В drain входят старт контейнеров, подключение к PG/CH и вступление consumers в группу Redpanda: это часть «возврата workers». Корректность (1000 ready, 0 пропавших, 0 failed, 0 дублей) и производительность (drain ≤ 60 с, `BACKLOG_DRAIN_GATE`) показаны отдельными строками.

Параметры: `BACKLOG_COUNT=1000`, `BACKLOG_VUS=50`, `BACKLOG_DRAIN_GATE=60s`, `DRAIN_DEADLINE=600s`.

### Burst с продолжающимся трафиком — `make bench-burst` (только вручную)

Workers работают весь тест, пары профиль/год новые. Подача — два сценария `constant-arrival-rate`: базовый 10 POST/с на все 310 с и дополнительный 90 POST/с с 120-й по 130-ю секунду. Скачок мгновенный, в пике суммарно 100 POST/с. Нужно 4000 уникальных пар, их число вычисляется из параметров.

- До всплеска (окно от 30 с после старта до начала пика): backlog min/max/среднее, возраст старейшей работы, ready/с, ingress и наклон backlog. Если наклон больше `BURST_MAX_BASELINE_SLOPE` (по умолчанию 0,1 задачи/с), базовая нагрузка уже растит очередь, и вердикт это фиксирует.
- Пик: заданный и фактический ingress, `dropped_iterations` отдельно по base и spike, ошибки POST, пик backlog и возраста, p95 приёма POST по фазам pre/spike/post.
- Server time to ready считается для работ, поступивших в эти 10 секунд, включая завершившиеся позже (`reconcile-burst.json` → `server_time_to_ready.spike`).
- Recovery — время от конца пика до первого момента, когда backlog и возраст старейшей работы вернулись в диапазон до всплеска (≤ максимума базового окна плюс допуск) и продержались там `BURST_HOLD_S=30` с при продолжающихся 10 POST/с. Удержание должно уложиться в окно наблюдения post. Если восстановления не было, отчёт так и пишет.
- Drain после остановки подачи показан отдельно (`drain-burst.json`).
- Итоговая строка «стенд выдержал burst» требует, чтобы нагрузка была достигнута (без drops и исчерпания), базовая очередь не росла, ошибки были в бюджете и было восстановление. Иначе отчёт пишет, что по прогону нельзя утверждать, что burst выдержан.

Параметры: `BURST_BASE_RATE=10`, `BURST_PEAK_RATE=100`, `BURST_PRE_S=120`, `BURST_SPIKE_S=10`, `BURST_POST_S=180`, `BURST_HOLD_S=30`, `BURST_BACKLOG_TOLERANCE=0`, `BURST_AGE_TOLERANCE=0`, `BURST_MAX_BASELINE_SLOPE=0.1`. Разрешение рядов — 5 с, это интервал sampler.

### Soak — `make bench-soak` (только вручную)

Прогрев 120 с на отдельных профилях, ожидание пустой очереди, затем 5 cold POST/с в течение 60 минут. Это 18 000 профилей × 50 событий = 900 000 событий. Вместе с прогревом (600) и пользовательской когортой (1 POST в 30 с, 120) — 18 720 профилей. Все данные создаются до начала измерения. После подачи стенд ждёт drain до `SOAK_DRAIN_DEADLINE=1800s` и выполняет полную сверку.

По 5-минутным окнам сохраняются (`soak.json`, `soak-windows.csv`):

- фактический ingress, новые работы/с и ready/с;
- p95 server time to ready по работам, созданным в окне (из PG);
- backlog среднее и максимум, возраст старейшей работы, retries, failed-попытки и failed-работы;
- рестарты процессов, RSS, heap после GC и goroutines по процессам;
- PG pool: максимум in-use и прирост wait duration за окно.

Временные ряды с шагом 15 с лежат в `soak-series.json`. Первые и последние 15 минут сравниваются средними. «Устойчивый рост» означает, что последнее среднее выше первого более чем на 10% и не меньше 80% шагов растут. Это наблюдение baseline, а не порог: один пик RSS утечку не доказывает. Размеры таблиц и базы показаны отдельно, их рост ожидаем.

Параметры: `SOAK_RATE=5` (если steady показал насыщение, просто уменьшите, например `SOAK_RATE=2.5`), `SOAK_DURATION_S=3600`, `SOAK_WARMUP_S=120`, `SOAK_USER_EVERY_S=30`, `SOAK_WINDOW=5m`, `SOAK_COMPARE=15m`, `SOAK_DRAIN_DEADLINE=1800s`. Для чистых размеров таблиц запускайте с `BENCH_RESET=1`.

### Общие параметры

`WORKERS=3`, `BENCH_COMMAND_SOURCE=broker` (`database` или `hybrid` для сравнений), `EVENTS_PER_PROFILE=50`, `YEAR=2026`, `SEED`, `RUN_ID`, `TESTID`, `DRAIN_DEADLINE=600s`, `BENCH_RESET=0`, `BENCH_STATS_INTERVAL=5`, `BENCH_PROJECT=avito-recap-bench`.

## Где лежат результаты

`bench/results/<run-id>/`, где `<run-id>` = `<UTC timestamp>-<scenario>`:

| Файл | Содержимое |
|---|---|
| `summary.md` | короткая сводка: вердикты по категориям (измеритель, нагрузка, корректность, производительность, конфигурация, восстановление), таблица проверок, детали |
| `verdict.json` | те же проверки в JSON |
| `run.env`, `phases.log` | параметры прогона, seed, git SHA, UTC-границы фаз |
| `environment.json` | хост, версии, **эффективные значения конфигурации, с которыми стартовали процессы** (`effective_config` из `recap_config_value` каждого target), режим narrative по процессам, число targets, env контейнеров по allow-list без секретов |
| `host/` | исходные файлы для `environment.json`: `host.json`, `container-env.txt`, `images.txt` |
| `preflight.json` | проверка данных перед нагрузкой |
| `profiles.json` | ID профилей в порядке индексов (вход k6 и сверки) |
| `manifest-<phase>.log` | manifest попыток: JSON-логи k6, строки `MANIFEST {...}` с индексом итерации, профилем, статусом, ID и классом исхода |
| `k6-<phase>-summary.json`, `k6-<phase>.exit` | итог k6 и exit code (99 — сработали пороги k6) |
| `reconcile-<phase>.json` | **сверка manifest с PostgreSQL** по ID и бизнес-ключу (profile_id, year) |
| `drain-<phase>.json`, `queued.json` | ожидание завершения и подтверждение backlog |
| `metrics-start.json`, `metrics-end.json` | targets, свежесть scrape, ряды k6, режим и исходы narrative, серверные показатели окна |
| `burst.json` / `soak.json`, `soak-windows.csv`, `soak-series.json` | анализ burst и soak |
| `docker-stats.jsonl`, `restarts.txt`, `compose-ps.txt`, `logs/` | CPU и память контейнеров (включая k6 и VM), рестарты, логи сервисов |

Классы исходов в сверке: новый 202, повторный 202, готовый 200, failed 202, 4xx, transport/5xx (неоднозначно), нарушение контракта. Подтверждённая работа может оказаться ready со snapshot, ready без валидного snapshot, failed по коду, незавершённой на deadline или отсутствующей в PG. Только последнее считается потерей. Клиентский timeout сам по себе потерей не считается: такой исход ищется в PG по (profile_id, year) и попадает в `ambiguous_resolution` как `resolved_ready`, `resolved_queued`, … или `absent_in_pg`.

## Режим narrative без ключа Mistral

Путь в коде:

- `cmd/server/main.go`, `cmd/worker/main.go` создают `narrative.MistralHTTPProvider`, только если `Config.NarrativeProviderMode()` вернул `mistral`. Иначе provider остаётся `nil`.
- `internal/recap/narrative/service.go` (`Service.Build`) при `Provider == nil` сразу возвращает `fallbackNarrative` (`source = "template"`). HTTP-вызова, ожидания `MISTRAL_TIMEOUT` и повторов нет. При включённом provider ошибка запроса или валидации тоже возвращает template — это fallback после ошибки.

Новая переменная `NARRATIVE_PROVIDER`:

| Значение | Поведение |
|---|---|
| `auto` (по умолчанию, в том числе если переменная не задана) | как раньше: Mistral при непустом `MISTRAL_API_KEY`, иначе template |
| `disabled` | всегда template, ключ игнорируется |
| `mistral` | требует ключ; без него процесс не стартует (явная ошибка конфигурации) |

Bench-стенд задаёт для API и worker `NARRATIVE_PROVIDER=disabled` и `MISTRAL_API_KEY=""`. Это две независимые защиты: даже экспортированный на хосте ключ в контейнеры не попадает. Обычный запуск (`make up`) не меняется, реальный ключ по-прежнему передаётся через окружение.

Различие штатного template и fallback видно в метриках и отчёте:

- `recap_narrative_outcomes_total{outcome}`: `provider_disabled` — template по конфигурации; `provider_success`; `fallback_provider_error`; `fallback_validation_error`;
- `recap_narrative_provider_calls_total` — вызовы внешнего provider (в bench ожидается 0);
- `recap_narrative_provider_mode{mode}` — эффективный режим каждого процесса.

`metrics-end.json` → `narrative_effective` и проверка «режим narrative» в `summary.md` требуют template при отключённом provider и 0 вызовов provider. В `reconcile-*.json` → `narrative_sources` видно распределение `narrative_source` в snapshots. Поле snapshot в обоих случаях равно `template`, поэтому различие берётся из метрик.

## Retry, backoff, lease: откуда берётся ожидание

### Путь повторной попытки worker

1. Ошибка в `service.ProcessRecapRequest` → `worker.Runner.processClaimed` → `classifyError`. Повторяемые коды: `dependency_unavailable` (ClickHouse), `generation_interrupted`, `internal_error` и `worker_shutdown` при остановке процесса. Неповторяемые: `insufficient_activity`, `profile_not_found`, `invalid_argument`, `dependency_misconfigured`.
2. Если ошибка повторяемая и `attempt_count < max_attempts`, задержка `retryDelay(attempt_count) = base · 2^(attempt−1)` (не больше `max`). `RequeueRecapRequest` в одной транзакции ставит `status=queued`, `available_at = now + delay` и пишет новую команду в outbox с тем же `available_at`.
3. Повторный claim:
   - broker — outbox публикует команду, только когда наступил `available_at` (опрос раз в `OUTBOX_POLL_INTERVAL=250ms`), consumer вызывает `ClaimRecapRequestByID` с условием `available_at <= now`;
   - database/hybrid — `ClaimNextRecapRequest` опрашивает PG раз в `WORKER_POLL_INTERVAL=500ms`.
4. При `attempt_count >= max_attempts` → `failed` (retryable=true) и событие в DLQ.

`attempt_count` увеличивается при claim. При трёх попытках повтор назначается только после 1-й и 2-й, поэтому реально достижимы задержки `base` и `2·base`:

| Профиль | base / max / attempts | Достижимые задержки | Суммарное ожидание backoff до 3-й попытки |
|---|---|---|---|
| dev `docker-compose.yml` | 2s / 30s / 3 | 2 с, 4 с | 6 с (+ до 250 мс опроса outbox на повтор) |
| bench | 250ms / 1s / 3 | 250 мс, 500 мс | 0,75 с (+ до 250 мс на повтор) |

Cap 30 с в dev недостижим: он сработал бы только с 5-й попытки. Cap 1 с в bench тоже недостижим при трёх попытках и оставлен как предохранитель на случай увеличения `BENCH_WORKER_MAX_ATTEMPTS`. Таблицу закрепляет тест `TestReachableRetryDelaysWithThreeAttempts`. Слой retry не добавлялся: меняются только значения существующего backoff.

`max_attempts` записывается в строку работы **процессом API** при создании (`service.RequestRecap` берёт `WORKER_MAX_ATTEMPTS` из окружения API). Решение о повторе worker принимает по значению из строки, поэтому изменение `WORKER_MAX_ATTEMPTS` только у worker ни на что не влияет. Bench задаёт одно значение и API, и worker (`x-retry-env`).

### Что задано в `docker-compose.yml` напрямую

- worker: `WORKER_LEASE_DURATION=30s`, `WORKER_HEARTBEAT_INTERVAL=10s`, `WORKER_RETRY_BASE=2s`, `WORKER_RETRY_MAX=30s`, `WORKER_MAX_ATTEMPTS=3` — литералы в `environment:`. Они перекрывают `env_file`, а переменной окружения хоста их не переопределить.
- worker: через окружение хоста переопределяются только `WORKER_COMMAND_SOURCE`, `WORKER_POLL_INTERVAL` и `MISTRAL_API_KEY`, плюс `REDPANDA_VERSION` для образа.
- backend (API): `WORKER_MAX_ATTEMPTS=3` и `RECAP_POLL_AFTER=500ms` приходят из `backend/.env.example` через `env_file`; `SSE_*` — литералы. С хоста они не переопределяются.
- outbox: `OUTBOX_POLL_INTERVAL=250ms`, `OUTBOX_LEASE_DURATION=15s`, `OUTBOX_BATCH_SIZE=50` — литералы.

В dev-стеке эти значения меняются только правкой файла или через `docker-compose.override.yml` (он в `.gitignore`). В bench-файле все параметры, которые нужно менять, заданы через `${BENCH_*:-default}`.

### Другие задержки и решения по ним

| Механизм | Где | Значение | Влияет на MVP / burst / soak? | Решение |
|---|---|---|---|---|
| Retry публикации outbox | `internal/outbox/publisher.go` | 1 с, удвоение до 30 с, попытки не ограничены. `RetryBase/RetryMax` не читаются из env: `cmd/outbox` их не передаёт | Только при ошибке публикации, то есть при недоступности или ошибке Redpanda. В шести сценариях отказов брокера нет | Не менялось. Эффективные значения экспортируются (`recap_config_value{param="outbox_retry_*"}`), задержки видны на dashboard (outbox retry delay, `publish requeued`). Понадобится на этапе fault-тестов |
| Переподключение consumer | `cmd/worker/main.go` | ошибка или создание — 1 с; `ErrCommandDeferred` — 250 мс и **пересоздание consumer**, то есть новый член группы и rebalance | Deferred возникает, когда команда пришла для работы с живой lease у другого worker: в hybrid при гонке с опросом PG или после kill до истечения lease. В broker-режиме без kill не ожидается | Не менялось (константы), счётчик `recap_broker_consumer_restarts_total{reason}`. Bench по умолчанию использует broker. Hybrid даёт дополнительные rebalance — это нужно учитывать при A/B |
| Истечение lease после kill worker | `recap_requests.lease_expires_at` | lease 30 с, heartbeat 10 с: работа «висит» 20–30 с после kill (с последнего heartbeat) | В MVP workers останавливаются штатно (SIGTERM) и без работ в процессе, поэтому lease не участвует | Lease не сокращён. **После kill worker основное ожидание даёт lease**: в broker-режиме переотправленная команда уходит в deferred-цикл до истечения lease, в database/hybrid `ClaimNextRecapRequest` подбирает истёкшую lease. Если для fault-этапа нужен короткий bench-lease (`BENCH_WORKER_LEASE_DURATION`/`BENCH_WORKER_HEARTBEAT_INTERVAL`), держите lease ≥ 3 × heartbeat с запасом на задержку UPDATE heartbeat под нагрузкой, например 6s/2s. Длительность живой попытки ограничений не задаёт, потому что heartbeat продлевает lease. `normalizeConfig` проверяет только heartbeat < lease |
| Consumer HTTP proxy после kill -9 | Redpanda Pandaproxy | сессия мёртвого члена группы держит партиции до таймаута | только kill-сценарии | Не проверялось; проверить на этапе fault |
| Таймаут Mistral | `MISTRAL_TIMEOUT=15s` | — | нет: provider отключён | — |
| HTTP-таймауты и polling | k6 `POST_TIMEOUT_S=10`, `REDPANDA_HTTP_TIMEOUT=15s`, `RECAP_POLL_AFTER=500ms` | — | отдельные механизмы | Не уменьшались вместе с backoff |

Эффективные значения, с которыми реально стартовали процессы, попадают в `environment.json` → `effective_config`. Это значения после defaults и нормализации, по каждому target: retry base/max, lease, heartbeat, poll, max attempts в API и worker, outbox poll/lease/batch/retry, константы consumer, таймауты Mistral и Redpanda.

## Метрики

Endpoint `/metrics` включается переменной `METRICS_ADDR` (в bench — `:9100`) на отдельном listener. Публичный API на `:8080` не меняется. В обычном запуске `METRICS_ADDR` пуст, endpoint выключен. Метки ограничены конечными перечнями: шаблон маршрута, метод, класс статуса, stage, outcome, source, error_code. ID, URL и тексты ошибок в метки не попадают.

| Процесс | Ряды |
|---|---|
| API | `recap_http_requests_total`, `recap_http_request_duration_seconds` (route template), `recap_create_outcomes_total{outcome=created/existing_request/snapshot_hit/restarted_failed/error}` |
| worker | `recap_worker_claims_total{source}`, `recap_worker_queue_wait_seconds`, `recap_worker_attempt_results_total{result}`, `recap_worker_failures_total{code}`, `recap_worker_retry_delay_seconds`, `recap_worker_stage_duration_seconds{stage=activity_read/generate/pg_finalize/attempt}`, `recap_worker_busy`, `recap_worker_expired_lease_failed_total`, `recap_event_publish_total{kind,result}`, `recap_broker_consumer_active`, `recap_broker_consumer_restarts_total`, `recap_narrative_*` |
| outbox | `recap_outbox_claimed_total`, `recap_outbox_publish_total{result}`, `recap_outbox_publish_duration_seconds`, `recap_outbox_retry_delay_seconds` |
| все Go-процессы | `go_goroutines`, `go_gc_heap_live_bytes` (heap после GC), `process_resident_memory_bytes`, `process_cpu_seconds_total`, `process_start_time_seconds`, `go_sql_*` (in use, idle, wait count/duration), `recap_config_value{param}` |
| sampler (PG, раз в 5 с) | `recap_bench_requests{status}`, `recap_bench_backlog`, `recap_bench_oldest_*_age_seconds`, `recap_bench_delayed_retries`, `recap_bench_expired_leases`, `recap_bench_outbox{status}`, `recap_bench_outbox_oldest_unpublished_age_seconds`, `recap_bench_failed{error_code}`, размеры таблиц, `recap_bench_sampler_up`, `recap_bench_sampler_last_success_timestamp_seconds` |
| k6 (remote write) | `k6_posts_started_total`, `k6_target_post_rate`, `k6_dropped_iterations_total`, `k6_post_accept_202_p95`, `k6_posts_accepted_202_total`, `k6_posts_ready_200_total`, `k6_post_failures_total`, `k6_user_time_to_ready_p95`, … с тегами `testid`, `scenario`, `phase` (системный тег `url` отключён) |

Реплики worker обнаруживаются через Docker DNS (`dns_sd_configs` по имени сервиса). Число живых `up{job="worker"}` сравнивается с `WORKERS` до и после нагрузки. При сбое выборки sampler выставляет `recap_bench_sampler_up=0` и не отдаёт устаревшие значения.

Dashboards (`bench/grafana/dashboards`): «Вход и результат» и «Где образовалась очередь». Lag Redpanda считается из `/public_metrics` (`redpanda_kafka_max_offset − redpanda_kafka_consumer_group_committed_offset`). «No data» означает недоступную метрику, а не нулевой lag.

## Что проверить первым запуском

1. `make bench-mvp-smoke` должен завершиться с exit 0. В `metrics-end.json` нужны `k6_series_for_testid > 0` и `targets_up.worker = 3`. Если рядов k6 нет, проверьте имена в VMUI (`http://127.0.0.1:18428/vmui`, запрос `{__name__=~"k6_.*"}`). Имена Trend зависят от версии k6 и `K6_PROMETHEUS_RW_TREND_STATS`.
2. В `manifest-smoke.log` строки должны быть JSON-логами k6, а в `reconcile-smoke.json` должно быть `manifest_complete: true`. Если k6 пишет файл не в JSON, все строки дадут `parse_errors`, и проверка «manifest полный» провалится.
3. Lag Redpanda на втором dashboard. Если «No data», в используемой версии Redpanda может быть выключен `enable_consumer_group_metrics`.
4. Намеренно проваленный порог, например `BACKLOG_DRAIN_GATE=1s make bench-backlog` или короткий `POST_P95_MS=1 STEADY_WARMUP_S=10 STEADY_DURATION_S=30 make bench-steady`, должен сохранить `summary.md` и вернуть ненулевой exit code.

## Известные ограничения

- Не запускались ни сборка, ни `go vet`, ни `go test`, ни `gofmt`, ни Compose, ни k6. Перед коммитом выполните проверки из раздела ниже.
- Окна burst и soak строятся по рядам с шагом 5 с (sampler). Точность recovery — ±5 с. Начало теста берётся по первому POST в manifest (часы клиента).
- Quantiles k6 на dashboards вычисляются за интервал отправки remote write (5 с). Итоговые p95 берутся из k6 summary за весь прогон, не усреднением.
- `docker-stats.jsonl` собирается с хоста через `docker stats --no-stream` каждые 5 с: это моментальные значения, а не интегральные.
- Прогон одного сценария — это не вывод о capacity. Для устойчивой пропускной способности нужны минимум три сравнимых прогона (spec §6.3).

## Проверки кода (выполнить на хосте с Go)

```bash
cd backend
gofmt -l ./cmd ./internal ./pkg      # должен быть пустым; иначе gofmt -w
go vet ./...
go test ./internal/telemetry ./internal/config ./internal/recap/narrative ./internal/worker ./cmd/bench-verify
go test ./...
go test -race ./internal/worker ./internal/outbox ./internal/broker
docker compose -p avito-recap-bench -f ../bench/compose.bench.yml config >/dev/null
```

---

## Прежние скрипты (`scripts/benchmark-*.sh`)

Работают как раньше, на dev `docker-compose.yml`, и требуют Go на хосте. Внимание: `RESET=1` (по умолчанию) делает `docker compose down -v` для **dev-стека**.

```bash
make bench-smoke     # burst из 100 запросов с конкурентностью 20
make bench-paths     # PostgreSQL против Redpanda на чистых volumes
make bench-matrix    # 20/50/100 запросов в секунду и burst 1000
make bench-faults    # остановка workers, Redpanda, ClickHouse под нагрузкой
```

Параметры: `PROFILES=1000 EVENTS_PER_PROFILE=100 REQUESTS=1000 CONCURRENCY=50 make bench-smoke`. Хостовые утилиты (`go run ./cmd/bench-data`, `./cmd/bench-api`) ожидают PostgreSQL на `localhost:5432`, а dev Compose этот порт не публикует. Для них нужен собственный `docker-compose.override.yml` с портом. Новый стенд эту проблему обходит: его утилиты работают внутри bench-сети.

Ориентиры для текущего стенда: p95 принятия `POST /recaps` ≤ 150 мс, ошибки API < 1% в steady/ramp, потерянные команды и повторяющиеся snapshots — 0, backlog из 1000 команд обрабатывается за ≤ 60 с на указанной машине. Это проектные ориентиры, а не универсальные гарантии.
