# Работа с Git

## Ветки

Основные ветки:

```text
main        стабильная версия
dev         интеграционная ветка
<username>  рабочая ветка участника
```

Изменения попадают в `dev` через Pull Request из персональной или feature-ветки. В `main` изменения переносятся отдельным Pull Request из `dev` после проверки сборки и основного сценария.

Прямые push в `main` и `dev` не используются.

## Начало работы

```bash
git switch dev
git pull origin dev
git switch -c <username>
git push -u origin <username>
```

Для отдельной задачи можно создать ветку от персональной:

```bash
git switch <username>
git switch -c feature/short-name
```

## Названия веток

Формат:

```text
<тип>/<краткое-описание>
```

Типы:

| Тип | Для чего |
|---|---|
| `feature` | новая функция |
| `fix` | исправление ошибки |
| `refactor` | изменение структуры без нового поведения |
| `test` | тесты |
| `docs` | документация |
| `db` | миграции и схема |
| `build` | сборка и зависимости |
| `ci` | автоматические проверки |
| `chore` | техническая работа |

Примеры:

```text
feature/async-recap
fix/redpanda-producer
refactor/recap-handler
test/worker-lease
docs/update-readme
```

## Коммиты

Рекомендуемый формат:

```text
<тип>(<область>): <действие>
```

Примеры:

```text
feat(worker): add lease heartbeat
fix(broker): handle successful proxy response
docs(readme): describe post-hackathon changes
```

Коммит должен содержать одно логическое изменение и собираться вместе с нужными миграциями и тестами.

## Синхронизация с dev

Перед Pull Request:

```bash
git fetch origin
git switch <your-branch>
git merge origin/dev
```

После разрешения конфликтов нужно повторно запустить проверки.

## Pull Request

В описании достаточно указать:

~~~markdown
## Что изменено
- ...

## Как проверить
```bash
...
```

## Риски
- миграции;
- изменение API;
- обратная совместимость;
- новые переменные окружения.
~~~

Перед объединением проверяются:

- тесты и линтеры;
- сборка Docker-образов;
- отсутствие секретов;
- соответствие OpenAPI;
- миграции и обратная совместимость;
- основной пользовательский сценарий.

## Локальные проверки

```bash
cd backend
gofmt -w ./cmd ./internal ./pkg
go vet ./...
go test ./...

cd ../frontend
npm ci
npm run check

cd ..
docker compose config
docker compose up -d --build
```

## Конфликты и экстренные исправления

Конфликты разрешает автор ветки, после чего просит повторное ревью. Срочное исправление создаётся отдельной `fix/...` веткой от актуальной стабильной ветки и затем переносится обратно в `dev`, чтобы история не разошлась.
