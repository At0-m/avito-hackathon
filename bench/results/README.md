# Результаты нагрузочных тестов

В этом каталоге нет заранее подготовленных показателей.

Новый изолированный стенд (см. `bench/README.md`) пишет каждый прогон в `bench/results/<UTC timestamp>-<scenario>/`:

```bash
make bench-mvp-smoke
make bench-steady
make bench-hotkey
make bench-backlog
make bench-burst   # только вручную
make bench-soak    # только вручную
```

Главные файлы прогона: `summary.md`, `verdict.json`, `environment.json`, `reconcile-*.json`, `manifest-*.log`, `k6-*-summary.json`.

Прежние скрипты запускаются так:

```bash
make bench-smoke
make bench-paths
make bench-matrix
make bench-faults
```

Каждый запуск создаёт отдельную папку с JSON-отчётами, Markdown-сводкой, сведениями об окружении и логами сервисов. При сравнении архитектур храните весь набор файлов, а не только лучший показатель задержки.
