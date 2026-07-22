# Amber — product and engineering review (2026-07-22)

Это повторное сквозное ревью Amber после платформенного прохода 2026-07-07.
Цель — определить следующий рабочий minor-релиз, а не проектировать абстрактный
v1. Patch-релизы `v0.N.P` остаются допустимыми для совместимых исправлений, но
основная разработка ведётся через `v0.(N+1).0`.

Снимок ревью: рабочее дерево `06107b8` (`production-operations`), по содержимому
соответствующее актуальному объединённому состоянию проекта; последний тег —
`v0.3.0`, после него в снимке 39 коммитов. Репозиторий чист до добавления этого
ревью.

## Итог

Amber уже не pre-alpha-прототип из первого обзора. Он принимает все три OTLP-
сигнала по HTTP и gRPC, корректно сообщает backpressure, имеет crash recovery,
индексы и кэши с проверками эквивалентности, S3-tier, offline backup/restore с
MinIO integration test, self-observability и канонический OTLP replay journal.
Локальный quality gate зелёный:

- `go test ./... -race -count=1`;
- `go vet ./...`;
- `golangci-lint run` — `0 issues`;
- `gofmt` и `go mod tidy -diff` не находят изменений;
- GitHub CI проверяет build, race suite и S3 snapshot round trip.

Но следующий minor сейчас выпускать рано. Два системных свойства появились
после `v0.3.0`, не будучи замкнутыми в production-контракт:

1. migration с `v0.3.0` сейчас реализована в незарелизованном batch, но ещё не
   прошла production-sized/non-empty fixture и release verification;
2. HTTP/gRPC lifecycle вручную собран так, что bind/serve failure не останавливает
   процесс, а shutdown не ждёт завершения gRPC до закрытия storage.

Параллельно Amber использует только низкоуровневую часть старого Reef `v0.1.0`
и совсем не использует Gyre. Поэтому обязательные интеграции Reef `v0.3.0` и
Gyre `v0.6.0` — не косметический refactor, а часть исправления реальных
security и lifecycle дефектов.

Вердикт: **Amber — сильный alpha с широкой функциональной поверхностью, но не
release candidate для `v0.4.0`**. Следующий этап должен быть operational
contract release, а не добавление ещё одного пользовательского интерфейса или
движка запросов.

## Что изменилось после обзора 2026-07-07

| Старый блокер | Статус сейчас | Evidence |
|---|---|---|
| HTTP queue-full мог выглядеть успехом | Исправлен: retryable OTLP responses и durability barrier | `internal/api/http/otlp.go`, `internal/api/grpc/ingest.go` |
| Нестандартные OTLP HTTP responses | Исправлен proto/JSON OTLP response contract | `internal/api/http/otlp.go` |
| gRPC без auth | Исправлен через Reef interceptors | `cmd/amber/main.go`, `internal/api/grpc/server.go` |
| gzip ingest отсутствовал | Исправлен с лимитом распакованного тела | `internal/api/http/otlp.go` |
| TLS отсутствовал | Базовый TLS/mTLS добавлен | `internal/config/config.go`, `cmd/amber/main.go` |
| Trace search был scan-heavy | Добавлены covering indexes, duration buckets и equivalence tests | `internal/index/coverindex.go`, `internal/query/span_cover.go` |
| Metrics cache/RSS долг | Добавлены общий cache budget, generation safety и mixed-RSS tests | `internal/metricsengine/store/`, `internal/runtime/runtime.go` |
| Backup/restore отсутствовал | Есть offline snapshot, verify, restore drill и S3 transport | `internal/backup/`, `cmd/amber-backup/` |
| Полный OTLP replay отсутствовал | Добавлен lossless AOT4 journal для нового OTLP ingest | `internal/otlpv4/` |
| AOT4 journal не имел retention | Исправлен: startup/scheduled prune, delete-pending recovery и retention stats | `internal/otlpv4/journal.go`, `internal/runtime/runtime.go` |
| Resource/scope fidelity в log/trace queries | **Не исправлен** | `internal/ingest/otlp.go` |
| Единые health/readiness conventions | **Частично**: `/readyz` есть, `/healthz` и aggregate readiness нет | `internal/api/http/routes.go` |

## P0 — гейты следующего minor-релиза

### P0 resolved. Канонический OTLP journal уже участвует в retention

В текущем snapshot `internal/otlpv4/journal.go` уже имеет `RetentionPolicy`,
`Prune`, delete-pending recovery, age/bytes/segments limits и operational
`RetentionStats`. `runtime.New` выполняет initial prune, а `startRetention`
запускает периодический journal sweep (`internal/runtime/runtime.go`). Есть
тесты на age, active rotation, segment limit, restart/replay, concurrent prune
и backup round trip.

Это снимает первоначальный P0, но оставляет release-hardening: production-sized
fixture для согласования journal/projection/backup lifecycle, disk watermarks и
повторную benchmark campaign после dual-write.

### P0-1. Migration с `v0.3.0` есть в batch, но ещё не release-verified

`validateOTLPV4Root` в `internal/runtime/runtime.go` по-прежнему требует
каталог `otlp_v4`. Незакоммиченный batch теперь добавляет
`internal/otlpv4/legacy_replay.go`, copy-on-write `MigrateLegacyV3`,
`cmd/amber-migrate` и явный `FidelityNormalizedV3` marker.

Для `v0.4.0` ещё требуется release gate:

- прогнать non-empty v0.3 fixture со всеми сигналами, проверить semantic digest,
  query result и rollback source root;
- опубликовать offline one-way procedure и upgrade compatibility table;
- если подтверждено, что сохраняемых инсталляций нет, объявить `v0.4.0`
  fresh-root release и дать точную процедуру export/reingest. Молчаливый отказ
  на старте не является upgrade policy.

До прохождения fixture нельзя обещать, что `v0.4.0` обновляет `v0.3.0`.

### P0-2. Server lifecycle допускает «живой процесс без сервера» и гонку shutdown

В `cmd/amber/main.go` HTTP и gRPC запускаются в goroutines. Ошибка bind или
неожиданное завершение `Serve` только логируется; `run()` продолжает ждать
SIGTERM. Процесс может оставаться живым с закрытым портом, а его другой
listener и `/readyz` не отражают отказ компонента.

На shutdown `grpcServer.GracefulStop()` также запускается отдельно, но main не
ждёт его перед `stack.Close()`. Активный gRPC handler может продолжать работу
после начала закрытия journal/store. Нет deadline и fallback к `Stop()`.
HTTP shutdown ожидается, pprof и gRPC — нет.

Интеграция Gyre должна закрыть это структурно:

- компоненты `storage`, `http`, `grpc`, `pprof` с явными dependencies;
- listener bind синхронно внутри `Start`, чтобы ошибка вернулась вызывающему;
- API закрывается раньше storage в reverse dependency order;
- bounded graceful shutdown и force fallback;
- aggregate `/healthz`, `/readyz`, `/status` и Gyre conformance suite;
- startup rollback при частично поднятых компонентах.

### P0-3. Reef подключён частично и оставляет security-контракт незамкнутым

`go.mod` фиксирует Reef `v0.1.0`; production wiring напрямую вызывает
`tlsconf.Server`, `bearer.Require` и `grpcreef.ServerOptions`. Актуальный Reef
`v0.3.0` предоставляет high-level `edge.NewHTTPServer` и
`grpcreef.NewServerEdge`: fail-closed policy по фактическому bind, managed
rotation, credential status, observer и `Close`.

Текущий Amber имеет четыре практических разрыва:

1. `validateAPISecurity` считает наличие bearer достаточной защитой. На внешнем
   plaintext bind token разрешён без отдельного
   `danger_allow_bearer_over_plaintext`; Reef лишь пишет warning. Токен можно
   передавать открытым случайно.
2. file-backed certificate/token lifecycle не управляется: нет rotation,
   статуса поколений и корректного закрытия reload workers.
3. Reef кладёт имя ключа в `bearer.PrincipalFromContext`, но Amber access log
   читает собственный `APIKeyNameFromContext`. В standalone wiring внутренний
   `RoutesConfig.APIKeys` пуст, поэтому `api_key_name` в audit log пуст даже у
   аутентифицированного запроса.
4. `amberctl` понимает только `--addr` и статический `--api-key`; у него нет
   Reef client edge, CA/client certificate, token file и rotation. Им нельзя
   нормально управлять mTLS/custom-CA deployment. Отдельный pprof listener
   также может быть выставлен наружу без Reef policy.

`v0.4.0` должен использовать managed server/client edges на всех сетевых
границах, прокинуть Reef principal в audit, отдать secret-free credential
status через Gyre и покрыть rotation/conformance tests. Переход конфигурации
нужно сделать намеренно: legacy keys можно прочитать один minor с warning, но
runtime policy должна быть одна.

## P1 — надёжность, границы ресурсов и честность данных

### P1-1. Query projection теряет существенную часть OTLP logs/traces

`ExtractResource` в `internal/ingest/otlp.go` извлекает только
`service.name` и `host.name`; resource attributes, schema URL и instrumentation
scope не переходят в event store. Log attributes превращаются в строки.
Span projection дополнительно не сохраняет kind, trace state, flags, events,
links и status message; log projection — observed timestamp и flags.

Lossless journal сохраняет оригинальный OTLP для нового ingest, поэтому это не
безвозвратная потеря и projection можно перестроить. Но сегодняшний query/API
не видит `service.namespace`, `service.instance.id`, `deployment.environment`,
`k8s.*`, `cloud.*` и scope identity. Значит, обогащение Coral и ресурсная
идентичность Gyre не дают пользователю ценности в Amber.

До расширения формата нужно зафиксировать query contract: гарантированный
набор resource keys плюс настраиваемый allowlist, типизированные значения и
словарное хранение низкокардинальных resource/scope данных вместо безусловного
дублирования per record.

### P1-2. Projection и journal подтверждаются двумя независимыми записями

OTLP logs/traces сначала проходят queue + `FlushLogs/FlushSpans`, затем handler
добавляет принятый subset в journal. Metrics сначала фиксируются store, затем
journal. Если вторая запись падает, клиент получает retryable error, хотя
projection уже содержит данные; retry создаёт дубликаты и не гарантирует, что
canonical journal совпал с serving projection.

Нужен формальный at-least-once consistency contract и fault-injection suite.
Варианты дизайна: canonical-first с детерминированным projection replay,
commit marker/reconciliation или request identity + dedup. Выбор нельзя делать
только по happy-path latency.

### P1-3. Query API не имеет единого budget/deadline contract

Logs имеют внутренний max limit, но отрицательный/слишком большой `limit`
проходит HTTP parse и превращается в 500 от executor вместо клиентского 400.
Trace list молча игнорирует невалидные числа, не ограничивает `limit`/`offset`
и вычисляет `offset+limit` без overflow check. `rate_range` допускает
произвольный диапазон и минимальный положительный step, формируя потенциально
огромный массив шагов. Quantile без `window` сканирует все sealed blocks.

Нужны общие лимиты page size, range, steps, groups, scanned bytes/segments и
server-side deadline; ошибки должны быть typed и отображаться в 400/413/429,
а не в 500. Oversize body через `http.MaxBytesReader` сейчас в нескольких
`io.ReadAll` путях также отображается как 400 вместо 413.

### P1-4. Readiness уже, чем реальная способность принимать трафик

`/readyz` проверяет только завершение bootstrap индексов. Он остаётся зелёным
при открытом ingest breaker, degraded runtime status, проблеме reconcile и
отказе одного listener. `/health` всегда отвечает 200, включая `degraded`,
стандартного `/healthz` и gRPC health service нет.

Gyre aggregate readiness должен включать обязательные зависимости и условия
восстановления. `/health` можно оставить compatibility alias на один minor;
канонические endpoints — `/healthz`, `/readyz`, `/status`.

### P1-5. Конфигурация и документация расходятся с исполняемым контрактом

`config.Load` использует non-strict `yaml.Unmarshal`, поэтому опечатка в ключе
молча включает default. Отсутствующий явно переданный файл также молча даёт
defaults. Валидация покрывает только часть duration/size/cardinality полей.

`config.example.yaml`, вопреки README, не содержит всех S3, metrics, ingest,
timeout, debug и API options. README обещает Go 1.25+, тогда как `go.mod` и CI
требуют Go 1.26; default HTTP bind — `localhost:8080`, example — `:8080`.
Таблица defaults в README также содержит устаревшие значения и flat retention.

Нужны strict known-fields, явный `--config`, различие «default path не найден»
и «переданный path не найден», полная semantic validation и генерируемый либо
тестируемый example contract.

### P1-6. Backup production-grade по целостности, но только offline

`backup.Create` захватывает data-root lock и требует, чтобы Amber был
остановлен. Это корректно и безопасно, но означает downtime на каждую копию.
Нет встроенной политики list/prune/latest, расписания или шифрования; S3
transport и restore drill уже являются хорошим фундаментом.

Для `v0.4.0` достаточно честно документировать maintenance-window SLA и
автоматизировать restore drill. Online checkpoint/snapshot стоит делать
отдельным minor только при подтверждённой потребности в zero-downtime backup.

### P1-7. Performance evidence устарело после изменения write path

Публичные benchmark-выводы получены до обязательного AOT4 journal и per-request
durability barriers. Теперь каждый OTLP request пишет serving projection и
полную canonical copy. Metrics path может выполнять несколько durable append
операций внутри одного export request.

Перед релизом нужны повторные ingest/storage/RSS/query кампании на текущем
production path, включая journal retention, restart/replay и mixed-signal
нагрузку. Старые цифры нельзя использовать для release claim.

### P1-8. Нет защиты от заполнения диска

При выключенном retention Amber принимает данные до ошибки файловой системы.
Нет disk watermark, admission/read-only режима или readiness condition по
свободному месту. Journal делает риск выше, потому что дублирует ingest.

Минимум для operational baseline: storage byte gauges по каждому store,
настраиваемые warning/stop watermarks и явный retryable ingest refusal до
полного исчерпания диска.

## P2 — release engineering и продуктовые расширения

- В репозитории есть только CI workflow, но нет release workflow, checksums,
  SBOM/signing и автоматически собираемых `amber`, `amberctl`,
  `amber-backup`. У GitHub release `v0.3.0` нет бинарных assets.
- Нет `CHANGELOG.md`, upgrade guide и versioned on-disk/API compatibility
  policy. Для database продукта это важнее дополнительной команды CLI.
- README описывает log query `offset`, хотя реализация перешла на cursor;
  подобные contract drifts должны ловиться tests/docs generation.
- `clientIP` безусловно доверяет `X-Forwarded-For`, поэтому audit remote
  spoofable вне настроенного trusted proxy. Нужна proxy trust policy или
  отказ от XFF по умолчанию.
- Main server lifecycle почти не покрыт тестами; особенно нужны bind failure,
  partial startup rollback, blocked gRPC stream, forced shutdown и Reef
  rotation. `internal/otlpmetrics` не имеет прямого package-level test suite,
  хотя HTTP/gRPC integration tests покрывают часть пути косвенно.

## Контракт Reef + Gyre для Amber

Рекомендуемая форма интеграции:

```text
Gyre Runtime
├── amber-storage       (data root, journal, projections, workers)
├── amber-http          depends on amber-storage; Reef HTTP edge
├── amber-grpc          depends on amber-storage; Reef gRPC edge
└── amber-pprof         optional; loopback-only or Reef-protected

shutdown: pprof/grpc/http -> storage
status: Gyre snapshots + Reef credential generations + Amber data conditions
```

Storage adapter владеет существующим `runtime.Stack`. API adapters синхронно
bind listeners в `Start`, публикуют readiness только после успешного bind и
останавливаются до storage. Reef edges являются credential sources; bridge в
Gyre публикует только name/source/generation/expiry/error без секретов. Gyre
config store/admin обязательно монтируется за Reef.

Hot reload в первом релизе должен быть узким. Без restart безопасно менять
log level, разрешённые query budgets и reloadable credentials. Data dir,
listener addresses, storage format, cache topology и retention semantics
возвращают `RestartRequired`; симулировать их горячее применение нельзя.

## Что считать фичами, а что долгом

Обязательная работа `v0.4.0` — не feature expansion, а завершение уже
обещанных свойств: durability, retention, upgradeability, secure edge и
управляемый lifecycle.

Первая настоящая следующая продуктовая фича — **полноценная OTLP fidelity в
query projection и cross-signal correlation**. Она разблокирует поиск по
Coral enrichment и общую resource identity. Online backup, exemplars, native
float codec, более широкий metrics query language и UI — отдельные кандидаты,
которые следует брать только после измерения пользовательского запроса и цены.

Fathom не должен блокировать ближайший Amber release: его интеграция остаётся
отдельным decision gate после завершения Fathom. Reef и Gyre, напротив,
являются обязательной частью ближайшего minor.

Детальная последовательность и release gates зафиксированы в
[`ROADMAP.md`](ROADMAP.md).
