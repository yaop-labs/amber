# Amber roadmap

Roadmap основана на [`REVIEW.md`](REVIEW.md) от 2026-07-22. Она намеренно
не привязана к v1: ближайшая работа выпускается последовательными `0.X.0`, а
дальние ставки начинаются только после измеримого decision gate.

## Release policy

- `v0.N.P` — совместимый bug/security fix, regression fix, документация или
  безопасный backport без нового storage/API/config contract.
- `v0.(N+1).0` — новая возможность, новый operational contract, изменение
  defaults/config или on-disk migration.
- Breaking changes до v1 допустимы, но никогда не молчаливы: release notes
  содержат upgrade/rollback procedure и compatibility table.
- Minor не выпускается по проценту выполненного списка. Он выпускается, когда
  пройдены его acceptance gates.
- Даты не назначаются до оценки work packages после spike/decision item.

`v0.3.1` не создаётся ради номера. Он нужен только если остаются живые
deployment на линии v0.3 и туда требуется backport критического security или
data-integrity исправления. Иначе работа сразу идёт в `v0.4.0`.

## Current baseline — v0.4.0 release candidate

Уже реализовано после тега:

- стандартные retryable OTLP HTTP/gRPC responses и durability barriers;
- все три сигнала по HTTP/gRPC;
- Reef TLS/mTLS + bearer на базовом уровне;
- trace covering indexes и metrics cache/RSS hardening;
- offline backup/verify/restore, S3 transport и restore drill;
- lossless AOT4 journal для нового OTLP ingest;
- AOT4 journal retention: age/bytes/segment limits, startup/scheduled prune,
  delete-pending recovery и retention stats;
- незарелизованный `amber-migrate otlp-v4` с legacy replay, copy-on-write
  staging, digest verification и `FidelityNormalizedV3` marker;
- runtime/backup/replay self-observability.

Исходное ревью не разрешало назвать эту работу `v0.4.0`, пока upgrade path,
server lifecycle и Reef/Gyre operational contract не были замкнуты. Эти
условия закрыты release-hardening batch от 2026-07-25.

## v0.4.0 — operational contract

Цель: Amber можно безопасно запустить, обновить, ограничить по ресурсам,
остановить и восстановить как single-node source of truth.

Статус 2026-07-25: **локальные acceptance gates пройдены**. В дополнение к
исходному batch:

- non-empty v0.3 fixture с logs/traces/metrics проходит copy-on-write migration,
  query verification и доказывает неизменность rollback root;
- synchronous disk admission использует warning/stop free-byte watermarks,
  красит readiness/status и fail-closed возвращает retryable OTLP ошибки до
  открытия stores/ENOSPC;
- fault injection фиксирует at-least-once dual-write contract: сбой journal
  после projection возвращает retryable error, повтор может создать duplicate,
  а успешная попытка попадает в canonical replay journal;
- Reef v0.3 и Gyre v0.6 управляют HTTP/gRPC/storage lifecycle, aggregate health
  и bounded reverse shutdown;
- strict config/example, query/HTTP bounds, offline upgrade/rollback,
  checksummed artifacts, clean-host smoke и benchmark/RSS evidence включены в
  release gates;
- tag workflow запускает race, MinIO restore, secure artifact smoke и
  benchmark jobs, после чего публикует четыре бинарника и checksums.

### Workstream A — data lifecycle and upgrade

Статус 2026-07-22: **retention и migration batches уже реализованы в рабочем
дереве** —
`retention.journal.{max_age,max_bytes,max_segments}`, acceptance-time clock,
crash-retryable sealed-segment prune, немедленный startup sweep, operational
metrics и тест `retention -> restart -> replay -> backup`; migration добавляет
legacy replay, atomic staging и digest verification. Ниже остаются non-empty
fixture, disk admission, fault injection и release verification.

- Согласовать уже реализованный atomic prune protocol и terminal deletion
  между journal, log/span projections, metrics store, S3 tier и backup
  manifests на production-sized fixture.
- Проверить release-surface metrics последнего sweep, retained bytes/records,
  failures и oldest retained timestamp.
- Защитить ingest disk watermarks: warning и retryable stop до ENOSPC.
- Зафиксировать at-least-once dual-write contract и reconciliation strategy;
  покрыть сбои после projection commit и до journal commit.
- Провести non-empty v0.3 fixture со всеми сигналами, проверить query digest,
  rollback source root и опубликовать offline upgrade procedure;
- fresh-root-only оставить fallback только после явного подтверждения отсутствия
  пользовательских данных.

Acceptance gates:

- `ingest -> retention -> restart -> replay` не восстанавливает expired data;
- backup после retention не содержит expired payloads;
- migration fixture `v0.3.0 -> v0.4.0` либо утверждённый fresh-root runbook
  выполняется в CI;
- fault injection доказывает заявленную ack/retry/dedup semantics;
- low-disk test закрывает admission до повреждения stores.

### Workstream B — Reef v0.3.0 complete edge integration

- Обновить dependency и заменить low-level production wiring на
  `edge.NewHTTPServer` / `grpcreef.NewServerEdge`.
- Использовать одну fail-closed policy для HTTP, gRPC и optional pprof:
  external plaintext требует `insecure`; bearer over plaintext — отдельного
  danger opt-in.
- Включить managed certificate, CA и bearer-file rotation; корректно закрывать
  lifecycle workers.
- Передавать `bearer.PrincipalFromContext` в access/audit logs.
- Добавить trusted-proxy policy; не доверять XFF по умолчанию.
- Перевести `amberctl` на Reef client edge: system/custom CA, mTLS,
  token/token-file, rotation status, явные insecure flags.
- Legacy `api_key`/`api_keys` принимать один minor с deprecation warning и
  однозначным преобразованием в новый edge config.

Acceptance gates:

- Amber integration tests для Reef server/client policy и rotation проходят
  с `-race`;
- external plaintext и bearer-over-plaintext fail closed без явных opt-ins;
- audit log содержит principal для HTTP и gRPC без token material;
- mTLS + rotating token end-to-end работает для `amberctl` и OTLP exporter;
- credential status не содержит секретов и доступен только за auth boundary.

### Workstream C — Gyre v0.6.0 lifecycle integration

- Добавить dependency и Amber adapters для storage, HTTP, gRPC и optional
  pprof; API components зависят от storage.
- Bind listeners синхронно в `Start`; serve failure переводит component/runtime
  в failed и завершает процесс с ошибкой.
- Reverse bounded shutdown: перестать принимать трафик, дождаться HTTP/gRPC,
  при deadline вызвать force stop, затем drain/close storage.
- Подключить aggregate `/healthz`, `/readyz`, `/status`; оставить `/health`
  compatibility alias на один minor.
- Добавить gRPC health service, отражающий тот же aggregate readiness.
- Отобразить breaker, bootstrap, storage, journal, reconcile, disk watermark и
  Reef credential conditions в Gyre snapshots.
- Ввести typed Gyre errors и единое HTTP/gRPC mapping.
- Подключить ConfigStore/ApplyWith/RollbackWith только за Reef. Начальный
  reload scope: log level, query budgets, credentials; остальные изменения
  возвращают `RestartRequired`.
- Запустить `gyre/conformance` на adapter factory.

Acceptance gates:

- bind collision возвращает startup error, а не оставляет живой процесс;
- partial startup откатывается в обратном порядке без утечки listener/lock;
- заблокированный gRPC stream завершается в bounded deadline с force fallback;
- storage не закрывается до завершения API handlers;
- readiness краснеет и восстанавливается на dependency failure/recovery;
- Gyre lifecycle/reload/conformance suite проходит с `-race`.

### Workstream D — bounded public contract

- Strict YAML known-fields и полная semantic validation.
- Явная CLI config policy: переданный отсутствующий файл — ошибка; defaults без
  файла доступны только через документированный режим.
- Синхронизировать code defaults, `config.example.yaml`, README и Go version.
- Ввести query budgets: page, offset/cursor, range, step count, groups, scanned
  segments/bytes и deadline; корректные 400/413/429 responses.
- Исправить body-too-large mapping на HTTP 413.
- Документировать offline backup maintenance window и автоматизированный
  restore drill.
- Добавить `CHANGELOG.md`, upgrade guide, storage/API compatibility policy.
- Собрать release artifacts для `amber`, `amberctl`, `amber-backup` с
  checksums; SBOM/signing — если выбран общий платформенный release standard.
- Перезапустить current-path benchmark campaign: dual write, journal retention,
  mixed signals, restart/replay, RSS и storage amplification.

Acceptance gates:

- config typo и missing explicit file fail before storage open;
- adversarial query suite не создаёт unbounded allocation/response;
- README/example contract проверяется тестом;
- clean-host install, start, secure `amberctl`, backup, restore и upgrade smoke
  выполняются только из release artifacts;
- опубликован benchmark report с hardware/dataset/config и regression budget.

### v0.4.0 release decision

Релиз разрешён только после всех четырёх workstreams. Если объём нужно делить,
деление делается на prerelease (`v0.4.0-alpha.N`/`beta.N`), а не выпуском minor
с известной дырой в retention или upgrade contract.

## v0.5.0 — OTLP fidelity and correlation

Цель: данные, которыми Coral/OTel обогащают сигнал, реально доступны в Amber
queries, а три сигнала имеют общую resource identity.

- Зафиксировать гарантированный набор resource attrs:
  `service.name`, `service.namespace`, `service.instance.id`,
  `deployment.environment`, `host.name`, основные `k8s.*` и `cloud.*`.
- Добавить configurable allowlist/denylist остальных resource/scope attrs.
- Сохранять instrumentation scope name/version/attrs и schema URL.
- Сохранить typed OTLP values; определить отображение arrays/maps/bytes.
- Расширить span projection: kind, trace state, flags, events, links, status
  message; log projection: observed timestamp и flags.
- Хранить повторяющиеся resource/scope значения словарно на segment/batch,
  измерив storage/index cost.
- Добавить resource-aware selectors и cross-signal переходы
  logs <-> trace <-> metrics по trace/resource/time.
- Сделать projection rebuild из AOT4 journal версионным и прерываемым, со
  status/progress и атомарным publish.

Acceptance gates:

- OTLP fidelity corpus сравнивает accepted request, journal и query projection;
- Coral enrichment end-to-end находится через Amber API;
- rebuild на копии production-sized journal даёт тот же query result digest;
- опубликованы storage amplification и query latency до/после;
- high-cardinality corpus не обходит ingest/query budgets.

## v0.6.0 — availability and operator workflow

Этот minor начинается только при наличии реальной потребности после v0.4
restore drills. Предполагаемый набор:

- online consistent checkpoint или короткая quiesce phase вместо полного
  shutdown на backup;
- backup catalog/list/prune/latest и retention policy для local/S3 copies;
- scheduler с Gyre status, последним success/failure и alertable metrics;
- encryption/KMS policy на backup boundary;
- automated upgrade/rollback and disaster-recovery campaign;
- capacity planning по ingest rate, journal amplification, retention и S3.

Decision gate: измеренная длительность v0.4 offline backup и допустимое окно
простоя. Если offline SLA достаточен, этот minor можно заменить более нужной
продуктовой работой.

## Candidate minors after evidence

Это не обещания и не фиксированный порядок:

- **Metrics fidelity:** native float codec, exemplars, explicit delta
  temporality policy, richer query language. Gate — corpus, где scaled-int64
  или отсутствие exemplars реально мешает.
- **Query/API evolution:** streaming large results, saved queries, richer
  aggregations. Gate — профили реальных запросов после v0.5.
- **UI:** отдельная web surface поверх стабильных correlation APIs, не новый
  storage contract внутри Amber.
- **Fathom integration:** replay/derived-data flow и incident links после
  завершения собственного Fathom decision gate; ближайшие Amber releases от
  него не зависят.
- **Multi-tenancy/cluster:** вне текущей single-node роли. Начинать только при
  подтверждённом deployment demand, потому что это меняет security, storage и
  consistency contract целиком.

## Recommended execution order

1. Сначала P0 spike: journal retention/dual-write и upgrade decision.
2. Затем общий Amber component skeleton на Gyre и managed Reef edges — эти
   изменения дают правильную рамку для всех последующих server tests.
3. В этой рамке завершить data lifecycle, query/config bounds и release docs.
4. Запустить fault/restore/benchmark campaigns и выпустить `v0.4.0`.
5. Только после стабильного operational baseline менять event projection в
   `v0.5.0`.

Такой порядок сохраняет уже сделанную работу, не тянет Amber к искусственному
v1 и превращает каждый `0.X.0` в проверяемое улучшение контракта.
