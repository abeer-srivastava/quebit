# Quebit

[![CI](https://github.com/abeer-srivastava/quebit/actions/workflows/ci.yml/badge.svg)](https://github.com/abeer-srivastava/quebit/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)
![Postgres](https://img.shields.io/badge/Postgres-source%20of%20truth-4169E1?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis%20%C2%B7%20Asynq-disposable%20queue-DC382D?logo=redis&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

**An asynchronous payment pipeline in Go: idempotent, retry-safe, and priority-routed.**

Charging a card is slow, flaky, and must never happen twice. Quebit separates *accepting* a payment from *processing* it. PostgreSQL holds the durable state; Redis/Asynq distributes the work. The HTTP layer validates and persists the payment **together with an outbox marker**; a relay hands it to the queue; a pool of workers charges an external provider and records the outcome — staying correct across duplicate requests, duplicate deliveries, transient provider failures, and worker crashes.

> **Status:** portfolio project. The core pipeline is complete and tested; the hardening work that closes the remaining design gaps is tracked in the [roadmap](#roadmap).

<details>
<summary><b>Table of contents</b></summary>

- [Architecture](#architecture)
- [Design decisions](#design-decisions)
- [Guarantees and non-guarantees](#guarantees-and-non-guarantees)
- [Features](#features)
- [Payment lifecycle](#payment-lifecycle)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API reference](#api-reference)
- [Reliability and failure handling](#reliability-and-failure-handling)
- [Observability](#observability)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Roadmap](#roadmap)
- [License](#license)

</details>

---

## Architecture

```mermaid
flowchart TD
    C[Client] --> MW["Middleware<br/>logger → panic recovery → rate limiter"]
    MW --> H[HTTP handler]
    H -->|"validate · INSERT payment<br/>(published_at IS NULL)"| PG[(PostgreSQL)]
    H -.->|202 Accepted| C
    RL[Outbox relay] -->|"claim unpublished<br/>FOR UPDATE SKIP LOCKED"| PG
    RL -->|publish| Q[(Redis / Asynq)]
    RL -->|"mark published<br/>in the same txn"| PG
    Q --> W[Asynq worker pool]
    W -->|"Claim + lease"| PG
    W -->|"POST /charge<br/>Idempotency-Key"| P[Payment provider]
    W -->|update status| PG
    R[Processing-timeout reaper] -->|"expired lease → pending,<br/>clear marker"| PG
    S[Stale-pending sweeper] -->|"stale pending → clear marker"| PG
```

PostgreSQL is the source of truth; Redis only ever holds *work in flight*. The HTTP handler never talks to Redis: it writes the payment and an unpublished marker in one `INSERT`, and the **outbox relay** publishes it. The **sweeper** and **reaper** are pure recovery paths — they clear the marker on payments whose task was lost or whose worker died mid-charge, and the relay re-publishes them.

**Queue routing by amount.** Higher-value payments get more worker attention without starving the rest.

| Amount | Queue | Weight |
|---|---|---|
| `≥ 10,000` | `critical` | 6 |
| `1,000 – 9,999` | `default` | 3 |
| `< 1,000` | `low` | 1 |

The weight controls how often the worker polls each queue, so critical work drains first while `default` and `low` still make progress.

---

## Design decisions

**Why a queue instead of charging inline?**
Provider calls are slow and fail transiently. Doing them inside the request would tie API latency and availability to the provider's. With a queue the API returns `202` quickly and retries happen out of band.

**Why Asynq rather than Kafka or RabbitMQ?**
The workload is a *task queue* — retries, backoff, priorities, and delayed retries — not an event log. Asynq provides all of that on Redis, keeping the operational footprint to two dependencies.

**Why is Postgres the source of truth and Redis disposable?**
Payment state must survive anything. If Redis is flushed, no payment is lost: the rows are still in Postgres, and the sweeper clears the outbox marker on anything stuck in `pending` so the relay publishes it again. Losing Redis costs throughput, not money.

**Why three layers of idempotency?**
Each layer covers a different failure:
1. A `UNIQUE` constraint on `idempotency_key` stops duplicate *requests* from creating duplicate rows.
2. Queue-level `TaskID` deduplication makes a duplicate *publish* a no-op.
3. An atomic `Claim` (`UPDATE … WHERE status IN ('pending','retrying') RETURNING`) stops duplicate *deliveries* from charging twice — only one worker can move a payment into `processing`.

The provider also receives `Idempotency-Key: <payment_id>`, so a retry after an ambiguous failure cannot double-charge on the provider side.

**Why a request hash on the idempotency key?**
A key that comes back with a different amount or currency is a client bug. Returning the original row would silently mis-charge. The stored request hash is compared on replay and a mismatch is rejected with `409`.

**Why exponential backoff with jitter?**
Plain backoff makes every failed task retry at the same instant, producing synchronized load spikes against a provider that is already struggling. Up to 20% jitter spreads them out, and the 10-minute cap bounds how long a payment can wait.

**Why an outbox marker instead of enqueuing from the handler?**
Writing the payment and enqueuing it are two systems, and a crash between them used to strand a payment in `pending` with nothing to process it. The `INSERT` now leaves `published_at` unset; a **relay** finds every unpublished payment and publishes it, marking it published *in the same transaction*. A payment therefore cannot exist without something that will pick it up, the handler never depends on Redis being up, and a crash after publishing merely causes a harmless re-publish. The marker is a column on `payments` rather than a separate outbox table because the message is exactly 1:1 with the payment — a second table would add a foreign key, a backfill, and a join without changing the guarantee.

**Why a processing lease plus a reaper?**
`Claim` moves a payment to `processing` and takes a 90-second lease (`locked_until`). If the worker dies between the provider call and the DB write, the lease expires and the reaper returns the payment to `pending`. Re-charging is safe because the provider call is idempotent per payment ID.

**Why restrict `Claim` to `pending`/`retrying`?**
So a payment can have exactly one owner at a time. If a duplicate task arrives while the payment is already `processing`, the second worker's `UPDATE … WHERE status IN (…)` matches nothing and it exits — no second charge. Recovery of genuinely stuck payments is the reaper's job, not a concurrent claim's.

**Why does the rate limiter fail open?**
If Redis is down, rejecting all traffic would turn a limiter outage into a full API outage. Failing open trades temporary loss of protection for availability. For a payments API you may prefer the opposite; that is a deliberate, documented tradeoff.

**Why classify provider errors?**
Retrying a `422` can never succeed and wastes the retry budget, while giving up on a `503` loses a payment that would have worked. Network errors, `429`, and `5xx` are retryable; other `4xx` are terminal.

---

## Guarantees and non-guarantees

| | |
|---|---|
| ✅ **At-least-once processing** | A task is redelivered until acknowledged, so a crash does not lose work. |
| ✅ **Effectively-once charging** | The `Claim` guard plus `Idempotency-Key: <payment_id>` stop a redelivered task from charging twice. |
| ✅ **Terminal states are final** | A payment reaches `succeeded` or `failed_final` once; only the explicit retry endpoint can reopen a `failed_final` payment. |
| ✅ **No loss between the API and the queue** | The payment and its unpublished marker are written by one `INSERT`, and the relay publishes every unpublished row — even after a crash between the two. |
| ✅ **Stuck `processing` is recovered** | A crashed worker's payment is returned to `pending` once its 90 s lease expires. |
| ✅ **Lost queue tasks are re-published** | If Redis drops a queued task (for example on a flush), the payment stays `pending`; the sweeper clears its marker and the relay publishes it again. |
| ⚠️ **Delivery is asynchronous** | A payment is queued within one relay interval (250 ms) of its commit, not synchronously with the HTTP request. |
| ⚠️ **Recovery is bounded, not instant** | A stuck payment is re-published after its 90 s lease expires (reaper) or after a 60 s stale window (sweeper), so recovery is measured in tens of seconds, not milliseconds. |
| ❌ **No ordering guarantee** | Payments are processed concurrently across queues; there is no FIFO guarantee between them. |
| ❌ **Not exactly-once at the provider** | Correctness relies on the provider honoring `Idempotency-Key`. |

---

## Features

- **Asynchronous queueing** — submission is decoupled from processing via Asynq, so the API stays fast and the pipeline survives restarts.
- **Dynamic priority routing** — `critical` / `default` / `low` queues chosen by amount.
- **Three-layer idempotency** — a unique key constraint, queue `TaskID` dedup, and an atomic `Claim`.
- **Idempotency-key fingerprint** — a reused key with a different amount/currency is rejected (`409`) instead of silently returning the original payment.
- **Processing lease + reaper** — a crashed worker cannot strand a payment in `processing`; the lease expires and the relay re-publishes the payment.
- **Retries with exponential backoff and jitter** — transient failures retry up to 8 times, capped at 10 minutes plus up to 20% jitter.
- **Retry-exhaustion safety net** — when the budget is spent the payment moves to `failed_final` instead of sticking in `retrying`.
- **Terminal error handling** — unrecoverable errors (e.g. `422`) mark the payment `failed_final` immediately and tell Asynq to skip retries.
- **Transactional outbox** — the payment row and its "not yet queued" marker are written by one `INSERT`; a relay commits the publish and the marker together, so a crash can never leave a payment that nothing will pick up.
- **Manual replay** — `POST /v1/payments/{id}/retry` atomically moves a `failed_final` payment back to `pending` and clears its outbox marker, so the relay re-publishes it; concurrent retries cannot both win.
- **Stale pending sweeper** — clears the outbox marker on payments stuck in `pending` in a single `UPDATE`, so the relay drains the whole backlog instead of 100 rows per tick.
- **Sliding-window rate limiting** — an atomic Redis Lua script enforces a per-IP budget and fails open. `X-Forwarded-For` / `X-Real-IP` are honored **only** from configured trusted proxy networks, so the limit cannot be spoofed.
- **Money is explicit** — `amount` is always in the currency's **minor unit** (cents), and `currency` is validated against an ISO 4217 allowlist and normalized.
- **Panic recovery** — handler panics become `500` responses with a logged stack trace instead of crashing the process.
- **Request tracing** — every response carries an `X-Request-ID` (echoed if supplied) that appears in the logs.
- **Structured logging** — JSON via `slog`, level set by `LOG_LEVEL`.
- **Graceful shutdown** — `errgroup` + `signal.NotifyContext` drain the HTTP server, then the worker, on `SIGINT`/`SIGTERM`.
- **Health check** — `GET /healthz` pings Postgres and Redis.
- **Bounded I/O** — request body capped at 1 MiB, provider response at 1 KiB, provider timeout 10 s.
- **CI** — GitHub Actions builds, vets, applies migrations with `ON_ERROR_STOP=1`, asserts the schema, and runs the race-enabled suite against a live Postgres.

---

## Payment lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending: create
    pending --> processing: Claim (lease)
    processing --> succeeded: provider 2xx
    processing --> retrying: transient error
    retrying --> processing: retry (backoff + jitter)
    processing --> pending: lease expired (reaper)
    processing --> failed_final: terminal error (4xx)
    retrying --> failed_final: retries exhausted
    failed_final --> pending: manual POST /retry
    succeeded --> [*]
```

Statuses: `pending`, `processing`, `retrying`, `succeeded`, `failed_final`. The `Claim` guard, plus the provider idempotency key, ensure a redelivered task cannot charge twice.

---

## Quick start

**Prerequisites:** Go 1.26+, Docker & Docker Compose.

```bash
# 1. Start Postgres + Redis (migrations auto-apply on first boot)
make up

# 2. Start the mock payment provider (separate terminal)
go run ./cmd/mockprovider

# 3. Start the server: HTTP API + Asynq worker + outbox relay + reaper + sweeper
make dev
```

Create a payment and watch it move through the pipeline:

```bash
# Create — routes to `default` (amount 5,000)
curl -s -X POST localhost:8080/v1/payments \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"order-123","amount":5000,"currency":"USD"}'
```

```json
{
  "payment": {
    "id": "3df0e063-00ff-42d0-8fbe-7e58e0ade07a",
    "idempotency_key": "order-123",
    "amount": 5000,
    "currency": "USD",
    "status": "pending",
    "attempts": 0,
    "created_at": "2026-10-18T01:23:16.551361+05:30",
    "updated_at": "2026-10-18T01:23:16.551361+05:30"
  },
  "created": true,
  "enqueued": true
}
```

`202 Accepted` — the payment is persisted and a task is on the queue. A worker picks it up within a second or so.

```bash
# Look it up a moment later — a worker has charged the provider
curl -s localhost:8080/v1/payments/3df0e063-00ff-42d0-8fbe-7e58e0ade07a
```

```json
{
  "payment": {
    "id": "3df0e063-00ff-42d0-8fbe-7e58e0ade07a",
    "idempotency_key": "order-123",
    "amount": 5000,
    "currency": "USD",
    "status": "succeeded",
    "attempts": 1,
    "provider_ref": "prov_3df0e063-00ff-42d0-8fbe-7e58e0ade07a",
    "created_at": "2026-10-18T01:23:16.551361+05:30",
    "updated_at": "2026-10-18T01:23:16.685365+05:30"
  }
}
```

The same request replayed with the same key returns the same row with `"created": false` and nothing re-published; the same key with a different amount or currency is rejected with `409 Conflict`.

A ready-made request collection lives in [`api.http`](./api.http).

### Make targets

| Target | Description |
|---|---|
| `make up` / `make down` | Start / stop Postgres + Redis (`down` removes volumes) |
| `make dev` | Run the server locally |
| `make test` | All tests with the race detector |
| `make test-unit` | Tests that need no external services |
| `make lint` | `go vet` (+ `staticcheck` if installed) |
| `make build` | Build `bin/quebit` |
| `make fmt` | `gofmt -s -w .` |

### Driving failure paths locally

The mock provider switches behavior on the **amount**:

| Amount | Provider result |
|---|---|
| `999999` | `422` (terminal) → the payment goes straight to `failed_final` |
| `888888` | `503` (retryable) → the payment retries, then becomes `failed_final` when the budget is exhausted |
| anything else | `200` with a generated `provider_ref` |

> Sentinels are amounts, not currencies, because the API validates `currency` against an ISO 4217 allowlist — a sentinel currency is rejected with `422` before a worker ever runs.

Real responses (abridged):

```jsonc
// POST with amount 999999 → terminal 422 at the provider → failed_final, no retry
{ "payment": { "id": "a601048a-…", "status": "failed_final", "attempts": 1,
  "last_error": "provider returned 422: {\"error\":\"invalid card\"}" } }

// POST with amount 888888 → retryable 503 → retrying (backoff pending)
{ "payment": { "id": "b6e4ede5-…", "status": "retrying", "attempts": 1,
  "last_error": "provider returned 503: {\"error\":\"temporarily down\"}" } }
```

```http
POST /v1/payments/{id}/retry        → 202 {"status":"retrying"}
POST /v1/payments  (reused key, different amount) → 409
POST /v1/payments  (unknown currency "XYZ")       → 422
GET  /v1/payments/not-a-uuid                      → 400
GET  /v1/payments/11111111-…                      → 404
```

```jsonc
// 409 Conflict
{ "error": { "code": "conflict", "message": "idempotency key was reused with different request parameters" } }
// 422 Unprocessable Entity
{ "error": { "code": "validation_error", "message": "unsupported currency", "field": "currency" } }
```

---

## Configuration

All configuration is via environment variables. A `.env` file is loaded automatically if present — see [`.env.example`](./.env.example).

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://quebit:quebit@localhost:5432/quebit?sslmode=disable` | Postgres connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `PROVIDER_URL` | `http://localhost:9090` | Payment provider base URL |
| `PROVIDER_API_KEY` | *(empty)* | Optional bearer token sent to the provider |
| `WORKER_CONCURRENCY` | `10` | Concurrent Asynq worker goroutines |
| `RATE_LIMIT_RPS` | `100` | Max requests per second per client IP |
| `TRUSTED_PROXIES` | *(empty)* | Comma-separated CIDRs whose `X-Forwarded-For` / `X-Real-IP` headers are trusted for rate limiting. Empty means those headers are ignored. |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` |

`WORKER_CONCURRENCY` and `RATE_LIMIT_RPS` must be positive integers; invalid or missing values fall back to the defaults. A malformed `TRUSTED_PROXIES` value fails startup.

> **Deploying behind a load balancer?** Set `TRUSTED_PROXIES` to the proxy's CIDR. Otherwise the rate limiter keys on the proxy's IP and all clients share one budget.

### Provider contract

Quebit calls `POST {PROVIDER_URL}/charge`:

```json
{ "payment_id": "…", "amount": 5000, "currency": "USD" }
```

Headers: `Content-Type: application/json`, `Idempotency-Key: <payment_id>`, and `Authorization: Bearer <PROVIDER_API_KEY>` when configured.

A `2xx` response may include `{"provider_ref": "…"}`. Any non-`2xx` is an error: `429` and `5xx` are retryable, other `4xx` are terminal.

---

## API reference

### `POST /v1/payments`

Create a payment and queue it for processing.

```json
{ "idempotency_key": "order-123", "amount": 5000, "currency": "USD" }
```

`amount` is in the currency's **minor unit**: `5000` = `50.00 USD`.

**Validation:** `amount > 0`; non-empty `idempotency_key` (≤ 255 chars); `currency` present and in the ISO 4217 allowlist (case-insensitive, normalized to upper case). Malformed JSON → `400`; validation failure → `422`.

| Status | Meaning |
|---|---|
| `202 Accepted` | New payment accepted for processing |
| `200 OK` | Idempotent replay — returns the existing payment with `"created": false` |
| `409 Conflict` | The idempotency key was reused with different parameters |

```json
{
  "payment": {
    "id": "…", "status": "pending", "amount": 5000,
    "currency": "USD", "idempotency_key": "order-123", "attempts": 0,
    "created_at": "…", "updated_at": "…"
  },
  "created": true,
  "enqueued": true
}
```

### `GET /v1/payments/{id}`

```json
{ "payment": { "id": "…", "status": "succeeded", "provider_ref": "prov_…", "attempts": 1 } }
```

`404` if not found, `400` if the id is not a UUID.

### `POST /v1/payments/{id}/retry`

Reopen a `failed_final` payment.

| Status | Meaning |
|---|---|
| `202 Accepted` | `{ "status": "retrying" }` |
| `404 Not Found` | No such payment |
| `409 Conflict` | Payment is not in `failed_final` |

### `GET /healthz`

```json
{ "postgres": true, "redis": true }
```

`200` when both dependencies respond within 3 s, `503` otherwise.

### Rate limiting

Every request counts against a per-IP sliding window (`RATE_LIMIT_RPS` per second). Over-budget requests get `429` with a `Retry-After` header. The client IP is `RemoteAddr` unless it falls inside `TRUSTED_PROXIES`, in which case `X-Forwarded-For` / `X-Real-IP` is honored. If Redis is unreachable, requests are allowed through (fail-open).

---

## Reliability and failure handling

| Failure | Behavior |
|---|---|
| Duplicate `POST` (same idempotency key) | Returns the original payment, `created=false`, nothing re-published |
| Same key, different amount/currency | `409 Conflict`; the original payment is untouched |
| DB insert succeeds, process dies before publish | Payment stays `pending` with its marker unset; the **relay** publishes it on the next poll, including after a restart |
| Redis drops a queued task (flush, restart) | Payment stays `pending` with its marker set; the **sweeper** clears the marker and the **relay** re-publishes it |
| Duplicate task delivery | `Claim` finds no claimable row, returns `ErrAlreadyDone`; the payment is skipped |
| Worker dies mid-charge | Its lease expires; the **reaper** returns the payment to `pending` and clears its marker so the relay re-publishes it (safe — the provider call is idempotent) |
| Provider network error / `429` / `5xx` | Marked `retrying`; retried with exponential backoff + jitter (max 8) |
| Retry budget exhausted | Marked `failed_final`; the task is archived (`SkipRetry`), so recovery is manual via the retry endpoint |
| Provider `4xx` (non-retryable, e.g. `422`) | Marked `failed_final` immediately; no retries |
| Two concurrent manual retries | Only one wins the atomic `failed_final → pending` transition |
| Spoofed `X-Forwarded-For` | Ignored unless the peer is in `TRUSTED_PROXIES` |
| Handler panic | Recovered by middleware → `500`, stack logged, process survives |
| Redis down (rate limiter) | Fails open — requests proceed |
| `SIGINT` / `SIGTERM` | HTTP server drained, then the Asynq worker drained, within a 30 s window |

### Verified under failure

Two experiments were run against the local stack (Postgres 16 + Redis 7 in Docker, mock provider with 0–50 ms latency, `WORKER_CONCURRENCY=10`, `RATE_LIMIT_RPS` raised so the limiter would not throttle the client):

**1. Redis data loss mid-run.** 2,000 payments were submitted in ~2.4 s, and a `redis-cli flushall` was issued 0.5 s in — wiping both the queue and the rate-limiter keys. Every payment whose task was dropped was recovered: **2,000 / 2,000 reached `succeeded` and none was lost**.

Recovery took **79 s**, almost all of it the 60 s stale window: the sweeper then requeued 374 payments in a *single* `UPDATE` and the relay drained them a batch of 100 at a time.

```text
status    | count
----------+-------
succeeded |  2000

total | distinct_provider_refs
------+-----------------------
 2000 |                  2000
```

Counting the worker's charge attempts across the run: **2,002 payments charged, 2,002 distinct payment ids, one charge each** (the two extra are from an earlier smoke test) — i.e. **no payment was charged twice**.

> Before the outbox, this same experiment took over four minutes: the old sweeper re-enqueued at most 100 rows per 30 s tick, so ~600 stuck payments trickled back one tick at a time.

**2. 100 concurrent identical requests.** 100 simultaneous `POST`s carrying the same idempotency key produced **one** payment row, one `provider_ref`, and `attempts = 1`. 99 requests got `200` (the idempotent replay) and exactly one got `202`.

```text
requests:       100   concurrency: 100
status codes:   map[200:99 202:1]
distinct ids:   1
created=true:   1

 rows | keys | provider_refs | total_attempts
------+------+---------------+----------------
    1 |    1 |             1 |              1
```

> The mock provider is stateless, so experiment 1 demonstrates no-loss recovery, not provider-side idempotency. The no-double-charge guarantee is the `Claim` guard plus the per-payment `Idempotency-Key`, exercised in experiment 2 and in the race-enabled test suite below.

---

## Observability

- **Structured JSON logs** via `log/slog`. Set `LOG_LEVEL=DEBUG` to trace request entry/exit, publish/reschedule, provider calls, and status transitions.
- **Request logs** include `request_id`, method, path, status, duration, and bytes written.
- **Request correlation** via the `X-Request-ID` header (generated if absent).

Prometheus metrics and OpenTelemetry tracing are tracked in the [Roadmap](#roadmap).

---

## Testing

Tests live in a `test/` subdirectory of each module and exercise packages through their public API.

```bash
# Unit tests: no external dependencies (rate-limit tests use miniredis)
go test ./internal/provider/test/ ./internal/queue/test/ ./internal/httpapi/test/

# Full suite (integration tests skip themselves unless DATABASE_URL is set)
DATABASE_URL=postgres://quebit:quebit@localhost:5432/quebit?sslmode=disable \
  go test -v -race ./...
```

The suite includes a **concurrency invariant test**: ten goroutines process the same payment at once and the provider is charged exactly once (`internal/service/test`), plus store-level tests for the lease/reaper, the outbox (`PublishPending` / `ResetStalePending`), the idempotency fingerprint, and the atomic retry transition, and a unit test for the relay's publish loop (`internal/queue/test`).

### Load test

The load generator lives in [`cmd/loadgen`](./cmd/loadgen) and uses only the standard library; it `POST`s to `/v1/payments` with unique idempotency keys and reports throughput, status codes, and latency percentiles.

```bash
make load           # equivalent: go run ./cmd/loadgen -n 1000 -c 50
```

The server, Postgres, Redis, and the mock provider all ran on one machine (12th Gen Intel i5‑12500H, 16 threads, 15 GiB RAM, Linux 6.18), so these numbers characterize the pipeline rather than a production network. `RATE_LIMIT_RPS` was raised for the run so the per-IP limiter would not throttle the client.

```text
requests:       1000
concurrency:    50
elapsed:        1.19 s
throughput:     840 req/s
status codes:   map[202:1000]
latency p50:    59.7 ms
latency p95:    71.2 ms
latency p99:    73.0 ms
```

The throughput is latency-bound (`50 concurrent / 59.7 ms ≈ 838 req/s`). Every request was accepted and persisted, with no `5xx` or `429`. See [Verified under failure](#verified-under-failure) for the recovery experiments.

---

## Project layout

```
cmd/server/main.go        entry point: config, wiring, graceful shutdown
cmd/mockprovider/main.go  runnable fake provider for local development
internal/payment/         Payment model, repository/outbox interfaces, Postgres store, lease reaper
internal/provider/        Charger interface, HTTP client, provider error classification
internal/queue/           Asynq client/server, task types & routing, handler, outbox relay, sweeper, reaper
internal/httpapi/         HTTP handlers, error types, request logger, rate limiter, recovery, health
internal/service/         business logic coordinating the store and provider
migrations/               ordered SQL migrations (applied in order)
*/*/test/                 black-box tests (package test)
```

---

## Roadmap

### Done

- [x] **Transactional outbox** — the payment and its unpublished marker are written by one `INSERT`; a relay publishes unpublished rows (`FOR UPDATE SKIP LOCKED`) and marks them published in the same transaction. The handler no longer touches Redis, so a crash can no longer strand a payment.
- [x] **Sweeper without a per-tick cap** — the sweeper clears the marker for stale pending payments in a single `UPDATE` and the relay drains the backlog, instead of 100 rows per 30 s tick.
- [x] **Processing-timeout reaper** — a processing lease (`locked_until`) with a background reaper that returns payments whose worker died mid-charge to `pending`.
- [x] **Idempotency payload check** — a stored request hash and a `409` when a key is reused with a different amount/currency.
- [x] **Atomic manual retry** — the `failed_final → pending` transition is a single `UPDATE … WHERE … RETURNING`, so concurrent retries cannot both succeed.
- [x] **Trusted-proxy handling** — `X-Forwarded-For` / `X-Real-IP` are honored only from configured proxy CIDRs.
- [x] **ISO 4217 currency validation** and `amount` documented as minor units.

### Next

- [ ] **Circuit breaker** around the provider — fail fast during outages; with 8 retries and 10 workers, a provider outage otherwise hammers it.
- [ ] **Recover payments stuck mid-retry** — a task lost while a payment is `retrying` (its backoff lives in Redis) is not covered by the sweeper, which only scans `pending`. A stale-`retrying` scan with a threshold above the 10-minute backoff cap would close it.
- [ ] **Repeatable chaos harness** — the one-off numbers live in [Verified under failure](#verified-under-failure); make the load and Redis-flush experiments a scripted, repeatable harness.

### Ideas

- [ ] Prometheus metrics — queue depth (via the Asynq Inspector), payments by terminal status, provider latency histogram.
- [ ] `/livez` (process) vs `/readyz` (DB + Redis) split.
- [ ] OpenTelemetry tracing across HTTP, Asynq tasks, and provider calls.
- [ ] Per-merchant API keys, scoped rate limits, and namespaced idempotency keys (today the key is global, so two merchants using `order-123` collide).
- [ ] Signed outbound webhooks (HMAC) with their own retry queue, so a merchant can be told the outcome.
- [ ] Dead-letter queue with an operator view.
- [ ] Versioned migrations (`golang-migrate`) with up/down checks in CI.
- [ ] Secrets from a secret store instead of plain env vars.
- [ ] Table-driven handler tests, property-based tests for routing/backoff, and `testcontainers-go` for self-contained integration tests.

---

## License

[MIT](./LICENSE) © 2026 Abeer Srivastava
