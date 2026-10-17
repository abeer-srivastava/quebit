# Quebit

An asynchronous, queue-based payment processing pipeline written in Go, backed by **PostgreSQL** (durable state) and **Redis/Asynq** (work distribution).

Quebit decouples *accepting* a payment from *processing* it. The HTTP layer validates and persists a payment, then hands it to a Redis-backed task queue. A pool of workers pulls tasks, charges an external provider, and writes the outcome back to Postgres — all while remaining safe against duplicates, transient failures, and restarts.

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Payment Lifecycle](#payment-lifecycle)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Reliability & Failure Handling](#reliability--failure-handling)
- [Observability](#observability)
- [Testing](#testing)
- [Project Layout](#project-layout)
- [Roadmap / Suggested Improvements](#roadmap--suggested-improvements)

---

## Architecture

```
HTTP Request
   │
   ▼
[ Logger middleware ] → [ Panic recovery ] → [ Rate limiter ]
   │
   ▼
HTTP Handler ──► validate ──► Postgres (INSERT … ON CONFLICT) ──► Redis/Asynq (enqueue)
                                 │                                    │
                            202 Accepted                              ▼
                                                          Asynq Worker (dequeue)
                                                                 │
                                                                 ▼
                                                     Provider HTTP call (/charge)
                                                                 │
                                                                 ▼
                                                     Postgres (update status)
```

**Queue routing by amount** — higher-value payments get proportionally more worker attention:

| Amount | Queue | Weight |
|---|---|---|
| `≥ 10,000` | `critical` | 6 |
| `1,000 – 9,999` | `default` | 3 |
| `< 1,000` | `low` | 1 |

The weight controls how often the worker polls each queue, so critical work is drained first without starving the others.

---

## Features

- **Asynchronous queueing** — Submission is decoupled from processing via Asynq (Redis-backed), so the API stays fast and the pipeline survives restarts.
- **Dynamic priority routing** — Payments are routed to `critical` / `default` / `low` queues by amount (see table above).
- **Idempotency guard** — Three layers of protection:
  1. A Postgres `UNIQUE` constraint on `idempotency_key` (duplicate creates return the original row).
  2. Queue-level `TaskID` deduplication.
  3. An atomic `Claim` update that refuses to re-process terminal payments.
- **Automatic retries with exponential backoff + jitter** — Transient provider failures (network errors, `429`, `5xx`) are retried up to **8 times** with backoff capped at 10 minutes plus up to 20% jitter.
- **Retry-exhaustion safety net** — When the retry budget is used up, the payment is moved to `failed_final` instead of being left stuck in `retrying`.
- **Terminal error handling** — Unrecoverable provider errors (e.g. `422`) immediately mark the payment `failed_final` and tell Asynq to skip further retries.
- **Manual replay** — `POST /v1/payments/{id}/retry` replays an archived task via the Asynq Inspector, falling back to a fresh enqueue if the archived task is gone.
- **Stale pending sweeper** — A background loop re-enqueues payments stuck in `pending`, covering the gap where the DB insert succeeds but the Redis enqueue fails.
- **Sliding-window rate limiting** — An atomic Redis Lua script enforces a per-IP request budget, honoring `X-Forwarded-For` / `X-Real-IP`, and fails open if Redis is unavailable.
- **Panic recovery** — A middleware converts handler panics into `500` responses (logged with a stack trace) instead of crashing the process.
- **Request tracing** — Every response carries an `X-Request-ID` (echoed if supplied), included in request logs.
- **Structured, configurable logging** — JSON logs via `slog`, level controlled by `LOG_LEVEL`.
- **Graceful shutdown** — `errgroup` + `signal.NotifyContext` drain the HTTP server before stopping the Asynq worker on `SIGINT`/`SIGTERM`.
- **Health check** — `GET /healthz` pings both Postgres and Redis.
- **Tunable I/O** — HTTP body capped at 1 MiB, provider responses capped at 1 KiB, provider HTTP timeout of 10s.
- **CI** — GitHub Actions builds, vets, applies migrations, and runs the race-enabled test suite against a live Postgres.

---

## Payment Lifecycle

```
                    ┌──────────┐   Claim    ┌────────────┐  provider 2xx   ┌───────────┐
  create  ───────►  │ pending  │ ─────────► │ processing │ ──────────────► │ succeeded │
                    └──────────┘            └────────────┘                 └───────────┘
                                                     │
                                    transient error  │  terminal error (4xx, non-retryable)
                                                     ▼                          │
                                              ┌───────────┐                     ▼
                                              │ retrying  │             ┌───────────────┐
                                              └───────────┘             │  failed_final │
                                                    │                   └───────────────┘
                              retries exhausted     │                          ▲
                                                    └──────────────────────────┘
                                            (or manual POST …/retry resets to pending)
```

- **Statuses**: `pending`, `processing`, `retrying`, `succeeded`, `failed_final`.
- A payment only becomes `succeeded` or `failed_final` once — the `Claim` guard prevents a redelivered task from charging twice.

---

## Quick Start

**Prerequisites:** Go 1.26+, Docker & Docker Compose.

```bash
# 1. Start Postgres + Redis (migrations auto-applied on first boot)
docker compose up -d          # or: make up

# 2. Start the mock payment provider (separate terminal)
go run ./cmd/mockprovider

# 3. Start the server: HTTP API + Asynq worker + sweeper
LOG_LEVEL=DEBUG go run ./cmd/server       # or: make dev
```

Create a payment and watch it flow through the pipeline:

```bash
# Create (routes to `default`, amount 5,000)
curl -i -X POST http://localhost:8080/v1/payments \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"order-123","amount":5000,"currency":"USD"}'

# Look it up
curl http://localhost:8080/v1/payments/<id>

# Health
curl http://localhost:8080/healthz
```

Other useful `make` targets:

| Target | Description |
|---|---|
| `make up` / `make down` | Start / stop Postgres + Redis (down removes volumes) |
| `make dev` | Run the server locally |
| `make test` | All tests with the race detector |
| `make test-unit` | Tests that need no external services |
| `make lint` | `go vet` (+ `staticcheck` if installed) |
| `make build` | Build `bin/quebit` |
| `make fmt` | `gofmt -s -w .` |

A ready-made request collection lives in [`api.http`](./api.http).

### Driving failure paths locally

The mock provider switches behavior based on the `currency` sentinel:

| Currency | Result |
|---|---|
| `FAIL_TERMINAL` | `422` → payment goes straight to `failed_final` |
| `FAIL_RETRYABLE` | `503` → payment retries, then becomes `failed_final` when the budget is exhausted |
| anything else | `200` with a generated `provider_ref` |

---

## Configuration

All configuration is via environment variables (a `.env` file is loaded automatically if present — see [`.env.example`](./.env.example)).

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://quebit:quebit@localhost:5432/quebit?sslmode=disable` | Postgres connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `PROVIDER_URL` | `http://localhost:9090` | External payment provider base URL |
| `PROVIDER_API_KEY` | *(empty)* | Optional bearer token sent to the provider |
| `WORKER_CONCURRENCY` | `10` | Number of concurrent Asynq worker goroutines |
| `RATE_LIMIT_RPS` | `100` | Max requests per second per client IP |
| `LOG_LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARN`, `ERROR` |

> `WORKER_CONCURRENCY` and `RATE_LIMIT_RPS` must be positive integers; invalid or missing values fall back to the defaults.

### Provider contract

Quebit calls `POST {PROVIDER_URL}/charge` with:

```json
{ "payment_id": "…", "amount": 5000, "currency": "USD" }
```

Headers: `Content-Type: application/json`, `Idempotency-Key: <payment_id>`, and `Authorization: Bearer <PROVIDER_API_KEY>` when configured. A `2xx` response may include `{"provider_ref": "…"}`; any non-`2xx` is treated as an error (`429`/`5xx` retryable, other `4xx` terminal).

---

## API Reference

### `POST /v1/payments`

Create and enqueue a payment.

**Request**
```json
{ "idempotency_key": "order-123", "amount": 5000, "currency": "USD" }
```

**Validation:** `amount > 0`, non-empty `idempotency_key` (≤ 255 chars), non-empty `currency`. Malformed JSON → `400`; validation failure → `422`.

**Responses**
- `202 Accepted` — new payment accepted for processing:
  ```json
  {
    "payment": { "id": "…", "status": "pending", "amount": 5000, "idempotency_key": "order-123", "attempts": 0, "created_at": "…", "updated_at": "…" },
    "created": true,
    "enqueued": true
  }
  ```
- `200 OK` — idempotent replay; returns the existing payment with `"created": false`.

### `GET /v1/payments/{id}`

```json
{ "payment": { "id": "…", "status": "succeeded", "provider_ref": "prov_…", "attempts": 1, "…": "…" } }
```

Returns `404` if not found, `400` if the `id` is not a UUID.

### `POST /v1/payments/{id}/retry`

Replay a `failed_final` payment.

- `202 Accepted` → `{ "status": "retrying" }`
- `404 Not Found` — no such payment
- `409 Conflict` — payment is not in `failed_final`

### `GET /healthz`

```json
{ "postgres": true, "redis": true }
```

`200` when both dependencies respond within 3s, `503` otherwise.

### Rate limiting

Every request is counted against a per-IP sliding window (`RATE_LIMIT_RPS` per second). Over-budget requests receive `429` with a `Retry-After` header. If Redis is unreachable, requests are allowed through (fail-open).

---

## Reliability & Failure Handling

| Failure | Behavior |
|---|---|
| Duplicate `POST` (same idempotency key) | Returns the original payment, `created=false`, no re-enqueue |
| DB insert succeeds, enqueue fails | Payment stays `pending`; the **sweeper** re-enqueues it |
| Duplicate task delivery | `Claim` returns `ErrAlreadyDone`; the payment is skipped |
| Provider network error / `429` / `5xx` | Marked `retrying`, retried with exponential backoff + jitter (max 8) |
| Retry budget exhausted | Marked `failed_final`; task is archived (`SkipRetry`) |
| Provider `4xx` (non-retryable, e.g. `422`) | Marked `failed_final` immediately; no retries |
| Handler panic | Recovered by middleware → `500`, stack logged, process survives |
| Redis down (rate limiter) | Fails open — requests proceed |
| `SIGINT` / `SIGTERM` | HTTP server drained, then Asynq worker drained, within a 30s window |

---

## Observability

- **Structured JSON logs** via `log/slog`; set `LOG_LEVEL=DEBUG` to trace request entry/exit, enqueue/reschedule, provider calls, and status transitions.
- **Request logs** include `request_id`, method, path, status, duration, and bytes written.
- **Request correlation** via the `X-Request-ID` header (generated if absent).

---

## Testing

```bash
# Unit tests — no external dependencies (rate-limit tests use miniredis)
go test ./internal/provider/test/ ./internal/queue/test/ ./internal/httpapi/test/

# Full suite (integration tests skip themselves unless DATABASE_URL is set)
DATABASE_URL=postgres://quebit:quebit@localhost:5432/quebit?sslmode=disable \
  go test -v -race ./...
```

Tests live in a `test/` subdirectory of each module and exercise packages through their public API.

---

## Project Layout

```
cmd/server/main.go             — entry point: config, wiring, graceful shutdown
cmd/mockprovider/main.go       — runnable fake provider for local development
internal/payment/              — Payment model, repository interfaces, Postgres store, stale sweeper
internal/provider/             — Charger interface, HTTP client, provider error classification
internal/queue/                — Asynq client/server, task types & routing, handler, inspector, sweeper
internal/httpapi/              — HTTP handlers, error types, request logger, rate limiter, recovery, health
internal/service/              — Business logic coordinating store, queue, and provider
migrations/                    — SQL schema migrations (applied in order)
*/*/test/                      — black-box tests (package test)
```

---

## Roadmap / Suggested Improvements

These are intentionally *not* implemented yet — they're the next steps toward a production-grade system:

- **Circuit breaker for the provider** — stop hammering a failing provider; fail fast and let the sweeper/retry path recover.
- **Transactional outbox** — persist the intent to enqueue in the same transaction as the payment insert, then relay to Redis, eliminating the sweeper's eventual-consistency window.
- **Dead-letter queue + alerting** — route exhausted tasks to a DLQ with an operator view rather than only archiving them.
- **Metrics (Prometheus)** — queue depth, processing latency, retry counts, error rates, and sweeper throughput.
- **Distributed tracing (OpenTelemetry)** — propagate trace context across HTTP, Asynq tasks, and provider calls.
- **Readiness vs. liveness** — split `/healthz` into `/livez` (process) and `/readyz` (DB + Redis).
- **Per-route rate limits & auth** — API keys/JWT, scoped limits, and CORS/security headers.
- **Migration tooling** — move to a versioned migration tool (e.g. `golang-migrate`) with up/down and checksums in CI.
- **Secrets management** — load credentials from a secret store instead of plain env vars.
- **Broader test coverage** — table-driven handler tests, property-based tests for routing/backoff, and chaos tests for Redis/DB blips.
