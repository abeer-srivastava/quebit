# Quebit

Asynchronous, queue-based payment processing pipeline in Go using PostgreSQL and Redis.

## Architecture

```
HTTP Request → Rate Limiter → Handler → Postgres (create) → Redis/Asynq (enqueue)
                                                                    ↓
                                              Asynq Worker ← Redis (dequeue)
                                                    ↓
                                              Provider (charge)
                                                    ↓
                                          Postgres (update status)
```

**Queue routing by amount:**
- `critical` (6x weight) — payments ≥ 10,000
- `default` (3x weight) — payments 1,000–9,999
- `low` (1x weight) — payments < 1,000

## Features

- **Idempotency** — DB unique constraint on `idempotency_key` + queue-level `TaskID` dedup + `Claim` guard against double-processing
- **Automatic retries** — Up to 8 retries with exponential backoff + jitter for transient failures (429, 5xx)
- **Terminal error handling** — Immediate abort and `failed_final` for unrecoverable errors (422)
- **Manual retry** — `POST /v1/payments/{id}/retry` replays archived tasks via Asynq Inspector
- **Sliding-window rate limiting** — Redis sorted set-based per-IP rate limiter
- **Stale pending sweeper** — Background goroutine re-enqueues payments stuck in `pending` (handles enqueue failures)
- **Graceful shutdown** — `errgroup` + `signal.NotifyContext` drains HTTP and Asynq on SIGINT/SIGTERM
- **Structured logging** — JSON logs via `slog`, configurable level via `LOG_LEVEL` env var
- **Health check** — `GET /healthz` pings Postgres and Redis

## Quick Start

```bash
# Start Postgres and Redis
docker compose up -d

# Start the mock payment provider (separate terminal)
go run ./cmd/mockprovider

# Run the server
LOG_LEVEL=DEBUG go run ./cmd/server

# Create a payment
curl -X POST http://localhost:8080/v1/payments \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"pay-001","amount":5000,"currency":"USD"}'

# Check payment status
curl http://localhost:8080/v1/payments/<id>

# Retry a failed payment
curl -X POST http://localhost:8080/v1/payments/<id>/retry

# Health check
curl http://localhost:8080/healthz
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://quebit:quebit@localhost:5432/quebit?sslmode=disable` | Postgres connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `PROVIDER_URL` | `http://localhost:9090` | External payment provider base URL |
| `PROVIDER_API_KEY` | *(empty)* | Optional bearer token sent to the provider |
| `LOG_LEVEL` | `INFO` | Log level: `DEBUG`, `INFO`, `WARN`, `ERROR` |

## API

### `POST /v1/payments`
Create a new payment. Returns `202 Accepted` for new (accepted for async processing), `200` for idempotent duplicate.

```json
{
  "idempotency_key": "order-123",
  "amount": 5000,
  "currency": "USD"
}
```

### `GET /v1/payments/{id}`
Get payment by UUID.

### `POST /v1/payments/{id}/retry`
Retry a `failed_final` payment. Returns `202 Accepted`.

### `GET /healthz`
Health check. Returns `200` if both Postgres and Redis are reachable, `503` otherwise.

## Testing

```bash
# Unit tests (no external deps needed)
go test ./internal/provider/ ./internal/queue/

# Integration tests (needs Postgres running)
DATABASE_URL=postgres://quebit:quebit@localhost:5432/quebit?sslmode=disable \
  go test -v ./...
```

## Project Layout

```
cmd/server/main.go           — entry point, wiring, graceful shutdown
internal/payment/payment.go   — Payment model and status constants
internal/payment/store.go     — Postgres store (CRUD, Claim, idempotency)
internal/payment/sweeper.go   — stale pending query
internal/provider/provider.go     — external provider HTTP client
internal/provider/mock.go         — in-memory mock provider (tests/dev)
cmd/mockprovider/main.go         — runnable fake provider for local dev
internal/queue/tasks.go       — task types, queue routing, Asynq client
internal/queue/server.go      — Asynq server factory with backoff
internal/queue/handler.go     — task handler (claim → charge → mark)
internal/queue/inspector.go   — Inspector wrapper for replaying tasks
internal/queue/sweeper.go     — background re-enqueue loop
internal/httpapi/handler.go   — HTTP handlers
internal/httpapi/ratelimit.go — sliding-window rate limiter
internal/httpapi/logger.go    — request logger middleware
internal/httpapi/health.go    — health check endpoint
migrations/001_init.sql       — database schema
```

## Improvements in this revision

- `migrations/001_init.sql` now includes `provider_ref`, preventing `column "provider_ref" does not exist` on fresh databases.
- CI applies all SQL migrations in `migrations/` in order.
- `payment.Repository` now declares `Delete`, matching the store implementation.
- Rate limiter recognises `X-Forwarded-For` / `X-Real-IP` for proxy-aware limiting.
- HTTP request bodies are capped at 1 MiB via `http.MaxBytesReader`.
- `CreatePayment` validates idempotency key length.
- Every response carries an `X-Request-ID`, and request logs include it.
- Provider client supports an optional bearer token (`PROVIDER_API_KEY`).
- Graceful shutdown now drains HTTP before the Asynq worker.
- A runnable mock provider (`cmd/mockprovider`) and an in-memory `MockCharger` simplify local development and tests.
