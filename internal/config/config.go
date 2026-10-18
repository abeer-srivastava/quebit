// Package config is the single source of truth for quebit's runtime
// configuration. Every environment variable, default, and tunable that the
// server reads at startup lives here — no other package should call
// os.Getenv for operational settings.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config groups every runtime tunable by the subsystem it affects.
type Config struct {
	Database Database
	Redis    Redis
	HTTP     HTTP
	Provider Provider
	Worker   Worker
	Outbox   Outbox
	Log      Log
}

// Database holds the Postgres connection settings.
type Database struct {
	// URL is the Postgres connection string.
	URL string
}

// Redis holds the Redis connection settings, shared by the rate limiter and
// the Asynq queue.
type Redis struct {
	// Addr is the host:port of the Redis server.
	Addr string
}

// HTTP holds the API server settings.
type HTTP struct {
	// Addr is the listen address, e.g. ":8080".
	Addr string
	// ReadTimeout bounds how long reading the request (headers + body) may take.
	ReadTimeout time.Duration
	// WriteTimeout bounds how long writing the response may take.
	WriteTimeout time.Duration
	// IdleTimeout bounds how long a keep-alive connection may sit idle.
	IdleTimeout time.Duration
	// ShutdownTimeout bounds graceful shutdown of the HTTP server.
	ShutdownTimeout time.Duration
	// HealthTimeout bounds the Postgres/Redis pings in /healthz.
	HealthTimeout time.Duration
	// RateLimitRPS is the max requests per second per client IP.
	RateLimitRPS int
	// TrustedProxies is a comma-separated list of CIDRs whose
	// X-Forwarded-For / X-Real-IP headers are honored.
	TrustedProxies string
}

// Provider holds the external payment provider settings.
type Provider struct {
	// URL is the provider's base URL.
	URL string
	// APIKey is an optional bearer token sent to the provider.
	APIKey string
	// Timeout bounds a single provider HTTP call.
	Timeout time.Duration
}

// Worker holds the Asynq worker settings.
type Worker struct {
	// Concurrency is the number of Asynq worker goroutines.
	Concurrency int
	// ShutdownTimeout bounds how long Asynq waits for in-flight tasks
	// during shutdown.
	ShutdownTimeout time.Duration
}

// Outbox holds the transactional-outbox pipeline settings: the relay that
// publishes persisted payments, the sweeper that re-queues lost pendings, and
// the reaper that reclaims expired processing leases.
type Outbox struct {
	// RelayInterval is how often the relay polls the outbox for unpublished
	// payments. The API answers 202 as soon as the row is committed, so any
	// delay here is added request latency.
	RelayInterval time.Duration
	// RelayBatch is the max number of payments published per poll.
	RelayBatch int
	// SweepInterval is how often the sweeper looks for stale pending payments.
	SweepInterval time.Duration
	// SweepStaleAge is how long a payment may stay pending before the
	// sweeper treats it as lost and re-queues it.
	SweepStaleAge time.Duration
	// ReapInterval is how often the reaper reclaims expired processing leases.
	ReapInterval time.Duration
}

// Log holds the logging settings.
type Log struct {
	// Level is one of DEBUG, INFO, WARN, ERROR (case-insensitive).
	Level string
}

// Defaults, matching the behavior documented in the README.
const (
	DefaultDatabaseURL  = "postgres://quebit:quebit@localhost:5432/quebit?sslmode=disable"
	DefaultRedisAddr    = "localhost:6379"
	DefaultHTTPAddr     = ":8080"
	DefaultProviderURL  = "http://localhost:9090"
	DefaultLogLevel     = "INFO"
	DefaultWorkerConc   = 10
	DefaultRateLimitRPS = 100
)

// Load reads the configuration from the environment, falling back to the
// defaults above for anything unset or invalid. A .env file should be loaded
// by the caller (godotenv) before this is called.
func Load() Config {
	return Config{
		Database: Database{
			URL: envOr("DATABASE_URL", DefaultDatabaseURL),
		},
		Redis: Redis{
			Addr: envOr("REDIS_ADDR", DefaultRedisAddr),
		},
		HTTP: HTTP{
			Addr:            envOr("HTTP_ADDR", DefaultHTTPAddr),
			ReadTimeout:     envDurationOr("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:    envDurationOr("HTTP_WRITE_TIMEOUT", 10*time.Second),
			IdleTimeout:     envDurationOr("HTTP_IDLE_TIMEOUT", 60*time.Second),
			ShutdownTimeout: envDurationOr("HTTP_SHUTDOWN_TIMEOUT", 30*time.Second),
			HealthTimeout:   envDurationOr("HEALTH_TIMEOUT", 3*time.Second),
			RateLimitRPS:    envIntOr("RATE_LIMIT_RPS", DefaultRateLimitRPS),
			TrustedProxies:  os.Getenv("TRUSTED_PROXIES"),
		},
		Provider: Provider{
			URL:     envOr("PROVIDER_URL", DefaultProviderURL),
			APIKey:  os.Getenv("PROVIDER_API_KEY"),
			Timeout: envDurationOr("PROVIDER_TIMEOUT", 10*time.Second),
		},
		Worker: Worker{
			Concurrency:     envIntOr("WORKER_CONCURRENCY", DefaultWorkerConc),
			ShutdownTimeout: envDurationOr("WORKER_SHUTDOWN_TIMEOUT", 30*time.Second),
		},
		Outbox: Outbox{
			RelayInterval: envDurationOr("OUTBOX_RELAY_INTERVAL", 250*time.Millisecond),
			RelayBatch:    envIntOr("OUTBOX_RELAY_BATCH", 100),
			SweepInterval: envDurationOr("OUTBOX_SWEEP_INTERVAL", 30*time.Second),
			SweepStaleAge: envDurationOr("OUTBOX_SWEEP_STALE_AGE", 60*time.Second),
			ReapInterval:  envDurationOr("OUTBOX_REAP_INTERVAL", 30*time.Second),
		},
		Log: Log{
			Level: envOr("LOG_LEVEL", DefaultLogLevel),
		},
	}
}

// envOr returns the value of the environment variable, or fallback when unset
// or empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envIntOr returns the integer value of the environment variable, falling back
// when it is unset, not a number, or not positive.
func envIntOr(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

// envDurationOr returns the time.Duration value of the environment variable
// (parsed with time.ParseDuration, e.g. "250ms", "30s", "10m"), falling back
// when it is unset, unparseable, or not positive.
func envDurationOr(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
