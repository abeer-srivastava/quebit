package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/abeer-srivastava/quebit/internal/httpapi"
	"github.com/abeer-srivastava/quebit/internal/payment"
	"github.com/abeer-srivastava/quebit/internal/provider"
	"github.com/abeer-srivastava/quebit/internal/queue"
	"github.com/abeer-srivastava/quebit/internal/service"
)

func main() {
	// Load .env file if present (ignore error if not found)
	_ = godotenv.Load()

	log := setupLogger()

	cfg := loadConfig()
	log.Info("starting quebit", "http_addr", cfg.HTTPAddr, "provider_url", cfg.ProviderURL)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Postgres
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Error("postgres ping failed", "err", err)
		os.Exit(1)
	}
	log.Info("connected to postgres")

	// Redis
	redisOpt := asynq.RedisClientOpt{Addr: cfg.RedisAddr}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis ping failed", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()
	log.Info("connected to redis")

	// Dependencies
	store := payment.NewStore(pool)
	providerCli := provider.NewClient(cfg.ProviderURL)
	providerCli.APIKey = cfg.ProviderAPIKey
	queueCli := queue.NewClient(redisOpt)
	defer queueCli.Close()
	inspector := queue.NewInspector(redisOpt)
	defer inspector.Close()

	// Service layer — single point of business logic
	svc := service.New(store, queueCli, inspector, providerCli, log)

	// Asynq worker
	asynqServer := queue.NewServer(redisOpt, cfg.WorkerConcurrency)
	mux := asynq.NewServeMux()
	queueHandler := queue.NewHandler(svc, log)
	queueHandler.Register(mux)

	// Stale pending sweeper
	sweeper := queue.NewSweeper(store, queueCli, log)

	// HTTP server
	httpMux := http.NewServeMux()
	httpHandler := httpapi.NewHandler(svc, log)
	httpHandler.RegisterRoutes(httpMux)

	healthChecker := httpapi.NewHealthChecker(pool, rdb)
	healthChecker.RegisterRoutes(httpMux)

	rateLimiter := httpapi.NewRateLimiter(rdb, cfg.RateLimitRPS, time.Second)

	var handler http.Handler = httpMux
	handler = rateLimiter.Middleware(handler)
	handler = httpapi.RecoverMiddleware(log, handler)
	handler = httpapi.LoggerMiddleware(log, handler)

	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Run everything with errgroup
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		log.Info("asynq worker starting")
		if err := asynqServer.Run(mux); err != nil {
			return fmt.Errorf("asynq server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		log.Info("http server starting", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		return sweeper.Run(gctx)
	})

	// Graceful shutdown goroutine
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down...")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		log.Info("http server stopped")

		asynqServer.Shutdown()
		log.Info("asynq worker stopped")
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Error("server exited with error", "err", err)
		os.Exit(1)
	}
	log.Info("quebit stopped cleanly")
}

type config struct {
	DatabaseURL       string
	RedisAddr         string
	HTTPAddr          string
	ProviderURL       string
	ProviderAPIKey    string
	WorkerConcurrency int
	RateLimitRPS      int
}

func loadConfig() config {
	return config{
		DatabaseURL:       envOr("DATABASE_URL", "postgres://quebit:quebit@localhost:5432/quebit?sslmode=disable"),
		RedisAddr:         envOr("REDIS_ADDR", "localhost:6379"),
		HTTPAddr:          envOr("HTTP_ADDR", ":8080"),
		ProviderURL:       envOr("PROVIDER_URL", "http://localhost:9090"),
		ProviderAPIKey:    envOr("PROVIDER_API_KEY", ""),
		WorkerConcurrency: envOrInt("WORKER_CONCURRENCY", 10),
		RateLimitRPS:      envOrInt("RATE_LIMIT_RPS", 100),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envOrInt returns the integer value of the environment variable, falling back
// to fallback when it is unset, not a number, or not positive.
func envOrInt(key string, fallback int) int {
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

func setupLogger() *slog.Logger {
	var level slog.LevelVar
	level.Set(slog.LevelInfo)

	raw := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	switch raw {
	case "DEBUG":
		level.Set(slog.LevelDebug)
	case "WARN":
		level.Set(slog.LevelWarn)
	case "ERROR":
		level.Set(slog.LevelError)
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: &level}))
}
