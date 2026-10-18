package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/abeer-srivastava/quebit/internal/config"
	"github.com/abeer-srivastava/quebit/internal/httpapi"
	"github.com/abeer-srivastava/quebit/internal/payment"
	"github.com/abeer-srivastava/quebit/internal/provider"
	"github.com/abeer-srivastava/quebit/internal/queue"
	"github.com/abeer-srivastava/quebit/internal/service"
)

func main() {
	// Load .env file if present (ignore error if not found)
	_ = godotenv.Load()

	// All runtime settings live in internal/config.
	cfg := config.Load()
	log := cfg.NewLogger()

	log.Info("starting quebit", "http_addr", cfg.HTTP.Addr, "provider_url", cfg.Provider.URL)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Postgres
	pool, err := pgxpool.New(ctx, cfg.Database.URL)
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
	redisOpt := asynq.RedisClientOpt{Addr: cfg.Redis.Addr}
	rdb := redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis ping failed", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()
	log.Info("connected to redis")

	// Dependencies
	store := payment.NewStore(pool)

	providerCli := provider.NewClient(cfg.Provider.URL, provider.WithTimeout(cfg.Provider.Timeout))
	providerCli.APIKey = cfg.Provider.APIKey

	// The queue client is only used by the outbox relay; the HTTP path never
	// enqueues directly.
	queueCli := queue.NewClient(redisOpt)
	defer queueCli.Close()

	// Service layer — single point of business logic
	svc := service.New(store, providerCli, log)

	// Asynq worker
	asynqServer := queue.NewServer(redisOpt, cfg.Worker.Concurrency, cfg.Worker.ShutdownTimeout)
	mux := asynq.NewServeMux()
	queueHandler := queue.NewHandler(svc, log)
	queueHandler.Register(mux)

	// Outbox relay — publishes persisted payments to the queue
	relay := queue.NewRelay(store, queueCli, log,
		queue.WithRelayInterval(cfg.Outbox.RelayInterval),
		queue.WithRelayBatch(cfg.Outbox.RelayBatch),
	)

	// Stale pending sweeper — re-queues payments whose task was lost
	sweeper := queue.NewSweeper(store, log,
		queue.WithSweepInterval(cfg.Outbox.SweepInterval),
		queue.WithSweepStaleAge(cfg.Outbox.SweepStaleAge),
	)

	// Expired processing-lease reaper
	reaper := queue.NewReaper(store, log,
		queue.WithReapInterval(cfg.Outbox.ReapInterval),
	)

	// HTTP server
	httpMux := http.NewServeMux()
	httpHandler := httpapi.NewHandler(svc, log)
	httpHandler.RegisterRoutes(httpMux)

	healthChecker := httpapi.NewHealthChecker(pool, rdb, cfg.HTTP.HealthTimeout)
	healthChecker.RegisterRoutes(httpMux)

	trustedProxies, err := httpapi.ParseTrustedProxies(cfg.HTTP.TrustedProxies)
	if err != nil {
		log.Error("invalid TRUSTED_PROXIES", "err", err)
		os.Exit(1)
	}
	if len(trustedProxies) > 0 {
		log.Info("trusting proxy headers from configured networks", "networks", cfg.HTTP.TrustedProxies)
	}

	rateLimiter := httpapi.NewRateLimiter(rdb, cfg.HTTP.RateLimitRPS, time.Second, trustedProxies...)

	var handler http.Handler = httpMux
	handler = rateLimiter.Middleware(handler)
	handler = httpapi.RecoverMiddleware(log, handler)
	handler = httpapi.LoggerMiddleware(log, handler)

	httpServer := &http.Server{
		Addr:         cfg.HTTP.Addr,
		Handler:      handler,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
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
		log.Info("http server starting", "addr", cfg.HTTP.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		return relay.Run(gctx)
	})

	g.Go(func() error {
		return sweeper.Run(gctx)
	})

	g.Go(func() error {
		return reaper.Run(gctx)
	})

	// Graceful shutdown goroutine
	g.Go(func() error {
		<-gctx.Done()
		log.Info("shutting down...")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
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
