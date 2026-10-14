package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type HealthChecker struct {
	pool *pgxpool.Pool
	rdb  *redis.Client
}

func NewHealthChecker(pool *pgxpool.Pool, rdb *redis.Client) *HealthChecker {
	return &HealthChecker{pool: pool, rdb: rdb}
}

func (hc *HealthChecker) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", hc.Health)
}

func (hc *HealthChecker) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	pgOK := hc.pool.Ping(ctx) == nil
	redisOK := hc.rdb.Ping(ctx).Err() == nil

	status := http.StatusOK
	if !pgOK || !redisOK {
		status = http.StatusServiceUnavailable
	}

	writeJSON(w, status, map[string]any{
		"postgres": pgOK,
		"redis":    redisOK,
	})
}
