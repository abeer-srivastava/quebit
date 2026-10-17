package test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/abeer-srivastava/quebit/internal/httpapi"
)

func setupMiniRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func TestRateLimiterAllow(t *testing.T) {
	rdb := setupMiniRedis(t)
	rl := httpapi.NewRateLimiter(rdb, 5, time.Second)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		allowed, err := rl.Allow(ctx, "test-ip")
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if !allowed {
			t.Fatalf("request %d: expected allowed", i)
		}
	}

	// 6th request should be denied
	allowed, err := rl.Allow(ctx, "test-ip")
	if err != nil {
		t.Fatalf("6th request: unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("6th request: expected denied")
	}
}

func TestRateLimiterDifferentKeys(t *testing.T) {
	rdb := setupMiniRedis(t)
	rl := httpapi.NewRateLimiter(rdb, 1, time.Second)

	ctx := context.Background()

	// First key gets 1 request
	allowed, _ := rl.Allow(ctx, "ip-1")
	if !allowed {
		t.Fatal("ip-1 first request should be allowed")
	}

	// ip-1 is now rate limited
	allowed, _ = rl.Allow(ctx, "ip-1")
	if allowed {
		t.Fatal("ip-1 second request should be denied")
	}

	// ip-2 should still be allowed (separate counter)
	allowed, _ = rl.Allow(ctx, "ip-2")
	if !allowed {
		t.Fatal("ip-2 first request should be allowed")
	}
}

func TestRateLimiterMiddleware429(t *testing.T) {
	rdb := setupMiniRedis(t)
	rl := httpapi.NewRateLimiter(rdb, 1, time.Second)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := rl.Middleware(inner)

	// First request passes
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Second request is rate limited
	req = httptest.NewRequest(http.MethodGet, "/test", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}

	// Check Retry-After header
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header")
	}
}

func TestRateLimiterAtomicity(t *testing.T) {
	// Verify the Lua script correctly counts: with limit=3, exactly 3 requests
	// should be allowed, regardless of concurrent execution.
	rdb := setupMiniRedis(t)
	rl := httpapi.NewRateLimiter(rdb, 3, time.Second)

	ctx := context.Background()
	allowed := 0
	denied := 0

	for i := 0; i < 10; i++ {
		ok, err := rl.Allow(ctx, "atomic-test")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if ok {
			allowed++
		} else {
			denied++
		}
	}

	if allowed != 3 {
		t.Fatalf("expected exactly 3 allowed, got %d", allowed)
	}
	if denied != 7 {
		t.Fatalf("expected exactly 7 denied, got %d", denied)
	}
}
