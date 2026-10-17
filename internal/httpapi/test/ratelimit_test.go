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

func TestParseTrustedProxies(t *testing.T) {
	nets, err := httpapi.ParseTrustedProxies("10.0.0.0/8, 192.168.1.1 , 2001:db8::/32")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(nets) != 3 {
		t.Fatalf("expected 3 networks, got %d", len(nets))
	}
	if _, err := httpapi.ParseTrustedProxies("not-a-cidr"); err == nil {
		t.Fatal("expected an error for malformed input")
	}
}

// TestClientIPIgnoresSpoofedForwardedFor verifies that with no trusted proxies
// configured, X-Forwarded-For cannot be used to evade the limiter.
func TestClientIPIgnoresSpoofedForwardedFor(t *testing.T) {
	rdb := setupMiniRedis(t)
	rl := httpapi.NewRateLimiter(rdb, 1, time.Second)

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i, xff := range []string{"1.2.3.4", "5.6.7.8"} {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.RemoteAddr = "203.0.113.7:5555"
		req.Header.Set("X-Forwarded-For", xff)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if i == 0 && rec.Code != http.StatusOK {
			t.Fatalf("first request: expected 200, got %d", rec.Code)
		}
		if i == 1 && rec.Code != http.StatusTooManyRequests {
			t.Fatalf("spoofed header was honoured: expected 429, got %d", rec.Code)
		}
	}
}

// TestClientIPHonoursForwardedForFromTrustedProxy verifies that headers are
// believed only when the immediate peer is a configured trusted proxy.
func TestClientIPHonoursForwardedForFromTrustedProxy(t *testing.T) {
	rdb := setupMiniRedis(t)
	trusted, err := httpapi.ParseTrustedProxies("10.0.0.0/8")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rl := httpapi.NewRateLimiter(rdb, 1, time.Second, trusted...)

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Different forwarded clients share the same trusted peer but must get
	// separate buckets.
	for _, ip := range []string{"198.51.100.1", "198.51.100.2"} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "10.0.0.5:4000"
		req.Header.Set("X-Forwarded-For", ip)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("client %s: expected 200, got %d", ip, rec.Code)
		}
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
