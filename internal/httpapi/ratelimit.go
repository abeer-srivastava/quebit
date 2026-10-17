package httpapi

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ParseTrustedProxies parses a comma-separated list of CIDRs (bare IPs are
// accepted and treated as /32 or /128). It returns an error on malformed input
// so the server can fail fast on a bad configuration.
func ParseTrustedProxies(raw string) ([]*net.IPNet, error) {
	var nets []*net.IPNet
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "/") {
			ip := net.ParseIP(part)
			if ip == nil {
				return nil, fmt.Errorf("invalid trusted proxy %q", part)
			}
			bits := 128
			if ip.To4() != nil {
				bits = 32
			}
			part = fmt.Sprintf("%s/%d", part, bits)
		}
		_, n, err := net.ParseCIDR(part)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy %q: %w", part, err)
		}
		nets = append(nets, n)
	}
	return nets, nil
}

// clientIP returns the best-effort client IP used as the rate-limit key.
//
// Proxy headers (X-Forwarded-For / X-Real-IP) are only honoured when the
// immediate peer (RemoteAddr) is inside the configured trusted set. Without
// that check any client could spoof the header and bypass the limiter entirely.
func clientIP(r *http.Request, trusted []*net.IPNet) string {
	remote := hostOnly(r.RemoteAddr)
	if remote != "" && ipInAny(net.ParseIP(remote), trusted) {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if i := strings.IndexByte(xff, ','); i >= 0 {
				xff = xff[:i]
			}
			if ip := strings.TrimSpace(xff); ip != "" {
				return ip
			}
		}
		if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); xri != "" {
			return xri
		}
	}
	if remote != "" {
		return remote
	}
	return r.RemoteAddr
}

// hostOnly strips the port from an "ip:port" RemoteAddr.
func hostOnly(remoteAddr string) string {
	if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return h
	}
	return remoteAddr
}

func ipInAny(ip net.IP, nets []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// luaRateLimit is an atomic sliding-window rate limiter implemented as a Lua script.
// It runs entirely within Redis, eliminating the race condition of a pipeline approach
// where ZREMRANGEBYSCORE + ZCARD + ZADD execute as separate commands.
//
// The script atomically:
// 1. Removes entries outside the time window
// 2. Counts remaining entries
// 3. If under limit, adds the new request
// 4. Sets an expiry so keys don't leak
// Returns 1 if allowed, 0 if rate limited.
var luaRateLimit = redis.NewScript(`
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ns = tonumber(ARGV[2])
local now_ns = tonumber(ARGV[3])
local window_start = now_ns - window_ns

-- Remove entries outside the window
redis.call('ZREMRANGEBYSCORE', key, '0', tostring(window_start))

-- Count entries in current window
local count = redis.call('ZCARD', key)

if count < limit then
    -- Under limit: add the request and set expiry
    redis.call('ZADD', key, now_ns, tostring(now_ns))
    redis.call('PEXPIRE', key, math.ceil(window_ns / 1000000) + 1000)
    return 1
else
    -- Over limit: set expiry but don't add
    redis.call('PEXPIRE', key, math.ceil(window_ns / 1000000) + 1000)
    return 0
end
`)

type RateLimiter struct {
	rdb     *redis.Client
	limit   int
	window  time.Duration
	trusted []*net.IPNet
}

// NewRateLimiter builds a limiter. trusted lists the proxy networks whose
// forwarding headers may be believed; when empty, only RemoteAddr is used.
func NewRateLimiter(rdb *redis.Client, limit int, window time.Duration, trusted ...*net.IPNet) *RateLimiter {
	return &RateLimiter{rdb: rdb, limit: limit, window: window, trusted: trusted}
}

// Allow checks if the given key (e.g. client IP) is within the rate limit
// using an atomic Redis Lua script with a sliding window.
func (rl *RateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	now := time.Now()
	redisKey := fmt.Sprintf("ratelimit:%s", key)

	result, err := luaRateLimit.Run(ctx, rl.rdb, []string{redisKey},
		rl.limit,
		rl.window.Nanoseconds(),
		now.UnixNano(),
	).Int()
	if err != nil {
		return false, err
	}

	return result == 1, nil
}

// Middleware returns an HTTP middleware that rate limits by client IP.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := clientIP(r, rl.trusted)

		allowed, err := rl.Allow(r.Context(), key)
		if err != nil {
			// If Redis is down, let the request through rather than blocking everything
			next.ServeHTTP(w, r)
			return
		}

		if !allowed {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", int(rl.window.Seconds())))
			writeError(w, http.StatusTooManyRequests,
				&APIError{Code: "rate_limited", Message: "rate limit exceeded"})
			return
		}

		next.ServeHTTP(w, r)
	})
}
