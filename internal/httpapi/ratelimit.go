package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

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
	rdb    *redis.Client
	limit  int
	window time.Duration
}

func NewRateLimiter(rdb *redis.Client, limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{rdb: rdb, limit: limit, window: window}
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
		key := r.RemoteAddr

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
