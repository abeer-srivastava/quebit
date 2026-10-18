package queue

import (
	"math/rand/v2"
	"time"

	"github.com/hibiken/asynq"
)

// NewServer builds the Asynq worker server. shutdownTimeout bounds how long
// it waits for in-flight tasks when SIGINT/SIGTERM arrives.
func NewServer(opt asynq.RedisClientOpt, concurrency int, shutdownTimeout time.Duration) *asynq.Server {
	return asynq.NewServer(opt, asynq.Config{
		Concurrency: concurrency,
		// Weights control how often each queue is polled.
		// Critical gets 6x the attention of low — but low never starves.
		Queues: map[string]int{
			QueueCritical: 6,
			QueueDefault:  3,
			QueueLow:      1,
		},
		RetryDelayFunc:  exponentialBackoff,
		ShutdownTimeout: shutdownTimeout,
	})
}

// exponentialBackoff: 1s, 2s, 4s … capped at 10 min, plus up to 20% jitter.
func exponentialBackoff(n int, _ error, _ *asynq.Task) time.Duration {
	if n > 10 {
		n = 10
	}
	d := time.Second << n
	if d > 10*time.Minute {
		d = 10 * time.Minute
	}
	jitter := time.Duration(rand.Int64N(int64(d)/5 + 1))
	return d + jitter
}
