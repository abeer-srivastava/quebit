package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

// Handler is a thin adapter that parses Asynq tasks and delegates to a Processor.
type Handler struct {
	proc Processor
	log  *slog.Logger
}

func NewHandler(proc Processor, l *slog.Logger) *Handler {
	return &Handler{proc: proc, log: l}
}

func (h *Handler) Register(mux *asynq.ServeMux) {
	mux.HandleFunc(TypeProcessPayment, h.ProcessPayment)
}

func (h *Handler) ProcessPayment(ctx context.Context, t *asynq.Task) error {
	var pl ProcessPayload
	if err := json.Unmarshal(t.Payload(), &pl); err != nil {
		return fmt.Errorf("bad payload: %v: %w", err, asynq.SkipRetry)
	}

	id, err := uuid.Parse(pl.PaymentID)
	if err != nil {
		return fmt.Errorf("bad id: %v: %w", err, asynq.SkipRetry)
	}

	err = h.proc.ProcessPayment(ctx, id)
	if err == nil {
		return nil
	}

	// Terminal errors should not be retried by Asynq.
	var termErr *TerminalError
	if errors.As(err, &termErr) {
		return fmt.Errorf("%v: %w", err, asynq.SkipRetry)
	}

	// If the worker is shutting down the context is cancelled; don't treat this
	// as a permanent failure — let Asynq requeue/retry the task after restart.
	if ctx.Err() != nil {
		return err
	}

	// Transient error. Once the task has used up its retry budget, mark the
	// payment failed_final and tell Asynq to archive the task instead of
	// scheduling another retry. Without this, a payment whose retries are
	// exhausted would sit in "retrying" forever.
	if retried, maxRetry, ok := retryMetadata(ctx); ok && RetryExhausted(retried, maxRetry) {
		h.log.Warn("retries exhausted, marking payment failed_final",
			"id", id, "retried", retried, "max_retry", maxRetry, "err", err)
		if mErr := h.proc.MarkExhausted(ctx, id, err.Error()); mErr != nil {
			h.log.Error("failed to mark payment failed_final after exhausted retries", "id", id, "err", mErr)
		}
		return fmt.Errorf("retries exhausted: %v: %w", err, asynq.SkipRetry)
	}

	// Transient error with retries remaining — let Asynq schedule another attempt.
	return err
}

// retryMetadata returns Asynq's retry bookkeeping from the task context. ok is
// true only when both values are present (i.e. running under a real worker).
func retryMetadata(ctx context.Context) (retried, maxRetry int, ok bool) {
	retried, okRetried := asynq.GetRetryCount(ctx)
	maxRetry, okMaxRetry := asynq.GetMaxRetry(ctx)
	return retried, maxRetry, okRetried && okMaxRetry
}

// RetryExhausted reports whether retried has reached maxRetry, meaning the task
// is on its final attempt and should be treated as permanently failed.
func RetryExhausted(retried, maxRetry int) bool {
	return retried >= maxRetry
}
