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

	// Terminal errors should not be retried by Asynq
	var termErr *TerminalError
	if errors.As(err, &termErr) {
		return fmt.Errorf("%v: %w", err, asynq.SkipRetry)
	}

	// Transient errors — let Asynq handle retry/exhaustion
	retried, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)
	if retried >= maxRetry {
		h.log.Warn("retries exhausted", "id", id, "err", err)
	}
	return err
}
