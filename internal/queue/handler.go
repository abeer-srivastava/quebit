package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/abeer-srivastava/quebit/internal/payment"
	"github.com/abeer-srivastava/quebit/internal/provider"
)

type Handler struct {
	store    *payment.Store
	provider *provider.Client
	log      *slog.Logger
}

func NewHandler(s *payment.Store, p *provider.Client, l *slog.Logger) *Handler {
	return &Handler{store: s, provider: p, log: l}
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

	// Claim prevents double-processing on at-least-once delivery
	p, err := h.store.Claim(ctx, id)
	if errors.Is(err, payment.ErrAlreadyDone) {
		h.log.Debug("payment already terminal, skipping", "id", id)
		return nil
	}
	if err != nil {
		return err // DB error — let Asynq retry
	}

	h.log.Debug("charging provider", "id", id, "amount", p.Amount, "attempt", p.Attempts)

	err = h.provider.Charge(ctx, provider.ChargeRequest{
		PaymentID: id.String(),
		Amount:    p.Amount,
		Currency:  p.Currency,
	})
	if err == nil {
		h.log.Info("payment succeeded", "id", id)
		return h.store.MarkSucceeded(ctx, id)
	}

	// Terminal provider error (e.g. 422) — retrying won't help
	var perr *provider.Error
	if errors.As(err, &perr) && !perr.Retryable() {
		h.log.Warn("terminal provider error", "id", id, "err", err)
		_ = h.store.MarkFailedFinal(ctx, id, err.Error())
		return fmt.Errorf("%v: %w", err, asynq.SkipRetry)
	}

	// Transient error — mark retrying or exhausted
	retried, _ := asynq.GetRetryCount(ctx)
	maxRetry, _ := asynq.GetMaxRetry(ctx)
	if retried >= maxRetry {
		_ = h.store.MarkFailedFinal(ctx, id, "retries exhausted: "+err.Error())
	} else {
		_ = h.store.MarkRetrying(ctx, id, err.Error())
	}
	return err
}
