package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/abeer-srivastava/quebit/internal/payment"
	"github.com/abeer-srivastava/quebit/internal/provider"
	"github.com/abeer-srivastava/quebit/internal/queue"
)

// CreateResult is returned by CreatePayment to tell the HTTP handler
// whether the payment was newly created and whether it was enqueued.
type CreateResult struct {
	Payment  *payment.Payment
	Created  bool
	Enqueued bool
}

// Service encapsulates all payment business logic. Both the HTTP handler
// and the queue handler delegate to Service rather than calling store/queue directly.
type Service struct {
	store    payment.Repository
	provider provider.Charger
	log      *slog.Logger
}

// New creates a Service with all dependencies injected via interfaces.
func New(
	store payment.Repository,
	prov provider.Charger,
	log *slog.Logger,
) *Service {
	return &Service{
		store:    store,
		provider: prov,
		log:      log,
	}
}

// CreatePayment validates and persists a new payment.
//
// It does not touch Redis: the INSERT leaves the outbox marker unset, so the
// relay is guaranteed to hand the payment to the queue. There is no window in
// which the database records a payment that nothing will ever process.
//
// If the idempotency key already exists, it returns the existing payment with
// Created=false.
func (s *Service) CreatePayment(ctx context.Context, key string, amount int64, currency string) (*CreateResult, error) {
	p := &payment.Payment{
		ID:             uuid.New(),
		IdempotencyKey: key,
		Amount:         amount,
		Currency:       currency,
		RequestHash:    payment.RequestHash(amount, currency),
	}

	created, isNew, err := s.store.Create(ctx, p)
	if err != nil {
		// Wrapped so the HTTP layer can detect key reuse (409) via errors.Is.
		return nil, fmt.Errorf("create payment: %w", err)
	}

	return &CreateResult{
		Payment: created,
		Created: isNew,
		// A new row is always queued for delivery; the relay owns the enqueue.
		Enqueued: isNew,
	}, nil
}

// GetPayment retrieves a payment by ID.
func (s *Service) GetPayment(ctx context.Context, id uuid.UUID) (*payment.Payment, error) {
	return s.store.Get(ctx, id)
}

// RetryPayment returns a failed_final payment to the queue. The transition back
// to pending is atomic, so concurrent retries cannot both succeed; the same
// statement clears the outbox marker, so the relay re-publishes the payment.
func (s *Service) RetryPayment(ctx context.Context, id uuid.UUID) error {
	// Atomically claim the retry: only one concurrent caller can move
	// failed_final → pending.
	if _, err := s.store.ResetForRetry(ctx, id); err != nil {
		return err // ErrNotFound | ErrNotRetryable | DB error
	}
	return nil
}

// ProcessPayment is called by the queue worker. It claims the payment,
// charges the provider, and updates the status accordingly.
func (s *Service) ProcessPayment(ctx context.Context, id uuid.UUID) error {
	// Claim prevents double-processing on at-least-once delivery
	p, err := s.store.Claim(ctx, id)
	if errors.Is(err, payment.ErrAlreadyDone) {
		s.log.Debug("payment already terminal, skipping", "id", id)
		return nil
	}
	if err != nil {
		return err // DB error — let Asynq retry
	}

	s.log.Debug("charging provider", "id", id, "amount", p.Amount, "attempt", p.Attempts)

	result, err := s.provider.Charge(ctx, provider.ChargeRequest{
		PaymentID: id.String(),
		Amount:    p.Amount,
		Currency:  p.Currency,
	})
	if err == nil {
		s.log.Info("payment succeeded", "id", id, "provider_ref", result.ProviderRef)
		return s.store.MarkSucceeded(ctx, id, result.ProviderRef)
	}

	// Terminal provider error (e.g. 422) — retrying won't help
	var perr *provider.Error
	if errors.As(err, &perr) && !perr.Retryable() {
		s.log.Warn("terminal provider error", "id", id, "err", err)
		if mErr := s.store.MarkFailedFinal(ctx, id, err.Error()); mErr != nil {
			s.log.Error("failed to mark payment failed_final", "id", id, "err", mErr)
		}
		return &queue.TerminalError{Cause: err}
	}

	// Transient error — mark retrying for now, caller (queue handler) decides retry behavior
	if mErr := s.store.MarkRetrying(ctx, id, err.Error()); mErr != nil {
		s.log.Error("failed to mark payment retrying", "id", id, "err", mErr)
	}
	return err
}

// MarkExhausted transitions a payment to failed_final once its retry budget is
// used up. The queue handler calls this on the final failed attempt so the
// payment does not remain stuck in "retrying" after Asynq archives the task.
func (s *Service) MarkExhausted(ctx context.Context, id uuid.UUID, reason string) error {
	s.log.Warn("retry budget exhausted, marking failed_final", "id", id, "reason", reason)
	return s.store.MarkFailedFinal(ctx, id, reason)
}
