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
	queue    queue.Enqueuer
	replayer queue.TaskReplayer
	provider provider.Charger
	log      *slog.Logger
}

// New creates a Service with all dependencies injected via interfaces.
func New(
	store payment.Repository,
	q queue.Enqueuer,
	replayer queue.TaskReplayer,
	prov provider.Charger,
	log *slog.Logger,
) *Service {
	return &Service{
		store:    store,
		queue:    q,
		replayer: replayer,
		provider: prov,
		log:      log,
	}
}

// CreatePayment validates, persists, and enqueues a new payment.
// If the idempotency key already exists, it returns the existing payment
// with Created=false. Enqueue failures are logged but not fatal — the
// sweeper will pick up orphaned pending payments.
func (s *Service) CreatePayment(ctx context.Context, key string, amount int64, currency string) (*CreateResult, error) {
	p := &payment.Payment{
		ID:             uuid.New(),
		IdempotencyKey: key,
		Amount:         amount,
		Currency:       currency,
	}

	created, isNew, err := s.store.Create(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("create payment: %w", err)
	}

	result := &CreateResult{
		Payment:  created,
		Created:  isNew,
		Enqueued: false,
	}

	if isNew {
		if err := s.queue.EnqueueProcess(ctx, created.ID, created.Amount); err != nil {
			s.log.Error("failed to enqueue payment", "id", created.ID, "err", err)
			// Payment exists in DB as pending — sweeper can pick it up
		} else {
			result.Enqueued = true
		}
	}

	return result, nil
}

// GetPayment retrieves a payment by ID.
func (s *Service) GetPayment(ctx context.Context, id uuid.UUID) (*payment.Payment, error) {
	return s.store.Get(ctx, id)
}

// RetryPayment replays a failed_final payment. It first tries to replay
// the archived Asynq task; if that fails, it resets the status and enqueues fresh.
func (s *Service) RetryPayment(ctx context.Context, id uuid.UUID) error {
	p, err := s.store.Get(ctx, id)
	if err != nil {
		return err
	}

	if p.Status != payment.StatusFailedFinal {
		return ErrNotRetryable
	}

	// Try to replay the archived task via Asynq Inspector
	if err := s.replayer.ReplayArchivedTask(id.String()); err != nil {
		s.log.Warn("inspector replay failed, re-enqueuing fresh task", "id", id, "err", err)
		// Fallback: reset state and enqueue a fresh task
		if err := s.store.ResetForRetry(ctx, id); err != nil {
			return fmt.Errorf("reset for retry: %w", err)
		}
		if err := s.queue.EnqueueProcess(ctx, id, p.Amount); err != nil {
			return fmt.Errorf("re-enqueue: %w", err)
		}
	} else {
		if err := s.store.ResetForRetry(ctx, id); err != nil {
			s.log.Error("failed to reset payment after replay", "id", id, "err", err)
		}
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
		_ = s.store.MarkFailedFinal(ctx, id, err.Error())
		return &queue.TerminalError{Cause: err}
	}

	// Transient error — mark retrying for now, caller (queue handler) decides retry behavior
	_ = s.store.MarkRetrying(ctx, id, err.Error())
	return err
}

// ErrNotRetryable is returned when trying to retry a payment that isn't in failed_final state.
var ErrNotRetryable = errors.New("only failed_final payments can be retried")
