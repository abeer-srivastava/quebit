package test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/abeer-srivastava/quebit/internal/queue"
)

// fakeProcessor is a test double for queue.Processor.
type fakeProcessor struct {
	processErr error
	markErr    error
	marked     []string
}

func (f *fakeProcessor) ProcessPayment(ctx context.Context, id uuid.UUID) error {
	return f.processErr
}

func (f *fakeProcessor) MarkExhausted(ctx context.Context, id uuid.UUID, reason string) error {
	f.marked = append(f.marked, reason)
	return f.markErr
}

func newTestHandler(p queue.Processor) *queue.Handler {
	return queue.NewHandler(p, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newTask(payload string) *asynq.Task {
	return asynq.NewTask(queue.TypeProcessPayment, []byte(payload))
}

func TestHandlerBadPayload(t *testing.T) {
	h := newTestHandler(&fakeProcessor{})
	err := h.ProcessPayment(context.Background(), newTask("{not json"))
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("expected SkipRetry, got %v", err)
	}
}

func TestHandlerBadID(t *testing.T) {
	h := newTestHandler(&fakeProcessor{})
	err := h.ProcessPayment(context.Background(), newTask(`{"payment_id":"not-a-uuid"}`))
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("expected SkipRetry, got %v", err)
	}
}

func TestHandlerSuccess(t *testing.T) {
	h := newTestHandler(&fakeProcessor{})
	if err := h.ProcessPayment(context.Background(), newTask(`{"payment_id":"`+uuid.New().String()+`"}`)); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestHandlerTerminalErrorSkipsRetry(t *testing.T) {
	fp := &fakeProcessor{processErr: &queue.TerminalError{Cause: errors.New("422 unprocessable")}}
	h := newTestHandler(fp)

	err := h.ProcessPayment(context.Background(), newTask(`{"payment_id":"`+uuid.New().String()+`"}`))
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("expected SkipRetry, got %v", err)
	}
	if len(fp.marked) != 0 {
		t.Fatalf("expected no MarkExhausted call, got %v", fp.marked)
	}
}

func TestHandlerTransientErrorWithoutRetryMetadata(t *testing.T) {
	// A bare context carries no Asynq retry metadata, so the handler must
	// defer to Asynq and return the raw error without marking failed_final.
	fp := &fakeProcessor{processErr: errors.New("503 service unavailable")}
	h := newTestHandler(fp)

	err := h.ProcessPayment(context.Background(), newTask(`{"payment_id":"`+uuid.New().String()+`"}`))
	if err == nil || errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("expected plain transient error, got %v", err)
	}
	if len(fp.marked) != 0 {
		t.Fatalf("expected no MarkExhausted call, got %v", fp.marked)
	}
}

func TestRetryExhausted(t *testing.T) {
	tests := []struct {
		retried  int
		maxRetry int
		want     bool
	}{
		{0, 0, true},  // no retries allowed → first failure is final
		{0, 8, false}, // first attempt, retries remain
		{7, 8, false}, // one retry left
		{8, 8, true},  // final attempt
		{9, 8, true},  // defensive: past the budget
	}

	for _, tt := range tests {
		if got := queue.RetryExhausted(tt.retried, tt.maxRetry); got != tt.want {
			t.Errorf("RetryExhausted(%d, %d) = %v, want %v", tt.retried, tt.maxRetry, got, tt.want)
		}
	}
}
