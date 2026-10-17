package test

import (
	"testing"

	"github.com/abeer-srivastava/quebit/internal/queue"
)

func TestQueueFor(t *testing.T) {
	tests := []struct {
		amount int64
		want   string
	}{
		{500, queue.QueueLow},
		{999, queue.QueueLow},
		{1000, queue.QueueDefault},
		{5000, queue.QueueDefault},
		{9999, queue.QueueDefault},
		{10_000, queue.QueueCritical},
		{100_000, queue.QueueCritical},
	}

	for _, tt := range tests {
		got := queue.QueueFor(tt.amount)
		if got != tt.want {
			t.Errorf("QueueFor(%d) = %q, want %q", tt.amount, got, tt.want)
		}
	}
}
