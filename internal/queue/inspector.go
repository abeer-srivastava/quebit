package queue

import "github.com/hibiken/asynq"

// Inspector wraps asynq.Inspector to replay archived (failed) tasks.
type Inspector struct {
	i *asynq.Inspector
}

func NewInspector(opt asynq.RedisClientOpt) *Inspector {
	return &Inspector{i: asynq.NewInspector(opt)}
}

func (insp *Inspector) Close() error {
	return insp.i.Close()
}

// ReplayArchivedTask searches all three queues for the archived task
// and runs it. Asynq archives tasks that hit SkipRetry or exhaust retries.
func (insp *Inspector) ReplayArchivedTask(taskID string) error {
	queues := []string{QueueCritical, QueueDefault, QueueLow}
	for _, q := range queues {
		err := insp.i.RunTask(q, taskID)
		if err == nil {
			return nil
		}
		if err == asynq.ErrTaskNotFound {
			continue
		}
		return err
	}
	return asynq.ErrTaskNotFound
}
