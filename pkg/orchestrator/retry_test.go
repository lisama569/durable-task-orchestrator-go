package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/durable-task-orchestrator-go/pkg/retry"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/store"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/task"
)

func TestRetryExhaustedDistinguishesReason(t *testing.T) {
	c := newCounter()
	tasks := []task.Task{
		{
			ID: "always-fails",
			Run: func(ctx context.Context) error {
				c.inc("always-fails")
				return errBoom
			},
		},
	}
	cfg := Config{
		BatchID:     "retry-exhaust",
		Concurrency: 1,
		Retry:       retry.FixedPolicy{Attempts: 3, Delay: time.Millisecond},
		Store:       store.NewMemoryStore(),
		Logger:      &sliceLogger{},
		Tasks:       tasks,
	}
	o, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	snap, runErr := o.Run(context.Background())
	// Some tasks failing does not make Run return an error; the caller reads
	// terminal states from the snapshot.
	if runErr != nil {
		t.Fatalf("task failure is reported via snapshot, got run error: %v", runErr)
	}
	st := snap.States["always-fails"]
	if st.Status != task.StatusFailed {
		t.Fatalf("want failed, got %s", st.Status)
	}
	if st.FailureReason != task.ReasonRetryExhausted {
		t.Fatalf("want %q, got %q", task.ReasonRetryExhausted, st.FailureReason)
	}
	if st.Attempt != 3 {
		t.Fatalf("want exactly 3 attempts, got %d", st.Attempt)
	}
	if got := c.get("always-fails"); got != 3 {
		t.Fatalf("function invoked %d times, want 3", got)
	}
	if st.LastError != errBoom.Error() {
		t.Fatalf("last error not recorded: %q", st.LastError)
	}
}

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	c := newCounter()
	tasks := []task.Task{
		{
			ID: "flaky",
			Run: func(ctx context.Context) error {
				if c.inc("flaky") < 3 {
					return errBoom
				}
				return nil
			},
		},
	}
	cfg := Config{
		BatchID: "retry-ok",
		Retry:   retry.FixedPolicy{Attempts: 5},
		Store:   store.NewMemoryStore(),
		Tasks:   tasks,
	}
	o, _ := New(cfg)
	snap, err := o.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	st := snap.States["flaky"]
	if st.Status != task.StatusSucceeded || st.Attempt != 3 {
		t.Fatalf("want success on attempt 3, got %+v", st)
	}
	if st.FailureReason != "" {
		t.Fatalf("succeeded task must carry no failure reason, got %q", st.FailureReason)
	}
}

func TestTimeoutFailureReason(t *testing.T) {
	tasks := []task.Task{
		{
			ID:      "slow",
			Timeout: 15 * time.Millisecond,
			Run: func(ctx context.Context) error {
				select {
				case <-time.After(2 * time.Second):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
	}
	cfg := Config{
		BatchID: "timeout",
		Retry:   retry.FixedPolicy{Attempts: 2},
		Store:   store.NewMemoryStore(),
		Logger:  &sliceLogger{},
		Tasks:   tasks,
	}
	o, _ := New(cfg)
	start := time.Now()
	snap, _ := o.Run(context.Background())
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("timeout did not cancel the running task: %v", d)
	}
	st := snap.States["slow"]
	if st.Status != task.StatusFailed {
		t.Fatalf("want failed, got %s", st.Status)
	}
	if st.FailureReason != task.ReasonTimeout {
		t.Fatalf("want timeout reason, got %q (last err: %s)", st.FailureReason, st.LastError)
	}
	if st.Attempt != 2 {
		t.Fatalf("want 2 timeout attempts, got %d", st.Attempt)
	}
}

func TestFailureReasonsAreDistinct(t *testing.T) {
	if task.ReasonTimeout == task.ReasonRetryExhausted ||
		task.ReasonTimeout == task.ReasonDependencyFailed ||
		task.ReasonRetryExhausted == task.ReasonDependencyFailed ||
		task.ReasonCanceled == task.ReasonTimeout {
		t.Fatal("failure reason constants must be distinct")
	}
}
