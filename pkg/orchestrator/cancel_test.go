package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/durable-task-orchestrator-go/pkg/store"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/task"
)

func TestCancelPropagatesAndLeavesNoAmbiguousState(t *testing.T) {
	lg := &sliceLogger{}
	var observed int32
	tasks := []task.Task{
		{
			ID: "long",
			Run: func(ctx context.Context) error {
				<-ctx.Done() // only returns when cancellation is propagated
				atomic.StoreInt32(&observed, 1)
				return ctx.Err()
			},
		},
		{
			ID:   "waiting",
			Deps: []string{"long"},
			Run:  func(ctx context.Context) error { return nil },
		},
	}
	o, err := New(Config{
		BatchID: "cancel-batch",
		Store:   store.NewMemoryStore(),
		Logger:  lg,
		Tasks:   tasks,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	snap, runErr := o.Run(ctx)
	if runErr != context.Canceled {
		t.Fatalf("want context.Canceled from Run, got %v", runErr)
	}
	if atomic.LoadInt32(&observed) != 1 {
		t.Fatal("cancellation never reached the running task body")
	}
	// No ambiguous intermediate state may survive cancellation.
	for _, st := range snap.States {
		if st.Status == task.StatusRunning {
			t.Fatalf("task %s left in ambiguous running state", st.ID)
		}
	}
	if snap.States["long"].Status != task.StatusPending {
		t.Fatalf("interrupted task must be resettable to pending, got %s", snap.States["long"].Status)
	}
}

func TestSchedulingLogsContainInputsAndDecisions(t *testing.T) {
	lg := &sliceLogger{}
	tasks := []task.Task{
		{ID: "a", Run: func(ctx context.Context) error { return nil }},
		{ID: "b", Deps: []string{"a"}, Run: func(ctx context.Context) error { return errBoom }},
	}
	o, err := New(Config{
		BatchID: "log-batch",
		Store:   store.NewMemoryStore(),
		Logger:  lg,
		Tasks:   tasks,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	required := []string{
		"info batch_start",
		"info task_scheduled",
		"task_id=a",
		"task_id=b",
		"deps=[a]",
		"decision=deps_succeeded_and_budget_available",
		"error task_failed_terminal",
		"decision=retry_budget_exhausted",
		"info batch_summary",
	}
	for _, sub := range required {
		if !lg.contains(sub) {
			t.Fatalf("scheduling log missing %q\n--- logs ---\n%v", sub, lg.Lines())
		}
	}
}

func TestConcurrentRaceUnderLoad(t *testing.T) {
	const n = 50
	var executed int64
	tasks := make([]task.Task, n)
	for i := range tasks {
		id := rune('a' + i%26)
		tasks[i] = task.Task{
			ID: string(id) + "-" + itoa(i),
			Run: func(ctx context.Context) error {
				atomic.AddInt64(&executed, 1)
				return nil
			},
		}
	}
	o, err := New(Config{
		BatchID:     "load",
		Concurrency: 8,
		Store:       store.NewMemoryStore(),
		Logger:      &sliceLogger{},
		Tasks:       tasks,
	})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := o.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if executed != n {
		t.Fatalf("executed %d tasks, want %d", executed, n)
	}
	for _, st := range snap.States {
		if st.Status != task.StatusSucceeded {
			t.Fatalf("task %s not succeeded", st.ID)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
