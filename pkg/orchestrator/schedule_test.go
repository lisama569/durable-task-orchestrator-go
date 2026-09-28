package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/durable-task-orchestrator-go/pkg/retry"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/store"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/task"
)

func TestDAGValidationRejectedBeforeRun(t *testing.T) {
	cases := []struct {
		name  string
		tasks []task.Task
		want  error
	}{
		{
			name:  "missing dependency",
			tasks: []task.Task{{ID: "a", Deps: []string{"ghost"}, Run: nil}},
			want:  nil, // checked via string below together with dag errors
		},
		{
			name: "cycle",
			tasks: []task.Task{
				{ID: "a", Deps: []string{"b"}, Run: nil},
				{ID: "b", Deps: []string{"a"}, Run: nil},
			},
		},
		{
			name:  "duplicate ids",
			tasks: []task.Task{{ID: "a", Run: nil}, {ID: "a", Run: nil}},
		},
		{
			name:  "empty id",
			tasks: []task.Task{{ID: "", Run: nil}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(Config{BatchID: "b-" + tc.name, Tasks: tc.tasks, Store: store.NewMemoryStore()})
			if err == nil {
				t.Fatal("invalid graph must be rejected at construction")
			}
		})
	}
}

func TestDependencyOrdering(t *testing.T) {
	var mu sync.Mutex
	order := []string{}
	record := func(id string, delay time.Duration) task.Task {
		return task.Task{
			ID: id,
			Run: func(ctx context.Context) error {
				time.Sleep(delay)
				mu.Lock()
				order = append(order, id)
				mu.Unlock()
				return nil
			},
		}
	}
	t1 := record("a", 0)
	t2 := record("b", 5*time.Millisecond)
	t3 := record("c", 0)
	t3.Deps = []string{"a", "b"}
	t4 := record("d", 0)
	t4.Deps = []string{"c"}

	cfg := Config{
		BatchID:     "order",
		Concurrency: 4,
		Retry:       retry.NoRetry,
		Store:       store.NewMemoryStore(),
		Logger:      &sliceLogger{},
		Tasks:       []task.Task{t1, t2, t3, t4},
	}
	o, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := o.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pos := map[string]int{}
	for i, id := range order {
		pos[id] = i
	}
	if !(pos["a"] < pos["c"] && pos["b"] < pos["c"] && pos["c"] < pos["d"]) {
		t.Fatalf("dependency order violated: %v", order)
	}
	for _, st := range snap.States {
		if st.Status != task.StatusSucceeded {
			t.Fatalf("task %s not succeeded: %+v", st.ID, st)
		}
	}
}

func TestConcurrencyCapQueuesTasks(t *testing.T) {
	const capN = 2
	var (
		mu      sync.Mutex
		current int
		maxSeen int
	)
	tasks := make([]task.Task, 6)
	for i := range tasks {
		tasks[i] = task.Task{
			ID: fmt.Sprintf("t%d", i),
			Run: func(ctx context.Context) error {
				mu.Lock()
				current++
				if current > maxSeen {
					maxSeen = current
				}
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				mu.Lock()
				current--
				mu.Unlock()
				return nil
			},
		}
	}
	cfg := Config{
		BatchID:     "cap",
		Concurrency: capN,
		Store:       store.NewMemoryStore(),
		Tasks:       tasks,
	}
	o, _ := New(cfg)
	snap, err := o.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if maxSeen > capN {
		t.Fatalf("concurrency cap exceeded: max simultaneous = %d", maxSeen)
	}
	if maxSeen < capN {
		t.Fatalf("expected cap to be reached, max simultaneous = %d", maxSeen)
	}
	for _, st := range snap.States {
		if st.Status != task.StatusSucceeded {
			t.Fatalf("task %s not succeeded", st.ID)
		}
	}
}

func TestFailurePropagatesToDependents(t *testing.T) {
	c := newCounter()
	tasks := []task.Task{
		{ID: "ok", Run: func(ctx context.Context) error { c.inc("ok"); return nil }},
		{ID: "bad", Run: func(ctx context.Context) error { c.inc("bad"); return errBoom }},
		{ID: "child", Deps: []string{"bad"}, Run: func(ctx context.Context) error { c.inc("child"); return nil }},
		{ID: "grand", Deps: []string{"child", "ok"}, Run: func(ctx context.Context) error { c.inc("grand"); return nil }},
	}
	o, err := New(Config{BatchID: "prop", Concurrency: 4, Store: store.NewMemoryStore(), Tasks: tasks})
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := o.Run(context.Background())

	if snap.States["bad"].FailureReason != task.ReasonRetryExhausted {
		t.Fatalf("bad: want retry_exhausted, got %q", snap.States["bad"].FailureReason)
	}
	if snap.States["child"].FailureReason != task.ReasonDependencyFailed {
		t.Fatalf("child: want dependency_failed, got %q", snap.States["child"].FailureReason)
	}
	if snap.States["grand"].FailureReason != task.ReasonDependencyFailed {
		t.Fatalf("grand: want dependency_failed, got %q", snap.States["grand"].FailureReason)
	}
	if c.get("child") != 0 || c.get("grand") != 0 {
		t.Fatal("dependents of a failed task must never execute")
	}
	if snap.States["ok"].Status != task.StatusSucceeded {
		t.Fatal("independent task should still succeed")
	}
}
