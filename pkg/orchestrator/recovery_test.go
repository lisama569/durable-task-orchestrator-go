package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/durable-task-orchestrator-go/pkg/retry"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/store"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/task"
)

// buildRecoveryTasks builds a diamond batch:
//
//	a (succeeds early) and c (can block), b depends on a, d depends on b and c.
//
// The blocking task lets us cancel the first run mid-flight.
func buildRecoveryTasks(dir string, cCounter *int32) []task.Task {
	// Side-effect files prove each succeeded task body runs at most once
	// across process "restarts".
	onceFile := func(name string) task.Task {
		path := filepath.Join(dir, name+".done")
		return task.Task{
			ID: name,
			Run: func(ctx context.Context) error {
				if _, err := os.Stat(path); err == nil {
					t := time.NewTicker(time.Millisecond)
					defer t.Stop()
					for {
						select {
						case <-ctx.Done():
							return errors.New("side effect unexpectedly executed twice")
						case <-t.C:
						}
					}
				}
				if err := os.WriteFile(path, []byte("1"), 0o600); err != nil {
					return err
				}
				return nil
			},
		}
	}
	a := onceFile("a")
	b := onceFile("b")
	b.Deps = []string{"a"}
	c := task.Task{
		ID: "c",
		Run: func(ctx context.Context) error {
			atomic.AddInt32(cCounter, 1)
			started := filepath.Join(dir, "c.started")
			if _, err := os.Stat(started); err == nil {
				// Resumed attempt after interruption: now completes normally.
				return os.WriteFile(filepath.Join(dir, "c.done"), []byte("1"), 0o600)
			}
			if err := os.WriteFile(started, []byte("1"), 0o600); err != nil {
				return err
			}
			<-ctx.Done() // first attempt is interrupted
			return ctx.Err()
		},
	}
	d := onceFile("d")
	d.Deps = []string{"b", "c"}
	return []task.Task{a, b, c, d}
}

func TestRecoveryAfterInterruption(t *testing.T) {
	dir := t.TempDir()
	storeDir := filepath.Join(dir, "state")
	var cRuns int32
	tasks1 := buildRecoveryTasks(storeDir, &cRuns)

	// First "process": cancel while task c is in flight; a and b succeeded.
	st1, err := store.NewFileStore(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	ctx1, cancel := context.WithCancel(context.Background())
	o1, err := New(Config{
		BatchID:     "batch-resume",
		Concurrency: 4,
		Retry:       retry.NoRetry,
		Store:       st1,
		Logger:      &sliceLogger{},
		Tasks:       tasks1,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()
	snap1, runErr := o1.Run(ctx1)
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("first run should report cancellation, got %v", runErr)
	}
	if snap1.States["a"].Status != task.StatusSucceeded ||
		snap1.States["b"].Status != task.StatusSucceeded {
		t.Fatalf("a and b should have succeeded before cancel: %+v %+v",
			snap1.States["a"], snap1.States["b"])
	}
	if snap1.States["c"].Status != task.StatusPending {
		t.Fatalf("interrupted task c should be reset to pending for resume, got %s",
			snap1.States["c"].Status)
	}
	if snap1.States["c"].FailureReason != "" {
		t.Fatalf("interruption must not be a terminal failure, got reason %q",
			snap1.States["c"].FailureReason)
	}
	if snap1.States["d"].Status != task.StatusPending {
		t.Fatalf("d should still be pending (c will be retried on resume), got %s",
			snap1.States["d"].Status)
	}

	// Second "process": fresh orchestrator, same batch id and store directory.
	var cRuns2 int32
	tasks2 := buildRecoveryTasks(storeDir, &cRuns2)
	st2, err := store.NewFileStore(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	o2, err := New(Config{
		BatchID:     "batch-resume",
		Concurrency: 4,
		Retry:       retry.NoRetry,
		Store:       st2,
		Logger:      &sliceLogger{},
		Tasks:       tasks2,
	})
	if err != nil {
		t.Fatal(err)
	}
	snap2, err := o2.Run(context.Background())
	if err != nil {
		t.Fatalf("resumed run must finish: %v", err)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if snap2.States[id].Status != task.StatusSucceeded {
			t.Fatalf("after resume %s should succeed, got %+v", id, snap2.States[id])
		}
	}

	// Idempotency core: a and b were already durable before the crash; their
	// bodies must not run again (they self-detect duplicate side effects and
	// would fail). If resume succeeded for them, their counters prove at-most-once.
	if atomic.LoadInt32(&cRuns2) == 0 {
		t.Fatal("interrupted task c must be retried after restart")
	}
	for _, name := range []string{"a", "b", "d"} {
		if _, err := os.Stat(filepath.Join(storeDir, name+".done")); err != nil {
			t.Fatalf("side effect file for %s missing: %v", name, err)
		}
	}
}

func TestDuplicateSubmissionDoesNotRerunSucceededTasks(t *testing.T) {
	dir := TempStoreDirHelper(t)
	var runs int32
	taskSet := func() []task.Task {
		return []task.Task{
			{
				ID: "x",
				Run: func(ctx context.Context) error {
					atomic.AddInt32(&runs, 1)
					return nil
				},
			},
		}
	}
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		BatchID: "same-batch",
		Store:   st,
		Retry:   retry.FixedPolicy{Attempts: 3},
		Tasks:   taskSet(),
	}
	o1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := o1.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.States["x"].Status != task.StatusSucceeded {
		t.Fatalf("first run: %+v", snap.States["x"])
	}

	// Re-submit exactly the same batch from scratch (same BatchID, new
	// orchestrator instance, new store handle) — mimics a duplicate request.
	st2, _ := store.NewFileStore(dir)
	o2, err := New(Config{
		BatchID: "same-batch",
		Store:   st2,
		Retry:   retry.FixedPolicy{Attempts: 3},
		Tasks:   taskSet(),
		Logger:  &sliceLogger{},
	})
	if err != nil {
		t.Fatal(err)
	}
	snap2, err := o2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap2.States["x"].Status != task.StatusSucceeded {
		t.Fatalf("duplicate run state: %+v", snap2.States["x"])
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("task body ran %d times across duplicate submissions, want exactly 1", got)
	}
}

func TestConcurrentSameBatchSubmissionsAreSerialized(t *testing.T) {
	st := store.NewMemoryStore()
	var runs int32
	mkCfg := func() Config {
		return Config{
			BatchID: "locked-batch",
			Store:   st,
			Tasks: []task.Task{
				{
					ID: "z",
					Run: func(ctx context.Context) error {
						atomic.AddInt32(&runs, 1)
						time.Sleep(10 * time.Millisecond)
						return nil
					},
				},
			},
		}
	}
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			o, err := New(mkCfg())
			if err != nil {
				errs <- err
				return
			}
			_, err = o.Run(context.Background())
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent submission failed: %v", err)
		}
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("two concurrent same-batch submissions ran the body %d times, want 1", got)
	}
}
