// Command demo runs a small dependent batch against a file-backed store so
// you can observe scheduling, retry, dependency skipping and recovery.
//
//	go run ./cmd/demo -batch demo -dir ./.state
//
// Run it twice with the same -batch to see completed tasks being skipped.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/lisama569/durable-task-orchestrator-go/pkg/orchestrator"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/retry"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/store"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/task"
)

func main() {
	batchID := flag.String("batch", "demo", "batch id (same id resumes)")
	dir := flag.String("dir", "./.state", "state directory")
	flag.Parse()

	st, err := store.NewFileStore(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var flakyAttempts int32
	tasks := []task.Task{
		{ID: "fetch-config", Run: func(ctx context.Context) error {
			fmt.Println("[fetch-config] doing work")
			return nil
		}},
		{ID: "flaky-api", Deps: []string{"fetch-config"}, Timeout: 2 * time.Second, Run: func(ctx context.Context) error {
			n := atomic.AddInt32(&flakyAttempts, 1)
			fmt.Printf("[flaky-api] attempt %d\n", n)
			if n < 3 {
				return fmt.Errorf("transient failure on attempt %d", n)
			}
			return nil
		}},
		{ID: "render-report", Deps: []string{"flaky-api"}, Run: func(ctx context.Context) error {
			fmt.Println("[render-report] doing work")
			return nil
		}},
	}

	cfg := orchestrator.Config{
		BatchID:     *batchID,
		Tasks:       tasks,
		Concurrency: 2,
		Retry:       retry.FixedPolicy{Attempts: 5, Delay: 10 * time.Millisecond},
		Store:       st,
		Logger:      orchestrator.NewTextLogger(os.Stdout),
	}

	o, err := orchestrator.New(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid graph:", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	snap, err := o.Run(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run ended:", err)
	}
	fmt.Println("--- final states ---")
	for _, t := range tasks {
		st := snap.States[t.ID]
		fmt.Printf("task=%-15s status=%-10s attempts=%d reason=%s\n",
			t.ID, st.Status, st.Attempt, st.FailureReason)
	}
}
