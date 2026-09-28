package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/durable-task-orchestrator-go/pkg/task"
)

func sampleSnapshot() *task.Snapshot {
	return &task.Snapshot{
		BatchID: "batch-1",
		Version: 2,
		States: map[string]*task.RunState{
			"a": {ID: "a", Status: task.StatusSucceeded, Attempt: 1, Result: "ok", UpdatedAt: time.Now().UTC()},
			"b": {ID: "b", Status: task.StatusFailed, Attempt: 3, FailureReason: task.ReasonRetryExhausted, LastError: "boom"},
		},
	}
}

func TestMemoryStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	got, err := s.Load(ctx, "missing")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("missing batch must return nil snapshot, got %+v", got)
	}

	if err := s.Save(ctx, sampleSnapshot()); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.Load(ctx, "batch-1")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.States["a"].Status != task.StatusSucceeded || loaded.Version != 2 {
		t.Fatalf("round trip mismatch: %+v", loaded)
	}
	if loaded.States["b"].FailureReason != task.ReasonRetryExhausted {
		t.Fatalf("failure reason not persisted: %+v", loaded.States["b"])
	}

	// Returned snapshot must be a copy: mutating it must not change the store.
	loaded.States["a"].Status = task.StatusFailed
	reloaded, _ := s.Load(ctx, "batch-1")
	if reloaded.States["a"].Status != task.StatusSucceeded {
		t.Fatal("store must return defensive copies")
	}
}

func TestMemoryStoreLockExclusive(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	unlock, err := s.Lock(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	lockCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := s.Lock(lockCtx, "b"); err == nil {
		t.Fatal("second holder must block until timeout/error")
	}
	unlock()
	unlock2, err := s.Lock(ctx, "b")
	if err != nil {
		t.Fatalf("lock must be acquirable after release: %v", err)
	}
	unlock2()
}

func TestFileStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	missing, err := s.Load(ctx, "nope")
	if err != nil {
		t.Fatal(err)
	}
	if missing != nil {
		t.Fatal("new dir must have no snapshots")
	}

	if err := s.Save(ctx, sampleSnapshot()); err != nil {
		t.Fatal(err)
	}
	// New store instance, same directory: simulates process restart.
	s2, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := s2.Load(ctx, "batch-1")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.States["a"].Status != task.StatusSucceeded {
		t.Fatalf("status not recovered after restart: %+v", loaded.States["a"])
	}
	if loaded.States["b"].FailureReason != task.ReasonRetryExhausted {
		t.Fatalf("failure reason not recovered: %+v", loaded.States["b"])
	}
	if filepath.Join(dir, "batch-1.json") != s.path("batch-1") {
		t.Fatal("unexpected snapshot path")
	}
}

func TestFileStoreAtomicReplace(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, _ := NewFileStore(dir)
	for i := 0; i < 20; i++ {
		snap := sampleSnapshot()
		snap.Version = i
		if err := s.Save(ctx, snap); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	loaded, err := s.Load(ctx, "batch-1")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version != 19 {
		t.Fatalf("want final version 19, got %d", loaded.Version)
	}
}

func TestFileStoreRejectsBadInputs(t *testing.T) {
	if _, err := NewFileStore(""); err == nil {
		t.Fatal("empty dir must be rejected")
	}
	s, _ := NewFileStore(t.TempDir())
	if err := s.Save(context.Background(), &task.Snapshot{BatchID: ""}); err == nil {
		t.Fatal("empty batch id must be rejected")
	}
}

func TestFileStoreLockCancelable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, _ := NewFileStore(dir)
	unlock, err := s.Lock(ctx, "batch-x")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	lockCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = s.Lock(lockCtx, "batch-x")
	if err == nil {
		t.Fatal("expected second lock attempt to fail while first is held")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("canceled flock blocked too long; ctx not honored")
	}
}
