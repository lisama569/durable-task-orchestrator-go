package orchestrator

import (
	"context"
	"testing"
	"time"
)

func TestFileStore_LoadNotFound(t *testing.T) {
	s := NewFileStore(t.TempDir())
	if _, err := s.Load(context.Background(), "missing"); err != ErrStateNotFound {
		t.Fatalf("期望 ErrStateNotFound，得到 %v", err)
	}
}

func TestFileStore_SaveLoadRoundTrip(t *testing.T) {
	s := NewFileStore(t.TempDir())
	now := time.Now().UTC()
	orig := &BatchState{
		BatchID:   "roundtrip",
		Version:   7,
		UpdatedAt: now,
		Jobs: map[JobID]*JobState{
			"a": {ID: "a", Status: StatusSucceeded, Attempts: 2, FinishedAt: &now},
			"b": {ID: "b", Status: StatusFailed, FailureCode: FailureRetriesExhausted, LastFailure: FailureTimeout},
		},
	}
	if err := s.Save(context.Background(), orig); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(context.Background(), "roundtrip")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 7 || got.BatchID != "roundtrip" {
		t.Fatalf("批次头信息不匹配: %+v", got)
	}
	if got.Jobs["a"].Status != StatusSucceeded || got.Jobs["b"].FailureCode != FailureRetriesExhausted {
		t.Fatalf("作业状态不匹配: %+v", got.Jobs)
	}
	if got.Jobs["b"].LastFailure != FailureTimeout {
		t.Fatalf("LastFailure 未正确往返: %s", got.Jobs["b"].LastFailure)
	}
}

func TestFileStore_SaveSerializesConcurrentWrites(t *testing.T) {
	s := NewFileStore(t.TempDir())
	done := make(chan error, 20)
	for i := 0; i < 20; i++ {
		v := i
		go func() {
			done <- s.Save(context.Background(), &BatchState{
				BatchID: "ser", Version: v,
				Jobs: map[JobID]*JobState{"a": {ID: "a", Status: StatusPending}},
			})
		}()
	}
	for i := 0; i < 20; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Load(context.Background(), "ser")
	if err != nil {
		t.Fatal(err)
	}
	// 每次保存都是完整快照，读回的必须是某一份完整状态而不是撕裂数据。
	if got.Jobs["a"] == nil || got.Jobs["a"].ID != "a" {
		t.Fatalf("并发保存后状态不完整: %+v", got.Jobs)
	}
}
