// Package store provides durable state storage for task batches.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lisama569/durable-task-orchestrator-go/pkg/task"
)

// Store persists batch snapshots so that state survives process restarts
// and duplicate submissions.
type Store interface {
	// Load returns (nil, nil) when the batch does not exist yet.
	Load(ctx context.Context, batchID string) (*task.Snapshot, error)
	// Save persists the snapshot atomically.
	Save(ctx context.Context, snap *task.Snapshot) error
}

// Locker serializes execution of the same batch across processes or
// goroutines. A store that implements Locker lets the orchestrator hold a
// batch lock for the whole run, preventing two workers from re-running the
// same batch side by side.
type Locker interface {
	Lock(ctx context.Context, batchID string) (unlock func(), err error)
}

// ErrLockNotHeld is returned when unlock is called without an acquired lock.
var ErrLockNotHeld = errors.New("store: lock not held")

// FileStore stores each batch as one JSON file inside Dir.
type FileStore struct {
	Dir string
}

// NewFileStore creates (or opens) a file-backed store rooted at dir.
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("store: empty directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create dir: %w", err)
	}
	return &FileStore{Dir: dir}, nil
}

func (s *FileStore) path(batchID string) string {
	return filepath.Join(s.Dir, batchID+".json")
}

func (s *FileStore) lockPath(batchID string) string {
	return filepath.Join(s.Dir, batchID+".lock")
}

// Load reads and decodes the snapshot of batchID.
func (s *FileStore) Load(ctx context.Context, batchID string) (*task.Snapshot, error) {
	data, err := os.ReadFile(s.path(batchID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read %s: %w", batchID, err)
	}
	var snap task.Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("store: decode %s: %w", batchID, err)
	}
	if snap.States == nil {
		snap.States = map[string]*task.RunState{}
	}
	return &snap, nil
}

// Save atomically replaces the snapshot file (temp file + fsync + rename).
func (s *FileStore) Save(ctx context.Context, snap *task.Snapshot) error {
	if snap == nil || snap.BatchID == "" {
		return errors.New("store: snapshot and batch id required")
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("store: encode: %w", err)
	}
	tmp, err := os.CreateTemp(s.Dir, ".batch-*.tmp")
	if err != nil {
		return fmt.Errorf("store: create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("store: fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("store: close temp: %w", err)
	}
	if err := os.Rename(tmpName, s.path(snap.BatchID)); err != nil {
		cleanup()
		return fmt.Errorf("store: rename: %w", err)
	}
	// Best-effort fsync of the directory so the rename is durable too.
	if dir, err := os.Open(s.Dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// Lock acquires an exclusive, process-crash-safe advisory lock for the batch.
// It polls a non-blocking flock so ctx cancellation is honored promptly.
func (s *FileStore) Lock(ctx context.Context, batchID string) (func(), error) {
	f, err := os.OpenFile(s.lockPath(batchID), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("store: open lock: %w", err)
	}
	// Try once immediately, then poll until the lock is free or ctx ends.
	if err := flock(f); err != nil {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = f.Close()
				return nil, ctx.Err()
			case <-ticker.C:
				if err := flock(f); err == nil {
					goto acquired
				}
			}
		}
	}
acquired:
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = funlock(f)
			_ = f.Close()
		})
	}, nil
}

// MemoryStore is an in-process Store, mainly for tests and examples.
type MemoryStore struct {
	mu    sync.Mutex
	data  map[string]*task.Snapshot
	locks map[string]chan struct{}
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data:  map[string]*task.Snapshot{},
		locks: map[string]chan struct{}{},
	}
}

// Load returns the stored snapshot or nil.
func (s *MemoryStore) Load(ctx context.Context, batchID string) (*task.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := s.data[batchID]
	if snap == nil {
		return nil, nil
	}
	return cloneSnapshot(snap), nil
}

// Save replaces the stored snapshot with a deep copy.
func (s *MemoryStore) Save(ctx context.Context, snap *task.Snapshot) error {
	if snap == nil || snap.BatchID == "" {
		return errors.New("store: snapshot and batch id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[snap.BatchID] = cloneSnapshot(snap)
	return nil
}

// Lock acquires an exclusive in-process lock for the batch.
func (s *MemoryStore) Lock(ctx context.Context, batchID string) (func(), error) {
	s.mu.Lock()
	ch, ok := s.locks[batchID]
	if !ok {
		ch = make(chan struct{}, 1)
		s.locks[batchID] = ch
	}
	s.mu.Unlock()
	select {
	case ch <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() { once.Do(func() { <-ch }) }, nil
}

func cloneSnapshot(snap *task.Snapshot) *task.Snapshot {
	data, err := json.Marshal(snap)
	if err != nil {
		panic("store: snapshot not json-encodable: " + err.Error())
	}
	var out task.Snapshot
	if err := json.Unmarshal(data, &out); err != nil {
		panic("store: snapshot clone failed: " + err.Error())
	}
	return &out
}

// errLockBusy indicates the batch lock is held by someone else.
var errLockBusy = errors.New("store: batch lock held by another runner")
