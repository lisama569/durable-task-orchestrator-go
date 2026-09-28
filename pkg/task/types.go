// Package task defines the core types of the durable task orchestrator.
package task

import (
	"context"
	"time"
)

// Status represents the lifecycle state of a task run.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Task is a unit of work submitted to the orchestrator.
type Task struct {
	// ID is the globally unique identifier of the task within a batch.
	ID string
	// Deps lists the IDs of tasks that must succeed before this task runs.
	Deps []string
	// Timeout bounds a single attempt; zero means no per-attempt timeout.
	Timeout time.Duration
	// Run is the idempotent unit of work.
	Run func(ctx context.Context) error
}

// RunState is the persisted state of one task.
type RunState struct {
	ID        string    `json:"id"`
	Status    Status    `json:"status"`
	Attempt   int       `json:"attempt"`
	UpdatedAt time.Time `json:"updated_at"`
	// Result is non-empty for succeeded tasks.
	Result string `json:"result,omitempty"`
	// LastError records the error of the latest attempt.
	LastError string `json:"last_error,omitempty"`
	// FailureReason categorizes a terminal failure.
	FailureReason string `json:"failure_reason,omitempty"`
	// ReadyAt is scheduling-only state (not persisted): a pending task is
	// not eligible before this time because it is waiting out a retry backoff.
	ReadyAt time.Time `json:"-"`
}

// Snapshot is the durable state of a whole batch.
type Snapshot struct {
	BatchID string               `json:"batch_id"`
	States  map[string]*RunState `json:"states"`
	Version int                  `json:"version"`
}
