package task

import "errors"

var (
	// ErrRetryExhausted indicates all retries of a task have been used.
	ErrRetryExhausted = errors.New("task: retries exhausted")
	// ErrDependencyFailed indicates the task was skipped because a dependency failed.
	ErrDependencyFailed = errors.New("task: dependency failed")
	// ErrTimeout indicates a task attempt exceeded its timeout.
	ErrTimeout = errors.New("task: attempt timed out")
)

// Failure reasons recorded in RunState.FailureReason; tests and callers can
// distinguish these stable strings.
const (
	// ReasonRetryExhausted: all attempts failed with ordinary errors.
	ReasonRetryExhausted = "retry_exhausted"
	// ReasonTimeout: the final attempt failed because the per-attempt
	// timeout (or the parent deadline) expired.
	ReasonTimeout = "timeout"
	// ReasonDependencyFailed: never ran because a prerequisite failed.
	ReasonDependencyFailed = "dependency_failed"
	// ReasonCanceled: the run was interrupted while this task was in flight.
	ReasonCanceled = "canceled"
)
