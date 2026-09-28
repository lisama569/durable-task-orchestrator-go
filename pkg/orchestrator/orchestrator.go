// Package orchestrator schedules and executes a batch of dependent tasks
// with retries, concurrency limits, cancellation and durable recovery.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/durable-task-orchestrator-go/pkg/dag"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/retry"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/store"
	"github.com/lisama569/durable-task-orchestrator-go/pkg/task"
)

// Config configures an Orchestrator.
type Config struct {
	// BatchID uniquely identifies the submission. Re-running Run with the
	// same BatchID resumes from the persisted snapshot instead of redoing
	// succeeded work.
	BatchID string
	// Tasks is the set of tasks of the batch; IDs must be unique.
	Tasks []task.Task
	// Concurrency bounds simultaneously running tasks; <=0 means len(Tasks).
	Concurrency int
	// Retry decides per-task attempt count and backoff; nil means no retry.
	Retry retry.Policy
	// Store persists state; nil uses an in-memory store (no crash recovery).
	Store store.Store
	// Logger receives scheduling decision logs; nil disables logging.
	Logger Logger
}

// Orchestrator runs one batch of tasks.
type Orchestrator struct {
	cfg Config
}

// New builds an Orchestrator after validating the dependency graph.
// Invalid graphs (empty/duplicate IDs, missing deps, cycles) are rejected
// here, before any task runs, with an error pointing at the cause.
func New(cfg Config) (*Orchestrator, error) {
	if cfg.BatchID == "" {
		return nil, errors.New("orchestrator: batch id required")
	}
	if len(cfg.Tasks) == 0 {
		return nil, errors.New("orchestrator: at least one task required")
	}
	ids := make([]string, len(cfg.Tasks))
	deps := make(map[string][]string, len(cfg.Tasks))
	for i, t := range cfg.Tasks {
		ids[i] = t.ID
		deps[t.ID] = t.Deps
	}
	if err := dag.Validate(ids, deps); err != nil {
		return nil, fmt.Errorf("orchestrator: invalid dependency graph: %w", err)
	}
	if cfg.Store == nil {
		cfg.Store = store.NewMemoryStore()
	}
	if cfg.Retry == nil {
		cfg.Retry = retry.NoRetry
	}
	return &Orchestrator{cfg: cfg}, nil
}

// attemptResult is sent from a worker goroutine to the coordinator.
type attemptResult struct {
	id       string
	attempt  int
	status   task.Status
	err      error
	duration time.Duration
}

// Run executes the batch until all tasks reach a terminal state or ctx is
// canceled. It is safe to call again with the same Config after a crash:
// succeeded tasks are skipped and their stored results returned.
func (o *Orchestrator) Run(ctx context.Context) (*task.Snapshot, error) {
	log := defaultLogger(o.cfg.Logger)

	// Hold an exclusive batch lock for the whole run so that two processes
	// (or two goroutines) submitting the same batch cannot duplicate side
	// effects by racing on the same tasks.
	unlock, err := o.lockBatch(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()

	allDeps := make(map[string][]string, len(o.cfg.Tasks))
	for _, t := range o.cfg.Tasks {
		allDeps[t.ID] = t.Deps
	}

	snap, err := o.cfg.Store.Load(ctx, o.cfg.BatchID)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		snap = &task.Snapshot{BatchID: o.cfg.BatchID, States: map[string]*task.RunState{}}
	}
	if snap.States == nil {
		snap.States = map[string]*task.RunState{}
	}

	// Recover from an interrupted previous run: "running" never reached a
	// terminal state, so reset it to pending and retry the attempt.
	for _, t := range o.cfg.Tasks {
		st, ok := snap.States[t.ID]
		if !ok {
			snap.States[t.ID] = &task.RunState{ID: t.ID, Status: task.StatusPending}
			continue
		}
		st.ReadyAt = time.Time{}
		if st.Status == task.StatusRunning {
			log.Log("warn", "recover_interrupted",
				"batch_id", o.cfg.BatchID, "task_id", t.ID,
				"previous_attempt", st.Attempt,
				"decision", "reset_running_to_pending")
			st.Status = task.StatusPending
			// The interrupted attempt never reached a terminal outcome, so it
			// must not consume the task's retry budget.
			st.Attempt = 0
			st.FailureReason = ""
			st.LastError = "interrupted during previous attempt"
		}
	}

	// A recovered snapshot may already contain failed tasks; propagate the
	// failure to their dependents up front.
	o.markDependentsFailed(snap, allDeps, log)
	if err := o.cfg.Store.Save(ctx, snap); err != nil {
		return nil, err
	}

	maxConcurrent := o.cfg.Concurrency
	if maxConcurrent <= 0 || maxConcurrent > len(o.cfg.Tasks) {
		maxConcurrent = len(o.cfg.Tasks)
	}

	log.Log("info", "batch_start",
		"batch_id", o.cfg.BatchID,
		"tasks", len(o.cfg.Tasks),
		"concurrency", maxConcurrent,
		"max_attempts", o.cfg.Retry.MaxAttempts())

	sem := make(chan struct{}, maxConcurrent)
	results := make(chan attemptResult, maxConcurrent)
	var wg sync.WaitGroup

	hasInFlight := func() bool {
		for _, st := range snap.States {
			if st.Status == task.StatusRunning {
				return true
			}
		}
		return false
	}

	allTerminal := func() bool {
		for _, st := range snap.States {
			if st.Status == task.StatusPending || st.Status == task.StatusRunning {
				return false
			}
		}
		return true
	}

	// nextWake returns the earliest backoff deadline among pending tasks.
	nextWake := func() time.Time {
		var t time.Time
		now := time.Now()
		for _, st := range snap.States {
			if st.Status != task.StatusPending || st.ReadyAt.IsZero() || !st.ReadyAt.After(now) {
				continue
			}
			if t.IsZero() || st.ReadyAt.Before(t) {
				t = st.ReadyAt
			}
		}
		return t
	}

	// schedule starts every task that is startable and fits the concurrency
	// budget. Runs only in the coordinator goroutine.
	schedule := func() {
		now := time.Now()
		for _, t := range o.cfg.Tasks {
			st := snap.States[t.ID]
			if st.Status != task.StatusPending {
				continue
			}
			if !st.ReadyAt.IsZero() && st.ReadyAt.After(now) {
				continue // waiting out a retry backoff
			}
			startable := true
			for _, dep := range t.Deps {
				if snap.States[dep].Status != task.StatusSucceeded {
					startable = false
					break
				}
			}
			if !startable {
				continue
			}
			select {
			case sem <- struct{}{}:
			default:
				// Concurrency budget exhausted: the task stays pending and
				// queues without spawning any goroutine or other resource.
				continue
			}
			st.Status = task.StatusRunning
			st.Attempt++
			st.UpdatedAt = time.Now().UTC()
			st.LastError = ""
			st.FailureReason = ""
			// Persist "running" before invoking the user function so a crash
			// leaves no ambiguous record: recovery resets it to pending.
			o.persist(snap, log)
			log.Log("info", "task_scheduled",
				"batch_id", o.cfg.BatchID,
				"task_id", t.ID,
				"attempt", st.Attempt,
				"deps", fmt.Sprint(t.Deps),
				"decision", "deps_succeeded_and_budget_available")

			wg.Add(1)
			go func(t task.Task, attempt int) {
				defer wg.Done()
				r := attemptResult{id: t.ID, attempt: attempt}
				started := time.Now()
				runCtx, cancel := context.WithCancel(ctx)
				if t.Timeout > 0 {
					runCtx, cancel = context.WithTimeout(ctx, t.Timeout)
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					r.err = t.Run(runCtx)
				}()
				select {
				case <-done:
					cancel()
					if r.err == nil {
						r.status = task.StatusSucceeded
					} else {
						r.status = task.StatusFailed
					}
				case <-runCtx.Done():
					// Parent canceled or per-attempt deadline expired.
					cancel()
					<-done // wait for the user function to observe cancellation
					r.status = task.StatusFailed
					if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
						r.err = fmt.Errorf("%w after %s", task.ErrTimeout, t.Timeout)
					} else {
						r.err = runCtx.Err()
					}
				}
				r.duration = time.Since(started)
				results <- r
			}(t, st.Attempt)
		}
	}

	var runErr error
	for {
		schedule()
		if allTerminal() {
			break
		}
		wake := nextWake()
		var timer *time.Timer
		var timerC <-chan time.Time
		// Only sleep on a backoff timer when nothing is running; an in-flight
		// attempt completing will itself wake the coordinator.
		if !wake.IsZero() && !hasInFlight() {
			timer = time.NewTimer(time.Until(wake))
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			runErr = o.handleCancellation(ctx, snap, sem, results, &wg, log)
			goto finished
		case <-timerC:
			// A retry backoff elapsed; loop around and schedule.
		case r := <-results:
			if timer != nil {
				timer.Stop()
			}
			o.applyResult(snap, r, allDeps, log)
			<-sem
		}
	}

	if err := o.cfg.Store.Save(ctx, snap); err != nil {
		return snap, err
	}
	log.Log("info", "batch_finished", "batch_id", o.cfg.BatchID)

finished:
	o.logSummary(snap, log)
	if runErr != nil {
		return snap, runErr
	}
	return snap, nil
}

// handleCancellation waits for in-flight attempts (their context is already
// canceled) and resets interrupted tasks to a non-terminal pending state, so a
// later submission resumes them. No task is left in the ambiguous "running"
// state, and no premature terminal failure is recorded for an interruption.
func (o *Orchestrator) handleCancellation(
	ctx context.Context, snap *task.Snapshot,
	sem chan struct{}, results chan attemptResult, wg *sync.WaitGroup,
	log Logger,
) error {
	log.Log("warn", "batch_canceled", "batch_id", o.cfg.BatchID, "err", ctx.Err())
	go func() { wg.Wait(); close(results) }()
	for r := range results {
		o.applyCanceledResult(snap, r, log)
		<-sem
	}
	// Defensive: anything still marked running goes back to pending so the
	// snapshot contains only unambiguous states.
	for _, st := range snap.States {
		if st.Status == task.StatusRunning {
			st.Status = task.StatusPending
			st.FailureReason = ""
			st.LastError = "interrupted before reaching a terminal state"
			st.ReadyAt = time.Time{}
			st.UpdatedAt = time.Now().UTC()
		}
	}
	o.persist(snap, log)
	return ctx.Err()
}

// applyResult records one finished attempt and either retries, marks the task
// terminal, or records success. Runs only in the coordinator goroutine.
func (o *Orchestrator) applyResult(snap *task.Snapshot, r attemptResult, allDeps map[string][]string, log Logger) {
	st := snap.States[r.id]
	st.UpdatedAt = time.Now().UTC()

	if r.status == task.StatusSucceeded {
		st.Status = task.StatusSucceeded
		st.ReadyAt = time.Time{}
		st.FailureReason = ""
		st.LastError = ""
		log.Log("info", "task_succeeded",
			"batch_id", o.cfg.BatchID, "task_id", r.id,
			"attempt", r.attempt, "duration_ms", r.duration.Milliseconds())
		o.persist(snap, log)
		return
	}

	st.LastError = r.err.Error()
	backoff := o.cfg.Retry.NextBackoff(r.attempt, r.err)
	if backoff >= 0 {
		st.Status = task.StatusPending
		if backoff > 0 {
			st.ReadyAt = time.Now().Add(backoff)
		} else {
			st.ReadyAt = time.Time{}
		}
		log.Log("warn", "task_attempt_failed_retry",
			"batch_id", o.cfg.BatchID, "task_id", r.id,
			"attempt", r.attempt,
			"err_kind", errorKind(r.err),
			"err", r.err,
			"backoff_ms", backoff.Milliseconds(),
			"decision", "retry_budget_remaining")
		o.persist(snap, log)
		return
	}

	// Terminal: retries are exhausted. Classify the final cause distinctly.
	st.Status = task.StatusFailed
	st.ReadyAt = time.Time{}
	st.FailureReason = classifyFailure(r.err)
	log.Log("error", "task_failed_terminal",
		"batch_id", o.cfg.BatchID, "task_id", r.id,
		"attempts", r.attempt,
		"reason", st.FailureReason,
		"err", r.err,
		"decision", "retry_budget_exhausted")
	o.markDependentsFailed(snap, allDeps, log)
	o.persist(snap, log)
}

// applyCanceledResult handles attempts that ended while the batch was
// canceled. A success that landed just before cancellation stays durable;
// an interrupted attempt is reset to pending for a later resume.
func (o *Orchestrator) applyCanceledResult(snap *task.Snapshot, r attemptResult, log Logger) {
	st := snap.States[r.id]
	if r.status == task.StatusSucceeded {
		st.Status = task.StatusSucceeded
		st.ReadyAt = time.Time{}
		st.FailureReason = ""
		st.LastError = ""
		log.Log("info", "task_succeeded", "batch_id", o.cfg.BatchID, "task_id", r.id, "attempt", r.attempt)
		return
	}
	st.Status = task.StatusPending
	st.ReadyAt = time.Time{}
	// The interrupted attempt never reached a terminal outcome, so it must
	// not consume the task's retry budget.
	st.Attempt = 0
	st.FailureReason = ""
	st.LastError = "interrupted before reaching a terminal state"
	st.UpdatedAt = time.Now().UTC()
	log.Log("warn", "task_interrupted_reset",
		"batch_id", o.cfg.BatchID, "task_id", r.id, "attempt", r.attempt,
		"decision", "non_terminal_pending_for_resume")
}

// persist saves the snapshot, logging persistence errors.
func (o *Orchestrator) persist(snap *task.Snapshot, log Logger) {
	if err := o.cfg.Store.Save(context.Background(), snap); err != nil {
		log.Log("error", "persist_failed", "batch_id", snap.BatchID, "err", err)
	}
}

// markDependentsFailed marks every pending task whose (transitive)
// dependency is failed as failed with ReasonDependencyFailed.
func (o *Orchestrator) markDependentsFailed(snap *task.Snapshot, allDeps map[string][]string, log Logger) {
	changed := true
	for changed {
		changed = false
		for id, deps := range allDeps {
			st := snap.States[id]
			if st.Status != task.StatusPending {
				continue
			}
			for _, dep := range deps {
				if snap.States[dep].Status == task.StatusFailed {
					st.Status = task.StatusFailed
					st.FailureReason = task.ReasonDependencyFailed
					st.LastError = fmt.Sprintf("dependency %q failed: %s", dep, snap.States[dep].FailureReason)
					st.UpdatedAt = time.Now().UTC()
					log.Log("warn", "task_skipped_dependency_failed",
						"batch_id", o.cfg.BatchID, "task_id", id,
						"failed_dep", dep,
						"decision", "prerequisite_terminal_failure")
					changed = true
					break
				}
			}
		}
	}
}

// classifyFailure maps an attempt error to a stable failure reason string.
func classifyFailure(err error) string {
	switch {
	case errors.Is(err, task.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return task.ReasonTimeout
	case errors.Is(err, context.Canceled):
		return task.ReasonCanceled
	default:
		return task.ReasonRetryExhausted
	}
}

// errorKind gives a short, non-terminal label for logging intermediate attempts.
func errorKind(err error) string {
	switch {
	case errors.Is(err, task.ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "error"
	}
}

func (o *Orchestrator) logSummary(snap *task.Snapshot, log Logger) {
	var succeeded, failed int
	for _, st := range snap.States {
		switch st.Status {
		case task.StatusSucceeded:
			succeeded++
		case task.StatusFailed:
			failed++
		}
	}
	log.Log("info", "batch_summary",
		"batch_id", o.cfg.BatchID,
		"succeeded", succeeded, "failed", failed, "total", len(snap.States))
}

// lockBatch serializes same-batch runs when the store supports locking.
func (o *Orchestrator) lockBatch(ctx context.Context) (func(), error) {
	if lk, ok := o.cfg.Store.(store.Locker); ok {
		unlock, err := lk.Lock(ctx, o.cfg.BatchID)
		if err != nil {
			return nil, fmt.Errorf("orchestrator: acquire batch lock: %w", err)
		}
		return unlock, nil
	}
	return func() {}, nil
}
