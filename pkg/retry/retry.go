// Package retry provides retry policies for task attempts.
package retry

import "time"

// Policy decides whether and how long to wait before the next attempt.
type Policy interface {
	// MaxAttempts returns the total number of attempts allowed per task
	// (1 means no retry).
	MaxAttempts() int
	// NextBackoff returns the wait before retrying after failedAttempt
	// (1-based) just failed with err. A negative duration means retries
	// are exhausted.
	NextBackoff(failedAttempt int, err error) time.Duration
}

// FixedPolicy retries up to Attempts times with a constant Delay.
type FixedPolicy struct {
	Attempts int
	Delay    time.Duration
}

func (p FixedPolicy) maxAttempts() int {
	if p.Attempts < 1 {
		return 1
	}
	return p.Attempts
}

// MaxAttempts implements Policy.
func (p FixedPolicy) MaxAttempts() int { return p.maxAttempts() }

// NextBackoff implements Policy.
// Attempts are 1-based: after attempt 1 fails, wait Delay then run
// attempt 2, etc. After the final allowed attempt fails, return -1.
func (p FixedPolicy) NextBackoff(failedAttempt int, err error) time.Duration {
	if failedAttempt >= p.maxAttempts() {
		return -1
	}
	return p.Delay
}

// ExponentialPolicy doubles the backoff up to MaxDelay.
type ExponentialPolicy struct {
	Attempts  int
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// MaxAttempts implements Policy.
func (p ExponentialPolicy) MaxAttempts() int {
	if p.Attempts < 1 {
		return 1
	}
	return p.Attempts
}

// NextBackoff implements Policy.
func (p ExponentialPolicy) NextBackoff(failedAttempt int, err error) time.Duration {
	if failedAttempt >= p.MaxAttempts() {
		return -1
	}
	d := p.BaseDelay
	for i := 1; i < failedAttempt; i++ {
		d *= 2
		if p.MaxDelay > 0 && d > p.MaxDelay {
			d = p.MaxDelay
			break
		}
	}
	return d
}

// NoRetry fails the task after its first attempt.
var NoRetry Policy = FixedPolicy{Attempts: 1}
