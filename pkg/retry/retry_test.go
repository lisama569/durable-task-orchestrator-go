package retry

import (
	"errors"
	"testing"
	"time"
)

var errFake = errors.New("fake")

func TestFixedPolicy(t *testing.T) {
	p := FixedPolicy{Attempts: 3, Delay: 10 * time.Millisecond}
	if p.MaxAttempts() != 3 {
		t.Fatalf("want 3 attempts, got %d", p.MaxAttempts())
	}
	if d := p.NextBackoff(1, errFake); d != 10*time.Millisecond {
		t.Fatalf("after attempt 1 want 10ms, got %v", d)
	}
	if d := p.NextBackoff(2, errFake); d != 10*time.Millisecond {
		t.Fatalf("after attempt 2 want 10ms, got %v", d)
	}
	if d := p.NextBackoff(3, errFake); d >= 0 {
		t.Fatalf("after final attempt want negative (exhausted), got %v", d)
	}
}

func TestFixedPolicyDefaultsToOne(t *testing.T) {
	p := FixedPolicy{}
	if p.MaxAttempts() != 1 {
		t.Fatalf("zero value should mean 1 attempt, got %d", p.MaxAttempts())
	}
	if d := p.NextBackoff(1, errFake); d >= 0 {
		t.Fatalf("no-retry policy must signal exhaustion, got %v", d)
	}
}

func TestExponentialPolicy(t *testing.T) {
	p := ExponentialPolicy{Attempts: 4, BaseDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond}
	want := []time.Duration{5, 10, 20}
	for i, w := range want {
		if d := p.NextBackoff(i+1, errFake); d != w*time.Millisecond {
			t.Fatalf("attempt %d: want %v, got %v", i+1, w, d)
		}
	}
	if d := p.NextBackoff(4, errFake); d >= 0 {
		t.Fatalf("want exhausted after 4 attempts, got %v", d)
	}
}

func TestExponentialMaxDelay(t *testing.T) {
	p := ExponentialPolicy{Attempts: 10, BaseDelay: 10 * time.Millisecond, MaxDelay: 15 * time.Millisecond}
	for i := 1; i < 10; i++ {
		if d := p.NextBackoff(i, errFake); d > 15*time.Millisecond {
			t.Fatalf("backoff %v exceeds MaxDelay at attempt %d", d, i)
		}
	}
}

func TestNoRetry(t *testing.T) {
	if NoRetry.MaxAttempts() != 1 {
		t.Fatalf("NoRetry must allow exactly 1 attempt")
	}
}
