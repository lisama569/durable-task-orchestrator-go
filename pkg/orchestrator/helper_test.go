package orchestrator

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var errBoom = errors.New("boom")

// sliceLogger keeps every log line in memory for assertions.
type sliceLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *sliceLogger) Log(level, event string, kv ...any) {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", level, event)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(&b, " %v=%v", kv[i], kv[i+1])
	}
	l.mu.Lock()
	l.lines = append(l.lines, b.String())
	l.mu.Unlock()
}

func (l *sliceLogger) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.lines))
	copy(out, l.lines)
	return out
}

func (l *sliceLogger) contains(sub string) bool {
	for _, line := range l.Lines() {
		if strings.Contains(line, sub) {
			return true
		}
	}
	return false
}

// counter is a concurrency-safe invocation counter for idempotency checks.
type counter struct {
	mu sync.Mutex
	m  map[string]int
}

func newCounter() *counter { return &counter{m: map[string]int{}} }

func (c *counter) inc(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[id]++
	return c.m[id]
}

func (c *counter) get(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[id]
}
