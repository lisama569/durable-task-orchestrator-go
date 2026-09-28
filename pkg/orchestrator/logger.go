package orchestrator

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

// Logger receives structured scheduling decision logs.
type Logger interface {
	// Log records an event with key/value pairs, e.g.
	// Log("info", "task_scheduled", "task_id", "a", "deps", "[b c]").
	Log(level, event string, kv ...any)
}

// TextLogger writes human-readable structured lines to a writer
// (default stderr). It is safe for concurrent use.
type TextLogger struct {
	l *log.Logger
}

// NewTextLogger creates a logger writing to w (stderr when nil).
func NewTextLogger(w io.Writer) *TextLogger {
	if w == nil {
		w = os.Stderr
	}
	return &TextLogger{l: log.New(w, "", 0)}
}

// Log implements Logger.
func (t *TextLogger) Log(level, event string, kv ...any) {
	var b strings.Builder
	b.WriteString(time.Now().UTC().Format(time.RFC3339))
	b.WriteString(" level=")
	b.WriteString(level)
	b.WriteString(" event=")
	b.WriteString(event)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(&b, " %v=%v", kv[i], kv[i+1])
	}
	if len(kv)%2 == 1 {
		fmt.Fprintf(&b, " %v=?", kv[len(kv)-1])
	}
	t.l.Print(b.String())
}

// NopLogger discards all logs.
type NopLogger struct{}

// Log implements Logger.
func (NopLogger) Log(level, event string, kv ...any) {}

// defaultLogger returns the configured logger or a NopLogger.
func defaultLogger(l Logger) Logger {
	if l == nil {
		return NopLogger{}
	}
	return l
}
