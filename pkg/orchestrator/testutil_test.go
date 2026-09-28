package orchestrator

import (
	"path/filepath"
	"testing"
)

// TempStoreDirHelper returns a fresh directory path under t.TempDir().
func TempStoreDirHelper(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "state")
}
