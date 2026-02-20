package ralph

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTelegramPaths(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if got, want := paths.TelegramPIDFile(), filepath.Join(paths.RalphDir, "telegram.pid"); got != want {
		t.Fatalf("telegram pid path mismatch: got=%q want=%q", got, want)
	}
	if got, want := paths.TelegramLogFile(), filepath.Join(paths.LogsDir, "telegram.out"); got != want {
		t.Fatalf("telegram log path mismatch: got=%q want=%q", got, want)
	}
}

func TestEnsureLayoutCreatesTelegramLog(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := os.Stat(paths.TelegramLogFile()); err != nil {
		t.Fatalf("telegram log file should exist: %v", err)
	}
}
