package ralph

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseDetailInt(t *testing.T) {
	t.Parallel()

	detail := "streak=4; wait_sec=20; role_scope=all"
	if got := parseDetailInt(detail, "streak"); got != 4 {
		t.Fatalf("parseDetailInt mismatch: got=%d want=%d", got, 4)
	}
	if got := parseDetailInt(detail, "missing"); got != 0 {
		t.Fatalf("missing key should return 0, got=%d", got)
	}
}

func TestLatestPermissionErrorSummary(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	lines := []string{
		`{"time_utc":"2026-02-20T00:00:00Z","type":"busy_wait_detected","result":"detected"}`,
		`{"time_utc":"2026-02-20T00:01:00Z","type":"process_permission_error","error":"operation not permitted","detail":"streak=3; wait_sec=20; role_scope=all"}`,
	}
	if err := os.WriteFile(paths.BusyWaitEventsFile, []byte(lines[0]+"\n"+lines[1]+"\n"), 0o644); err != nil {
		t.Fatalf("write events file: %v", err)
	}

	streak, cause := latestPermissionErrorSummary(paths.BusyWaitEventsFile)
	if streak != 3 {
		t.Fatalf("streak mismatch: got=%d want=%d", streak, 3)
	}
	if cause != "operation not permitted" {
		t.Fatalf("cause mismatch: got=%q", cause)
	}
}

func TestCodexRetryCountFromLog(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	logPath := filepath.Join(dir, "run.log")
	content := "" +
		"[ralph] codex attempt 1/3\n" +
		"[ralph] codex attempt 1 failed (codex_exit_1); retrying in 10s\n" +
		"[ralph] codex attempt 2/3\n" +
		"[ralph] codex attempt 3/3\n"
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write log file: %v", err)
	}
	if got := codexRetryCountFromLog(logPath); got != 2 {
		t.Fatalf("retry count mismatch: got=%d want=%d", got, 2)
	}
}

func TestLatestBlockedFailure(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	blockedPath := filepath.Join(paths.BlockedDir, "I-20260220T000000Z-0001.md")
	content := "" +
		"id: I-20260220T000000Z-0001\n" +
		"role: developer\n" +
		"status: blocked\n" +
		"title: t\n\n" +
		"## Ralph Result\n" +
		"- status: blocked\n" +
		"- reason: codex_failed_after_3_attempts: codex_exit_1\n" +
		"- log_file: /tmp/test.log\n" +
		"- updated_at_utc: 2026-02-20T00:10:00Z\n"
	if err := os.WriteFile(blockedPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write blocked issue: %v", err)
	}

	reason, updatedAt, logFile := latestBlockedFailure(paths.BlockedDir)
	if reason == "" || updatedAt == "" || logFile == "" {
		t.Fatalf("latestBlockedFailure should return non-empty fields")
	}
}
