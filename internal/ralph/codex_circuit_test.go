package ralph

import (
	"testing"
	"time"
)

func TestLoadSaveCodexCircuitState(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	want := CodexCircuitState{
		ConsecutiveFailures: 3,
		OpenUntil:           time.Date(2026, time.February, 20, 10, 0, 0, 0, time.UTC),
		LastFailure:         "codex_failed_after_3_attempts",
		LastOpenedAt:        time.Date(2026, time.February, 20, 9, 59, 0, 0, time.UTC),
		LastSuccessAt:       time.Date(2026, time.February, 20, 9, 0, 0, 0, time.UTC),
	}
	if err := SaveCodexCircuitState(paths, want); err != nil {
		t.Fatalf("save codex circuit state: %v", err)
	}

	got, err := LoadCodexCircuitState(paths)
	if err != nil {
		t.Fatalf("load codex circuit state: %v", err)
	}
	if got.ConsecutiveFailures != want.ConsecutiveFailures {
		t.Fatalf("consecutive failures mismatch: got=%d want=%d", got.ConsecutiveFailures, want.ConsecutiveFailures)
	}
	if !got.OpenUntil.Equal(want.OpenUntil) {
		t.Fatalf("open until mismatch: got=%s want=%s", got.OpenUntil.Format(time.RFC3339), want.OpenUntil.Format(time.RFC3339))
	}
	if got.LastFailure != want.LastFailure {
		t.Fatalf("last failure mismatch: got=%q want=%q", got.LastFailure, want.LastFailure)
	}
}

func TestCodexCircuitIsOpen(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 20, 10, 0, 0, 0, time.UTC)
	state := CodexCircuitState{
		OpenUntil: now.Add(30 * time.Second),
	}
	if !state.IsOpen(now) {
		t.Fatalf("circuit should be open")
	}
	if state.IsOpen(now.Add(31 * time.Second)) {
		t.Fatalf("circuit should be closed after open_until")
	}
}
