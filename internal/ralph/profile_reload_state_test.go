package ralph

import (
	"testing"
	"time"
)

func TestProfileReloadStateSaveLoad(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	want := ProfileReloadState{
		LastReloadAt: time.Date(2026, time.February, 20, 8, 0, 0, 0, time.UTC),
		ReloadCount:  3,
		LastSummary:  "plugin=universal-default codex_model=auto",
	}
	if err := SaveProfileReloadState(paths, want); err != nil {
		t.Fatalf("save profile reload state: %v", err)
	}

	got, err := LoadProfileReloadState(paths)
	if err != nil {
		t.Fatalf("load profile reload state: %v", err)
	}
	if got.LastReloadAt.Format(time.RFC3339) != want.LastReloadAt.Format(time.RFC3339) {
		t.Fatalf("last reload mismatch: got=%q want=%q", got.LastReloadAt.Format(time.RFC3339), want.LastReloadAt.Format(time.RFC3339))
	}
	if got.ReloadCount != want.ReloadCount {
		t.Fatalf("reload count mismatch: got=%d want=%d", got.ReloadCount, want.ReloadCount)
	}
	if got.LastSummary != want.LastSummary {
		t.Fatalf("last summary mismatch: got=%q want=%q", got.LastSummary, want.LastSummary)
	}
}

func TestGetStatusIncludesProfileReloadState(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	at := time.Date(2026, time.February, 20, 8, 10, 0, 0, time.UTC)
	if err := SaveProfileReloadState(paths, ProfileReloadState{
		LastReloadAt: at,
		ReloadCount:  2,
	}); err != nil {
		t.Fatalf("save profile reload state: %v", err)
	}

	st, err := GetStatus(paths)
	if err != nil {
		t.Fatalf("get status: %v", err)
	}
	if st.LastProfileReloadAt != at.Format(time.RFC3339) {
		t.Fatalf("last_profile_reload_at mismatch: got=%q want=%q", st.LastProfileReloadAt, at.Format(time.RFC3339))
	}
	if st.ProfileReloadCount != 2 {
		t.Fatalf("profile_reload_count mismatch: got=%d want=%d", st.ProfileReloadCount, 2)
	}
}
