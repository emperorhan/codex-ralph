package ralph

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveCodexHomePathDefault(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	profile := DefaultProfile()
	got, err := ResolveCodexHomePath(paths, profile)
	if err != nil {
		t.Fatalf("resolve codex home path: %v", err)
	}
	want := filepath.Join(paths.ProjectDir, ".codex-home")
	if got != want {
		t.Fatalf("codex home default mismatch: got=%q want=%q", got, want)
	}
}

func TestResolveCodexHomePathRelativeOverride(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	profile := DefaultProfile()
	profile.CodexHome = ".custom-codex-home"
	got, err := ResolveCodexHomePath(paths, profile)
	if err != nil {
		t.Fatalf("resolve codex home path: %v", err)
	}
	want := filepath.Join(paths.ProjectDir, ".custom-codex-home")
	if got != want {
		t.Fatalf("codex home relative mismatch: got=%q want=%q", got, want)
	}
}

func TestEnsureCodexHomeSeedsAuthFromGlobal(t *testing.T) {
	paths := newTestPaths(t)
	profile := DefaultProfile()

	global := t.TempDir()
	writeFile(t, filepath.Join(global, "auth.json"), `{"token":"abc"}`)
	writeFile(t, filepath.Join(global, "config.toml"), "model = \"auto\"\n")
	t.Setenv("CODEX_HOME", global)

	codexHome, err := EnsureCodexHome(paths, profile)
	if err != nil {
		t.Fatalf("ensure codex home: %v", err)
	}
	if codexHome != filepath.Join(paths.ProjectDir, ".codex-home") {
		t.Fatalf("unexpected codex home path: %s", codexHome)
	}
	if _, err := os.Stat(filepath.Join(codexHome, "auth.json")); err != nil {
		t.Fatalf("seed auth.json missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(codexHome, "config.toml")); err != nil {
		t.Fatalf("seed config.toml missing: %v", err)
	}
}

func TestEnvWithCodexHome(t *testing.T) {
	t.Parallel()

	base := []string{
		"A=1",
		"CODEX_HOME=/tmp/old",
	}
	out := EnvWithCodexHome(base, "/tmp/new")
	found := false
	for _, line := range out {
		if line == "CODEX_HOME=/tmp/new" {
			found = true
		}
		if line == "CODEX_HOME=/tmp/old" {
			t.Fatalf("old CODEX_HOME should be replaced")
		}
	}
	if !found {
		t.Fatalf("new CODEX_HOME not found")
	}
}
