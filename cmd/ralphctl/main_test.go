package main

import (
	"os"
	"path/filepath"
	"testing"
	"unicode/utf8"

	"codex-ralph/internal/ralph"
)

func TestDefaultControlDirUsesHome(t *testing.T) {
	t.Setenv("HOME", "/tmp/ralph-home")
	got := defaultControlDir("/tmp/fallback")
	want := filepath.Join("/tmp/ralph-home", ".ralph-control")
	if got != want {
		t.Fatalf("defaultControlDir mismatch: got=%q want=%q", got, want)
	}
}

func TestCommandNeedsControlAssets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		cmd  string
		want bool
	}{
		{cmd: "setup", want: true},
		{cmd: "reload", want: true},
		{cmd: "fleet", want: true},
		{cmd: "registry", want: true},
		{cmd: "service", want: true},
		{cmd: "telegram", want: true},
		{cmd: "status", want: false},
		{cmd: "run", want: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := commandNeedsControlAssets(tc.cmd); got != tc.want {
				t.Fatalf("commandNeedsControlAssets(%q)=%t want=%t", tc.cmd, got, tc.want)
			}
		})
	}
}

func TestCompactSingleLine(t *testing.T) {
	t.Parallel()

	got := compactSingleLine(" a\nb   c ", 4)
	if got != "a..." {
		t.Fatalf("compactSingleLine mismatch: got=%q want=%q", got, "a...")
	}
}

func TestCompactSingleLineUnicodeSafe(t *testing.T) {
	t.Parallel()

	// 4-byte emoji + Korean should not be cut mid-rune.
	got := compactSingleLine("🔥비트코인 자동화", 5)
	if !utf8.ValidString(got) {
		t.Fatalf("output must be valid UTF-8: %q", got)
	}
	if got == "" {
		t.Fatalf("output should not be empty")
	}
}

func TestCompactSingleLineInvalidUTF8Sanitized(t *testing.T) {
	t.Parallel()

	raw := string([]byte{0xff, 0xfe, 'a', 'b', 'c'})
	got := compactSingleLine(raw, 10)
	if !utf8.ValidString(got) {
		t.Fatalf("output must be valid UTF-8: %q", got)
	}
	if got == "" {
		t.Fatalf("output should not be empty")
	}
}

func TestResolveReloadTargetsFleetOnlyWhenCurrentUnmanaged(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	controlDir := filepath.Join(root, "control")
	currentDir := filepath.Join(root, "current")
	fleetDir := filepath.Join(root, "fleet-a")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("mkdir control: %v", err)
	}
	if err := os.MkdirAll(currentDir, 0o755); err != nil {
		t.Fatalf("mkdir current: %v", err)
	}
	if err := os.MkdirAll(fleetDir, 0o755); err != nil {
		t.Fatalf("mkdir fleet: %v", err)
	}

	currentPaths, err := ralph.NewPaths(controlDir, currentDir)
	if err != nil {
		t.Fatalf("new current paths: %v", err)
	}

	cfg := ralph.FleetConfig{
		Version: 1,
		Projects: []ralph.FleetProject{
			{
				ID:            "wallet",
				ProjectDir:    fleetDir,
				Plugin:        "universal-default",
				AssignedRoles: append([]string(nil), ralph.RequiredAgentRoles...),
			},
		},
	}
	if err := ralph.SaveFleetConfig(controlDir, cfg); err != nil {
		t.Fatalf("save fleet config: %v", err)
	}

	targets, err := resolveReloadTargets(controlDir, currentPaths, false)
	if err != nil {
		t.Fatalf("resolve reload targets: %v", err)
	}
	if len(targets) != 1 {
		t.Fatalf("target count mismatch: got=%d want=1", len(targets))
	}
	if targets[0].ID != "wallet" {
		t.Fatalf("target id mismatch: got=%s want=wallet", targets[0].ID)
	}
}

func TestResolveReloadTargetsCurrentOnlyRequiresManagedProject(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	controlDir := filepath.Join(root, "control")
	currentDir := filepath.Join(root, "current")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("mkdir control: %v", err)
	}
	if err := os.MkdirAll(currentDir, 0o755); err != nil {
		t.Fatalf("mkdir current: %v", err)
	}
	currentPaths, err := ralph.NewPaths(controlDir, currentDir)
	if err != nil {
		t.Fatalf("new current paths: %v", err)
	}

	if _, err := resolveReloadTargets(controlDir, currentPaths, true); err == nil {
		t.Fatalf("expected error for unmanaged current project")
	}
}

func TestResolveRunEngineAutoFromCutover(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	controlDir := filepath.Join(root, "control")
	projectDir := filepath.Join(root, "project")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("mkdir control: %v", err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project: %v", err)
	}

	got, state, err := resolveRunEngine(projectDir, "auto")
	if err != nil {
		t.Fatalf("resolveRunEngine(auto) failed: %v", err)
	}
	if got != "v1" {
		t.Fatalf("engine mismatch: got=%s want=v1", got)
	}
	if state.Mode != "v1" {
		t.Fatalf("cutover mode mismatch: got=%s want=v1", state.Mode)
	}

	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	got, state, err = resolveRunEngine(projectDir, "auto")
	if err != nil {
		t.Fatalf("resolveRunEngine(auto) after v2 failed: %v", err)
	}
	if got != "v2" {
		t.Fatalf("engine mismatch after v2: got=%s want=v2", got)
	}
	if state.Mode != "v2" {
		t.Fatalf("cutover mode mismatch after v2: got=%s want=v2", state.Mode)
	}
}
