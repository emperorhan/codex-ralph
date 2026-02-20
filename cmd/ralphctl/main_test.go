package main

import (
	"os"
	"path/filepath"
	"testing"

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
