package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"codex-ralph/internal/ralph"
)

func TestEnsureFleetRegistrationOnSetupAutoRegister(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	controlDir := filepath.Join(root, "control")
	projectDir := filepath.Join(root, "btc-service")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project: %v", err)
	}
	writeTestPlugin(t, controlDir, "universal-default")

	paths, err := ralph.NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths: %v", err)
	}
	if err := ralph.EnsureLayout(paths); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}

	got, err := ensureFleetRegistrationOnSetup(controlDir, paths, "", "PRD.md")
	if err != nil {
		t.Fatalf("ensure fleet registration: %v", err)
	}
	if got.Status != "registered" {
		t.Fatalf("status mismatch: got=%q want=%q", got.Status, "registered")
	}
	if got.Project.ID != "btc-service" {
		t.Fatalf("project id mismatch: got=%q want=%q", got.Project.ID, "btc-service")
	}
	if got.BootstrapCreated != len(ralph.RequiredAgentRoles) {
		t.Fatalf("bootstrap count mismatch: got=%d want=%d", got.BootstrapCreated, len(ralph.RequiredAgentRoles))
	}

	cfg, err := ralph.LoadFleetConfig(controlDir)
	if err != nil {
		t.Fatalf("load fleet config: %v", err)
	}
	if len(cfg.Projects) != 1 {
		t.Fatalf("fleet project count mismatch: got=%d want=1", len(cfg.Projects))
	}
}

func TestEnsureFleetRegistrationOnSetupIsIdempotent(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	controlDir := filepath.Join(root, "control")
	projectDir := filepath.Join(root, "wallet")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project: %v", err)
	}
	writeTestPlugin(t, controlDir, "universal-default")

	paths, err := ralph.NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths: %v", err)
	}
	if err := ralph.EnsureLayout(paths); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}

	first, err := ensureFleetRegistrationOnSetup(controlDir, paths, "", "PRD.md")
	if err != nil {
		t.Fatalf("first registration: %v", err)
	}
	second, err := ensureFleetRegistrationOnSetup(controlDir, paths, "", "PRD.md")
	if err != nil {
		t.Fatalf("second registration: %v", err)
	}
	if second.Status != "already-registered" {
		t.Fatalf("status mismatch: got=%q want=%q", second.Status, "already-registered")
	}
	if second.Project.ID != first.Project.ID {
		t.Fatalf("project id changed: first=%q second=%q", first.Project.ID, second.Project.ID)
	}
	if second.BootstrapCreated != 0 {
		t.Fatalf("bootstrap count mismatch: got=%d want=0", second.BootstrapCreated)
	}

	cfg, err := ralph.LoadFleetConfig(controlDir)
	if err != nil {
		t.Fatalf("load fleet config: %v", err)
	}
	if len(cfg.Projects) != 1 {
		t.Fatalf("fleet project count mismatch: got=%d want=1", len(cfg.Projects))
	}
}

func TestEnsureFleetRegistrationOnSetupRejectsMismatchedID(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	controlDir := filepath.Join(root, "control")
	projectDir := filepath.Join(root, "indexer")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project: %v", err)
	}
	writeTestPlugin(t, controlDir, "universal-default")

	paths, err := ralph.NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths: %v", err)
	}
	if err := ralph.EnsureLayout(paths); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}

	if _, err := ensureFleetRegistrationOnSetup(controlDir, paths, "alpha", "PRD.md"); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if _, err := ensureFleetRegistrationOnSetup(controlDir, paths, "beta", "PRD.md"); err == nil {
		t.Fatalf("expected mismatch error")
	}
}

func TestSuggestFleetProjectIDWhenBaseExists(t *testing.T) {
	t.Parallel()

	cfg := ralph.FleetConfig{
		Projects: []ralph.FleetProject{
			{ID: "project"},
		},
	}
	got := suggestFleetProjectID(cfg, "/tmp/project")
	if !strings.HasPrefix(got, "project-") {
		t.Fatalf("unexpected suggested id: %q", got)
	}
}

func writeTestPlugin(t *testing.T, controlDir, pluginName string) {
	t.Helper()

	pluginDir := filepath.Join(controlDir, "plugins", pluginName)
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatalf("mkdir plugin dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, "plugin.env"), []byte("PLUGIN="+pluginName+"\n"), 0o644); err != nil {
		t.Fatalf("write plugin env: %v", err)
	}
}
