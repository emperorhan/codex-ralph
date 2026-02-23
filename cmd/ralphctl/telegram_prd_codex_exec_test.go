package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"codex-ralph/internal/ralph"
)

func TestResolveTelegramCodexProjectDir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	projectDir := filepath.Join(root, "project")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}

	got, ok := resolveTelegramCodexProjectDir(projectDir)
	if !ok {
		t.Fatalf("expected existing directory to resolve")
	}
	want, err := filepath.Abs(projectDir)
	if err != nil {
		t.Fatalf("abs project dir: %v", err)
	}
	if got != want {
		t.Fatalf("resolved project dir mismatch: got=%q want=%q", got, want)
	}
}

func TestResolveTelegramCodexProjectDirMissing(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	missing := filepath.Join(root, "missing-project")
	if got, ok := resolveTelegramCodexProjectDir(missing); ok || got != "" {
		t.Fatalf("expected unresolved missing directory, got=%q ok=%t", got, ok)
	}
}

func TestBuildTelegramCodexExecArgsOmitsCDWhenProjectMissing(t *testing.T) {
	t.Parallel()

	profile := ralph.DefaultProfile()
	args := buildTelegramCodexExecArgs(profile, "gpt-5.3-codex", "", "/tmp/out.txt")
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "--cd") {
		t.Fatalf("args should not contain --cd when project dir is empty: %v", args)
	}
	if !strings.Contains(joined, "--output-last-message /tmp/out.txt") {
		t.Fatalf("args should contain output path: %v", args)
	}
}
