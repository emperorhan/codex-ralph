package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultTelegramOffsetFileIsProjectScoped(t *testing.T) {
	t.Parallel()

	controlDir := filepath.Join(t.TempDir(), "control")
	p1 := filepath.Join(t.TempDir(), "project-a")
	p2 := filepath.Join(t.TempDir(), "project-b")

	o1 := defaultTelegramOffsetFile(controlDir, p1)
	o2 := defaultTelegramOffsetFile(controlDir, p2)
	if o1 == o2 {
		t.Fatalf("offset paths should differ by project: %q", o1)
	}
	if !strings.Contains(o1, filepath.Join("telegram-offsets", "")) {
		t.Fatalf("offset path should use telegram-offsets dir: %q", o1)
	}
}

func TestEnsureTelegramTokenBoundEnforcesOneBotOneProject(t *testing.T) {
	t.Parallel()

	controlDir := filepath.Join(t.TempDir(), "control")
	token := "123456:ABCDEF"
	projectA := filepath.Join(t.TempDir(), "project-a")
	projectB := filepath.Join(t.TempDir(), "project-b")

	if err := ensureTelegramTokenBound(controlDir, token, projectA, false); err != nil {
		t.Fatalf("bind token to projectA failed: %v", err)
	}

	if err := ensureTelegramTokenBound(controlDir, token, projectA, false); err != nil {
		t.Fatalf("rebind same token to same project should pass: %v", err)
	}

	err := ensureTelegramTokenBound(controlDir, token, projectB, false)
	if err == nil {
		t.Fatalf("expected conflict when binding same token to another project")
	}
	if !strings.Contains(err.Error(), "1 bot = 1 project") {
		t.Fatalf("unexpected conflict error: %v", err)
	}

	if err := ensureTelegramTokenBound(controlDir, token, projectB, true); err != nil {
		t.Fatalf("rebind should pass with --rebind-bot semantics: %v", err)
	}
}

func TestTelegramTokenBindingStoreDoesNotPersistRawToken(t *testing.T) {
	t.Parallel()

	controlDir := filepath.Join(t.TempDir(), "control")
	token := "999:SECRET_TOKEN"
	projectDir := filepath.Join(t.TempDir(), "project")

	if err := ensureTelegramTokenBound(controlDir, token, projectDir, false); err != nil {
		t.Fatalf("bind token failed: %v", err)
	}

	data, err := os.ReadFile(telegramTokenBindingsPath(controlDir))
	if err != nil {
		t.Fatalf("read binding file failed: %v", err)
	}
	if strings.Contains(string(data), token) {
		t.Fatalf("binding file should not contain raw token")
	}
	info, err := os.Stat(telegramTokenBindingsPath(controlDir))
	if err != nil {
		t.Fatalf("stat binding file failed: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("binding file mode mismatch: got=%#o want=%#o", info.Mode().Perm(), 0o600)
	}
}
