package main

import (
	"path/filepath"
	"strings"
	"testing"

	"codex-ralph/internal/ralph"
)

func TestParseTelegramCommandLine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in       string
		wantCmd  string
		wantArgs string
	}{
		{in: "/status", wantCmd: "/status", wantArgs: ""},
		{in: "/status@ralphbot", wantCmd: "/status", wantArgs: ""},
		{in: "status", wantCmd: "/status", wantArgs: ""},
		{in: "/doctor_repair now", wantCmd: "/doctor_repair", wantArgs: "now"},
		{in: "   ", wantCmd: "", wantArgs: ""},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			gotCmd, gotArgs := parseTelegramCommandLine(tt.in)
			if gotCmd != tt.wantCmd || gotArgs != tt.wantArgs {
				t.Fatalf("parseTelegramCommandLine(%q)=(%q,%q) want=(%q,%q)", tt.in, gotCmd, gotArgs, tt.wantCmd, tt.wantArgs)
			}
		})
	}
}

func TestEnvBoolDefault(t *testing.T) {
	t.Setenv("RALPH_TELEGRAM_ALLOW_CONTROL", "true")
	if !envBoolDefault("RALPH_TELEGRAM_ALLOW_CONTROL", false) {
		t.Fatalf("expected true")
	}
	t.Setenv("RALPH_TELEGRAM_ALLOW_CONTROL", "invalid")
	if !envBoolDefault("RALPH_TELEGRAM_ALLOW_CONTROL", true) {
		t.Fatalf("invalid value should fallback to default")
	}
}

func TestEnvIntDefault(t *testing.T) {
	t.Setenv("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", "45")
	if got := envIntDefault("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", 30); got != 45 {
		t.Fatalf("envIntDefault mismatch: got=%d want=45", got)
	}
	t.Setenv("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", "invalid")
	if got := envIntDefault("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", 30); got != 30 {
		t.Fatalf("envIntDefault invalid fallback mismatch: got=%d want=30", got)
	}
}

func TestTelegramConfigFileFromArgs(t *testing.T) {
	t.Parallel()

	controlDir := "/tmp/ralph-control"
	if got := telegramConfigFileFromArgs(controlDir, nil); got != filepath.Join(controlDir, "telegram.env") {
		t.Fatalf("default config path mismatch: got=%q", got)
	}
	if got := telegramConfigFileFromArgs(controlDir, []string{"--config-file=/tmp/custom.env"}); got != "/tmp/custom.env" {
		t.Fatalf("inline config path mismatch: got=%q", got)
	}
	if got := telegramConfigFileFromArgs(controlDir, []string{"--config-file", "/tmp/custom2.env"}); got != "/tmp/custom2.env" {
		t.Fatalf("split config path mismatch: got=%q", got)
	}
}

func TestSaveLoadTelegramCLIConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "telegram.env")
	want := telegramCLIConfig{
		Token:                     "123456:ABC-DEF",
		ChatIDs:                   "1001,1002",
		AllowControl:              true,
		Notify:                    true,
		NotifyIntervalSec:         45,
		NotifyRetryThreshold:      3,
		NotifyPermStreakThreshold: 5,
	}
	if err := saveTelegramCLIConfig(path, want); err != nil {
		t.Fatalf("saveTelegramCLIConfig failed: %v", err)
	}
	got, err := loadTelegramCLIConfig(path)
	if err != nil {
		t.Fatalf("loadTelegramCLIConfig failed: %v", err)
	}

	if got.Token != want.Token {
		t.Fatalf("token mismatch: got=%q want=%q", got.Token, want.Token)
	}
	if got.ChatIDs != want.ChatIDs {
		t.Fatalf("chat ids mismatch: got=%q want=%q", got.ChatIDs, want.ChatIDs)
	}
	if got.AllowControl != want.AllowControl {
		t.Fatalf("allow control mismatch: got=%t want=%t", got.AllowControl, want.AllowControl)
	}
	if got.Notify != want.Notify {
		t.Fatalf("notify mismatch: got=%t want=%t", got.Notify, want.Notify)
	}
	if got.NotifyIntervalSec != want.NotifyIntervalSec {
		t.Fatalf("notify interval mismatch: got=%d want=%d", got.NotifyIntervalSec, want.NotifyIntervalSec)
	}
	if got.NotifyRetryThreshold != want.NotifyRetryThreshold {
		t.Fatalf("notify retry mismatch: got=%d want=%d", got.NotifyRetryThreshold, want.NotifyRetryThreshold)
	}
	if got.NotifyPermStreakThreshold != want.NotifyPermStreakThreshold {
		t.Fatalf("notify perm mismatch: got=%d want=%d", got.NotifyPermStreakThreshold, want.NotifyPermStreakThreshold)
	}
}

func TestBuildStatusAlerts(t *testing.T) {
	t.Parallel()

	prev := ralph.Status{
		ProjectDir:             "/tmp/p",
		Blocked:                1,
		LastFailureUpdatedAt:   "2026-02-20T08:00:00Z",
		LastCodexRetryCount:    0,
		LastBusyWaitDetectedAt: "",
		LastPermissionStreak:   0,
	}
	curr := ralph.Status{
		ProjectDir:             "/tmp/p",
		Blocked:                2,
		LastFailureCause:       "codex_failed_after_3_attempts",
		LastFailureUpdatedAt:   "2026-02-20T08:10:00Z",
		LastCodexRetryCount:    3,
		LastBusyWaitDetectedAt: "2026-02-20T08:11:00Z",
		LastBusyWaitIdleCount:  9,
		LastPermissionStreak:   4,
	}

	alerts := buildStatusAlerts(prev, curr, 2, 3)
	if len(alerts) < 4 {
		t.Fatalf("expected multiple alerts, got=%d", len(alerts))
	}
	joined := strings.Join(alerts, "\n")
	if !strings.Contains(joined, "[blocked]") {
		t.Fatalf("missing blocked alert: %q", joined)
	}
	if !strings.Contains(joined, "[retry]") {
		t.Fatalf("missing retry alert: %q", joined)
	}
	if !strings.Contains(joined, "[stuck]") {
		t.Fatalf("missing stuck alert: %q", joined)
	}
	if !strings.Contains(joined, "[permission]") {
		t.Fatalf("missing permission alert: %q", joined)
	}
}
