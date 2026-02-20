package main

import (
	"os"
	"path/filepath"
	"strconv"
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

func TestParseTelegramTargetSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        string
		wantAll   bool
		wantID    string
		wantError bool
	}{
		{name: "empty", in: "", wantAll: false, wantID: ""},
		{name: "all", in: "all", wantAll: true, wantID: ""},
		{name: "star", in: "*", wantAll: true, wantID: ""},
		{name: "id", in: "wallet", wantAll: false, wantID: "wallet"},
		{name: "invalid multi", in: "all wallet", wantError: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			spec, err := parseTelegramTargetSpec(tt.in)
			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error for %q", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if spec.All != tt.wantAll || spec.ProjectID != tt.wantID {
				t.Fatalf("parseTelegramTargetSpec(%q)=(all=%t,id=%q) want=(all=%t,id=%q)", tt.in, spec.All, spec.ProjectID, tt.wantAll, tt.wantID)
			}
		})
	}
}

func TestNormalizeNotifyScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "", want: "auto"},
		{in: "auto", want: "auto"},
		{in: "project", want: "project"},
		{in: "fleet", want: "fleet"},
		{in: "invalid", wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, err := normalizeNotifyScope(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("normalizeNotifyScope(%q)=%q want=%q", tt.in, got, tt.want)
			}
		})
	}
}

func TestRequiresUserAllowlistForControl(t *testing.T) {
	t.Parallel()

	if requiresUserAllowlistForControl(map[int64]struct{}{12345: {}}) {
		t.Fatalf("private chat should not require user allowlist")
	}
	if !requiresUserAllowlistForControl(map[int64]struct{}{-10012345: {}}) {
		t.Fatalf("group chat should require user allowlist")
	}
	if !requiresUserAllowlistForControl(map[int64]struct{}{12345: {}, -200: {}}) {
		t.Fatalf("mixed chats should require user allowlist")
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
		UserIDs:                   "2001,2002",
		AllowControl:              true,
		Notify:                    true,
		NotifyScope:               "fleet",
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
	if got.UserIDs != want.UserIDs {
		t.Fatalf("user ids mismatch: got=%q want=%q", got.UserIDs, want.UserIDs)
	}
	if got.AllowControl != want.AllowControl {
		t.Fatalf("allow control mismatch: got=%t want=%t", got.AllowControl, want.AllowControl)
	}
	if got.Notify != want.Notify {
		t.Fatalf("notify mismatch: got=%t want=%t", got.Notify, want.Notify)
	}
	if got.NotifyScope != want.NotifyScope {
		t.Fatalf("notify scope mismatch: got=%q want=%q", got.NotifyScope, want.NotifyScope)
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

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat telegram config failed: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("telegram config mode mismatch: got=%#o want=%#o", info.Mode().Perm(), 0o600)
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
		QueueReady:             1,
		InProgress:             1,
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

func TestBuildStatusAlertsSkipsStuckWhenNoWork(t *testing.T) {
	t.Parallel()

	prev := ralph.Status{
		ProjectDir:             "/tmp/p",
		LastBusyWaitDetectedAt: "",
	}
	curr := ralph.Status{
		ProjectDir:             "/tmp/p",
		QueueReady:             0,
		InProgress:             0,
		LastBusyWaitDetectedAt: "2026-02-20T10:00:00Z",
		LastBusyWaitIdleCount:  12,
	}

	alerts := buildStatusAlerts(prev, curr, 2, 3)
	joined := strings.Join(alerts, "\n")
	if strings.Contains(joined, "[stuck]") {
		t.Fatalf("stuck alert should be suppressed when queue is empty: %q", joined)
	}
}

func TestBuildStatusAlertsInputRequiredTransition(t *testing.T) {
	t.Parallel()

	prev := ralph.Status{
		ProjectDir: "/tmp/p",
		QueueReady: 1,
		InProgress: 0,
		Blocked:    0,
	}
	curr := ralph.Status{
		ProjectDir: "/tmp/p",
		QueueReady: 0,
		InProgress: 0,
		Blocked:    0,
	}

	alerts := buildStatusAlerts(prev, curr, 2, 3)
	joined := strings.Join(alerts, "\n")
	if !strings.Contains(joined, "[input_required]") {
		t.Fatalf("input_required alert should be emitted on transition: %q", joined)
	}
}

func TestEnsureTelegramForegroundArg(t *testing.T) {
	t.Parallel()

	original := []string{"--config-file", "/tmp/telegram.env"}
	got := ensureTelegramForegroundArg(original)
	if len(got) != len(original)+1 {
		t.Fatalf("length mismatch: got=%d want=%d", len(got), len(original)+1)
	}
	if got[len(got)-1] != "--foreground" {
		t.Fatalf("last arg mismatch: got=%q want=--foreground", got[len(got)-1])
	}
	if len(original) != 2 {
		t.Fatalf("original slice should not be mutated")
	}
}

func TestTelegramPIDState(t *testing.T) {
	t.Parallel()

	pidFile := filepath.Join(t.TempDir(), "telegram.pid")

	pid, running, stale := telegramPIDState(pidFile)
	if pid != 0 || running || stale {
		t.Fatalf("missing pid file should be stopped/non-stale: pid=%d running=%t stale=%t", pid, running, stale)
	}

	if err := os.WriteFile(pidFile, []byte("invalid\n"), 0o644); err != nil {
		t.Fatalf("write invalid pid file: %v", err)
	}
	_, running, stale = telegramPIDState(pidFile)
	if running || !stale {
		t.Fatalf("invalid pid file should be stale")
	}

	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		t.Fatalf("write running pid file: %v", err)
	}
	pid, running, stale = telegramPIDState(pidFile)
	if pid != os.Getpid() || !running || stale {
		t.Fatalf("running pid mismatch: pid=%d running=%t stale=%t", pid, running, stale)
	}

	if err := os.WriteFile(pidFile, []byte("999999\n"), 0o644); err != nil {
		t.Fatalf("write stale pid file: %v", err)
	}
	_, running, stale = telegramPIDState(pidFile)
	if running || !stale {
		t.Fatalf("non-running pid file should be stale")
	}
}
