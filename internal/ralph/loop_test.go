package ralph

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCodexRetryBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		baseSec int
		attempt int
		want    int
	}{
		{name: "zero-base", baseSec: 0, attempt: 1, want: 0},
		{name: "first-attempt", baseSec: 10, attempt: 1, want: 10},
		{name: "double-on-second-attempt", baseSec: 10, attempt: 2, want: 20},
		{name: "double-on-third-attempt", baseSec: 10, attempt: 3, want: 40},
		{name: "cap-to-300", baseSec: 100, attempt: 3, want: 300},
		{name: "cap-even-on-first-attempt", baseSec: 400, attempt: 1, want: 300},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := codexRetryBackoff(tt.baseSec, tt.attempt)
			if got != tt.want {
				t.Fatalf("codexRetryBackoff(%d, %d)=%d want=%d", tt.baseSec, tt.attempt, got, tt.want)
			}
		})
	}
}

func TestShouldRunWatchdogScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tickCount int
		scanLoops int
		want      bool
	}{
		{name: "scan-loops-zero", tickCount: 5, scanLoops: 0, want: true},
		{name: "scan-loops-one", tickCount: 5, scanLoops: 1, want: true},
		{name: "scan-loops-three-not-divisible", tickCount: 5, scanLoops: 3, want: false},
		{name: "scan-loops-three-divisible", tickCount: 6, scanLoops: 3, want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := shouldRunWatchdogScan(tt.tickCount, tt.scanLoops)
			if got != tt.want {
				t.Fatalf("shouldRunWatchdogScan(%d, %d)=%t want=%t", tt.tickCount, tt.scanLoops, got, tt.want)
			}
		})
	}
}

func TestCanRunBusyWaitSelfHeal(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.February, 20, 6, 0, 0, 0, time.UTC)

	disabledProfile := DefaultProfile()
	disabledProfile.BusyWaitSelfHealEnabled = false
	if ok, reason := canRunBusyWaitSelfHeal(now, BusyWaitState{}, disabledProfile); ok || !strings.Contains(reason, "disabled") {
		t.Fatalf("disabled case mismatch: ok=%t reason=%q", ok, reason)
	}

	maxAttemptProfile := DefaultProfile()
	maxAttemptProfile.BusyWaitSelfHealEnabled = true
	maxAttemptProfile.BusyWaitSelfHealMaxAttempts = 2
	maxAttemptState := BusyWaitState{SelfHealAttempts: 2}
	if ok, reason := canRunBusyWaitSelfHeal(now, maxAttemptState, maxAttemptProfile); ok || !strings.Contains(reason, "max attempts reached") {
		t.Fatalf("max-attempt case mismatch: ok=%t reason=%q", ok, reason)
	}

	cooldownProfile := DefaultProfile()
	cooldownProfile.BusyWaitSelfHealEnabled = true
	cooldownProfile.BusyWaitSelfHealMaxAttempts = 0
	cooldownProfile.BusyWaitSelfHealCooldownSec = 120
	cooldownState := BusyWaitState{LastSelfHealAt: now.Add(-30 * time.Second)}
	if ok, reason := canRunBusyWaitSelfHeal(now, cooldownState, cooldownProfile); ok || !strings.Contains(reason, "cooldown active") {
		t.Fatalf("cooldown case mismatch: ok=%t reason=%q", ok, reason)
	}

	readyState := BusyWaitState{LastSelfHealAt: now.Add(-5 * time.Minute)}
	if ok, reason := canRunBusyWaitSelfHeal(now, readyState, cooldownProfile); !ok || reason != "" {
		t.Fatalf("ready case mismatch: ok=%t reason=%q", ok, reason)
	}
}

func TestClassifyCodexFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		exitCode  int
		output    string
		want      string
		retryable bool
	}{
		{name: "auth", exitCode: 1, output: "not logged in, run: codex login", want: "codex_auth_error", retryable: false},
		{name: "invalid-args", exitCode: 2, output: "unknown option --foo", want: "codex_invalid_args", retryable: false},
		{name: "model", exitCode: 1, output: "unknown model gpt-x", want: "codex_model_error", retryable: false},
		{name: "permission", exitCode: 1, output: "operation not permitted", want: "codex_permission_denied", retryable: false},
		{name: "cancel", exitCode: 130, output: "", want: "codex_canceled", retryable: false},
		{name: "transient", exitCode: 1, output: "temporary network issue", want: "", retryable: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, retryable := classifyCodexFailure(tt.exitCode, strings.ToLower(tt.output))
			if got != tt.want || retryable != tt.retryable {
				t.Fatalf("classifyCodexFailure(%d, %q)=(%q,%t) want=(%q,%t)", tt.exitCode, tt.output, got, retryable, tt.want, tt.retryable)
			}
		})
	}
}

func TestTailBufferKeepsSuffix(t *testing.T) {
	t.Parallel()

	b := newTailBuffer(10)
	_, _ = b.Write([]byte("hello"))
	_, _ = b.Write([]byte(" world"))
	if got := b.String(); got != "ello world" {
		t.Fatalf("tail buffer mismatch: got=%q want=%q", got, "ello world")
	}
}

func TestIsLikelyPermissionErr(t *testing.T) {
	t.Parallel()

	if !isLikelyPermissionErr(os.ErrPermission) {
		t.Fatalf("os.ErrPermission should be detected")
	}
	if !isLikelyPermissionErr(errors.New("operation not permitted while opening file")) {
		t.Fatalf("operation-not-permitted message should be detected")
	}
	if isLikelyPermissionErr(errors.New("temporary network failure")) {
		t.Fatalf("non-permission error should not be detected")
	}
}

func TestPermissionErrorBackoffSec(t *testing.T) {
	t.Parallel()

	if got := permissionErrorBackoffSec(2, 1); got != 5 {
		t.Fatalf("streak1 backoff mismatch: got=%d want=%d", got, 5)
	}
	if got := permissionErrorBackoffSec(20, 2); got != 40 {
		t.Fatalf("streak2 backoff mismatch: got=%d want=%d", got, 40)
	}
	if got := permissionErrorBackoffSec(20, 10); got != 300 {
		t.Fatalf("cap mismatch: got=%d want=%d", got, 300)
	}
}

func TestReloadLoopProfileUnchanged(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	current, err := LoadProfile(paths)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	next, changed, err := reloadLoopProfile(paths, current)
	if err != nil {
		t.Fatalf("reload profile: %v", err)
	}
	if changed {
		t.Fatalf("reload should not report change")
	}
	if next.CodexModel != current.CodexModel {
		t.Fatalf("profile should remain unchanged: got=%q want=%q", next.CodexModel, current.CodexModel)
	}
}

func TestReloadLoopProfileChanged(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	current, err := LoadProfile(paths)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}

	writeFile(t, paths.ProfileLocalYAMLFile, `
codex_model_developer: gpt-5.3-codex-spark
idle_sleep_sec: 5
`)

	next, changed, err := reloadLoopProfile(paths, current)
	if err != nil {
		t.Fatalf("reload profile: %v", err)
	}
	if !changed {
		t.Fatalf("reload should report changed profile")
	}
	if next.CodexModelDeveloper != "gpt-5.3-codex-spark" {
		t.Fatalf("codex_model_developer mismatch: got=%q want=%q", next.CodexModelDeveloper, "gpt-5.3-codex-spark")
	}
	if next.IdleSleepSec != 5 {
		t.Fatalf("idle_sleep_sec mismatch: got=%d want=5", next.IdleSleepSec)
	}
}

func TestProfileReloadSummary(t *testing.T) {
	p := DefaultProfile()
	p.PluginName = "universal-default"
	p.CodexModel = "auto"
	p.CodexModelDeveloper = ""
	p.IdleSleepSec = 20
	p.CodexRetryMaxAttempts = 3
	p.CodexExecTimeoutSec = 900

	s := profileReloadSummary(p)
	if !strings.Contains(s, "codex_model=auto") {
		t.Fatalf("summary should include auto model: %q", s)
	}
	if !strings.Contains(s, "codex_model_developer=(inherit)") {
		t.Fatalf("summary should include inherit marker: %q", s)
	}
}
