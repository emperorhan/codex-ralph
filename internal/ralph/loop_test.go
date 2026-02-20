package ralph

import (
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
