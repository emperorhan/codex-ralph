package main

import "testing"

func TestResolveTelegramCodexTimeoutSec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    int
		fallback int
		want     int
	}{
		{name: "uses configured", input: 300, fallback: 90, want: 300},
		{name: "uses fallback when configured empty", input: 0, fallback: 90, want: 90},
		{name: "uses hard default when both empty", input: 0, fallback: 0, want: 60},
		{name: "caps max timeout", input: 3600, fallback: 90, want: telegramCodexTimeoutCapSec},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := resolveTelegramCodexTimeoutSec(tt.input, tt.fallback)
			if got != tt.want {
				t.Fatalf("resolve timeout mismatch: got=%d want=%d", got, tt.want)
			}
		})
	}
}

