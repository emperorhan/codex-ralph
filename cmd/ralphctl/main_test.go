package main

import (
	"path/filepath"
	"testing"
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
