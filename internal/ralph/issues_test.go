package ralph

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRetryBlockedIssuesByReason(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	blockedPermission := filepath.Join(paths.BlockedDir, "I-20260222T000001Z-0001.md")
	blockedNetwork := filepath.Join(paths.BlockedDir, "I-20260222T000002Z-0002.md")
	writeFile(t, blockedPermission, ""+
		"id: I-20260222T000001Z-0001\n"+
		"role: developer\n"+
		"status: blocked\n"+
		"title: permission fail\n\n"+
		"## Ralph Result\n"+
		"- status: blocked\n"+
		"- reason: codex_permission_denied\n")
	writeFile(t, blockedNetwork, ""+
		"id: I-20260222T000002Z-0002\n"+
		"role: planner\n"+
		"status: blocked\n"+
		"title: network fail\n\n"+
		"## Ralph Result\n"+
		"- status: blocked\n"+
		"- reason: codex_failed_after_3_attempts: codex_exit_1\n")

	moved, err := RetryBlockedIssues(paths, "failed_after", 0)
	if err != nil {
		t.Fatalf("retry blocked issues: %v", err)
	}
	if moved != 1 {
		t.Fatalf("moved mismatch: got=%d want=1", moved)
	}

	if _, err := os.Stat(blockedNetwork); !os.IsNotExist(err) {
		t.Fatalf("network blocked file should be moved: err=%v", err)
	}
	if _, err := os.Stat(blockedPermission); err != nil {
		t.Fatalf("permission blocked file should remain: %v", err)
	}

	readyMoved := filepath.Join(paths.IssuesDir, "I-20260222T000002Z-0002.md")
	meta, err := ReadIssueMeta(readyMoved)
	if err != nil {
		t.Fatalf("read moved issue meta: %v", err)
	}
	if meta.Status != "ready" {
		t.Fatalf("moved issue status mismatch: got=%s want=ready", meta.Status)
	}
}

func TestRetryBlockedIssuesLimit(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	for i := 0; i < 3; i++ {
		name := filepath.Join(paths.BlockedDir, fmt.Sprintf("I-20260222T00000%dZ-000%d.md", i+1, i+1))
		writeFile(t, name, ""+
			fmt.Sprintf("id: I-20260222T00000%dZ-000%d\n", i+1, i+1)+
			"role: qa\n"+
			"status: blocked\n"+
			"title: blocked\n\n"+
			"## Ralph Result\n"+
			"- status: blocked\n"+
			"- reason: codex_network\n")
	}

	moved, err := RetryBlockedIssues(paths, "", 2)
	if err != nil {
		t.Fatalf("retry blocked issues with limit: %v", err)
	}
	if moved != 2 {
		t.Fatalf("moved mismatch: got=%d want=2", moved)
	}
}
