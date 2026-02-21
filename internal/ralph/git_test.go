package ralph

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func requireGitCommand(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git command not found")
	}
}

func TestEnsureProjectGitVersioningInitializesRepo(t *testing.T) {
	t.Parallel()
	requireGitCommand(t)

	paths := newTestPaths(t)
	if err := EnsureProjectGitVersioning(paths); err != nil {
		t.Fatalf("EnsureProjectGitVersioning failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(paths.ProjectDir, ".git")); err != nil {
		t.Fatalf("git repo should be initialized: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(paths.ProjectDir, ".gitignore"))
	if err != nil {
		t.Fatalf("read .gitignore failed: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, ".ralph/") {
		t.Fatalf(".gitignore must include .ralph/: %q", content)
	}

	if err := EnsureProjectGitVersioning(paths); err != nil {
		t.Fatalf("EnsureProjectGitVersioning should be idempotent: %v", err)
	}
	data, err = os.ReadFile(filepath.Join(paths.ProjectDir, ".gitignore"))
	if err != nil {
		t.Fatalf("read .gitignore #2 failed: %v", err)
	}
	if strings.Count(string(data), ".ralph/") != 1 {
		t.Fatalf(".gitignore should not duplicate .ralph/ entry: %q", string(data))
	}
}

func TestAutoCommitIssueChanges(t *testing.T) {
	t.Parallel()
	requireGitCommand(t)

	paths := newTestPaths(t)
	if err := EnsureProjectGitVersioning(paths); err != nil {
		t.Fatalf("EnsureProjectGitVersioning failed: %v", err)
	}

	target := filepath.Join(paths.ProjectDir, "hello.txt")
	if err := os.WriteFile(target, []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write test file failed: %v", err)
	}
	temp := filepath.Join(paths.ProjectDir, "scratch.tmp")
	if err := os.WriteFile(temp, []byte("temp\n"), 0o644); err != nil {
		t.Fatalf("write temp file failed: %v", err)
	}

	meta := IssueMeta{
		ID:      "I-20260221T000000Z-0001",
		Role:    "developer",
		Title:   "add hello file",
		StoryID: "US-001",
	}
	hash, committed, err := AutoCommitIssueChanges(paths, meta)
	if err != nil {
		t.Fatalf("AutoCommitIssueChanges failed: %v", err)
	}
	if !committed {
		t.Fatalf("auto commit should commit staged changes")
	}
	if strings.TrimSpace(hash) == "" {
		t.Fatalf("commit hash should not be empty")
	}

	subject, err := runGitCommand(paths.ProjectDir, nil, "log", "-1", "--pretty=%s")
	if err != nil {
		t.Fatalf("read git log failed: %v", err)
	}
	if !strings.Contains(subject, "ralph(developer): I-20260221T000000Z-0001") {
		t.Fatalf("unexpected commit subject: %q", subject)
	}

	hash2, committed2, err := AutoCommitIssueChanges(paths, meta)
	if err != nil {
		t.Fatalf("AutoCommitIssueChanges second run failed: %v", err)
	}
	if committed2 {
		t.Fatalf("second auto commit should skip when no changes (hash=%q)", hash2)
	}

	files, err := runGitCommand(paths.ProjectDir, nil, "show", "--name-only", "--pretty=", "HEAD")
	if err != nil {
		t.Fatalf("read committed files failed: %v", err)
	}
	if strings.Contains(files, "scratch.tmp") {
		t.Fatalf("temporary files should be excluded from auto commit: %q", files)
	}
}

func TestAutoCommitIssueChangesSkipsTempOnlyChanges(t *testing.T) {
	t.Parallel()
	requireGitCommand(t)

	paths := newTestPaths(t)
	if err := EnsureProjectGitVersioning(paths); err != nil {
		t.Fatalf("EnsureProjectGitVersioning failed: %v", err)
	}
	if _, err := runGitCommand(paths.ProjectDir, nil, "add", ".gitignore"); err != nil {
		t.Fatalf("git add .gitignore failed: %v", err)
	}
	if _, err := runGitCommand(paths.ProjectDir, gitIdentityEnv(), "commit", "-m", "init gitignore"); err != nil {
		t.Fatalf("seed init commit failed: %v", err)
	}
	temp := filepath.Join(paths.ProjectDir, "only-temp.tmp")
	if err := os.WriteFile(temp, []byte("temp\n"), 0o644); err != nil {
		t.Fatalf("write temp file failed: %v", err)
	}

	meta := IssueMeta{
		ID:    "I-20260221T000000Z-0002",
		Role:  "developer",
		Title: "temp only",
	}
	hash, committed, err := AutoCommitIssueChanges(paths, meta)
	if err != nil {
		t.Fatalf("AutoCommitIssueChanges failed: %v", err)
	}
	if committed {
		t.Fatalf("temp-only changes should not produce commit (hash=%q)", hash)
	}
}

func TestAutoCommitIssueChangesSkipsWhenPreStagedExists(t *testing.T) {
	t.Parallel()
	requireGitCommand(t)

	paths := newTestPaths(t)
	if err := EnsureProjectGitVersioning(paths); err != nil {
		t.Fatalf("EnsureProjectGitVersioning failed: %v", err)
	}
	target := filepath.Join(paths.ProjectDir, "manual.txt")
	if err := os.WriteFile(target, []byte("manual\n"), 0o644); err != nil {
		t.Fatalf("write manual file failed: %v", err)
	}
	if _, err := runGitCommand(paths.ProjectDir, nil, "add", "manual.txt"); err != nil {
		t.Fatalf("git add manual.txt failed: %v", err)
	}

	meta := IssueMeta{
		ID:    "I-20260221T000000Z-0003",
		Role:  "developer",
		Title: "pre-staged",
	}
	hash, committed, err := AutoCommitIssueChanges(paths, meta)
	if err == nil || !strings.Contains(err.Error(), "pre-existing staged changes") {
		t.Fatalf("expected pre-existing staged changes error, got hash=%q committed=%t err=%v", hash, committed, err)
	}
	if committed {
		t.Fatalf("pre-staged changes should not auto-commit")
	}
}
