package ralph

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	defaultGitAuthorName  = "Ralph Agent"
	defaultGitAuthorEmail = "ralph-agent@local"
)

var ralphGitIgnoreEntries = []string{
	".ralph/",
	".ralph-control/",
	".DS_Store",
	"*.tmp",
	"*.temp",
	"*.swp",
	"*.swo",
	"*~",
}

func EnsureProjectGitVersioning(paths Paths) error {
	if _, err := exec.LookPath("git"); err != nil {
		return fmt.Errorf("git command not found")
	}
	isRepo, _, err := gitRepoRoot(paths.ProjectDir)
	if err != nil {
		return err
	}
	if !isRepo {
		if _, err := runGitCommand(paths.ProjectDir, nil, "init"); err != nil {
			return err
		}
	}
	if err := ensureProjectGitignoreEntries(paths.ProjectDir, ralphGitIgnoreEntries); err != nil {
		return err
	}
	return nil
}

func AutoCommitIssueChanges(paths Paths, meta IssueMeta) (string, bool, error) {
	if err := EnsureProjectGitVersioning(paths); err != nil {
		return "", false, err
	}
	preStaged, err := gitHasStagedChanges(paths.ProjectDir)
	if err != nil {
		return "", false, err
	}
	if preStaged {
		return "", false, fmt.Errorf("pre-existing staged changes detected")
	}

	changedPaths, err := gitChangedPathsForAutoCommit(paths.ProjectDir)
	if err != nil {
		return "", false, err
	}
	if len(changedPaths) == 0 {
		return "", false, nil
	}
	if err := gitStagePaths(paths.ProjectDir, changedPaths); err != nil {
		return "", false, err
	}
	staged, err := gitHasStagedChanges(paths.ProjectDir)
	if err != nil {
		return "", false, err
	}
	if !staged {
		return "", false, nil
	}

	role := sanitizeGitSingleLine(meta.Role, 24)
	if role == "" {
		role = "developer"
	}
	issueID := sanitizeGitSingleLine(meta.ID, 96)
	if issueID == "" {
		issueID = "unknown-issue"
	}
	title := sanitizeGitSingleLine(meta.Title, 96)
	if title == "" {
		title = "task completed"
	}
	storyID := sanitizeGitSingleLine(meta.StoryID, 96)
	if storyID == "" {
		storyID = "-"
	}

	subject := fmt.Sprintf("ralph(%s): %s %s", role, issueID, title)
	body := fmt.Sprintf("issue_id: %s\nrole: %s\nstory_id: %s\ngenerated_by: ralph-loop", issueID, role, storyID)
	if _, err := runGitCommand(paths.ProjectDir, gitIdentityEnv(), "commit", "-m", subject, "-m", body); err != nil {
		return "", false, err
	}
	hash, err := runGitCommand(paths.ProjectDir, nil, "rev-parse", "--short", "HEAD")
	if err != nil {
		return "", true, nil
	}
	return strings.TrimSpace(hash), true, nil
}

func gitRepoRoot(projectDir string) (bool, string, error) {
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	cmd.Dir = projectDir
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return false, "", nil
		}
		return false, "", fmt.Errorf("git rev-parse --show-toplevel failed: %w", err)
	}
	root := strings.TrimSpace(out.String())
	return root != "", root, nil
}

func gitHasStagedChanges(projectDir string) (bool, error) {
	cmd := exec.Command("git", "diff", "--cached", "--quiet", "--exit-code")
	cmd.Dir = projectDir
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return true, nil
		}
		return false, fmt.Errorf("git diff --cached --quiet failed: %w", err)
	}
	return false, nil
}

func gitChangedPathsForAutoCommit(projectDir string) ([]string, error) {
	raw, err := runGitCommandBytes(projectDir, nil, "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	parts := bytes.Split(raw, []byte{0})
	seen := map[string]struct{}{}
	out := make([]string, 0, len(parts))

	for i := 0; i < len(parts); i++ {
		entry := parts[i]
		if len(entry) == 0 {
			continue
		}
		if len(entry) < 4 {
			continue
		}

		status := string(entry[:2])
		path := strings.TrimSpace(string(entry[3:]))
		if len(status) > 0 {
			switch status[0] {
			case 'R', 'C':
				if i+1 < len(parts) {
					path = strings.TrimSpace(string(parts[i+1]))
					i++
				}
			}
		}

		candidate := filepath.ToSlash(filepath.Clean(path))
		if !isAutoCommitCandidatePath(candidate) {
			continue
		}
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	return out, nil
}

func isAutoCommitCandidatePath(path string) bool {
	p := strings.TrimSpace(filepath.ToSlash(path))
	if p == "" || p == "." {
		return false
	}
	lower := strings.ToLower(p)
	if strings.HasPrefix(lower, ".git/") || lower == ".git" {
		return false
	}
	if strings.HasPrefix(lower, ".ralph/") || lower == ".ralph" {
		return false
	}
	if strings.HasPrefix(lower, ".ralph-control/") || lower == ".ralph-control" {
		return false
	}

	base := strings.ToLower(filepath.Base(lower))
	if base == ".ds_store" || base == "thumbs.db" || base == "coverage.out" || base == "coverage.txt" {
		return false
	}
	if strings.HasPrefix(base, ".#") || strings.HasSuffix(base, "~") {
		return false
	}
	switch filepath.Ext(base) {
	case ".tmp", ".temp", ".swp", ".swo", ".bak", ".orig", ".rej", ".pid", ".lock", ".cache", ".log":
		return false
	}
	for _, seg := range strings.Split(lower, "/") {
		switch seg {
		case "", ".", "..":
			continue
		case "tmp", "temp", ".tmp", ".temp", ".cache":
			return false
		}
	}
	return true
}

func gitStagePaths(projectDir string, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	const chunkSize = 200
	for i := 0; i < len(paths); i += chunkSize {
		end := i + chunkSize
		if end > len(paths) {
			end = len(paths)
		}
		args := []string{"add", "-A", "--"}
		args = append(args, paths[i:end]...)
		if _, err := runGitCommand(projectDir, nil, args...); err != nil {
			return err
		}
	}
	return nil
}

func runGitCommand(projectDir string, extraEnv []string, args ...string) (string, error) {
	out, err := runGitCommandBytes(projectDir, extraEnv, args...)
	return strings.TrimSpace(string(out)), err
}

func runGitCommandBytes(projectDir string, extraEnv []string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = projectDir
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = strings.TrimSpace(out.String())
		}
		if msg != "" {
			return nil, fmt.Errorf("git %s failed: %w: %s", strings.Join(args, " "), err, msg)
		}
		return nil, fmt.Errorf("git %s failed: %w", strings.Join(args, " "), err)
	}
	return out.Bytes(), nil
}

func gitIdentityEnv() []string {
	name := strings.TrimSpace(os.Getenv("RALPH_GIT_AUTHOR_NAME"))
	if name == "" {
		name = defaultGitAuthorName
	}
	email := strings.TrimSpace(os.Getenv("RALPH_GIT_AUTHOR_EMAIL"))
	if email == "" {
		email = defaultGitAuthorEmail
	}
	return []string{
		"GIT_AUTHOR_NAME=" + name,
		"GIT_AUTHOR_EMAIL=" + email,
		"GIT_COMMITTER_NAME=" + name,
		"GIT_COMMITTER_EMAIL=" + email,
	}
}

func ensureProjectGitignoreEntries(projectDir string, entries []string) error {
	if len(entries) == 0 {
		return nil
	}
	path := filepath.Join(projectDir, ".gitignore")
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read .gitignore: %w", err)
	}
	existingLines := map[string]struct{}{}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		existingLines[trimmed] = struct{}{}
	}
	missing := make([]string, 0, len(entries))
	for _, entry := range entries {
		e := strings.TrimSpace(entry)
		if e == "" {
			continue
		}
		if _, ok := existingLines[e]; ok {
			continue
		}
		missing = append(missing, e)
	}
	if len(missing) == 0 {
		return nil
	}

	var b strings.Builder
	current := string(data)
	if strings.TrimSpace(current) != "" {
		b.WriteString(current)
		if !strings.HasSuffix(current, "\n") {
			b.WriteByte('\n')
		}
		b.WriteByte('\n')
	}
	b.WriteString("# Ralph runtime state\n")
	for _, entry := range missing {
		b.WriteString(entry)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("write .gitignore: %w", err)
	}
	return nil
}

func sanitizeGitSingleLine(raw string, maxRunes int) string {
	text := strings.TrimSpace(raw)
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return ""
	}
	if maxRunes <= 0 {
		return text
	}
	runes := []rune(text)
	if len(runes) <= maxRunes {
		return text
	}
	return string(runes[:maxRunes-3]) + "..."
}
