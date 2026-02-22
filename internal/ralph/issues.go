package ralph

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultIssuePriority = 1000

type IssueMeta struct {
	ID       string
	Role     string
	Status   string
	Title    string
	Priority int
	StoryID  string
}

type IssueCreateOptions struct {
	Priority           int
	StoryID            string
	Objective          string
	AcceptanceCriteria []string
	ExtraMeta          map[string]string
}

func CreateIssue(paths Paths, role, title string) (string, string, error) {
	return CreateIssueWithOptions(paths, role, title, IssueCreateOptions{})
}

func CreateIssueWithOptions(paths Paths, role, title string, opts IssueCreateOptions) (string, string, error) {
	if err := EnsureLayout(paths); err != nil {
		return "", "", err
	}
	role = strings.TrimSpace(role)
	if !IsSupportedRole(role) {
		return "", "", fmt.Errorf("invalid role: %s", role)
	}
	if strings.TrimSpace(title) == "" {
		return "", "", fmt.Errorf("title is required")
	}

	now := time.Now().UTC()
	id := fmt.Sprintf("I-%s-%04d", now.Format("20060102T150405Z"), now.Nanosecond()%10000)
	issuePath := filepath.Join(paths.IssuesDir, id+".md")
	headers := []string{
		fmt.Sprintf("id: %s", id),
		fmt.Sprintf("role: %s", role),
		"status: ready",
		fmt.Sprintf("title: %s", title),
		fmt.Sprintf("created_at_utc: %s", now.Format(time.RFC3339)),
	}
	if opts.Priority > 0 {
		headers = append(headers, fmt.Sprintf("priority: %d", opts.Priority))
	}
	if sid := strings.TrimSpace(opts.StoryID); sid != "" {
		headers = append(headers, fmt.Sprintf("story_id: %s", sid))
	}
	if len(opts.ExtraMeta) > 0 {
		keys := make([]string, 0, len(opts.ExtraMeta))
		for k := range opts.ExtraMeta {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			key := strings.TrimSpace(k)
			if key == "" {
				continue
			}
			switch key {
			case "id", "role", "status", "title", "created_at_utc", "priority", "story_id":
				continue
			}
			val := strings.TrimSpace(opts.ExtraMeta[k])
			if val == "" {
				continue
			}
			headers = append(headers, fmt.Sprintf("%s: %s", key, val))
		}
	}

	objective := strings.TrimSpace(opts.Objective)
	if objective == "" {
		objective = title
	}
	criteria := normalizeAcceptanceCriteria(opts.AcceptanceCriteria)
	if len(criteria) == 0 {
		criteria = []string{
			"- [ ] Required changes are implemented.",
			"- [ ] Validation command passes if this role requires validation.",
		}
	}

	bodyLines := []string{"## Objective", "- " + objective, "", "## Acceptance Criteria"}
	bodyLines = append(bodyLines, criteria...)
	content := strings.Join(headers, "\n") + "\n\n" + strings.Join(bodyLines, "\n") + "\n"
	if err := os.WriteFile(issuePath, []byte(content), 0o644); err != nil {
		return "", "", fmt.Errorf("write issue file: %w", err)
	}

	return issuePath, id, nil
}

func normalizeAcceptanceCriteria(items []string) []string {
	out := []string{}
	for _, raw := range items {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		switch {
		case strings.HasPrefix(item, "- [ ]"), strings.HasPrefix(item, "- [x]"), strings.HasPrefix(item, "- [X]"):
			out = append(out, item)
		case strings.HasPrefix(item, "- "):
			out = append(out, "- [ ] "+strings.TrimSpace(strings.TrimPrefix(item, "- ")))
		default:
			out = append(out, "- [ ] "+item)
		}
	}
	return out
}

func ReadIssueMeta(path string) (IssueMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return IssueMeta{}, err
	}
	defer f.Close()

	meta := IssueMeta{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if strings.TrimSpace(line) == "" {
			break
		}
		k, v, ok := splitMeta(line)
		if !ok {
			continue
		}
		switch k {
		case "id":
			meta.ID = v
		case "role":
			meta.Role = v
		case "status":
			meta.Status = v
		case "title":
			meta.Title = v
		case "priority":
			if n, convErr := strconv.Atoi(v); convErr == nil {
				meta.Priority = n
			}
		case "story_id":
			meta.StoryID = v
		}
	}
	if err := s.Err(); err != nil {
		return meta, err
	}
	if meta.ID == "" {
		meta.ID = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	if meta.Role == "" {
		meta.Role = "developer"
	}
	if meta.Status == "" {
		meta.Status = "ready"
	}
	return meta, nil
}

func splitMeta(line string) (string, string, bool) {
	i := strings.Index(line, ":")
	if i <= 0 {
		return "", "", false
	}
	k := strings.TrimSpace(line[:i])
	v := strings.TrimSpace(line[i+1:])
	return k, v, true
}

func SetIssueStatus(path, status string) error {
	input, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(input), "\n")
	replaced := false
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "status:") {
			lines[i] = "status: " + status
			replaced = true
			break
		}
	}
	if !replaced {
		insertAt := 0
		for i, line := range lines {
			if strings.TrimSpace(line) == "" {
				insertAt = i
				break
			}
		}
		newLines := make([]string, 0, len(lines)+1)
		newLines = append(newLines, lines[:insertAt]...)
		newLines = append(newLines, "status: "+status)
		newLines = append(newLines, lines[insertAt:]...)
		lines = newLines
	}
	return os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644)
}

func AppendIssueResult(path, status, reason, logFile string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "\n## Ralph Result\n- status: %s\n- reason: %s\n- log_file: %s\n- updated_at_utc: %s\n", status, reason, logFile, time.Now().UTC().Format(time.RFC3339))
	return err
}

func PickNextReadyIssue(paths Paths) (string, IssueMeta, error) {
	return PickNextReadyIssueForRoles(paths, nil)
}

func PickNextReadyIssueForRoles(paths Paths, allowedRoles map[string]struct{}) (string, IssueMeta, error) {
	files, err := filepath.Glob(filepath.Join(paths.IssuesDir, "I-*.md"))
	if err != nil {
		return "", IssueMeta{}, err
	}
	sort.Strings(files)

	bestPath := ""
	bestMeta := IssueMeta{}
	bestPriority := int(^uint(0) >> 1)

	for _, f := range files {
		meta, readErr := ReadIssueMeta(f)
		if readErr != nil {
			continue
		}
		if meta.Status != "ready" {
			continue
		}
		if len(allowedRoles) > 0 {
			if _, ok := allowedRoles[meta.Role]; !ok {
				continue
			}
		}
		priority := meta.Priority
		if priority <= 0 {
			priority = defaultIssuePriority
		}
		if bestPath == "" || priority < bestPriority || (priority == bestPriority && f < bestPath) {
			bestPath = f
			bestMeta = meta
			bestPriority = priority
		}
	}
	if bestPath != "" {
		return bestPath, bestMeta, nil
	}
	return "", IssueMeta{}, nil
}

func RecoverInProgress(paths Paths) error {
	_, err := RecoverInProgressWithCount(paths)
	return err
}

func RetryBlockedIssues(paths Paths, reasonContains string, limit int) (int, error) {
	files, err := filepath.Glob(filepath.Join(paths.BlockedDir, "I-*.md"))
	if err != nil {
		return 0, err
	}
	sort.Strings(files)

	filter := strings.ToLower(strings.TrimSpace(reasonContains))
	moved := 0
	for _, f := range files {
		if limit > 0 && moved >= limit {
			break
		}
		if _, statErr := os.Stat(f); os.IsNotExist(statErr) {
			continue
		}

		if filter != "" {
			reason, readErr := latestIssueResultReason(f)
			if readErr != nil {
				return moved, readErr
			}
			if !strings.Contains(strings.ToLower(reason), filter) {
				continue
			}
		}

		base := filepath.Base(f)
		dst := filepath.Join(paths.IssuesDir, base)
		if _, statErr := os.Stat(dst); statErr == nil {
			dst = filepath.Join(paths.IssuesDir, "retried-"+base)
		}
		if err := SetIssueStatus(f, "ready"); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return moved, err
		}
		if err := os.Rename(f, dst); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return moved, err
		}
		moved++
	}
	return moved, nil
}

func RecoverInProgressWithCount(paths Paths) (int, error) {
	files, err := filepath.Glob(filepath.Join(paths.InProgressDir, "I-*.md"))
	if err != nil {
		return 0, err
	}
	sort.Strings(files)
	moved := 0
	for _, f := range files {
		if _, statErr := os.Stat(f); os.IsNotExist(statErr) {
			continue
		}
		base := filepath.Base(f)
		dst := filepath.Join(paths.IssuesDir, base)
		if _, statErr := os.Stat(dst); statErr == nil {
			dst = filepath.Join(paths.IssuesDir, "recovered-"+base)
		}
		if err := SetIssueStatus(f, "ready"); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return moved, err
		}
		if err := os.Rename(f, dst); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return moved, err
		}
		moved++
	}
	return moved, nil
}

func latestIssueResultReason(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	reason := ""
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "- reason:") {
			reason = strings.TrimSpace(strings.TrimPrefix(trimmed, "- reason:"))
		}
	}
	return reason, nil
}

func RecoverStaleInProgressWithCount(paths Paths, staleAfter time.Duration) (int, error) {
	if staleAfter <= 0 {
		return 0, nil
	}

	files, err := filepath.Glob(filepath.Join(paths.InProgressDir, "I-*.md"))
	if err != nil {
		return 0, err
	}
	sort.Strings(files)

	now := time.Now().UTC()
	moved := 0
	for _, f := range files {
		info, statErr := os.Stat(f)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				continue
			}
			return moved, statErr
		}
		if now.Sub(info.ModTime()) < staleAfter {
			continue
		}

		if err := SetIssueStatus(f, "ready"); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return moved, err
		}

		base := filepath.Base(f)
		dst := filepath.Join(paths.IssuesDir, base)
		if _, err := os.Stat(dst); err == nil {
			dst = filepath.Join(paths.IssuesDir, fmt.Sprintf("recovered-%s-%s", now.Format("20060102T150405Z"), base))
			suffix := 1
			for {
				if _, err := os.Stat(dst); os.IsNotExist(err) {
					break
				}
				dst = filepath.Join(paths.IssuesDir, fmt.Sprintf("recovered-%s-%d-%s", now.Format("20060102T150405Z"), suffix, base))
				suffix++
			}
		} else if !os.IsNotExist(err) {
			return moved, err
		}

		if err := os.Rename(f, dst); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return moved, err
		}
		moved++
	}

	return moved, nil
}

func CountIssueFiles(dir string) (int, error) {
	files, err := filepath.Glob(filepath.Join(dir, "I-*.md"))
	if err != nil {
		return 0, err
	}
	return len(files), nil
}

func CountReadyIssues(paths Paths) (int, error) {
	files, err := filepath.Glob(filepath.Join(paths.IssuesDir, "I-*.md"))
	if err != nil {
		return 0, err
	}
	sort.Strings(files)
	count := 0
	for _, f := range files {
		meta, readErr := ReadIssueMeta(f)
		if readErr != nil {
			continue
		}
		if meta.Status == "ready" {
			count++
		}
	}
	return count, nil
}
