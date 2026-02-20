package ralph

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func AppendProgressEntry(paths Paths, meta IssueMeta, status, reason, logFile string) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	f, err := os.OpenFile(paths.ProgressJournal, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open progress journal: %w", err)
	}
	defer f.Close()

	priority := meta.Priority
	if priority <= 0 {
		priority = defaultIssuePriority
	}
	storyID := strings.TrimSpace(meta.StoryID)
	if storyID == "" {
		storyID = "-"
	}

	line := fmt.Sprintf(
		"- %s | issue=%s | role=%s | priority=%d | story=%s | status=%s | reason=%s | log=%s\n",
		time.Now().UTC().Format(time.RFC3339),
		sanitizeProgressField(meta.ID),
		sanitizeProgressField(meta.Role),
		priority,
		sanitizeProgressField(storyID),
		sanitizeProgressField(status),
		sanitizeProgressField(reason),
		sanitizeProgressField(logFile),
	)
	_, err = f.WriteString(line)
	return err
}

func sanitizeProgressField(v string) string {
	v = strings.TrimSpace(v)
	v = strings.ReplaceAll(v, "\n", " ")
	v = strings.ReplaceAll(v, "|", "/")
	if v == "" {
		return "-"
	}
	return v
}
