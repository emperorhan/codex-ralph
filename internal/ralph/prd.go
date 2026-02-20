package ralph

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type PRDImportResult struct {
	SourcePath      string
	StoriesTotal    int
	Imported        int
	SkippedPassed   int
	SkippedExisting int
	SkippedInvalid  int
	DryRun          bool
	CreatedPaths    []string
}

type prdDocument struct {
	Metadata    prdMetadata `json:"metadata"`
	UserStories []prdStory  `json:"userStories"`
}

type prdMetadata struct {
	Product string            `json:"product"`
	Context prdContextSummary `json:"context"`
}

type prdContextSummary struct {
	Problem     string `json:"problem"`
	Goal        string `json:"goal"`
	InScope     string `json:"in_scope"`
	OutOfScope  string `json:"out_of_scope"`
	Acceptance  string `json:"acceptance"`
	Constraints string `json:"constraints"`
}

type prdStory struct {
	ID                 string          `json:"id"`
	Title              string          `json:"title"`
	Description        string          `json:"description"`
	Role               string          `json:"role"`
	Priority           int             `json:"priority"`
	Passes             bool            `json:"passes"`
	Passed             bool            `json:"passed"`
	AcceptanceCriteria json.RawMessage `json:"acceptanceCriteria"`
}

func ImportPRDStories(paths Paths, prdPath, defaultRole string, dryRun bool) (PRDImportResult, error) {
	result := PRDImportResult{DryRun: dryRun}
	if err := EnsureLayout(paths); err != nil {
		return result, err
	}

	sourcePath := strings.TrimSpace(prdPath)
	if sourcePath == "" {
		sourcePath = "prd.json"
	}
	if !filepath.IsAbs(sourcePath) {
		sourcePath = filepath.Join(paths.ProjectDir, sourcePath)
	}
	absSourcePath, err := filepath.Abs(sourcePath)
	if err != nil {
		return result, fmt.Errorf("resolve prd file path: %w", err)
	}
	result.SourcePath = absSourcePath

	data, err := os.ReadFile(absSourcePath)
	if err != nil {
		return result, fmt.Errorf("read prd file: %w", err)
	}

	doc := prdDocument{}
	if err := json.Unmarshal(data, &doc); err != nil {
		return result, fmt.Errorf("parse prd json: %w", err)
	}
	if len(doc.UserStories) == 0 {
		return result, fmt.Errorf("prd json has no userStories")
	}

	roleFallback := strings.TrimSpace(defaultRole)
	if !IsSupportedRole(roleFallback) {
		roleFallback = "developer"
	}

	existingStoryIDs, err := indexStoryIDs(paths)
	if err != nil {
		return result, err
	}

	sourceFileName := filepath.Base(absSourcePath)
	globalContext := buildPRDGlobalContext(doc.Metadata)
	for _, story := range doc.UserStories {
		result.StoriesTotal++

		if story.Passes || story.Passed {
			result.SkippedPassed++
			continue
		}

		id := strings.TrimSpace(story.ID)
		title := strings.TrimSpace(story.Title)
		if id == "" || title == "" {
			result.SkippedInvalid++
			continue
		}
		if _, exists := existingStoryIDs[id]; exists {
			result.SkippedExisting++
			continue
		}

		role := strings.TrimSpace(story.Role)
		if !IsSupportedRole(role) {
			role = roleFallback
		}

		priority := story.Priority
		if priority <= 0 {
			priority = defaultIssuePriority
		}

		objective := strings.TrimSpace(story.Description)
		if objective == "" {
			objective = title
		}

		options := IssueCreateOptions{
			Priority:           priority,
			StoryID:            id,
			Objective:          objective,
			AcceptanceCriteria: parseAcceptanceCriteria(story.AcceptanceCriteria),
			ExtraMeta: map[string]string{
				"story_source": sourceFileName,
			},
		}

		result.Imported++
		if dryRun {
			existingStoryIDs[id] = "(dry-run)"
			continue
		}

		issuePath, _, err := CreateIssueWithOptions(paths, role, title, options)
		if err != nil {
			return result, err
		}
		if err := appendPRDContext(issuePath, id, priority, sourceFileName, story.Description, globalContext); err != nil {
			return result, err
		}

		existingStoryIDs[id] = issuePath
		result.CreatedPaths = append(result.CreatedPaths, issuePath)
	}

	return result, nil
}

func parseAcceptanceCriteria(raw json.RawMessage) []string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	var asStrings []string
	if err := json.Unmarshal(raw, &asStrings); err == nil {
		return asStrings
	}

	var asSingle string
	if err := json.Unmarshal(raw, &asSingle); err == nil {
		if strings.TrimSpace(asSingle) == "" {
			return nil
		}
		return []string{asSingle}
	}

	var asObjects []map[string]any
	if err := json.Unmarshal(raw, &asObjects); err == nil {
		out := []string{}
		for _, item := range asObjects {
			value := firstString(item, "text", "title", "description", "name")
			if strings.TrimSpace(value) != "" {
				out = append(out, value)
			}
		}
		return out
	}

	var asAny []any
	if err := json.Unmarshal(raw, &asAny); err == nil {
		out := []string{}
		for _, item := range asAny {
			switch v := item.(type) {
			case string:
				if strings.TrimSpace(v) != "" {
					out = append(out, v)
				}
			case map[string]any:
				value := firstString(v, "text", "title", "description", "name")
				if strings.TrimSpace(value) != "" {
					out = append(out, value)
				}
			}
		}
		return out
	}

	return nil
}

func firstString(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if raw, ok := m[key]; ok {
			if str, ok := raw.(string); ok {
				return str
			}
		}
	}
	return ""
}

func indexStoryIDs(paths Paths) (map[string]string, error) {
	out := map[string]string{}
	scanDirs := []string{
		paths.IssuesDir,
		paths.InProgressDir,
		paths.DoneDir,
		paths.BlockedDir,
	}
	for _, dir := range scanDirs {
		files, err := filepath.Glob(filepath.Join(dir, "I-*.md"))
		if err != nil {
			return nil, err
		}
		sort.Strings(files)
		for _, file := range files {
			meta, err := ReadIssueMeta(file)
			if err != nil {
				continue
			}
			storyID := strings.TrimSpace(meta.StoryID)
			if storyID == "" {
				continue
			}
			if _, exists := out[storyID]; !exists {
				out[storyID] = file
			}
		}
	}
	return out, nil
}

func appendPRDContext(issuePath, storyID string, priority int, sourceFileName, description, globalContext string) error {
	f, err := os.OpenFile(issuePath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	desc := strings.ReplaceAll(strings.TrimSpace(description), "\n", " ")
	_, err = fmt.Fprintf(
		f,
		"\n## PRD Context\n- story_id: %s\n- source: %s\n- priority: %d\n- imported_at_utc: %s\n",
		storyID,
		sourceFileName,
		priority,
		time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return err
	}
	if desc != "" {
		_, err = fmt.Fprintf(f, "- story_description: %s\n", desc)
		if err != nil {
			return err
		}
	}
	if strings.TrimSpace(globalContext) != "" {
		_, err = fmt.Fprintf(f, "- global_context: %s\n", globalContext)
	}
	return err
}

func buildPRDGlobalContext(meta prdMetadata) string {
	parts := []string{}
	if v := strings.TrimSpace(meta.Product); v != "" {
		parts = append(parts, "product="+singleLine(v))
	}
	if v := strings.TrimSpace(meta.Context.Problem); v != "" {
		parts = append(parts, "problem="+singleLine(v))
	}
	if v := strings.TrimSpace(meta.Context.Goal); v != "" {
		parts = append(parts, "goal="+singleLine(v))
	}
	if v := strings.TrimSpace(meta.Context.InScope); v != "" {
		parts = append(parts, "in_scope="+singleLine(v))
	}
	if v := strings.TrimSpace(meta.Context.OutOfScope); v != "" {
		parts = append(parts, "out_of_scope="+singleLine(v))
	}
	if v := strings.TrimSpace(meta.Context.Acceptance); v != "" {
		parts = append(parts, "acceptance="+singleLine(v))
	}
	if v := strings.TrimSpace(meta.Context.Constraints); v != "" {
		parts = append(parts, "constraints="+singleLine(v))
	}
	return strings.Join(parts, "; ")
}

func singleLine(v string) string {
	v = strings.TrimSpace(v)
	v = strings.ReplaceAll(v, "\n", " ")
	v = strings.ReplaceAll(v, "\r", " ")
	return strings.Join(strings.Fields(v), " ")
}
