package ralph

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type RoleHandoffSpec struct {
	RequiredStringFields []string
	RequiredArrayFields  []string
	EnumField            string
	EnumValues           []string
}

func HandoffFilePath(paths Paths, meta IssueMeta) string {
	base := sanitizeHandoffName(meta.ID)
	if strings.TrimSpace(meta.StoryID) != "" {
		base = sanitizeHandoffName(meta.StoryID) + "-" + base
	}
	return filepath.Join(paths.HandoffsDir, fmt.Sprintf("%s.%s.json", base, meta.Role))
}

func HandoffInstruction(meta IssueMeta, handoffPath, schema string) string {
	spec := roleHandoffSpec(meta.Role, schema)
	storyID := strings.TrimSpace(meta.StoryID)
	if storyID == "" {
		storyID = "-"
	}
	enumNote := ""
	if spec.EnumField != "" && len(spec.EnumValues) > 0 {
		enumNote = fmt.Sprintf("\n- `%s` must be one of: %s", spec.EnumField, strings.Join(spec.EnumValues, ", "))
	}

	sort.Strings(spec.RequiredStringFields)
	sort.Strings(spec.RequiredArrayFields)

	return fmt.Sprintf(
		`Write handoff JSON before completion.
- Output path: %s
- Required base fields: role, issue_id, story_id, summary
- Required role string fields: %s
- Required role string-array fields: %s%s
- role must equal "%s"
- issue_id must equal "%s"
- story_id should be "%s" (or "-" if not available)
- JSON only (no markdown)`,
		handoffPath,
		strings.Join(spec.RequiredStringFields, ", "),
		strings.Join(spec.RequiredArrayFields, ", "),
		enumNote,
		meta.Role,
		meta.ID,
		storyID,
	)
}

func ValidateRoleHandoff(meta IssueMeta, handoffPath, schema string) error {
	data, err := os.ReadFile(handoffPath)
	if err != nil {
		return fmt.Errorf("read handoff file: %w", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("parse handoff json: %w", err)
	}

	roleValue, err := requiredString(raw, "role")
	if err != nil {
		return err
	}
	if roleValue != meta.Role {
		return fmt.Errorf("handoff role mismatch: expected=%s actual=%s", meta.Role, roleValue)
	}

	issueID, err := requiredString(raw, "issue_id")
	if err != nil {
		return err
	}
	if issueID != meta.ID {
		return fmt.Errorf("handoff issue_id mismatch: expected=%s actual=%s", meta.ID, issueID)
	}

	if _, err := requiredString(raw, "summary"); err != nil {
		return err
	}

	storyID, err := requiredString(raw, "story_id")
	if err != nil {
		return err
	}
	if expected := strings.TrimSpace(meta.StoryID); expected != "" && storyID != expected {
		return fmt.Errorf("handoff story_id mismatch: expected=%s actual=%s", expected, storyID)
	}

	spec := roleHandoffSpec(meta.Role, schema)
	for _, field := range spec.RequiredStringFields {
		if _, err := requiredString(raw, field); err != nil {
			return err
		}
	}
	for _, field := range spec.RequiredArrayFields {
		if _, err := requiredStringArray(raw, field); err != nil {
			return err
		}
	}
	if spec.EnumField != "" {
		val, err := requiredString(raw, spec.EnumField)
		if err != nil {
			return err
		}
		if !containsString(spec.EnumValues, val) {
			return fmt.Errorf("field %s must be one of %s", spec.EnumField, strings.Join(spec.EnumValues, ", "))
		}
	}

	return nil
}

func roleHandoffSpec(role, schema string) RoleHandoffSpec {
	if normalizeHandoffSchema(schema) == "strict" {
		return roleHandoffSpecStrict(role)
	}
	return roleHandoffSpecUniversal(role)
}

func roleHandoffSpecStrict(role string) RoleHandoffSpec {
	switch role {
	case "manager":
		return RoleHandoffSpec{
			RequiredStringFields: []string{"priority_rationale"},
			RequiredArrayFields:  []string{"queue_decisions", "risks"},
		}
	case "planner":
		return RoleHandoffSpec{
			RequiredStringFields: []string{},
			RequiredArrayFields:  []string{"implementation_plan", "milestones", "dependencies"},
		}
	case "developer":
		return RoleHandoffSpec{
			RequiredStringFields: []string{},
			RequiredArrayFields:  []string{"change_summary", "files_touched", "test_results"},
		}
	case "qa":
		return RoleHandoffSpec{
			RequiredStringFields: []string{},
			RequiredArrayFields:  []string{"test_matrix", "defects"},
			EnumField:            "release_recommendation",
			EnumValues:           []string{"go", "conditional", "no-go"},
		}
	default:
		return RoleHandoffSpec{}
	}
}

func roleHandoffSpecUniversal(role string) RoleHandoffSpec {
	spec := RoleHandoffSpec{
		RequiredArrayFields: []string{"artifacts", "next_actions"},
	}
	if role == "qa" {
		spec.EnumField = "release_recommendation"
		spec.EnumValues = []string{"go", "conditional", "no-go"}
	}
	return spec
}

func requiredString(m map[string]any, key string) (string, error) {
	raw, ok := m[key]
	if !ok {
		return "", fmt.Errorf("missing field: %s", key)
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("field %s must be a string", key)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("field %s must be non-empty", key)
	}
	return value, nil
}

func requiredStringArray(m map[string]any, key string) ([]string, error) {
	raw, ok := m[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("field %s must be an array", key)
	}
	out := make([]string, 0, len(list))
	for idx, item := range list {
		value, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("field %s[%d] must be a string", key, idx)
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("field %s[%d] must be non-empty", key, idx)
		}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("field %s must contain at least one item", key)
	}
	return out, nil
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

var handoffSanitizer = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func sanitizeHandoffName(v string) string {
	value := strings.TrimSpace(v)
	value = handoffSanitizer.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-.")
	if value == "" {
		return "unknown"
	}
	return value
}
