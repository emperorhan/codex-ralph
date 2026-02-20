package ralph

import (
	"strings"
	"testing"
)

func TestValidateRoleHandoffUniversalQA(t *testing.T) {
	paths := newTestPaths(t)
	meta := IssueMeta{
		ID:      "I-0001",
		Role:    "qa",
		StoryID: "US-001",
	}
	handoffPath := HandoffFilePath(paths, meta)

	payload := map[string]any{
		"role":                   "qa",
		"issue_id":               "I-0001",
		"story_id":               "US-001",
		"summary":                "qa review completed",
		"artifacts":              []string{"tests/report.md"},
		"next_actions":           []string{"monitor error rate"},
		"release_recommendation": "go",
	}
	writeJSON(t, handoffPath, payload)

	if err := ValidateRoleHandoff(meta, handoffPath, "universal"); err != nil {
		t.Fatalf("validate universal handoff: %v", err)
	}

	payload["release_recommendation"] = "ship-it"
	writeJSON(t, handoffPath, payload)

	err := ValidateRoleHandoff(meta, handoffPath, "universal")
	if err == nil {
		t.Fatalf("expected invalid enum error")
	}
	if !strings.Contains(err.Error(), "must be one of") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRoleHandoffStrictDeveloperMissingField(t *testing.T) {
	paths := newTestPaths(t)
	meta := IssueMeta{
		ID:      "I-0002",
		Role:    "developer",
		StoryID: "US-002",
	}
	handoffPath := HandoffFilePath(paths, meta)

	validPayload := map[string]any{
		"role":     "developer",
		"issue_id": "I-0002",
		"story_id": "US-002",
		"summary":  "implementation done",
		"change_summary": []string{
			"add request timeout",
		},
		"files_touched": []string{
			"internal/ralph/loop.go",
		},
		"test_results": []string{
			"go test ./...",
		},
	}
	writeJSON(t, handoffPath, validPayload)
	if err := ValidateRoleHandoff(meta, handoffPath, "strict"); err != nil {
		t.Fatalf("validate strict handoff: %v", err)
	}

	delete(validPayload, "test_results")
	writeJSON(t, handoffPath, validPayload)

	err := ValidateRoleHandoff(meta, handoffPath, "strict")
	if err == nil {
		t.Fatalf("expected missing field error")
	}
	if !strings.Contains(err.Error(), "missing field: test_results") {
		t.Fatalf("unexpected error: %v", err)
	}
}
