package ralph

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildPRDGlobalContext(t *testing.T) {
	t.Parallel()

	meta := prdMetadata{
		Product: "Wallet",
		Context: prdContextSummary{
			Problem:     "결제 실패율이 높다",
			Goal:        "실패율을 낮춘다",
			InScope:     "재시도 로직",
			OutOfScope:  "신규 결제수단 도입",
			Acceptance:  "핵심 시나리오 통과",
			Constraints: "리소스 한정",
		},
	}
	got := buildPRDGlobalContext(meta)
	if !strings.Contains(got, "product=Wallet") {
		t.Fatalf("missing product in global context: %s", got)
	}
	if !strings.Contains(got, "problem=결제 실패율이 높다") {
		t.Fatalf("missing problem in global context: %s", got)
	}
}

func TestImportPRDStoriesAppendsGlobalContext(t *testing.T) {
	paths := newTestPaths(t)

	prdPath := filepath.Join(paths.ProjectDir, "prd.json")
	writeJSON(t, prdPath, map[string]any{
		"metadata": map[string]any{
			"product": "Wallet",
			"context": map[string]any{
				"problem":      "결제 실패율이 높다",
				"goal":         "실패율을 낮춘다",
				"in_scope":     "재시도 로직",
				"out_of_scope": "신규 결제수단 도입",
				"acceptance":   "핵심 시나리오 통과",
			},
		},
		"userStories": []map[string]any{
			{
				"id":          "US-001",
				"title":       "결제 실패 복구",
				"description": "실패 시 재시도로 이탈을 줄인다",
				"role":        "developer",
				"priority":    10,
			},
		},
	})

	result, err := ImportPRDStories(paths, prdPath, "developer", false)
	if err != nil {
		t.Fatalf("ImportPRDStories failed: %v", err)
	}
	if result.Imported != 1 || len(result.CreatedPaths) != 1 {
		t.Fatalf("unexpected import result: %+v", result)
	}

	content, err := os.ReadFile(result.CreatedPaths[0])
	if err != nil {
		t.Fatalf("read imported issue failed: %v", err)
	}
	body := string(content)
	if !strings.Contains(body, "global_context:") {
		t.Fatalf("issue should include global_context: %s", body)
	}
	if !strings.Contains(body, "product=Wallet") {
		t.Fatalf("issue global_context should include product: %s", body)
	}
}
