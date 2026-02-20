package ralph

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func EnsureRoleBootstrapIssues(paths Paths, prdPath string) ([]string, error) {
	if err := EnsureLayout(paths); err != nil {
		return nil, err
	}
	if strings.TrimSpace(prdPath) == "" {
		prdPath = "PRD.md"
	}

	created := []string{}
	for _, role := range RequiredAgentRoles {
		hasActive, err := hasActiveIssueForRole(paths, role)
		if err != nil {
			return created, err
		}
		if hasActive {
			continue
		}

		title := bootstrapTitle(role, prdPath)
		issuePath, _, err := CreateIssue(paths, role, title)
		if err != nil {
			return created, err
		}
		if err := appendBootstrapNote(issuePath, role, prdPath); err != nil {
			return created, err
		}
		created = append(created, issuePath)
	}

	return created, nil
}

func hasActiveIssueForRole(paths Paths, role string) (bool, error) {
	candidates := []string{paths.IssuesDir, paths.InProgressDir}
	for _, dir := range candidates {
		files, err := filepath.Glob(filepath.Join(dir, "I-*.md"))
		if err != nil {
			return false, err
		}
		sort.Strings(files)
		for _, f := range files {
			meta, err := ReadIssueMeta(f)
			if err != nil {
				continue
			}
			if meta.Role == role {
				return true, nil
			}
		}
	}
	return false, nil
}

func bootstrapTitle(role, prdPath string) string {
	switch role {
	case "manager":
		return fmt.Sprintf("[bootstrap][manager] PRD 기반 큐 운영 시작 (%s)", prdPath)
	case "planner":
		return fmt.Sprintf("[bootstrap][planner] PRD 기반 구현 계획 수립 (%s)", prdPath)
	case "developer":
		return fmt.Sprintf("[bootstrap][developer] PRD 핵심 기능 1차 구현 (%s)", prdPath)
	case "qa":
		return fmt.Sprintf("[bootstrap][qa] PRD 검증 시나리오 수립 (%s)", prdPath)
	default:
		return fmt.Sprintf("[bootstrap] %s", role)
	}
}

func appendBootstrapNote(issuePath, role, prdPath string) error {
	f, err := os.OpenFile(issuePath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "\n## Bootstrap Context\n- role: %s\n- prd_path: %s\n- note: 프로젝트별 독립 에이전트 세트(manager/planner/developer/qa) 초기화 이슈\n", role, prdPath)
	return err
}
