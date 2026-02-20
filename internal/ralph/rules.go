package ralph

import (
	"fmt"
	"os"
	"strings"
)

type RoleRuleBundle struct {
	Common string
	Role   string
}

func EnsureRoleRuleFiles(paths Paths) error {
	if err := os.MkdirAll(paths.RulesDir, 0o755); err != nil {
		return fmt.Errorf("create rules dir: %w", err)
	}
	if err := ensureDefaultRuleFile(paths.CommonRulesFile, defaultCommonRules()); err != nil {
		return err
	}
	for _, role := range RequiredAgentRoles {
		if err := ensureDefaultRuleFile(paths.RoleRulesFile(role), defaultRoleRules(role)); err != nil {
			return err
		}
	}
	return nil
}

func ensureDefaultRuleFile(path, content string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat rule file %s: %w", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write default rule file %s: %w", path, err)
	}
	return nil
}

func LoadRoleRuleBundle(paths Paths, role string) (RoleRuleBundle, error) {
	if !IsSupportedRole(role) {
		return RoleRuleBundle{}, fmt.Errorf("unsupported role: %s", role)
	}
	commonBytes, err := os.ReadFile(paths.CommonRulesFile)
	if err != nil {
		return RoleRuleBundle{}, fmt.Errorf("read common role rules: %w", err)
	}
	roleBytes, err := os.ReadFile(paths.RoleRulesFile(role))
	if err != nil {
		return RoleRuleBundle{}, fmt.Errorf("read %s role rules: %w", role, err)
	}
	common := strings.TrimSpace(string(commonBytes))
	roleRules := strings.TrimSpace(string(roleBytes))
	if common == "" {
		return RoleRuleBundle{}, fmt.Errorf("common role rules are empty: %s", paths.CommonRulesFile)
	}
	if roleRules == "" {
		return RoleRuleBundle{}, fmt.Errorf("%s role rules are empty: %s", role, paths.RoleRulesFile(role))
	}
	return RoleRuleBundle{
		Common: common,
		Role:   roleRules,
	}, nil
}

func defaultCommonRules() string {
	return `# Common Agent Contract

## Scope and Safety
- Work only inside the project directory.
- Keep changes minimal and focused on the active issue.
- Do not run destructive commands unless the issue explicitly requires it.

## Quality
- Respect acceptance criteria in the issue.
- Leave clear evidence in logs and outputs.
- If blocked, explain the exact blocking reason.

## Handoff
- Produce handoff JSON for downstream roles.
- Handoff must be fact-based and include concrete artifacts.
- In universal schema mode, always provide artifacts and next_actions arrays.
`
}

func defaultRoleRules(role string) string {
	switch role {
	case "manager":
		return `# Manager Contract

## Primary Responsibility
- Keep queue health and execution order aligned with project priority.
- Clarify scope and unblock ambiguous tasks before handing over.

## Required Output
- Queue decisions with clear rationale.
- Priority and risk notes for planner and developer.
`
	case "planner":
		return `# Planner Contract

## Primary Responsibility
- Convert requirements into implementable, testable steps.
- Minimize risk by sequencing work and surfacing dependencies.

## Required Output
- Implementation plan with ordered steps.
- Milestones and explicit dependency list.
`
	case "developer":
		return `# Developer Contract

## Primary Responsibility
- Implement requested behavior with minimal regression risk.
- Keep diffs coherent and focused on the issue scope.

## Required Output
- Change summary and touched file list.
- Test results with exact command outcomes.
`
	case "qa":
		return `# QA Contract

## Primary Responsibility
- Validate behavior against acceptance criteria and edge cases.
- Report defects with reproducible evidence.

## Required Output
- Test matrix and defect findings.
- Final release recommendation: go, conditional, or no-go.
`
	default:
		return "# Role Contract\n"
	}
}
