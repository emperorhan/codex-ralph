package ralph

import (
	"fmt"
	"os"
	"path/filepath"
)

type Paths struct {
	ControlDir             string
	ProjectDir             string
	RalphDir               string
	RulesDir               string
	IssuesDir              string
	InProgressDir          string
	DoneDir                string
	BlockedDir             string
	ReportsDir             string
	HandoffsDir            string
	LogsDir                string
	StateFile              string
	ProfileFile            string
	ProfileLocalFile       string
	ProfileYAMLFile        string
	ProfileLocalYAMLFile   string
	CommonRulesFile        string
	IssueTemplateFile      string
	PIDFile                string
	RunnerLogFile          string
	BusyWaitStateFile      string
	ProfileReloadStateFile string
	BusyWaitEventsFile     string
	ProgressJournal        string
	AgentSetFile           string
}

func NewPaths(controlDir, projectDir string) (Paths, error) {
	if controlDir == "" {
		return Paths{}, fmt.Errorf("control-dir is required")
	}
	if projectDir == "" {
		return Paths{}, fmt.Errorf("project-dir is required")
	}

	absControl, err := filepath.Abs(controlDir)
	if err != nil {
		return Paths{}, fmt.Errorf("resolve control-dir: %w", err)
	}
	absProject, err := filepath.Abs(projectDir)
	if err != nil {
		return Paths{}, fmt.Errorf("resolve project-dir: %w", err)
	}

	ralphDir := filepath.Join(absProject, ".ralph")
	rulesDir := filepath.Join(ralphDir, "rules")
	reportsDir := filepath.Join(ralphDir, "reports")
	return Paths{
		ControlDir:             absControl,
		ProjectDir:             absProject,
		RalphDir:               ralphDir,
		RulesDir:               rulesDir,
		IssuesDir:              filepath.Join(ralphDir, "issues"),
		InProgressDir:          filepath.Join(ralphDir, "in-progress"),
		DoneDir:                filepath.Join(ralphDir, "done"),
		BlockedDir:             filepath.Join(ralphDir, "blocked"),
		ReportsDir:             reportsDir,
		HandoffsDir:            filepath.Join(reportsDir, "handoffs"),
		LogsDir:                filepath.Join(ralphDir, "logs"),
		StateFile:              filepath.Join(ralphDir, "state.env"),
		ProfileFile:            filepath.Join(ralphDir, "profile.env"),
		ProfileLocalFile:       filepath.Join(ralphDir, "profile.local.env"),
		ProfileYAMLFile:        filepath.Join(ralphDir, "profile.yaml"),
		ProfileLocalYAMLFile:   filepath.Join(ralphDir, "profile.local.yaml"),
		CommonRulesFile:        filepath.Join(rulesDir, "common.md"),
		IssueTemplateFile:      filepath.Join(ralphDir, "issue-template.md"),
		PIDFile:                filepath.Join(ralphDir, "runner.pid"),
		RunnerLogFile:          filepath.Join(ralphDir, "logs", "runner.out"),
		BusyWaitStateFile:      filepath.Join(ralphDir, "state.busywait.env"),
		ProfileReloadStateFile: filepath.Join(ralphDir, "state.profile-reload.env"),
		BusyWaitEventsFile:     filepath.Join(ralphDir, "reports", "busywait-events.jsonl"),
		ProgressJournal:        filepath.Join(ralphDir, "reports", "progress-journal.log"),
		AgentSetFile:           filepath.Join(ralphDir, "agent-set.env"),
	}, nil
}

func (p Paths) RolePIDFile(role string) string {
	return filepath.Join(p.RalphDir, fmt.Sprintf("runner.%s.pid", role))
}

func (p Paths) RoleRunnerLogFile(role string) string {
	return filepath.Join(p.LogsDir, fmt.Sprintf("runner.%s.out", role))
}

func (p Paths) RoleRulesFile(role string) string {
	return filepath.Join(p.RulesDir, fmt.Sprintf("%s.md", role))
}

func EnsureLayout(paths Paths) error {
	dirs := []string{
		paths.RalphDir,
		paths.RulesDir,
		paths.IssuesDir,
		paths.InProgressDir,
		paths.DoneDir,
		paths.BlockedDir,
		paths.ReportsDir,
		paths.HandoffsDir,
		paths.LogsDir,
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create dir %s: %w", dir, err)
		}
	}

	if _, err := os.Stat(paths.StateFile); os.IsNotExist(err) {
		if err := os.WriteFile(paths.StateFile, []byte("RALPH_LOCAL_ENABLED=true\n"), 0o644); err != nil {
			return fmt.Errorf("write state file: %w", err)
		}
	}
	if _, err := os.Stat(paths.IssueTemplateFile); os.IsNotExist(err) {
		tpl := "id: I-0000\nrole: developer\nstatus: ready\ntitle: Example issue\ncreated_at_utc: 2026-01-01T00:00:00Z\n\n## Objective\n- Describe the required output.\n\n## Acceptance Criteria\n- [ ] Tests/checks pass.\n- [ ] Scope is limited to related files.\n"
		if err := os.WriteFile(paths.IssueTemplateFile, []byte(tpl), 0o644); err != nil {
			return fmt.Errorf("write issue template: %w", err)
		}
	}
	if _, err := os.Stat(paths.RunnerLogFile); os.IsNotExist(err) {
		f, createErr := os.OpenFile(paths.RunnerLogFile, os.O_CREATE|os.O_WRONLY, 0o644)
		if createErr != nil {
			return fmt.Errorf("create runner log: %w", createErr)
		}
		_ = f.Close()
	}
	if err := EnsureRoleRuleFiles(paths); err != nil {
		return err
	}

	return nil
}
