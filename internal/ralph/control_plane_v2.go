package ralph

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	controlPlaneSchemaVersion             = 1
	controlPlaneBlockedRateTarget         = 0.25
	controlPlaneRecoverySuccessRateTarget = 0.70
	controlPlaneMTTRSecondsTarget         = 300.0
	controlPlaneBlockedImprovementTarget  = 0.25
	controlPlaneRecoverMaxAttempts        = 5
	controlPlaneRecoverBackoffBaseSec     = 30
	controlPlaneRecoverBackoffCapSec      = 900
	controlPlaneRecoverCircuitThreshold   = 3
	controlPlaneRecoverCircuitCooldownSec = 300
)

const (
	ControlPlaneCutoverFailureCategoryDoctor        = "doctor"
	ControlPlaneCutoverFailureCategoryKPI           = "kpi"
	ControlPlaneCutoverFailureCategoryBaseline      = "baseline"
	ControlPlaneCutoverFailureCategorySoak          = "soak"
	ControlPlaneCutoverFailureCategoryDataIntegrity = "data_integrity"
)

const (
	ControlPlaneTaskStateDraft     = "draft"
	ControlPlaneTaskStatePlanned   = "planned"
	ControlPlaneTaskStateReady     = "ready"
	ControlPlaneTaskStateRunning   = "running"
	ControlPlaneTaskStateVerifying = "verifying"
	ControlPlaneTaskStateDone      = "done"
	ControlPlaneTaskStateBlocked   = "blocked"
)

var controlPlaneTaskStates = map[string]struct{}{
	ControlPlaneTaskStateDraft:     {},
	ControlPlaneTaskStatePlanned:   {},
	ControlPlaneTaskStateReady:     {},
	ControlPlaneTaskStateRunning:   {},
	ControlPlaneTaskStateVerifying: {},
	ControlPlaneTaskStateDone:      {},
	ControlPlaneTaskStateBlocked:   {},
}

type IntentSpecV1 struct {
	ID              string         `json:"id"`
	Version         int            `json:"version"`
	Goal            string         `json:"goal"`
	Constraints     []string       `json:"constraints"`
	SuccessCriteria []string       `json:"success_criteria"`
	NonGoals        []string       `json:"non_goals"`
	Epics           []IntentEpicV1 `json:"epics"`
}

type IntentEpicV1 struct {
	ID    string             `json:"id"`
	Title string             `json:"title"`
	Tasks []IntentTaskSpecV1 `json:"tasks"`
}

type IntentTaskSpecV1 struct {
	ID             string   `json:"id"`
	Title          string   `json:"title"`
	Role           string   `json:"role"`
	Priority       int      `json:"priority"`
	Deps           []string `json:"deps"`
	Acceptance     []string `json:"acceptance"`
	VerifyCmd      string   `json:"verify_cmd"`
	ExecuteCmd     string   `json:"execute_cmd,omitempty"`
	CodexObjective string   `json:"codex_objective,omitempty"`
	RiskLevel      string   `json:"risk_level"`
}

type TaskNodeV1 struct {
	ID             string   `json:"id"`
	EpicID         string   `json:"epic_id"`
	Title          string   `json:"title"`
	Role           string   `json:"role"`
	Priority       int      `json:"priority"`
	Deps           []string `json:"deps"`
	Acceptance     []string `json:"acceptance"`
	VerifyCmd      string   `json:"verify_cmd"`
	ExecuteCmd     string   `json:"execute_cmd,omitempty"`
	CodexObjective string   `json:"codex_objective,omitempty"`
	RiskLevel      string   `json:"risk_level"`
}

type TaskRunV1 struct {
	TaskID        string   `json:"task_id"`
	Attempt       int      `json:"attempt"`
	State         string   `json:"state"`
	StartedAtUTC  string   `json:"started_at_utc"`
	EndedAtUTC    string   `json:"ended_at_utc"`
	FailureReason string   `json:"failure_reason,omitempty"`
	Artifacts     []string `json:"artifacts,omitempty"`
}

type VerificationCheckV1 struct {
	Name   string `json:"name"`
	Type   string `json:"type,omitempty"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type VerificationResultV1 struct {
	TaskID        string                `json:"task_id"`
	Checks        []VerificationCheckV1 `json:"checks"`
	Pass          bool                  `json:"pass"`
	Evidence      []string              `json:"evidence"`
	FailureReason string                `json:"failure_reason,omitempty"`
	VerifiedAtUTC string                `json:"verified_at_utc"`
}

type LearningEventV1 struct {
	TimeUTC    string `json:"time_utc"`
	TaskID     string `json:"task_id"`
	Category   string `json:"category"`
	Lesson     string `json:"lesson"`
	ActionItem string `json:"action_item"`
}

type ControlPlaneEventV1 struct {
	TimeUTC   string `json:"time_utc"`
	Type      string `json:"type"`
	IntentID  string `json:"intent_id,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
	FromState string `json:"from_state,omitempty"`
	ToState   string `json:"to_state,omitempty"`
	Attempt   int    `json:"attempt,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

type TaskRecordV1 struct {
	TaskNodeV1
	IntentID            string `json:"intent_id"`
	State               string `json:"state"`
	Attempt             int    `json:"attempt"`
	BlockedCount        int    `json:"blocked_count"`
	NextRecoverAtUTC    string `json:"next_recover_at_utc,omitempty"`
	CircuitOpenUntilUTC string `json:"circuit_open_until_utc,omitempty"`
	LeaseOwner          string `json:"lease_owner,omitempty"`
	LeaseUntilUTC       string `json:"lease_until_utc,omitempty"`
	HeartbeatUTC        string `json:"heartbeat_utc,omitempty"`
	LastError           string `json:"last_error,omitempty"`
	UpdatedAtUTC        string `json:"updated_at_utc"`
}

type IntentRecordV1 struct {
	Spec          IntentSpecV1 `json:"spec"`
	SourcePath    string       `json:"source_path"`
	ImportedAtUTC string       `json:"imported_at_utc"`
}

type ControlPlaneDBV1 struct {
	SchemaVersion int                             `json:"schema_version"`
	CreatedAtUTC  string                          `json:"created_at_utc"`
	UpdatedAtUTC  string                          `json:"updated_at_utc"`
	Intents       map[string]IntentRecordV1       `json:"intents"`
	Tasks         map[string]TaskRecordV1         `json:"tasks"`
	TaskRuns      []TaskRunV1                     `json:"task_runs"`
	Verifications map[string]VerificationResultV1 `json:"verifications"`
	Learnings     []LearningEventV1               `json:"learnings"`
	Events        []ControlPlaneEventV1           `json:"events"`
}

type ControlPlanePaths struct {
	ProjectDir   string
	IntentDir    string
	GraphDir     string
	TasksDir     string
	RootDir      string
	DBFile       string
	EventsFile   string
	LearningFile string
	ArtifactsDir string
	ReportsDir   string
	CutoverFile  string
	BaselineFile string
}

type ControlPlaneInitResult struct {
	Paths       ControlPlanePaths
	Initialized bool
}

type ControlPlaneImportIntentResult struct {
	IntentID      string
	IntentVersion int
	SourcePath    string
	StoredPath    string
}

type ControlPlanePlanOptions struct {
	Force bool
}

type ControlPlanePlanResult struct {
	IntentID      string
	TasksTotal    int
	ReadyTasks    int
	PlannedTasks  int
	GraphPath     string
	TaskFileCount int
}

type ControlPlaneRunOptions struct {
	MaxWorkers       int
	MaxTasks         int
	LeaseSec         int
	ExecuteWithCodex bool
	ControlDir       string
}

type ControlPlaneRunResult struct {
	Processed      int `json:"processed"`
	Done           int `json:"done"`
	Blocked        int `json:"blocked"`
	Recovered      int `json:"recovered"`
	RemainingReady int `json:"remaining_ready"`
}

type ControlPlaneRecoverOptions struct {
	Limit int
	Force bool
}

type ControlPlaneRecoverResult struct {
	Recovered          int `json:"recovered"`
	SkippedDeps        int `json:"skipped_deps"`
	SkippedRetryBudget int `json:"skipped_retry_budget"`
	SkippedCooldown    int `json:"skipped_cooldown"`
	SkippedCircuitOpen int `json:"skipped_circuit_open"`
}

type ControlPlaneMetrics struct {
	UpdatedAtUTC         string  `json:"updated_at_utc"`
	TotalTasks           int     `json:"total_tasks"`
	DoneTasks            int     `json:"done_tasks"`
	BlockedTasks         int     `json:"blocked_tasks"`
	BlockedRate          float64 `json:"blocked_rate"`
	RecoveryEvents       int     `json:"recovery_events"`
	RecoverySuccesses    int     `json:"recovery_successes"`
	RecoverySuccessRate  float64 `json:"recovery_success_rate"`
	MeanTimeToRecovery   float64 `json:"mttr_seconds"`
	FalseDonePrevented   int     `json:"false_done_prevented"`
	VerificationFailures int     `json:"verification_failures"`
}

type ControlPlaneMetricTargets struct {
	BlockedRateMax         float64 `json:"blocked_rate_max"`
	RecoverySuccessRateMin float64 `json:"recovery_success_rate_min"`
	MTTRSecondsMax         float64 `json:"mttr_seconds_max"`
}

type ControlPlaneMetricsAssessment struct {
	BlockedRatePass         bool `json:"blocked_rate_pass"`
	RecoverySuccessRatePass bool `json:"recovery_success_rate_pass"`
	MTTRSecondsPass         bool `json:"mttr_seconds_pass"`
}

type ControlPlaneMetricsSummary struct {
	UpdatedAtUTC                string                        `json:"updated_at_utc"`
	Metrics                     ControlPlaneMetrics           `json:"metrics"`
	Targets                     ControlPlaneMetricTargets     `json:"targets"`
	Assessment                  ControlPlaneMetricsAssessment `json:"assessment"`
	BaselineAvailable           bool                          `json:"baseline_available"`
	BaselineCapturedAtUTC       string                        `json:"baseline_captured_at_utc,omitempty"`
	BaselineBlockedRate         float64                       `json:"baseline_blocked_rate,omitempty"`
	BaselineRecoverySuccessRate float64                       `json:"baseline_recovery_success_rate,omitempty"`
	BaselineMTTRSeconds         float64                       `json:"baseline_mttr_seconds,omitempty"`
	BlockedRateImprovementRatio float64                       `json:"blocked_rate_improvement_ratio,omitempty"`
}

type ControlPlaneStatus struct {
	UpdatedAtUTC    string              `json:"updated_at_utc"`
	SchemaVersion   int                 `json:"schema_version"`
	IntentsTotal    int                 `json:"intents_total"`
	TasksTotal      int                 `json:"tasks_total"`
	StateCounts     map[string]int      `json:"state_counts"`
	LastEventAtUTC  string              `json:"last_event_at_utc,omitempty"`
	ExpiredLeases   int                 `json:"expired_leases"`
	PendingReadyIDs []string            `json:"pending_ready_ids"`
	Metrics         ControlPlaneMetrics `json:"metrics"`
}

type ControlPlaneDoctorCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

type ControlPlaneDoctorReport struct {
	UpdatedAtUTC string                    `json:"updated_at_utc"`
	Checks       []ControlPlaneDoctorCheck `json:"checks"`
}

type ControlPlaneRepairAction struct {
	Name   string `json:"name"`
	Detail string `json:"detail"`
}

type ControlPlaneRepairResult struct {
	UpdatedAtUTC string                     `json:"updated_at_utc"`
	Actions      []ControlPlaneRepairAction `json:"actions"`
}

type ControlPlaneRepairOptions struct {
	AutoRecover      bool `json:"auto_recover"`
	RecoverLimit     int  `json:"recover_limit"`
	ForceRecover     bool `json:"force_recover"`
	ResetCircuit     bool `json:"reset_circuit"`
	ResetRetryBudget bool `json:"reset_retry_budget"`
}

type ControlPlaneMigrationResult struct {
	IntentID string `json:"intent_id"`
	DryRun   bool   `json:"dry_run"`
	Scanned  int    `json:"scanned"`
	Imported int    `json:"imported"`
	Skipped  int    `json:"skipped"`
}

type ControlPlaneMigrationVerifyResult struct {
	V1Counts map[string]int `json:"v1_counts"`
	V2Counts map[string]int `json:"v2_counts"`
	Matched  bool           `json:"matched"`
	Detail   string         `json:"detail"`
}

type ControlPlaneSoakSnapshot struct {
	TimeUTC string             `json:"time_utc"`
	Status  ControlPlaneStatus `json:"status"`
}

type ControlPlaneSoakReport struct {
	StartedAtUTC    string                     `json:"started_at_utc"`
	FinishedAtUTC   string                     `json:"finished_at_utc"`
	DurationSec     int                        `json:"duration_sec"`
	IntervalSec     int                        `json:"interval_sec"`
	Strict          bool                       `json:"strict"`
	Snapshots       []ControlPlaneSoakSnapshot `json:"snapshots"`
	FailureDetected bool                       `json:"failure_detected"`
	FailureDetail   string                     `json:"failure_detail,omitempty"`
}

type ControlPlaneCutoverState struct {
	Mode          string `json:"mode"`
	Canary        bool   `json:"canary"`
	UpdatedAtUTC  string `json:"updated_at_utc"`
	LastSwitchUTC string `json:"last_switch_utc"`
	Note          string `json:"note,omitempty"`
}

type ControlPlaneCutoverFailure struct {
	Category string `json:"category"`
	Code     string `json:"code"`
	Critical bool   `json:"critical"`
	Detail   string `json:"detail"`
}

type ControlPlaneCutoverEvaluateOptions struct {
	RequireBaseline bool   `json:"require_baseline"`
	RequireSoakPass bool   `json:"require_soak_pass"`
	SoakReportPath  string `json:"soak_report_path,omitempty"`
	MaxSoakAgeSec   int    `json:"max_soak_age_sec,omitempty"`
}

type ControlPlaneCutoverEvaluation struct {
	Ready                        bool                         `json:"ready"`
	EvaluatedAtUTC               string                       `json:"evaluated_at_utc"`
	CurrentMode                  string                       `json:"current_mode"`
	KPIs                         ControlPlaneMetrics          `json:"kpis"`
	DoctorFailures               int                          `json:"doctor_failures"`
	BaselineAvailable            bool                         `json:"baseline_available"`
	BaselineCapturedAtUTC        string                       `json:"baseline_captured_at_utc,omitempty"`
	BaselineBlockedRate          float64                      `json:"baseline_blocked_rate,omitempty"`
	BlockedRateImprovementRatio  float64                      `json:"blocked_rate_improvement_ratio,omitempty"`
	BlockedRateImprovementTarget float64                      `json:"blocked_rate_improvement_target"`
	SoakRequired                 bool                         `json:"soak_required"`
	SoakAvailable                bool                         `json:"soak_available"`
	SoakReportPath               string                       `json:"soak_report_path,omitempty"`
	SoakFailureDetected          bool                         `json:"soak_failure_detected"`
	SoakAgeSec                   int                          `json:"soak_age_sec"`
	CriticalFailureCount         int                          `json:"critical_failure_count"`
	FailureCategories            []string                     `json:"failure_categories,omitempty"`
	Failures                     []ControlPlaneCutoverFailure `json:"failures,omitempty"`
	FailureSummaries             []string                     `json:"failure_summaries,omitempty"`
}

type ControlPlaneFaultInjectResult struct {
	TaskID  string `json:"task_id"`
	Mode    string `json:"mode"`
	Applied bool   `json:"applied"`
	Detail  string `json:"detail"`
	State   string `json:"state"`
	Attempt int    `json:"attempt"`
}

type ControlPlaneMetricsBaseline struct {
	CapturedAtUTC        string  `json:"captured_at_utc"`
	BlockedRate          float64 `json:"blocked_rate"`
	RecoverySuccessRate  float64 `json:"recovery_success_rate"`
	MeanTimeToRecovery   float64 `json:"mttr_seconds"`
	FalseDonePrevented   int     `json:"false_done_prevented"`
	VerificationFailures int     `json:"verification_failures"`
	Note                 string  `json:"note,omitempty"`
}

type controlPlaneTaskGraph struct {
	IntentID       string       `json:"intent_id"`
	GeneratedAtUTC string       `json:"generated_at_utc"`
	Nodes          []TaskNodeV1 `json:"nodes"`
}

func NewControlPlanePaths(projectDir string) (ControlPlanePaths, error) {
	trimmed := strings.TrimSpace(projectDir)
	if trimmed == "" {
		return ControlPlanePaths{}, fmt.Errorf("project dir is required")
	}
	abs, err := filepath.Abs(trimmed)
	if err != nil {
		return ControlPlanePaths{}, fmt.Errorf("resolve project dir: %w", err)
	}
	root := filepath.Join(abs, ".ralph-v2")
	reports := filepath.Join(root, "reports")
	return ControlPlanePaths{
		ProjectDir:   abs,
		IntentDir:    filepath.Join(abs, "intent"),
		GraphDir:     filepath.Join(abs, "graph"),
		TasksDir:     filepath.Join(abs, "tasks"),
		RootDir:      root,
		DBFile:       filepath.Join(root, "controlplane.db"),
		EventsFile:   filepath.Join(root, "events.jsonl"),
		LearningFile: filepath.Join(root, "learning.jsonl"),
		ArtifactsDir: filepath.Join(root, "artifacts"),
		ReportsDir:   reports,
		CutoverFile:  filepath.Join(root, "cutover.json"),
		BaselineFile: filepath.Join(reports, "metrics-baseline.json"),
	}, nil
}

func EnsureControlPlaneLayout(projectDir string) (ControlPlanePaths, error) {
	cpPaths, err := NewControlPlanePaths(projectDir)
	if err != nil {
		return ControlPlanePaths{}, err
	}
	dirs := []string{
		cpPaths.IntentDir,
		cpPaths.GraphDir,
		cpPaths.TasksDir,
		cpPaths.RootDir,
		cpPaths.ArtifactsDir,
		cpPaths.ReportsDir,
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return ControlPlanePaths{}, fmt.Errorf("create dir %s: %w", dir, err)
		}
	}
	if _, err := os.Stat(cpPaths.EventsFile); os.IsNotExist(err) {
		f, createErr := os.OpenFile(cpPaths.EventsFile, os.O_CREATE|os.O_WRONLY, 0o644)
		if createErr != nil {
			return ControlPlanePaths{}, fmt.Errorf("create events file: %w", createErr)
		}
		_ = f.Close()
	}
	if _, err := os.Stat(cpPaths.LearningFile); os.IsNotExist(err) {
		f, createErr := os.OpenFile(cpPaths.LearningFile, os.O_CREATE|os.O_WRONLY, 0o644)
		if createErr != nil {
			return ControlPlanePaths{}, fmt.Errorf("create learning file: %w", createErr)
		}
		_ = f.Close()
	}
	if _, err := os.Stat(cpPaths.CutoverFile); os.IsNotExist(err) {
		if err := writeJSONFile(cpPaths.CutoverFile, defaultControlPlaneCutoverState()); err != nil {
			return ControlPlanePaths{}, fmt.Errorf("write cutover file: %w", err)
		}
	}
	if err := migrateLegacyJSONControlPlaneDBIfNeeded(cpPaths); err != nil {
		return ControlPlanePaths{}, err
	}
	if _, err := os.Stat(cpPaths.DBFile); os.IsNotExist(err) {
		db := newControlPlaneDB()
		if err := saveControlPlaneDB(cpPaths, db); err != nil {
			return ControlPlanePaths{}, err
		}
	} else if err := ensureSQLiteSchema(cpPaths); err != nil {
		return ControlPlanePaths{}, err
	}
	return cpPaths, nil
}

func newControlPlaneDB() ControlPlaneDBV1 {
	now := time.Now().UTC().Format(time.RFC3339)
	return ControlPlaneDBV1{
		SchemaVersion: controlPlaneSchemaVersion,
		CreatedAtUTC:  now,
		UpdatedAtUTC:  now,
		Intents:       map[string]IntentRecordV1{},
		Tasks:         map[string]TaskRecordV1{},
		TaskRuns:      []TaskRunV1{},
		Verifications: map[string]VerificationResultV1{},
		Learnings:     []LearningEventV1{},
		Events:        []ControlPlaneEventV1{},
	}
}

func loadControlPlaneDB(cpPaths ControlPlanePaths) (ControlPlaneDBV1, error) {
	if err := ensureSQLiteSchema(cpPaths); err != nil {
		return ControlPlaneDBV1{}, err
	}

	db := newControlPlaneDB()
	metaRows, err := runSQLiteJSONQuery(cpPaths.DBFile, "SELECT key, value FROM cp_meta;")
	if err != nil {
		return ControlPlaneDBV1{}, err
	}
	meta := map[string]string{}
	for _, row := range metaRows {
		meta[strings.TrimSpace(rowString(row, "key"))] = rowString(row, "value")
	}
	if v := strings.TrimSpace(meta["schema_version"]); v != "" {
		if n, convErr := strconv.Atoi(v); convErr == nil {
			db.SchemaVersion = n
		}
	}
	if strings.TrimSpace(meta["created_at_utc"]) != "" {
		db.CreatedAtUTC = strings.TrimSpace(meta["created_at_utc"])
	}
	if strings.TrimSpace(meta["updated_at_utc"]) != "" {
		db.UpdatedAtUTC = strings.TrimSpace(meta["updated_at_utc"])
	}

	intentRows, err := runSQLiteJSONQuery(cpPaths.DBFile, "SELECT intent_id, spec_json, source_path, imported_at_utc FROM cp_intents ORDER BY intent_id;")
	if err != nil {
		return ControlPlaneDBV1{}, err
	}
	for _, row := range intentRows {
		intentID := strings.TrimSpace(rowString(row, "intent_id"))
		if intentID == "" {
			continue
		}
		specJSON := rowString(row, "spec_json")
		spec := IntentSpecV1{}
		if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
			return ControlPlaneDBV1{}, fmt.Errorf("parse intent spec %s: %w", intentID, err)
		}
		db.Intents[intentID] = IntentRecordV1{
			Spec:          spec,
			SourcePath:    rowString(row, "source_path"),
			ImportedAtUTC: rowString(row, "imported_at_utc"),
		}
	}

	taskRows, err := runSQLiteJSONQuery(cpPaths.DBFile, `
SELECT
  task_id, intent_id, epic_id, title, role, priority,
  deps_json, acceptance_json, verify_cmd, execute_cmd, codex_objective, risk_level, state, attempt,
  blocked_count, next_recover_at_utc, circuit_open_until_utc,
  lease_owner, lease_until_utc, heartbeat_utc, last_error, updated_at_utc
FROM cp_tasks
ORDER BY task_id;
`)
	if err != nil {
		return ControlPlaneDBV1{}, err
	}
	for _, row := range taskRows {
		taskID := strings.TrimSpace(rowString(row, "task_id"))
		if taskID == "" {
			continue
		}
		deps := []string{}
		if err := json.Unmarshal([]byte(rowString(row, "deps_json")), &deps); err != nil {
			return ControlPlaneDBV1{}, fmt.Errorf("parse deps_json for task %s: %w", taskID, err)
		}
		acceptance := []string{}
		if err := json.Unmarshal([]byte(rowString(row, "acceptance_json")), &acceptance); err != nil {
			return ControlPlaneDBV1{}, fmt.Errorf("parse acceptance_json for task %s: %w", taskID, err)
		}
		db.Tasks[taskID] = TaskRecordV1{
			TaskNodeV1: TaskNodeV1{
				ID:             taskID,
				EpicID:         rowString(row, "epic_id"),
				Title:          rowString(row, "title"),
				Role:           rowString(row, "role"),
				Priority:       rowInt(row, "priority"),
				Deps:           deps,
				Acceptance:     acceptance,
				VerifyCmd:      rowString(row, "verify_cmd"),
				ExecuteCmd:     rowString(row, "execute_cmd"),
				CodexObjective: rowString(row, "codex_objective"),
				RiskLevel:      rowString(row, "risk_level"),
			},
			IntentID:            rowString(row, "intent_id"),
			State:               rowString(row, "state"),
			Attempt:             rowInt(row, "attempt"),
			BlockedCount:        rowInt(row, "blocked_count"),
			NextRecoverAtUTC:    strings.TrimSpace(rowString(row, "next_recover_at_utc")),
			CircuitOpenUntilUTC: strings.TrimSpace(rowString(row, "circuit_open_until_utc")),
			LeaseOwner:          rowString(row, "lease_owner"),
			LeaseUntilUTC:       rowString(row, "lease_until_utc"),
			HeartbeatUTC:        rowString(row, "heartbeat_utc"),
			LastError:           rowString(row, "last_error"),
			UpdatedAtUTC:        rowString(row, "updated_at_utc"),
		}
	}

	runRows, err := runSQLiteJSONQuery(cpPaths.DBFile, `
SELECT task_id, attempt, state, started_at_utc, ended_at_utc, failure_reason, artifacts_json
FROM cp_task_runs
ORDER BY id;
`)
	if err != nil {
		return ControlPlaneDBV1{}, err
	}
	db.TaskRuns = make([]TaskRunV1, 0, len(runRows))
	for _, row := range runRows {
		artifacts := []string{}
		if err := json.Unmarshal([]byte(rowString(row, "artifacts_json")), &artifacts); err != nil {
			return ControlPlaneDBV1{}, fmt.Errorf("parse artifacts_json: %w", err)
		}
		db.TaskRuns = append(db.TaskRuns, TaskRunV1{
			TaskID:        rowString(row, "task_id"),
			Attempt:       rowInt(row, "attempt"),
			State:         rowString(row, "state"),
			StartedAtUTC:  rowString(row, "started_at_utc"),
			EndedAtUTC:    rowString(row, "ended_at_utc"),
			FailureReason: rowString(row, "failure_reason"),
			Artifacts:     artifacts,
		})
	}

	verificationRows, err := runSQLiteJSONQuery(cpPaths.DBFile, "SELECT task_id, result_json FROM cp_verifications ORDER BY task_id;")
	if err != nil {
		return ControlPlaneDBV1{}, err
	}
	for _, row := range verificationRows {
		taskID := rowString(row, "task_id")
		result := VerificationResultV1{}
		if err := json.Unmarshal([]byte(rowString(row, "result_json")), &result); err != nil {
			return ControlPlaneDBV1{}, fmt.Errorf("parse verification result for task %s: %w", taskID, err)
		}
		db.Verifications[taskID] = result
	}

	learningRows, err := runSQLiteJSONQuery(cpPaths.DBFile, "SELECT event_json FROM cp_learnings ORDER BY id;")
	if err != nil {
		return ControlPlaneDBV1{}, err
	}
	db.Learnings = make([]LearningEventV1, 0, len(learningRows))
	for _, row := range learningRows {
		entry := LearningEventV1{}
		if err := json.Unmarshal([]byte(rowString(row, "event_json")), &entry); err != nil {
			return ControlPlaneDBV1{}, fmt.Errorf("parse learning event: %w", err)
		}
		db.Learnings = append(db.Learnings, entry)
	}

	eventRows, err := runSQLiteJSONQuery(cpPaths.DBFile, "SELECT event_json FROM cp_events ORDER BY id;")
	if err != nil {
		return ControlPlaneDBV1{}, err
	}
	db.Events = make([]ControlPlaneEventV1, 0, len(eventRows))
	for _, row := range eventRows {
		entry := ControlPlaneEventV1{}
		if err := json.Unmarshal([]byte(rowString(row, "event_json")), &entry); err != nil {
			return ControlPlaneDBV1{}, fmt.Errorf("parse control plane event: %w", err)
		}
		db.Events = append(db.Events, entry)
	}

	return db, nil
}

func saveControlPlaneDB(cpPaths ControlPlanePaths, db ControlPlaneDBV1) error {
	if err := ensureSQLiteSchema(cpPaths); err != nil {
		return err
	}
	db.SchemaVersion = controlPlaneSchemaVersion
	db.UpdatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(db.CreatedAtUTC) == "" {
		db.CreatedAtUTC = db.UpdatedAtUTC
	}
	if db.Intents == nil {
		db.Intents = map[string]IntentRecordV1{}
	}
	if db.Tasks == nil {
		db.Tasks = map[string]TaskRecordV1{}
	}
	if db.Verifications == nil {
		db.Verifications = map[string]VerificationResultV1{}
	}
	if db.TaskRuns == nil {
		db.TaskRuns = []TaskRunV1{}
	}
	if db.Learnings == nil {
		db.Learnings = []LearningEventV1{}
	}
	if db.Events == nil {
		db.Events = []ControlPlaneEventV1{}
	}

	script, err := buildSQLiteSaveScript(db)
	if err != nil {
		return err
	}
	if err := runSQLiteScript(cpPaths.DBFile, script); err != nil {
		return err
	}
	return syncTaskJSONFiles(cpPaths, db)
}

func appendControlPlaneEvent(cpPaths ControlPlanePaths, db *ControlPlaneDBV1, event ControlPlaneEventV1) error {
	now := time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(event.TimeUTC) == "" {
		event.TimeUTC = now
	}
	db.Events = append(db.Events, event)

	f, err := os.OpenFile(cpPaths.EventsFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open events file: %w", err)
	}
	defer f.Close()
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if _, err := f.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	insert := fmt.Sprintf(
		"INSERT INTO cp_event_ledger(recorded_at_utc,event_json) VALUES (%s,%s);",
		sqlQuote(now),
		sqlQuote(string(encoded)),
	)
	if err := runSQLiteScript(cpPaths.DBFile, insert); err != nil {
		return fmt.Errorf("append event ledger: %w", err)
	}
	return nil
}

func appendControlPlaneLearning(cpPaths ControlPlanePaths, db *ControlPlaneDBV1, learning LearningEventV1) error {
	now := time.Now().UTC().Format(time.RFC3339)
	if strings.TrimSpace(learning.TimeUTC) == "" {
		learning.TimeUTC = now
	}
	db.Learnings = append(db.Learnings, learning)

	encoded, err := json.Marshal(learning)
	if err != nil {
		return fmt.Errorf("marshal learning event: %w", err)
	}
	f, err := os.OpenFile(cpPaths.LearningFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open learning file: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("append learning file: %w", err)
	}
	insert := fmt.Sprintf(
		"INSERT INTO cp_learning_ledger(recorded_at_utc,event_json) VALUES (%s,%s);",
		sqlQuote(now),
		sqlQuote(string(encoded)),
	)
	if err := runSQLiteScript(cpPaths.DBFile, insert); err != nil {
		return fmt.Errorf("append learning ledger: %w", err)
	}
	return nil
}

func syncTaskJSONFiles(cpPaths ControlPlanePaths, db ControlPlaneDBV1) error {
	if err := os.MkdirAll(cpPaths.TasksDir, 0o755); err != nil {
		return err
	}

	entries, err := os.ReadDir(cpPaths.TasksDir)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			taskID := strings.TrimSuffix(entry.Name(), ".json")
			if _, ok := db.Tasks[taskID]; ok {
				continue
			}
			_ = os.Remove(filepath.Join(cpPaths.TasksDir, entry.Name()))
		}
	}

	ids := sortedTaskRecordIDs(db.Tasks)
	for _, id := range ids {
		record := db.Tasks[id]
		data, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal task %s: %w", id, err)
		}
		data = append(data, '\n')
		if err := os.WriteFile(filepath.Join(cpPaths.TasksDir, id+".json"), data, 0o644); err != nil {
			return fmt.Errorf("write task %s: %w", id, err)
		}
	}
	return nil
}

func migrateLegacyJSONControlPlaneDBIfNeeded(cpPaths ControlPlanePaths) error {
	data, err := os.ReadFile(cpPaths.DBFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read control plane db: %w", err)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil
	}
	if trimmed[0] != '{' && trimmed[0] != '[' {
		return nil
	}

	legacy := ControlPlaneDBV1{}
	if err := json.Unmarshal(trimmed, &legacy); err != nil {
		return fmt.Errorf("legacy db migration parse failed: %w", err)
	}
	backup := cpPaths.DBFile + ".legacy-json.bak"
	if err := os.WriteFile(backup, append(trimmed, '\n'), 0o644); err != nil {
		return fmt.Errorf("write legacy backup: %w", err)
	}
	if err := os.Remove(cpPaths.DBFile); err != nil {
		return fmt.Errorf("remove legacy json db: %w", err)
	}
	if err := saveControlPlaneDB(cpPaths, legacy); err != nil {
		return fmt.Errorf("migrate legacy json db to sqlite: %w", err)
	}
	return nil
}

func ensureSQLiteSchema(cpPaths ControlPlanePaths) error {
	script := `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS cp_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_intents (
  intent_id TEXT PRIMARY KEY,
  spec_json TEXT NOT NULL,
  source_path TEXT NOT NULL,
  imported_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_tasks (
  task_id TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL,
  epic_id TEXT NOT NULL,
  title TEXT NOT NULL,
  role TEXT NOT NULL,
  priority INTEGER NOT NULL,
  deps_json TEXT NOT NULL,
  acceptance_json TEXT NOT NULL,
  verify_cmd TEXT NOT NULL,
  execute_cmd TEXT NOT NULL DEFAULT '',
  codex_objective TEXT NOT NULL DEFAULT '',
  risk_level TEXT NOT NULL,
  state TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  blocked_count INTEGER NOT NULL DEFAULT 0,
  next_recover_at_utc TEXT NOT NULL DEFAULT '',
  circuit_open_until_utc TEXT NOT NULL DEFAULT '',
  lease_owner TEXT NOT NULL,
  lease_until_utc TEXT NOT NULL,
  heartbeat_utc TEXT NOT NULL,
  last_error TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_task_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  state TEXT NOT NULL,
  started_at_utc TEXT NOT NULL,
  ended_at_utc TEXT NOT NULL,
  failure_reason TEXT NOT NULL,
  artifacts_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_verifications (
  task_id TEXT PRIMARY KEY,
  result_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_learnings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_event_ledger (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  recorded_at_utc TEXT NOT NULL,
  event_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cp_learning_ledger (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  recorded_at_utc TEXT NOT NULL,
  event_json TEXT NOT NULL
);
INSERT OR IGNORE INTO cp_meta(key, value) VALUES ('schema_version', '1');
INSERT OR IGNORE INTO cp_meta(key, value) VALUES ('created_at_utc', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
INSERT OR IGNORE INTO cp_meta(key, value) VALUES ('updated_at_utc', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
`
	if err := runSQLiteScript(cpPaths.DBFile, script); err != nil {
		return err
	}
	if err := ensureSQLiteColumn(cpPaths.DBFile, "cp_tasks", "execute_cmd", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureSQLiteColumn(cpPaths.DBFile, "cp_tasks", "codex_objective", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureSQLiteColumn(cpPaths.DBFile, "cp_tasks", "blocked_count", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureSQLiteColumn(cpPaths.DBFile, "cp_tasks", "next_recover_at_utc", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureSQLiteColumn(cpPaths.DBFile, "cp_tasks", "circuit_open_until_utc", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := backfillControlPlaneLedgerTablesIfNeeded(cpPaths.DBFile); err != nil {
		return err
	}
	return nil
}

func backfillControlPlaneLedgerTablesIfNeeded(dbPath string) error {
	if err := backfillEventLedgerIfNeeded(dbPath); err != nil {
		return err
	}
	if err := backfillLearningLedgerIfNeeded(dbPath); err != nil {
		return err
	}
	return nil
}

func backfillEventLedgerIfNeeded(dbPath string) error {
	ledgerRows, err := countSQLiteTableRows(dbPath, "cp_event_ledger")
	if err != nil {
		return err
	}
	if ledgerRows > 0 {
		return nil
	}
	rows, err := runSQLiteJSONQuery(dbPath, "SELECT event_json FROM cp_events ORDER BY id;")
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	var script strings.Builder
	script.WriteString("BEGIN IMMEDIATE;\n")
	for _, row := range rows {
		raw := strings.TrimSpace(rowString(row, "event_json"))
		if raw == "" {
			continue
		}
		recordedAtUTC := time.Now().UTC().Format(time.RFC3339)
		entry := ControlPlaneEventV1{}
		if err := json.Unmarshal([]byte(raw), &entry); err == nil {
			if ts := strings.TrimSpace(entry.TimeUTC); ts != "" {
				recordedAtUTC = ts
			}
		}
		script.WriteString(
			fmt.Sprintf(
				"INSERT INTO cp_event_ledger(recorded_at_utc,event_json) VALUES (%s,%s);\n",
				sqlQuote(recordedAtUTC),
				sqlQuote(raw),
			),
		)
	}
	script.WriteString("COMMIT;\n")
	return runSQLiteScript(dbPath, script.String())
}

func backfillLearningLedgerIfNeeded(dbPath string) error {
	ledgerRows, err := countSQLiteTableRows(dbPath, "cp_learning_ledger")
	if err != nil {
		return err
	}
	if ledgerRows > 0 {
		return nil
	}
	rows, err := runSQLiteJSONQuery(dbPath, "SELECT event_json FROM cp_learnings ORDER BY id;")
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	var script strings.Builder
	script.WriteString("BEGIN IMMEDIATE;\n")
	for _, row := range rows {
		raw := strings.TrimSpace(rowString(row, "event_json"))
		if raw == "" {
			continue
		}
		recordedAtUTC := time.Now().UTC().Format(time.RFC3339)
		entry := LearningEventV1{}
		if err := json.Unmarshal([]byte(raw), &entry); err == nil {
			if ts := strings.TrimSpace(entry.TimeUTC); ts != "" {
				recordedAtUTC = ts
			}
		}
		script.WriteString(
			fmt.Sprintf(
				"INSERT INTO cp_learning_ledger(recorded_at_utc,event_json) VALUES (%s,%s);\n",
				sqlQuote(recordedAtUTC),
				sqlQuote(raw),
			),
		)
	}
	script.WriteString("COMMIT;\n")
	return runSQLiteScript(dbPath, script.String())
}

func buildSQLiteSaveScript(db ControlPlaneDBV1) (string, error) {
	var b strings.Builder
	b.WriteString("BEGIN IMMEDIATE;\n")
	b.WriteString("DELETE FROM cp_intents;\n")
	b.WriteString("DELETE FROM cp_tasks;\n")
	b.WriteString("DELETE FROM cp_task_runs;\n")
	b.WriteString("DELETE FROM cp_verifications;\n")
	b.WriteString("DELETE FROM cp_learnings;\n")
	b.WriteString("DELETE FROM cp_events;\n")
	b.WriteString(fmt.Sprintf("INSERT INTO cp_meta(key,value) VALUES ('schema_version','%d') ON CONFLICT(key) DO UPDATE SET value=excluded.value;\n", db.SchemaVersion))
	b.WriteString(fmt.Sprintf("INSERT INTO cp_meta(key,value) VALUES ('created_at_utc',%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value;\n", sqlQuote(db.CreatedAtUTC)))
	b.WriteString(fmt.Sprintf("INSERT INTO cp_meta(key,value) VALUES ('updated_at_utc',%s) ON CONFLICT(key) DO UPDATE SET value=excluded.value;\n", sqlQuote(db.UpdatedAtUTC)))

	intentIDs := make([]string, 0, len(db.Intents))
	for id := range db.Intents {
		intentIDs = append(intentIDs, id)
	}
	sort.Strings(intentIDs)
	for _, id := range intentIDs {
		record := db.Intents[id]
		specJSON, err := json.Marshal(record.Spec)
		if err != nil {
			return "", fmt.Errorf("marshal intent spec %s: %w", id, err)
		}
		b.WriteString(
			fmt.Sprintf(
				"INSERT INTO cp_intents(intent_id,spec_json,source_path,imported_at_utc) VALUES (%s,%s,%s,%s);\n",
				sqlQuote(id),
				sqlQuote(string(specJSON)),
				sqlQuote(record.SourcePath),
				sqlQuote(record.ImportedAtUTC),
			),
		)
	}

	taskIDs := sortedTaskRecordIDs(db.Tasks)
	for _, id := range taskIDs {
		record := db.Tasks[id]
		depsJSON, err := json.Marshal(record.Deps)
		if err != nil {
			return "", fmt.Errorf("marshal task deps %s: %w", id, err)
		}
		acceptanceJSON, err := json.Marshal(record.Acceptance)
		if err != nil {
			return "", fmt.Errorf("marshal task acceptance %s: %w", id, err)
		}
		b.WriteString(
			fmt.Sprintf(
				`INSERT INTO cp_tasks(
task_id,intent_id,epic_id,title,role,priority,deps_json,acceptance_json,verify_cmd,execute_cmd,codex_objective,risk_level,state,attempt,blocked_count,next_recover_at_utc,circuit_open_until_utc,lease_owner,lease_until_utc,heartbeat_utc,last_error,updated_at_utc
) VALUES (%s,%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s,%s,%d,%d,%s,%s,%s,%s,%s,%s,%s);
`,
				sqlQuote(record.ID),
				sqlQuote(record.IntentID),
				sqlQuote(record.EpicID),
				sqlQuote(record.Title),
				sqlQuote(record.Role),
				record.Priority,
				sqlQuote(string(depsJSON)),
				sqlQuote(string(acceptanceJSON)),
				sqlQuote(record.VerifyCmd),
				sqlQuote(record.ExecuteCmd),
				sqlQuote(record.CodexObjective),
				sqlQuote(record.RiskLevel),
				sqlQuote(record.State),
				record.Attempt,
				record.BlockedCount,
				sqlQuote(record.NextRecoverAtUTC),
				sqlQuote(record.CircuitOpenUntilUTC),
				sqlQuote(record.LeaseOwner),
				sqlQuote(record.LeaseUntilUTC),
				sqlQuote(record.HeartbeatUTC),
				sqlQuote(record.LastError),
				sqlQuote(record.UpdatedAtUTC),
			),
		)
	}

	for _, run := range db.TaskRuns {
		artifactsJSON, err := json.Marshal(run.Artifacts)
		if err != nil {
			return "", fmt.Errorf("marshal task run artifacts for task %s: %w", run.TaskID, err)
		}
		b.WriteString(
			fmt.Sprintf(
				"INSERT INTO cp_task_runs(task_id,attempt,state,started_at_utc,ended_at_utc,failure_reason,artifacts_json) VALUES (%s,%d,%s,%s,%s,%s,%s);\n",
				sqlQuote(run.TaskID),
				run.Attempt,
				sqlQuote(run.State),
				sqlQuote(run.StartedAtUTC),
				sqlQuote(run.EndedAtUTC),
				sqlQuote(run.FailureReason),
				sqlQuote(string(artifactsJSON)),
			),
		)
	}

	verificationIDs := make([]string, 0, len(db.Verifications))
	for id := range db.Verifications {
		verificationIDs = append(verificationIDs, id)
	}
	sort.Strings(verificationIDs)
	for _, taskID := range verificationIDs {
		resultJSON, err := json.Marshal(db.Verifications[taskID])
		if err != nil {
			return "", fmt.Errorf("marshal verification for task %s: %w", taskID, err)
		}
		b.WriteString(
			fmt.Sprintf(
				"INSERT INTO cp_verifications(task_id,result_json) VALUES (%s,%s);\n",
				sqlQuote(taskID),
				sqlQuote(string(resultJSON)),
			),
		)
	}

	for _, learning := range db.Learnings {
		entryJSON, err := json.Marshal(learning)
		if err != nil {
			return "", fmt.Errorf("marshal learning event: %w", err)
		}
		b.WriteString(fmt.Sprintf("INSERT INTO cp_learnings(event_json) VALUES (%s);\n", sqlQuote(string(entryJSON))))
	}

	for _, event := range db.Events {
		entryJSON, err := json.Marshal(event)
		if err != nil {
			return "", fmt.Errorf("marshal control plane event: %w", err)
		}
		b.WriteString(fmt.Sprintf("INSERT INTO cp_events(event_json) VALUES (%s);\n", sqlQuote(string(entryJSON))))
	}

	b.WriteString("COMMIT;\n")
	return b.String(), nil
}

func runSQLiteScript(dbPath, script string) error {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		return fmt.Errorf("sqlite3 command not found")
	}
	cmd := exec.Command("sqlite3", dbPath)
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sqlite3 script failed: %v (%s)", err, compactLoopText(string(out), 220))
	}
	return nil
}

func runSQLiteJSONQuery(dbPath, query string) ([]map[string]any, error) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		return nil, fmt.Errorf("sqlite3 command not found")
	}
	cmd := exec.Command("sqlite3", "-json", dbPath, query)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("sqlite3 query failed: %v (%s)", err, compactLoopText(string(out), 220))
	}
	trimmed := bytes.TrimSpace(out)
	if len(trimmed) == 0 {
		return []map[string]any{}, nil
	}
	rows := []map[string]any{}
	if err := json.Unmarshal(trimmed, &rows); err != nil {
		return nil, fmt.Errorf("parse sqlite json output: %w", err)
	}
	return rows, nil
}

func countSQLiteTableRows(dbPath, tableName string) (int, error) {
	rows, err := runSQLiteJSONQuery(dbPath, fmt.Sprintf("SELECT COUNT(*) AS cnt FROM %s;", tableName))
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rowInt(rows[0], "cnt"), nil
}

func ensureSQLiteColumn(dbPath, tableName, columnName, columnDef string) error {
	rows, err := runSQLiteJSONQuery(dbPath, fmt.Sprintf("PRAGMA table_info(%s);", tableName))
	if err != nil {
		return err
	}
	for _, row := range rows {
		if strings.TrimSpace(rowString(row, "name")) == columnName {
			return nil
		}
	}
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", tableName, columnName, columnDef)
	return runSQLiteScript(dbPath, stmt)
}

func sqlQuote(raw string) string {
	escaped := strings.ReplaceAll(raw, "'", "''")
	return "'" + escaped + "'"
}

func rowString(row map[string]any, key string) string {
	v, ok := row[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", t)
	}
}

func rowInt(row map[string]any, key string) int {
	v, ok := row[key]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(t))
		if err == nil {
			return n
		}
		return 0
	default:
		return 0
	}
}

func ControlPlaneInit(projectDir string) (ControlPlaneInitResult, error) {
	cpPaths, err := NewControlPlanePaths(projectDir)
	if err != nil {
		return ControlPlaneInitResult{}, err
	}
	_, statErr := os.Stat(cpPaths.DBFile)
	initialized := os.IsNotExist(statErr)
	ensuredPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneInitResult{}, err
	}
	return ControlPlaneInitResult{Paths: ensuredPaths, Initialized: initialized}, nil
}

func ControlPlaneImportIntent(projectDir, intentFile string) (ControlPlaneImportIntentResult, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneImportIntentResult{}, err
	}
	absPath, err := resolveInputPath(cpPaths.ProjectDir, intentFile)
	if err != nil {
		return ControlPlaneImportIntentResult{}, err
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return ControlPlaneImportIntentResult{}, fmt.Errorf("read intent file: %w", err)
	}
	var spec IntentSpecV1
	if err := json.Unmarshal(data, &spec); err != nil {
		return ControlPlaneImportIntentResult{}, fmt.Errorf("parse intent json: %w", err)
	}
	if err := validateIntentSpec(spec); err != nil {
		return ControlPlaneImportIntentResult{}, err
	}

	normalized, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return ControlPlaneImportIntentResult{}, fmt.Errorf("marshal normalized intent: %w", err)
	}
	normalized = append(normalized, '\n')
	storedName := fmt.Sprintf("%s.v%d.json", spec.ID, spec.Version)
	storedPath := filepath.Join(cpPaths.IntentDir, storedName)
	if err := os.WriteFile(storedPath, normalized, 0o644); err != nil {
		return ControlPlaneImportIntentResult{}, fmt.Errorf("write intent store file: %w", err)
	}

	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlaneImportIntentResult{}, err
	}
	db.Intents[spec.ID] = IntentRecordV1{
		Spec:          spec,
		SourcePath:    absPath,
		ImportedAtUTC: time.Now().UTC().Format(time.RFC3339),
	}
	if err := appendControlPlaneEvent(cpPaths, &db, ControlPlaneEventV1{
		Type:     "intent_imported",
		IntentID: spec.ID,
		Detail:   fmt.Sprintf("version=%d source=%s", spec.Version, absPath),
	}); err != nil {
		return ControlPlaneImportIntentResult{}, err
	}
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		return ControlPlaneImportIntentResult{}, err
	}
	return ControlPlaneImportIntentResult{
		IntentID:      spec.ID,
		IntentVersion: spec.Version,
		SourcePath:    absPath,
		StoredPath:    storedPath,
	}, nil
}

func ControlPlanePlanIntent(projectDir, intentID string, opts ControlPlanePlanOptions) (ControlPlanePlanResult, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlanePlanResult{}, err
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlanePlanResult{}, err
	}
	intentID = strings.TrimSpace(intentID)
	if intentID == "" {
		return ControlPlanePlanResult{}, fmt.Errorf("intent id is required")
	}
	record, ok := db.Intents[intentID]
	if !ok {
		return ControlPlanePlanResult{}, fmt.Errorf("intent not found: %s", intentID)
	}

	if !opts.Force {
		for _, task := range db.Tasks {
			if task.IntentID == intentID {
				return ControlPlanePlanResult{}, fmt.Errorf("intent %s already planned; rerun with --force to rebuild", intentID)
			}
		}
	} else {
		for id, task := range db.Tasks {
			if task.IntentID != intentID {
				continue
			}
			delete(db.Tasks, id)
			delete(db.Verifications, id)
		}
	}

	nodes, err := compileIntentToTaskNodes(record.Spec)
	if err != nil {
		return ControlPlanePlanResult{}, err
	}
	if err := validateTaskNodeGraph(nodes); err != nil {
		return ControlPlanePlanResult{}, err
	}
	taskIDs := []string{}
	for _, node := range nodes {
		record := TaskRecordV1{
			TaskNodeV1:    node,
			IntentID:      intentID,
			State:         ControlPlaneTaskStatePlanned,
			Attempt:       0,
			UpdatedAtUTC:  time.Now().UTC().Format(time.RFC3339),
			LeaseOwner:    "",
			LeaseUntilUTC: "",
			HeartbeatUTC:  "",
			LastError:     "",
		}
		db.Tasks[node.ID] = record
		taskIDs = append(taskIDs, node.ID)
	}

	readyCount := promotePlannedTasksToReady(&db, cpPaths)
	graph := controlPlaneTaskGraph{
		IntentID:       intentID,
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Nodes:          nodes,
	}
	graphPath := filepath.Join(cpPaths.GraphDir, intentID+".task_graph.json")
	if err := writeJSONFile(graphPath, graph); err != nil {
		return ControlPlanePlanResult{}, err
	}

	if err := appendControlPlaneEvent(cpPaths, &db, ControlPlaneEventV1{
		Type:     "plan_compiled",
		IntentID: intentID,
		Detail:   fmt.Sprintf("tasks=%d ready=%d", len(nodes), readyCount),
	}); err != nil {
		return ControlPlanePlanResult{}, err
	}
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		return ControlPlanePlanResult{}, err
	}
	return ControlPlanePlanResult{
		IntentID:      intentID,
		TasksTotal:    len(nodes),
		ReadyTasks:    readyCount,
		PlannedTasks:  len(nodes) - readyCount,
		GraphPath:     graphPath,
		TaskFileCount: len(taskIDs),
	}, nil
}

func ControlPlaneRun(projectDir string, opts ControlPlaneRunOptions) (ControlPlaneRunResult, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneRunResult{}, err
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlaneRunResult{}, err
	}
	if opts.MaxWorkers <= 0 {
		opts.MaxWorkers = 1
	}
	if opts.LeaseSec <= 0 {
		opts.LeaseSec = 120
	}
	result := ControlPlaneRunResult{}
	result.Recovered += reclaimExpiredLeases(&db, cpPaths)
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		return ControlPlaneRunResult{}, err
	}

	runResults := make(chan controlPlaneTaskWorkerResult, opts.MaxWorkers)
	heartbeats := make(chan controlPlaneHeartbeat, opts.MaxWorkers*2)
	active := 0
	processed := 0
	workerSeq := 0
	for {
		for active < opts.MaxWorkers && (opts.MaxTasks <= 0 || processed+active < opts.MaxTasks) {
			workerSeq++
			workerNum := ((workerSeq - 1) % opts.MaxWorkers) + 1
			claimedTask, claimed, claimErr := claimNextReadyControlPlaneTask(&db, cpPaths, workerNum, opts.LeaseSec)
			if claimErr != nil {
				return result, claimErr
			}
			if !claimed {
				break
			}
			if err := saveControlPlaneDB(cpPaths, db); err != nil {
				return result, err
			}
			active++
			go runControlPlaneTaskWorker(cpPaths, claimedTask, workerNum, opts, heartbeats, runResults)
		}
		if active == 0 {
			break
		}
		select {
		case hb := <-heartbeats:
			if applyControlPlaneHeartbeat(&db, hb, opts.LeaseSec) {
				if err := saveControlPlaneDB(cpPaths, db); err != nil {
					return result, err
				}
			}
		case workerResult := <-runResults:
			active--
			if workerResult.Err != nil {
				result.Processed = processed
				result.RemainingReady = len(collectReadyTaskIDs(db.Tasks))
				return result, workerResult.Err
			}
			finalState, applyErr := applyClaimedTaskOutcome(&db, cpPaths, workerResult.ClaimedTask, workerResult.Outcome)
			if applyErr != nil {
				result.Processed = processed
				result.RemainingReady = len(collectReadyTaskIDs(db.Tasks))
				return result, applyErr
			}
			processed++
			switch finalState {
			case ControlPlaneTaskStateDone:
				result.Done++
			case ControlPlaneTaskStateBlocked:
				result.Blocked++
			}
			if err := saveControlPlaneDB(cpPaths, db); err != nil {
				result.Processed = processed
				result.RemainingReady = len(collectReadyTaskIDs(db.Tasks))
				return result, err
			}
		}
	}
	result.Processed = processed
	result.RemainingReady = len(collectReadyTaskIDs(db.Tasks))
	return result, nil
}

func ControlPlaneVerifyTask(projectDir, taskID string) (VerificationResultV1, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return VerificationResultV1{}, err
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return VerificationResultV1{}, err
	}
	taskID = strings.TrimSpace(taskID)
	task, ok := db.Tasks[taskID]
	if !ok {
		return VerificationResultV1{}, fmt.Errorf("task not found: %s", taskID)
	}
	if task.State == ControlPlaneTaskStateBlocked {
		return VerificationResultV1{}, fmt.Errorf("task is blocked; run cp recover first: %s", taskID)
	}

	if task.State == ControlPlaneTaskStateDone {
		result, reason, err := verifyTaskCommand(cpPaths, task)
		if err != nil {
			return VerificationResultV1{}, err
		}
		if !result.Pass {
			return result, fmt.Errorf("verification failed for done task %s: %s", taskID, reason)
		}
		return result, nil
	}

	if task.State == ControlPlaneTaskStateReady || task.State == ControlPlaneTaskStateRunning {
		if err := setControlPlaneTaskState(&db, cpPaths, taskID, ControlPlaneTaskStateVerifying, "task_state_changed", "manual verify", false); err != nil {
			return VerificationResultV1{}, err
		}
	}

	task = db.Tasks[taskID]
	if task.State != ControlPlaneTaskStateVerifying {
		return VerificationResultV1{}, fmt.Errorf("task must be ready/running/verifying/done for verify: current=%s", task.State)
	}

	result, reason, err := verifyTaskCommand(cpPaths, task)
	if err != nil {
		return VerificationResultV1{}, err
	}
	db.Verifications[taskID] = result
	if result.Pass {
		if err := setControlPlaneTaskState(&db, cpPaths, taskID, ControlPlaneTaskStateDone, "task_state_changed", "manual verify passed", false); err != nil {
			return VerificationResultV1{}, err
		}
		task = db.Tasks[taskID]
		task.BlockedCount = 0
		task.NextRecoverAtUTC = ""
		task.CircuitOpenUntilUTC = ""
		task.LastError = ""
		task.UpdatedAtUTC = time.Now().UTC().Format(time.RFC3339)
		db.Tasks[taskID] = task
	} else {
		if strings.Contains(reason, "missing_evidence") {
			_ = appendControlPlaneEvent(cpPaths, &db, ControlPlaneEventV1{
				Type:   "false_done_prevented",
				TaskID: taskID,
				Detail: reason,
			})
		}
		if err := setControlPlaneTaskState(&db, cpPaths, taskID, ControlPlaneTaskStateBlocked, "verify_failed", reason, false); err != nil {
			return VerificationResultV1{}, err
		}
		task = db.Tasks[taskID]
		failedAt := time.Now().UTC()
		policyTask, circuitOpened := applyBlockedRecoveryPolicy(task, failedAt)
		task = policyTask
		task.LastError = reason
		task.UpdatedAtUTC = failedAt.Format(time.RFC3339)
		db.Tasks[taskID] = task
		if circuitOpened {
			_ = appendControlPlaneEvent(cpPaths, &db, ControlPlaneEventV1{
				Type:   "circuit_opened",
				TaskID: taskID,
				Detail: fmt.Sprintf("blocked_count=%d until=%s", task.BlockedCount, task.CircuitOpenUntilUTC),
			})
		}
	}
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		return VerificationResultV1{}, err
	}
	if !result.Pass {
		return result, fmt.Errorf("verification failed: %s", reason)
	}
	return result, nil
}

func ControlPlaneRecover(projectDir string, limit int) (ControlPlaneRecoverResult, error) {
	return ControlPlaneRecoverWithOptions(projectDir, ControlPlaneRecoverOptions{
		Limit: limit,
	})
}

func ControlPlaneRecoverWithOptions(projectDir string, opts ControlPlaneRecoverOptions) (ControlPlaneRecoverResult, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneRecoverResult{}, err
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlaneRecoverResult{}, err
	}
	if opts.Limit < 0 {
		return ControlPlaneRecoverResult{}, fmt.Errorf("limit must be >= 0")
	}
	result := ControlPlaneRecoverResult{}
	blockedIDs := collectTaskIDsByState(db.Tasks, ControlPlaneTaskStateBlocked)
	now := time.Now().UTC()
	for _, id := range blockedIDs {
		task := db.Tasks[id]
		if !depsSatisfied(task, db.Tasks) {
			result.SkippedDeps++
			continue
		}
		if task.Attempt >= controlPlaneRecoverMaxAttempts {
			result.SkippedRetryBudget++
			continue
		}
		if !opts.Force && circuitActive(task, now) {
			result.SkippedCircuitOpen++
			continue
		}
		if !opts.Force && recoverCooldownActive(task, now) {
			result.SkippedCooldown++
			continue
		}
		detail := "manual recover"
		if opts.Force {
			detail = "manual recover forced"
		}
		if err := setControlPlaneTaskState(&db, cpPaths, id, ControlPlaneTaskStateReady, "recovered", detail, false); err != nil {
			return ControlPlaneRecoverResult{}, err
		}
		task = db.Tasks[id]
		task.NextRecoverAtUTC = ""
		task.CircuitOpenUntilUTC = ""
		task.LeaseOwner = ""
		task.LeaseUntilUTC = ""
		task.HeartbeatUTC = ""
		task.LastError = ""
		task.UpdatedAtUTC = now.Format(time.RFC3339)
		db.Tasks[id] = task
		result.Recovered++
		if opts.Limit > 0 && result.Recovered >= opts.Limit {
			break
		}
	}
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		return ControlPlaneRecoverResult{}, err
	}
	return result, nil
}

func ControlPlaneStatusReport(projectDir string) (ControlPlaneStatus, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneStatus{}, err
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlaneStatus{}, err
	}
	counts := map[string]int{}
	for state := range controlPlaneTaskStates {
		counts[state] = 0
	}
	pendingReady := []string{}
	expired := 0
	now := time.Now().UTC()
	for _, task := range db.Tasks {
		counts[task.State]++
		if task.State == ControlPlaneTaskStateReady {
			pendingReady = append(pendingReady, task.ID)
		}
		if (task.State == ControlPlaneTaskStateRunning || task.State == ControlPlaneTaskStateVerifying) && leaseExpired(task, now) {
			expired++
		}
	}
	sort.Strings(pendingReady)
	lastEvent := ""
	if len(db.Events) > 0 {
		lastEvent = db.Events[len(db.Events)-1].TimeUTC
	}
	metrics := computeControlPlaneMetrics(db)
	return ControlPlaneStatus{
		UpdatedAtUTC:    time.Now().UTC().Format(time.RFC3339),
		SchemaVersion:   db.SchemaVersion,
		IntentsTotal:    len(db.Intents),
		TasksTotal:      len(db.Tasks),
		StateCounts:     counts,
		LastEventAtUTC:  lastEvent,
		ExpiredLeases:   expired,
		PendingReadyIDs: pendingReady,
		Metrics:         metrics,
	}, nil
}

func ControlPlaneMetricsReport(projectDir string) (ControlPlaneMetrics, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneMetrics{}, err
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlaneMetrics{}, err
	}
	return computeControlPlaneMetrics(db), nil
}

func ControlPlaneMetricsSummaryReport(projectDir string) (ControlPlaneMetricsSummary, error) {
	metrics, err := ControlPlaneMetricsReport(projectDir)
	if err != nil {
		return ControlPlaneMetricsSummary{}, err
	}
	targets := ControlPlaneMetricTargets{
		BlockedRateMax:         controlPlaneBlockedRateTarget,
		RecoverySuccessRateMin: controlPlaneRecoverySuccessRateTarget,
		MTTRSecondsMax:         controlPlaneMTTRSecondsTarget,
	}
	assessment := ControlPlaneMetricsAssessment{
		BlockedRatePass:         metrics.BlockedRate <= targets.BlockedRateMax,
		RecoverySuccessRatePass: metrics.RecoveryEvents == 0 || metrics.RecoverySuccessRate >= targets.RecoverySuccessRateMin,
		MTTRSecondsPass:         metrics.MeanTimeToRecovery == 0 || metrics.MeanTimeToRecovery <= targets.MTTRSecondsMax,
	}
	summary := ControlPlaneMetricsSummary{
		UpdatedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Metrics:      metrics,
		Targets:      targets,
		Assessment:   assessment,
	}
	baseline, ok, err := GetControlPlaneMetricsBaseline(projectDir)
	if err != nil {
		return ControlPlaneMetricsSummary{}, err
	}
	if !ok {
		return summary, nil
	}
	summary.BaselineAvailable = true
	summary.BaselineCapturedAtUTC = baseline.CapturedAtUTC
	summary.BaselineBlockedRate = baseline.BlockedRate
	summary.BaselineRecoverySuccessRate = baseline.RecoverySuccessRate
	summary.BaselineMTTRSeconds = baseline.MeanTimeToRecovery
	if baseline.BlockedRate > 0 {
		summary.BlockedRateImprovementRatio = (baseline.BlockedRate - metrics.BlockedRate) / baseline.BlockedRate
	}
	return summary, nil
}

func CaptureControlPlaneMetricsBaseline(projectDir, note string) (ControlPlaneMetricsBaseline, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneMetricsBaseline{}, err
	}
	metrics, err := ControlPlaneMetricsReport(projectDir)
	if err != nil {
		return ControlPlaneMetricsBaseline{}, err
	}
	baseline := ControlPlaneMetricsBaseline{
		CapturedAtUTC:        time.Now().UTC().Format(time.RFC3339),
		BlockedRate:          metrics.BlockedRate,
		RecoverySuccessRate:  metrics.RecoverySuccessRate,
		MeanTimeToRecovery:   metrics.MeanTimeToRecovery,
		FalseDonePrevented:   metrics.FalseDonePrevented,
		VerificationFailures: metrics.VerificationFailures,
		Note:                 strings.TrimSpace(note),
	}
	if err := writeJSONFile(cpPaths.BaselineFile, baseline); err != nil {
		return ControlPlaneMetricsBaseline{}, err
	}
	return baseline, nil
}

func GetControlPlaneMetricsBaseline(projectDir string) (ControlPlaneMetricsBaseline, bool, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneMetricsBaseline{}, false, err
	}
	data, err := os.ReadFile(cpPaths.BaselineFile)
	if err != nil {
		if os.IsNotExist(err) {
			return ControlPlaneMetricsBaseline{}, false, nil
		}
		return ControlPlaneMetricsBaseline{}, false, fmt.Errorf("read baseline file: %w", err)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return ControlPlaneMetricsBaseline{}, false, nil
	}
	baseline := ControlPlaneMetricsBaseline{}
	if err := json.Unmarshal(trimmed, &baseline); err != nil {
		return ControlPlaneMetricsBaseline{}, false, fmt.Errorf("parse baseline file: %w", err)
	}
	return baseline, true, nil
}

func loadCutoverSoakReport(projectDir, explicitPath string) (ControlPlaneSoakReport, string, bool, error) {
	cpPaths, err := NewControlPlanePaths(projectDir)
	if err != nil {
		return ControlPlaneSoakReport{}, "", false, err
	}
	reportPath := strings.TrimSpace(explicitPath)
	if reportPath != "" {
		if !filepath.IsAbs(reportPath) {
			reportPath = filepath.Join(projectDir, reportPath)
		}
		data, readErr := os.ReadFile(reportPath)
		if readErr != nil {
			if os.IsNotExist(readErr) {
				return ControlPlaneSoakReport{}, reportPath, false, nil
			}
			return ControlPlaneSoakReport{}, reportPath, false, fmt.Errorf("read soak report: %w", readErr)
		}
		report := ControlPlaneSoakReport{}
		if err := json.Unmarshal(bytes.TrimSpace(data), &report); err != nil {
			return ControlPlaneSoakReport{}, reportPath, false, fmt.Errorf("parse soak report: %w", err)
		}
		return report, reportPath, true, nil
	}

	candidates, globErr := filepath.Glob(filepath.Join(cpPaths.ReportsDir, "soak-*.json"))
	if globErr != nil {
		return ControlPlaneSoakReport{}, "", false, globErr
	}
	if len(candidates) == 0 {
		return ControlPlaneSoakReport{}, "", false, nil
	}
	sort.Strings(candidates)
	reportPath = candidates[len(candidates)-1]
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		if os.IsNotExist(readErr) {
			return ControlPlaneSoakReport{}, reportPath, false, nil
		}
		return ControlPlaneSoakReport{}, reportPath, false, fmt.Errorf("read soak report: %w", readErr)
	}
	report := ControlPlaneSoakReport{}
	if err := json.Unmarshal(bytes.TrimSpace(data), &report); err != nil {
		return ControlPlaneSoakReport{}, reportPath, false, fmt.Errorf("parse soak report: %w", err)
	}
	return report, reportPath, true, nil
}

func cutoverSoakCompletedAt(report ControlPlaneSoakReport) time.Time {
	if ts := strings.TrimSpace(report.FinishedAtUTC); ts != "" {
		if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
			return parsed
		}
	}
	if ts := strings.TrimSpace(report.StartedAtUTC); ts != "" {
		if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func ControlPlaneRunSoak(projectDir string, durationSec, intervalSec int, strict bool) (ControlPlaneSoakReport, error) {
	if durationSec < 0 {
		return ControlPlaneSoakReport{}, fmt.Errorf("duration must be >= 0")
	}
	if intervalSec <= 0 {
		intervalSec = 30
	}
	start := time.Now().UTC()
	deadline := start.Add(time.Duration(durationSec) * time.Second)
	report := ControlPlaneSoakReport{
		StartedAtUTC: start.Format(time.RFC3339),
		DurationSec:  durationSec,
		IntervalSec:  intervalSec,
		Strict:       strict,
		Snapshots:    []ControlPlaneSoakSnapshot{},
	}

	for {
		st, err := ControlPlaneStatusReport(projectDir)
		if err != nil {
			report.FinishedAtUTC = time.Now().UTC().Format(time.RFC3339)
			return report, err
		}
		report.Snapshots = append(report.Snapshots, ControlPlaneSoakSnapshot{
			TimeUTC: time.Now().UTC().Format(time.RFC3339),
			Status:  st,
		})
		if strict {
			doctor, err := RunControlPlaneDoctor(projectDir)
			if err != nil {
				report.FailureDetected = true
				report.FailureDetail = compactLoopText(err.Error(), 220)
				report.FinishedAtUTC = time.Now().UTC().Format(time.RFC3339)
				return report, nil
			}
			if doctor.HasFailures() {
				report.FailureDetected = true
				failures := []string{}
				for _, check := range doctor.Checks {
					if check.Status != "fail" {
						continue
					}
					failures = append(failures, check.Name+": "+check.Detail)
				}
				sort.Strings(failures)
				report.FailureDetail = compactLoopText(strings.Join(failures, " | "), 300)
				report.FinishedAtUTC = time.Now().UTC().Format(time.RFC3339)
				return report, nil
			}
		}

		now := time.Now().UTC()
		if durationSec == 0 || !now.Before(deadline) {
			break
		}
		wait := time.Duration(intervalSec) * time.Second
		remaining := deadline.Sub(now)
		if remaining <= 0 {
			break
		}
		if wait > remaining {
			wait = remaining
		}
		time.Sleep(wait)
	}
	report.FinishedAtUTC = time.Now().UTC().Format(time.RFC3339)
	return report, nil
}

func ControlPlaneGetCutoverState(projectDir string) (ControlPlaneCutoverState, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneCutoverState{}, err
	}
	return loadControlPlaneCutoverState(cpPaths)
}

func ControlPlaneSetCutoverMode(projectDir string, enableV2, canary bool, note string) (ControlPlaneCutoverState, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneCutoverState{}, err
	}
	state, err := loadControlPlaneCutoverState(cpPaths)
	if err != nil {
		return ControlPlaneCutoverState{}, err
	}
	prevMode := state.Mode
	if enableV2 {
		state.Mode = "v2"
		state.Canary = canary
	} else {
		state.Mode = "v1"
		state.Canary = false
	}
	state.Note = strings.TrimSpace(note)
	state.UpdatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	if prevMode != state.Mode {
		state.LastSwitchUTC = state.UpdatedAtUTC
	}
	if err := saveControlPlaneCutoverState(cpPaths, state); err != nil {
		return ControlPlaneCutoverState{}, err
	}

	db, err := loadControlPlaneDB(cpPaths)
	if err == nil {
		_ = appendControlPlaneEvent(cpPaths, &db, ControlPlaneEventV1{
			Type:   "cutover_changed",
			Detail: fmt.Sprintf("mode=%s canary=%t note=%s", state.Mode, state.Canary, compactLoopText(state.Note, 120)),
		})
		_ = saveControlPlaneDB(cpPaths, db)
	}
	return state, nil
}

func ControlPlaneEvaluateCutover(projectDir string) (ControlPlaneCutoverEvaluation, error) {
	return ControlPlaneEvaluateCutoverWithOptions(projectDir, ControlPlaneCutoverEvaluateOptions{})
}

func ControlPlaneEvaluateCutoverWithOptions(projectDir string, opts ControlPlaneCutoverEvaluateOptions) (ControlPlaneCutoverEvaluation, error) {
	state, err := ControlPlaneGetCutoverState(projectDir)
	if err != nil {
		return ControlPlaneCutoverEvaluation{}, err
	}
	metrics, err := ControlPlaneMetricsReport(projectDir)
	if err != nil {
		return ControlPlaneCutoverEvaluation{}, err
	}
	doctor, err := RunControlPlaneDoctor(projectDir)
	if err != nil {
		return ControlPlaneCutoverEvaluation{}, err
	}

	failures := []ControlPlaneCutoverFailure{}
	failCount := 0
	for _, check := range doctor.Checks {
		if check.Status != "fail" {
			continue
		}
		failCount++
		failure := newCutoverFailureFromDoctorCheck(check)
		failures = append(failures, failure)
	}

	baseline, baselineExists, baselineErr := GetControlPlaneMetricsBaseline(projectDir)
	if baselineErr != nil {
		return ControlPlaneCutoverEvaluation{}, baselineErr
	}
	blockedImprovement := 0.0
	if baselineExists && baseline.BlockedRate > 0 {
		blockedImprovement = (baseline.BlockedRate - metrics.BlockedRate) / baseline.BlockedRate
	}
	if baselineExists && baseline.BlockedRate > 0 && blockedImprovement < controlPlaneBlockedImprovementTarget {
		failures = append(failures, ControlPlaneCutoverFailure{
			Category: ControlPlaneCutoverFailureCategoryBaseline,
			Code:     "baseline.blocked_rate_improvement",
			Critical: false,
			Detail: fmt.Sprintf(
				"blocked_rate_improvement insufficient: current=%.4f baseline=%.4f improvement=%.4f target>=%.4f",
				metrics.BlockedRate,
				baseline.BlockedRate,
				blockedImprovement,
				controlPlaneBlockedImprovementTarget,
			),
		})
	}
	if opts.RequireBaseline && !baselineExists {
		failures = append(failures, ControlPlaneCutoverFailure{
			Category: ControlPlaneCutoverFailureCategoryBaseline,
			Code:     "baseline.missing",
			Critical: false,
			Detail:   "baseline is required but not found; run `cp baseline capture` first",
		})
	}

	soakRequired := opts.RequireSoakPass
	soakAvailable := false
	soakFailureDetected := false
	soakReportPath := ""
	soakAgeSec := 0
	shouldCheckSoak := opts.RequireSoakPass || strings.TrimSpace(opts.SoakReportPath) != "" || opts.MaxSoakAgeSec > 0
	if shouldCheckSoak {
		soakReport, resolvedPath, found, soakErr := loadCutoverSoakReport(projectDir, opts.SoakReportPath)
		if soakErr != nil {
			return ControlPlaneCutoverEvaluation{}, soakErr
		}
		soakAvailable = found
		soakReportPath = resolvedPath
		if found {
			soakFailureDetected = soakReport.FailureDetected
			if soakReport.FailureDetected {
				detail := strings.TrimSpace(soakReport.FailureDetail)
				if detail == "" {
					detail = "soak report indicates failure_detected=true"
				}
				failures = append(failures, ControlPlaneCutoverFailure{
					Category: ControlPlaneCutoverFailureCategorySoak,
					Code:     "soak.failed",
					Critical: false,
					Detail:   detail,
				})
			}
			if completed := cutoverSoakCompletedAt(soakReport); !completed.IsZero() {
				age := int(time.Since(completed).Seconds())
				if age < 0 {
					age = 0
				}
				soakAgeSec = age
				if opts.MaxSoakAgeSec > 0 && age > opts.MaxSoakAgeSec {
					failures = append(failures, ControlPlaneCutoverFailure{
						Category: ControlPlaneCutoverFailureCategorySoak,
						Code:     "soak.stale",
						Critical: false,
						Detail:   fmt.Sprintf("soak report too old: age_sec=%d max_age_sec=%d", age, opts.MaxSoakAgeSec),
					})
				}
			} else if opts.MaxSoakAgeSec > 0 {
				failures = append(failures, ControlPlaneCutoverFailure{
					Category: ControlPlaneCutoverFailureCategorySoak,
					Code:     "soak.timestamp_missing",
					Critical: false,
					Detail:   "soak report has no started_at_utc/finished_at_utc timestamps",
				})
			}
		}
		if opts.RequireSoakPass && !found {
			detail := "soak report required but not found"
			if strings.TrimSpace(resolvedPath) != "" {
				detail = fmt.Sprintf("soak report required but not found: %s", resolvedPath)
			}
			failures = append(failures, ControlPlaneCutoverFailure{
				Category: ControlPlaneCutoverFailureCategorySoak,
				Code:     "soak.missing",
				Critical: false,
				Detail:   detail,
			})
		}
	}

	sort.Slice(failures, func(i, j int) bool {
		if failures[i].Category == failures[j].Category {
			if failures[i].Code == failures[j].Code {
				return failures[i].Detail < failures[j].Detail
			}
			return failures[i].Code < failures[j].Code
		}
		return failures[i].Category < failures[j].Category
	})
	categories, criticalCount, failureSummaries := summarizeCutoverFailures(failures)
	ready := len(failures) == 0
	return ControlPlaneCutoverEvaluation{
		Ready:                        ready,
		EvaluatedAtUTC:               time.Now().UTC().Format(time.RFC3339),
		CurrentMode:                  state.Mode,
		KPIs:                         metrics,
		DoctorFailures:               failCount,
		BaselineAvailable:            baselineExists,
		BaselineCapturedAtUTC:        baseline.CapturedAtUTC,
		BaselineBlockedRate:          baseline.BlockedRate,
		BlockedRateImprovementRatio:  blockedImprovement,
		BlockedRateImprovementTarget: controlPlaneBlockedImprovementTarget,
		SoakRequired:                 soakRequired,
		SoakAvailable:                soakAvailable,
		SoakReportPath:               soakReportPath,
		SoakFailureDetected:          soakFailureDetected,
		SoakAgeSec:                   soakAgeSec,
		CriticalFailureCount:         criticalCount,
		FailureCategories:            categories,
		Failures:                     failures,
		FailureSummaries:             failureSummaries,
	}, nil
}

func newCutoverFailureFromDoctorCheck(check ControlPlaneDoctorCheck) ControlPlaneCutoverFailure {
	name := strings.TrimSpace(check.Name)
	category := ControlPlaneCutoverFailureCategoryDoctor
	code := "doctor." + name
	critical := false

	switch name {
	case "task_graph",
		"task_state_validity",
		"event_replay_consistency",
		"event_transition_consistency",
		"recovery_policy_consistency",
		"recovery_window_timestamp_validity",
		"done_verification_consistency",
		"task_json_consistency",
		"event_journal_consistency",
		"learning_journal_consistency",
		"event_ledger_consistency",
		"learning_ledger_consistency":
		category = ControlPlaneCutoverFailureCategoryDataIntegrity
		code = "integrity." + name
		critical = true
	case "kpi_blocked_rate", "kpi_recovery_success_rate", "kpi_mttr_seconds":
		category = ControlPlaneCutoverFailureCategoryKPI
		code = "kpi." + strings.TrimPrefix(name, "kpi_")
	}
	return ControlPlaneCutoverFailure{
		Category: category,
		Code:     code,
		Critical: critical,
		Detail:   name + ": " + check.Detail,
	}
}

func summarizeCutoverFailures(failures []ControlPlaneCutoverFailure) ([]string, int, []string) {
	categorySet := map[string]struct{}{}
	criticalCount := 0
	summaries := make([]string, 0, len(failures))
	for _, failure := range failures {
		categorySet[failure.Category] = struct{}{}
		if failure.Critical {
			criticalCount++
		}
		summaries = append(summaries, fmt.Sprintf("[%s] %s: %s", failure.Category, failure.Code, failure.Detail))
	}
	categories := make([]string, 0, len(categorySet))
	for category := range categorySet {
		categories = append(categories, category)
	}
	sort.Strings(categories)
	sort.Strings(summaries)
	return categories, criticalCount, summaries
}

func ControlPlaneFaultInject(projectDir, taskID, mode string) (ControlPlaneFaultInjectResult, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneFaultInjectResult{}, err
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlaneFaultInjectResult{}, err
	}
	taskID = strings.TrimSpace(taskID)
	task, ok := db.Tasks[taskID]
	if !ok {
		return ControlPlaneFaultInjectResult{}, fmt.Errorf("task not found: %s", taskID)
	}
	mode = strings.TrimSpace(mode)
	now := time.Now().UTC()
	detail := ""
	switch mode {
	case "lease-expire":
		task.State = ControlPlaneTaskStateRunning
		task.LeaseOwner = "fault-inject"
		task.LeaseUntilUTC = now.Add(-1 * time.Minute).Format(time.RFC3339)
		task.HeartbeatUTC = now.Add(-2 * time.Minute).Format(time.RFC3339)
		detail = "set task to running with expired lease"
	case "verify-fail":
		task.VerifyCmd = "false"
		detail = "verify_cmd forced to `false`"
	case "execute-fail":
		task.ExecuteCmd = "exit 9"
		detail = "execute_cmd forced to `exit 9`"
	case "permission-denied":
		fixturePath := filepath.Join(cpPaths.RootDir, "fault-permission-denied.txt")
		if err := os.WriteFile(fixturePath, []byte("fault injection fixture\n"), 0o600); err != nil {
			return ControlPlaneFaultInjectResult{}, fmt.Errorf("prepare permission-denied fixture: %w", err)
		}
		if err := os.Chmod(fixturePath, 0o000); err != nil {
			return ControlPlaneFaultInjectResult{}, fmt.Errorf("chmod permission-denied fixture: %w", err)
		}
		quotedPath := strconv.Quote(fixturePath)
		task.ExecuteCmd = "cat " + quotedPath
		detail = fmt.Sprintf("execute_cmd forced to permission denied fixture (%s)", fixturePath)
	default:
		return ControlPlaneFaultInjectResult{}, fmt.Errorf("unsupported fault mode: %s", mode)
	}
	task.UpdatedAtUTC = now.Format(time.RFC3339)
	db.Tasks[taskID] = task
	_ = appendControlPlaneEvent(cpPaths, &db, ControlPlaneEventV1{
		Type:   "fault_injected",
		TaskID: taskID,
		Detail: fmt.Sprintf("mode=%s detail=%s", mode, detail),
	})
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		return ControlPlaneFaultInjectResult{}, err
	}
	return ControlPlaneFaultInjectResult{
		TaskID:  taskID,
		Mode:    mode,
		Applied: true,
		Detail:  detail,
		State:   task.State,
		Attempt: task.Attempt,
	}, nil
}

func RepairControlPlane(projectDir string) (ControlPlaneRepairResult, error) {
	return RepairControlPlaneWithOptions(projectDir, ControlPlaneRepairOptions{
		AutoRecover: true,
	})
}

func RepairControlPlaneWithOptions(projectDir string, opts ControlPlaneRepairOptions) (ControlPlaneRepairResult, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneRepairResult{}, err
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlaneRepairResult{}, err
	}
	actions := []ControlPlaneRepairAction{}

	reclaimed := reclaimExpiredLeases(&db, cpPaths)
	actions = append(actions, ControlPlaneRepairAction{
		Name:   "reclaim_expired_leases",
		Detail: fmt.Sprintf("recovered=%d", reclaimed),
	})

	promoted := promotePlannedTasksToReady(&db, cpPaths)
	actions = append(actions, ControlPlaneRepairAction{
		Name:   "promote_planned_to_ready",
		Detail: fmt.Sprintf("promoted=%d", promoted),
	})

	cooldownCleared, circuitCleared := normalizeExpiredRecoveryWindows(&db)
	actions = append(actions, ControlPlaneRepairAction{
		Name:   "normalize_recovery_windows",
		Detail: fmt.Sprintf("cooldown_cleared=%d circuit_cleared=%d", cooldownCleared, circuitCleared),
	})
	if opts.ResetCircuit || opts.ResetRetryBudget {
		resetCircuitCount, resetRetryBudgetCount := resetBlockedRecoveryPolicies(&db, opts.ResetCircuit, opts.ResetRetryBudget)
		actions = append(actions, ControlPlaneRepairAction{
			Name: "reset_recovery_policies",
			Detail: fmt.Sprintf(
				"reset_circuit=%t reset_retry_budget=%t circuit_resets=%d retry_budget_resets=%d",
				opts.ResetCircuit,
				opts.ResetRetryBudget,
				resetCircuitCount,
				resetRetryBudgetCount,
			),
		})
	}

	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		return ControlPlaneRepairResult{}, err
	}
	if opts.AutoRecover {
		recoverRes, recoverErr := ControlPlaneRecoverWithOptions(projectDir, ControlPlaneRecoverOptions{
			Limit: opts.RecoverLimit,
			Force: opts.ForceRecover,
		})
		if recoverErr != nil {
			return ControlPlaneRepairResult{}, recoverErr
		}
		actions = append(actions, ControlPlaneRepairAction{
			Name: "auto_recover_blocked",
			Detail: fmt.Sprintf(
				"recovered=%d skipped_deps=%d skipped_retry_budget=%d skipped_cooldown=%d skipped_circuit_open=%d force=%t",
				recoverRes.Recovered,
				recoverRes.SkippedDeps,
				recoverRes.SkippedRetryBudget,
				recoverRes.SkippedCooldown,
				recoverRes.SkippedCircuitOpen,
				opts.ForceRecover,
			),
		})
	}
	return ControlPlaneRepairResult{
		UpdatedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Actions:      actions,
	}, nil
}

func RunControlPlaneDoctor(projectDir string) (ControlPlaneDoctorReport, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneDoctorReport{}, err
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlaneDoctorReport{}, err
	}
	checks := []ControlPlaneDoctorCheck{}
	status := "pass"
	if db.SchemaVersion != controlPlaneSchemaVersion {
		status = "warn"
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "schema_version",
		Status: status,
		Detail: fmt.Sprintf("current=%d expected=%d", db.SchemaVersion, controlPlaneSchemaVersion),
	})

	invalidStateCount := 0
	emptyVerifyCount := 0
	for _, task := range db.Tasks {
		if !isValidControlPlaneTaskState(task.State) {
			invalidStateCount++
		}
		if strings.TrimSpace(task.VerifyCmd) == "" {
			emptyVerifyCount++
		}
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "task_state_validity",
		Status: passOrFail(invalidStateCount == 0),
		Detail: fmt.Sprintf("invalid_states=%d", invalidStateCount),
	})
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "verify_command_presence",
		Status: passOrFail(emptyVerifyCount == 0),
		Detail: fmt.Sprintf("empty_verify_cmd=%d", emptyVerifyCount),
	})

	nodes := make([]TaskNodeV1, 0, len(db.Tasks))
	for _, task := range db.Tasks {
		nodes = append(nodes, task.TaskNodeV1)
	}
	if err := validateTaskNodeGraph(nodes); err != nil {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "task_graph",
			Status: "fail",
			Detail: err.Error(),
		})
	} else {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "task_graph",
			Status: "pass",
			Detail: "graph valid",
		})
	}
	replayed := replayTaskStatesFromEvents(db.Events)
	replayMismatch := 0
	for taskID, task := range db.Tasks {
		replayedState, ok := replayed[taskID]
		if !ok {
			continue
		}
		if replayedState != task.State {
			replayMismatch++
		}
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "event_replay_consistency",
		Status: passOrFail(replayMismatch == 0),
		Detail: fmt.Sprintf("mismatch=%d", replayMismatch),
	})
	eventTransitionInvalid := 0
	eventTransitionFromMismatch := 0
	lastEventStateByTask := map[string]string{}
	for _, event := range db.Events {
		taskID := strings.TrimSpace(event.TaskID)
		from := strings.TrimSpace(event.FromState)
		to := strings.TrimSpace(event.ToState)
		if taskID == "" || to == "" {
			continue
		}
		if !isValidControlPlaneTaskState(to) {
			eventTransitionInvalid++
			lastEventStateByTask[taskID] = to
			continue
		}
		if from != "" && !isValidControlPlaneTaskState(from) {
			eventTransitionInvalid++
		}
		if prev, ok := lastEventStateByTask[taskID]; ok {
			if from == "" {
				eventTransitionFromMismatch++
			} else if prev != from {
				eventTransitionFromMismatch++
			}
		}
		if from != "" && from != to && !isAllowedControlPlaneTransition(from, to) {
			eventTransitionInvalid++
		}
		lastEventStateByTask[taskID] = to
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "event_transition_consistency",
		Status: passOrFail(eventTransitionInvalid == 0 && eventTransitionFromMismatch == 0),
		Detail: fmt.Sprintf("invalid=%d from_mismatch=%d", eventTransitionInvalid, eventTransitionFromMismatch),
	})
	doneMissingVerification := 0
	doneFailingVerification := 0
	doneMissingEvidence := 0
	for _, task := range db.Tasks {
		if task.State != ControlPlaneTaskStateDone {
			continue
		}
		verification, ok := db.Verifications[task.ID]
		if !ok {
			doneMissingVerification++
			continue
		}
		if !verification.Pass {
			doneFailingVerification++
		}
		if len(verification.Evidence) == 0 {
			doneMissingEvidence++
		}
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "done_verification_consistency",
		Status: passOrFail(doneMissingVerification == 0 && doneFailingVerification == 0 && doneMissingEvidence == 0),
		Detail: fmt.Sprintf("missing_verification=%d failing_verification=%d missing_evidence=%d", doneMissingVerification, doneFailingVerification, doneMissingEvidence),
	})

	now := time.Now().UTC()
	staleLeases := 0
	recoveryPolicyMismatches := 0
	circuitOpenTasks := 0
	retryBudgetExceededTasks := 0
	invalidRecoveryTimestamp := 0
	for _, task := range db.Tasks {
		if (task.State == ControlPlaneTaskStateRunning || task.State == ControlPlaneTaskStateVerifying) && leaseExpired(task, now) {
			staleLeases++
		}
		if task.State != ControlPlaneTaskStateBlocked {
			continue
		}
		if next := strings.TrimSpace(task.NextRecoverAtUTC); next != "" {
			if _, err := time.Parse(time.RFC3339, next); err != nil {
				invalidRecoveryTimestamp++
			}
		}
		if until := strings.TrimSpace(task.CircuitOpenUntilUTC); until != "" {
			if _, err := time.Parse(time.RFC3339, until); err != nil {
				invalidRecoveryTimestamp++
			}
		}
		if task.Attempt >= controlPlaneRecoverMaxAttempts {
			retryBudgetExceededTasks++
		}
		if circuitActive(task, now) {
			circuitOpenTasks++
		}
		if task.BlockedCount > 1 && strings.TrimSpace(task.NextRecoverAtUTC) == "" && strings.TrimSpace(task.CircuitOpenUntilUTC) == "" {
			recoveryPolicyMismatches++
		}
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "stale_leases",
		Status: passOrFail(staleLeases == 0),
		Detail: fmt.Sprintf("stale=%d", staleLeases),
	})
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "recovery_window_timestamp_validity",
		Status: passOrFail(invalidRecoveryTimestamp == 0),
		Detail: fmt.Sprintf("invalid=%d", invalidRecoveryTimestamp),
	})
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "recovery_policy_consistency",
		Status: passOrFail(recoveryPolicyMismatches == 0),
		Detail: fmt.Sprintf("mismatch=%d", recoveryPolicyMismatches),
	})
	circuitStatus := "pass"
	if circuitOpenTasks > 0 {
		circuitStatus = "warn"
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "recovery_circuit_open_tasks",
		Status: circuitStatus,
		Detail: fmt.Sprintf("open=%d", circuitOpenTasks),
	})
	retryBudgetStatus := "pass"
	if retryBudgetExceededTasks > 0 {
		retryBudgetStatus = "warn"
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "retry_budget_exhausted_tasks",
		Status: retryBudgetStatus,
		Detail: fmt.Sprintf("exhausted=%d budget=%d", retryBudgetExceededTasks, controlPlaneRecoverMaxAttempts),
	})

	if _, err := os.Stat(cpPaths.EventsFile); err != nil {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "events_file",
			Status: "fail",
			Detail: fmt.Sprintf("events file error: %v", err),
		})
	} else {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "events_file",
			Status: "pass",
			Detail: cpPaths.EventsFile,
		})
	}
	if _, err := os.Stat(cpPaths.LearningFile); err != nil {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "learning_file",
			Status: "fail",
			Detail: fmt.Sprintf("learning file error: %v", err),
		})
	} else {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "learning_file",
			Status: "pass",
			Detail: cpPaths.LearningFile,
		})
	}
	taskJSONConsistency, taskJSONErr := assessControlPlaneTaskJSONConsistency(cpPaths, db.Tasks)
	if taskJSONErr != nil {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "task_json_consistency",
			Status: "fail",
			Detail: fmt.Sprintf("read task json files failed: %v", taskJSONErr),
		})
	} else {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "task_json_consistency",
			Status: passOrFail(taskJSONConsistency.Missing == 0 && taskJSONConsistency.Orphan == 0 && taskJSONConsistency.StateMismatch == 0 && taskJSONConsistency.ParseErrors == 0),
			Detail: fmt.Sprintf("missing=%d orphan=%d state_mismatch=%d parse_errors=%d", taskJSONConsistency.Missing, taskJSONConsistency.Orphan, taskJSONConsistency.StateMismatch, taskJSONConsistency.ParseErrors),
		})
	}
	eventJournalConsistency, eventJournalErr := readControlPlaneEventJournalConsistency(cpPaths.EventsFile)
	if eventJournalErr != nil {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "event_journal_consistency",
			Status: "fail",
			Detail: fmt.Sprintf("read events journal failed: %v", eventJournalErr),
		})
	} else {
		mismatch := eventJournalConsistency.LineCount - len(db.Events)
		if mismatch < 0 {
			mismatch = -mismatch
		}
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "event_journal_consistency",
			Status: passOrFail(mismatch == 0 && eventJournalConsistency.ParseErrors == 0),
			Detail: fmt.Sprintf("journal_lines=%d db_events=%d mismatch=%d parse_errors=%d", eventJournalConsistency.LineCount, len(db.Events), mismatch, eventJournalConsistency.ParseErrors),
		})
	}
	learningJournalConsistency, learningJournalErr := readControlPlaneLearningJournalConsistency(cpPaths.LearningFile)
	if learningJournalErr != nil {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "learning_journal_consistency",
			Status: "fail",
			Detail: fmt.Sprintf("read learning journal failed: %v", learningJournalErr),
		})
	} else {
		mismatch := learningJournalConsistency.LineCount - len(db.Learnings)
		if mismatch < 0 {
			mismatch = -mismatch
		}
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "learning_journal_consistency",
			Status: passOrFail(mismatch == 0 && learningJournalConsistency.ParseErrors == 0),
			Detail: fmt.Sprintf("journal_lines=%d db_learnings=%d mismatch=%d parse_errors=%d", learningJournalConsistency.LineCount, len(db.Learnings), mismatch, learningJournalConsistency.ParseErrors),
		})
	}
	eventLedgerConsistency, eventLedgerErr := readControlPlaneSQLiteLedgerConsistency(
		cpPaths.DBFile,
		"cp_event_ledger",
		func(raw []byte) error {
			entry := ControlPlaneEventV1{}
			return json.Unmarshal(raw, &entry)
		},
	)
	if eventLedgerErr != nil {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "event_ledger_consistency",
			Status: "fail",
			Detail: fmt.Sprintf("read event ledger failed: %v", eventLedgerErr),
		})
	} else {
		mismatch := eventLedgerConsistency.RowCount - len(db.Events)
		if mismatch < 0 {
			mismatch = -mismatch
		}
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "event_ledger_consistency",
			Status: passOrFail(mismatch == 0 && eventLedgerConsistency.ParseErrors == 0),
			Detail: fmt.Sprintf("ledger_rows=%d db_events=%d mismatch=%d parse_errors=%d", eventLedgerConsistency.RowCount, len(db.Events), mismatch, eventLedgerConsistency.ParseErrors),
		})
	}
	learningLedgerConsistency, learningLedgerErr := readControlPlaneSQLiteLedgerConsistency(
		cpPaths.DBFile,
		"cp_learning_ledger",
		func(raw []byte) error {
			entry := LearningEventV1{}
			return json.Unmarshal(raw, &entry)
		},
	)
	if learningLedgerErr != nil {
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "learning_ledger_consistency",
			Status: "fail",
			Detail: fmt.Sprintf("read learning ledger failed: %v", learningLedgerErr),
		})
	} else {
		mismatch := learningLedgerConsistency.RowCount - len(db.Learnings)
		if mismatch < 0 {
			mismatch = -mismatch
		}
		checks = append(checks, ControlPlaneDoctorCheck{
			Name:   "learning_ledger_consistency",
			Status: passOrFail(mismatch == 0 && learningLedgerConsistency.ParseErrors == 0),
			Detail: fmt.Sprintf("ledger_rows=%d db_learnings=%d mismatch=%d parse_errors=%d", learningLedgerConsistency.RowCount, len(db.Learnings), mismatch, learningLedgerConsistency.ParseErrors),
		})
	}
	metrics := computeControlPlaneMetrics(db)
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "kpi_blocked_rate",
		Status: passOrFail(metrics.BlockedRate <= controlPlaneBlockedRateTarget),
		Detail: fmt.Sprintf("current=%.4f target<=%.4f", metrics.BlockedRate, controlPlaneBlockedRateTarget),
	})
	recoveryStatus := "pass"
	if metrics.RecoveryEvents > 0 && metrics.RecoverySuccessRate < controlPlaneRecoverySuccessRateTarget {
		recoveryStatus = "fail"
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "kpi_recovery_success_rate",
		Status: recoveryStatus,
		Detail: fmt.Sprintf("current=%.4f target>=%.4f events=%d", metrics.RecoverySuccessRate, controlPlaneRecoverySuccessRateTarget, metrics.RecoveryEvents),
	})
	mttrStatus := "pass"
	if metrics.MeanTimeToRecovery > 0 && metrics.MeanTimeToRecovery > controlPlaneMTTRSecondsTarget {
		mttrStatus = "fail"
	}
	checks = append(checks, ControlPlaneDoctorCheck{
		Name:   "kpi_mttr_seconds",
		Status: mttrStatus,
		Detail: fmt.Sprintf("current=%.2f target<=%.2f", metrics.MeanTimeToRecovery, controlPlaneMTTRSecondsTarget),
	})

	return ControlPlaneDoctorReport{
		UpdatedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Checks:       checks,
	}, nil
}

func (r ControlPlaneDoctorReport) HasFailures() bool {
	for _, check := range r.Checks {
		if check.Status == "fail" {
			return true
		}
	}
	return false
}

func MigrateV1IssuesToControlPlane(projectDir string, dryRun bool) (ControlPlaneMigrationResult, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneMigrationResult{}, err
	}
	result := ControlPlaneMigrationResult{
		IntentID: "v1-migration",
		DryRun:   dryRun,
	}

	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return result, err
	}
	if _, ok := db.Intents[result.IntentID]; !ok {
		db.Intents[result.IntentID] = IntentRecordV1{
			Spec: IntentSpecV1{
				ID:              result.IntentID,
				Version:         1,
				Goal:            "Migrate v1 issue queue into v2 control plane",
				SuccessCriteria: []string{"v1 issue states captured in v2"},
				Constraints:     []string{"preserve original issue ids"},
				NonGoals:        []string{"rebuild historical logs"},
				Epics: []IntentEpicV1{
					{ID: "legacy", Title: "legacy-queue", Tasks: []IntentTaskSpecV1{}},
				},
			},
			SourcePath:    filepath.Join(cpPaths.ProjectDir, ".ralph"),
			ImportedAtUTC: time.Now().UTC().Format(time.RFC3339),
		}
	}

	legacyStateDirs := []struct {
		Dir   string
		State string
	}{
		{Dir: filepath.Join(cpPaths.ProjectDir, ".ralph", "issues"), State: ControlPlaneTaskStateReady},
		{Dir: filepath.Join(cpPaths.ProjectDir, ".ralph", "in-progress"), State: ControlPlaneTaskStateRunning},
		{Dir: filepath.Join(cpPaths.ProjectDir, ".ralph", "done"), State: ControlPlaneTaskStateDone},
		{Dir: filepath.Join(cpPaths.ProjectDir, ".ralph", "blocked"), State: ControlPlaneTaskStateBlocked},
	}

	for _, entry := range legacyStateDirs {
		files, err := filepath.Glob(filepath.Join(entry.Dir, "I-*.md"))
		if err != nil {
			return result, err
		}
		sort.Strings(files)
		for _, file := range files {
			result.Scanned++
			meta, err := ReadIssueMeta(file)
			if err != nil {
				result.Skipped++
				continue
			}
			if strings.TrimSpace(meta.ID) == "" {
				result.Skipped++
				continue
			}
			if _, exists := db.Tasks[meta.ID]; exists {
				result.Skipped++
				continue
			}

			acceptance := readIssueChecklist(file)
			if len(acceptance) == 0 {
				acceptance = []string{"legacy acceptance criteria preserved from v1 issue"}
			}
			verifyCmd := "echo \"migrated task: set verify_cmd before production run\""
			task := TaskRecordV1{
				TaskNodeV1: TaskNodeV1{
					ID:         meta.ID,
					EpicID:     "legacy",
					Title:      compactLoopText(meta.Title, 180),
					Role:       normalizeMigratedRole(meta.Role),
					Priority:   normalizeMigratedPriority(meta.Priority),
					Deps:       []string{},
					Acceptance: acceptance,
					VerifyCmd:  verifyCmd,
					RiskLevel:  "medium",
				},
				IntentID:     result.IntentID,
				State:        entry.State,
				Attempt:      0,
				LastError:    readIssueLastReason(file),
				UpdatedAtUTC: time.Now().UTC().Format(time.RFC3339),
			}
			if task.State == ControlPlaneTaskStateRunning {
				task.LeaseOwner = "migration"
				task.LeaseUntilUTC = time.Now().UTC().Add(-1 * time.Minute).Format(time.RFC3339)
			}
			if task.State == ControlPlaneTaskStateBlocked {
				task.BlockedCount = 1
				backoff := controlPlaneRecoveryBackoff(task.BlockedCount)
				if backoff > 0 {
					task.NextRecoverAtUTC = time.Now().UTC().Add(backoff).Format(time.RFC3339)
				}
			}
			if !dryRun {
				db.Tasks[task.ID] = task
			}
			result.Imported++
		}
	}

	if dryRun {
		return result, nil
	}

	if err := appendControlPlaneEvent(cpPaths, &db, ControlPlaneEventV1{
		Type:     "v1_migration",
		IntentID: result.IntentID,
		Detail:   fmt.Sprintf("imported=%d skipped=%d scanned=%d", result.Imported, result.Skipped, result.Scanned),
	}); err != nil {
		return result, err
	}
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		return result, err
	}
	return result, nil
}

func VerifyV1ToV2Migration(projectDir string) (ControlPlaneMigrationVerifyResult, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneMigrationVerifyResult{}, err
	}
	v1Counts := map[string]int{
		"ready":       0,
		"in_progress": 0,
		"done":        0,
		"blocked":     0,
	}
	countPattern := func(pattern string) (int, error) {
		files, err := filepath.Glob(pattern)
		if err != nil {
			return 0, err
		}
		return len(files), nil
	}
	if v1Counts["ready"], err = countPattern(filepath.Join(cpPaths.ProjectDir, ".ralph", "issues", "I-*.md")); err != nil {
		return ControlPlaneMigrationVerifyResult{}, err
	}
	if v1Counts["in_progress"], err = countPattern(filepath.Join(cpPaths.ProjectDir, ".ralph", "in-progress", "I-*.md")); err != nil {
		return ControlPlaneMigrationVerifyResult{}, err
	}
	if v1Counts["done"], err = countPattern(filepath.Join(cpPaths.ProjectDir, ".ralph", "done", "I-*.md")); err != nil {
		return ControlPlaneMigrationVerifyResult{}, err
	}
	if v1Counts["blocked"], err = countPattern(filepath.Join(cpPaths.ProjectDir, ".ralph", "blocked", "I-*.md")); err != nil {
		return ControlPlaneMigrationVerifyResult{}, err
	}

	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		return ControlPlaneMigrationVerifyResult{}, err
	}
	v2Counts := map[string]int{
		"ready":       0,
		"in_progress": 0,
		"done":        0,
		"blocked":     0,
	}
	for _, task := range db.Tasks {
		if task.IntentID != "v1-migration" {
			continue
		}
		switch task.State {
		case ControlPlaneTaskStateReady:
			v2Counts["ready"]++
		case ControlPlaneTaskStateRunning, ControlPlaneTaskStateVerifying:
			v2Counts["in_progress"]++
		case ControlPlaneTaskStateDone:
			v2Counts["done"]++
		case ControlPlaneTaskStateBlocked:
			v2Counts["blocked"]++
		}
	}

	matched := true
	diff := []string{}
	for _, key := range []string{"ready", "in_progress", "done", "blocked"} {
		if v1Counts[key] == v2Counts[key] {
			continue
		}
		matched = false
		diff = append(diff, fmt.Sprintf("%s:v1=%d v2=%d", key, v1Counts[key], v2Counts[key]))
	}
	detail := "counts matched"
	if !matched {
		detail = "count mismatch: " + strings.Join(diff, ", ")
	}
	return ControlPlaneMigrationVerifyResult{
		V1Counts: v1Counts,
		V2Counts: v2Counts,
		Matched:  matched,
		Detail:   detail,
	}, nil
}

type verificationPolicyCheck struct {
	Name    string
	Kind    string
	Command string
}

var verificationPolicyKinds = map[string]struct{}{
	"unit":        {},
	"integration": {},
	"lint":        {},
	"custom":      {},
}

func compileVerificationPolicy(task TaskRecordV1) ([]verificationPolicyCheck, error) {
	verifyCmd := strings.TrimSpace(task.VerifyCmd)
	if verifyCmd == "" {
		return nil, fmt.Errorf("verify_cmd is empty")
	}
	lines := strings.Split(verifyCmd, "\n")
	checks := []verificationPolicyCheck{}
	kindCount := map[string]int{}
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		kind := "custom"
		command := line
		if parsedKind, parsedCommand, ok := parseVerificationPolicyLine(line); ok {
			kind = parsedKind
			command = parsedCommand
		}
		if strings.TrimSpace(command) == "" {
			return nil, fmt.Errorf("verification command is empty for kind=%s", kind)
		}
		kindCount[kind]++
		checks = append(checks, verificationPolicyCheck{
			Name:    fmt.Sprintf("%s_%d", kind, kindCount[kind]),
			Kind:    kind,
			Command: command,
		})
	}
	if len(checks) == 0 {
		return nil, fmt.Errorf("verify_cmd produced no runnable checks")
	}
	return checks, nil
}

func parseVerificationPolicyLine(line string) (string, string, bool) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	kind := strings.ToLower(strings.TrimSpace(parts[0]))
	if kind == "" {
		return "", "", false
	}
	if _, ok := verificationPolicyKinds[kind]; !ok {
		return "", "", false
	}
	command := strings.TrimSpace(parts[1])
	return kind, command, true
}

func sanitizeVerificationCheckName(name string) string {
	raw := strings.ToLower(strings.TrimSpace(name))
	if raw == "" {
		return "check"
	}
	var b strings.Builder
	for _, ch := range raw {
		switch {
		case ch >= 'a' && ch <= 'z':
			b.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			b.WriteRune(ch)
		case ch == '-' || ch == '_':
			b.WriteRune(ch)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "check"
	}
	return out
}

func verifyTaskCommand(cpPaths ControlPlanePaths, task TaskRecordV1) (VerificationResultV1, string, error) {
	artifactDir := filepath.Join(cpPaths.ArtifactsDir, task.ID)
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return VerificationResultV1{}, "", fmt.Errorf("create artifact dir: %w", err)
	}
	policyChecks, policyErr := compileVerificationPolicy(task)
	checks := []VerificationCheckV1{}
	failureReason := ""
	evidence := []string{}
	hasEvidenceOutput := false
	stamp := time.Now().UTC().Format("20060102T150405Z")
	if policyErr != nil {
		checks = append(checks, VerificationCheckV1{
			Name:   "verify_policy",
			Type:   "policy",
			Status: "fail",
			Detail: compactLoopText(policyErr.Error(), 180),
		})
		failureReason = "verify_policy_invalid:" + compactLoopText(policyErr.Error(), 120)
	} else {
		checks = append(checks, VerificationCheckV1{
			Name:   "verify_policy",
			Type:   "policy",
			Status: "pass",
			Detail: fmt.Sprintf("checks=%d", len(policyChecks)),
		})
	}

	for _, policyCheck := range policyChecks {
		logPath := filepath.Join(artifactDir, fmt.Sprintf("verify-%s-%s.log", stamp, sanitizeVerificationCheckName(policyCheck.Name)))
		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return VerificationResultV1{}, "", fmt.Errorf("open verify log: %w", err)
		}
		capture := newTailBuffer(128 * 1024)
		cmd := exec.Command("bash", "-lc", policyCheck.Command)
		cmd.Dir = cpPaths.ProjectDir
		cmd.Stdout = io.MultiWriter(logFile, capture)
		cmd.Stderr = io.MultiWriter(logFile, capture)
		runErr := cmd.Run()
		_ = logFile.Close()
		evidence = append(evidence, logPath)

		if strings.TrimSpace(capture.String()) != "" {
			hasEvidenceOutput = true
		}
		if runErr != nil {
			checks = append(checks, VerificationCheckV1{
				Name:   policyCheck.Name,
				Type:   policyCheck.Kind,
				Status: "fail",
				Detail: compactLoopText(runErr.Error(), 180),
			})
			if failureReason == "" {
				failureReason = fmt.Sprintf("verify_%s_failed:%s", policyCheck.Kind, compactLoopText(runErr.Error(), 120))
			}
			continue
		}
		checks = append(checks, VerificationCheckV1{
			Name:   policyCheck.Name,
			Type:   policyCheck.Kind,
			Status: "pass",
			Detail: "exit=0",
		})
	}

	if !hasEvidenceOutput {
		checks = append(checks, VerificationCheckV1{
			Name:   "evidence_present",
			Type:   "policy",
			Status: "fail",
			Detail: "verification commands produced no output evidence",
		})
		if failureReason == "" {
			failureReason = "missing_evidence"
		}
	} else {
		checks = append(checks, VerificationCheckV1{
			Name:   "evidence_present",
			Type:   "policy",
			Status: "pass",
			Detail: "verification output captured",
		})
	}

	pass := true
	for _, check := range checks {
		if check.Status != "pass" {
			pass = false
			break
		}
	}
	result := VerificationResultV1{
		TaskID:        task.ID,
		Checks:        checks,
		Pass:          pass,
		Evidence:      evidence,
		FailureReason: failureReason,
		VerifiedAtUTC: time.Now().UTC().Format(time.RFC3339),
	}
	return result, failureReason, nil
}

func executeControlPlaneTask(cpPaths ControlPlanePaths, task TaskRecordV1, opts ControlPlaneRunOptions) ([]string, error) {
	artifacts := []string{}
	artifactDir := filepath.Join(cpPaths.ArtifactsDir, task.ID)
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return artifacts, fmt.Errorf("create artifact dir: %w", err)
	}

	executeCmd := strings.TrimSpace(task.ExecuteCmd)
	if executeCmd != "" {
		logPath := filepath.Join(artifactDir, "exec-"+time.Now().UTC().Format("20060102T150405Z")+".log")
		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return artifacts, fmt.Errorf("open execute log: %w", err)
		}
		tail := newTailBuffer(64 * 1024)
		cmd := exec.Command("bash", "-lc", executeCmd)
		cmd.Dir = cpPaths.ProjectDir
		cmd.Stdout = io.MultiWriter(logFile, tail)
		cmd.Stderr = io.MultiWriter(logFile, tail)
		runErr := cmd.Run()
		_ = logFile.Close()
		artifacts = append(artifacts, logPath)
		if runErr != nil {
			return artifacts, fmt.Errorf("execute_cmd_failed:%s", compactLoopText(runErr.Error(), 180))
		}
	}

	if !opts.ExecuteWithCodex {
		return artifacts, nil
	}

	paths, profile, err := loadControlPlaneExecutionProfile(cpPaths.ProjectDir, opts.ControlDir)
	if err != nil {
		return artifacts, err
	}
	if !profile.RequireCodex {
		return artifacts, fmt.Errorf("codex execution requested but RALPH_REQUIRE_CODEX=false")
	}
	codexLogPath := filepath.Join(artifactDir, "codex-"+time.Now().UTC().Format("20060102T150405Z")+".log")
	codexLog, err := os.OpenFile(codexLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return artifacts, fmt.Errorf("open codex log: %w", err)
	}
	lastMessagePath := ""
	if profile.CodexOutputLastMessage {
		lastMessagePath = strings.TrimSuffix(codexLogPath, ".log") + ".last.txt"
	}
	model := profile.CodexModelForRole(task.Role)
	prompt := buildControlPlaneCodexPrompt(cpPaths.ProjectDir, task)
	execErr := runCodexWithRetries(context.Background(), paths, profile, model, prompt, codexLog, lastMessagePath)
	_ = codexLog.Close()
	artifacts = append(artifacts, codexLogPath)
	if lastMessagePath != "" {
		if _, statErr := os.Stat(lastMessagePath); statErr == nil {
			artifacts = append(artifacts, lastMessagePath)
		}
	}
	if execErr != nil {
		return artifacts, fmt.Errorf("codex_execution_failed:%s", compactLoopText(execErr.Error(), 180))
	}
	return artifacts, nil
}

func loadControlPlaneExecutionProfile(projectDir, controlDir string) (Paths, Profile, error) {
	resolvedControlDir := strings.TrimSpace(controlDir)
	if resolvedControlDir == "" {
		home, err := os.UserHomeDir()
		if err == nil && strings.TrimSpace(home) != "" {
			resolvedControlDir = filepath.Join(home, ".ralph-control")
		} else {
			resolvedControlDir = projectDir
		}
	}
	paths, err := NewPaths(resolvedControlDir, projectDir)
	if err != nil {
		return Paths{}, Profile{}, err
	}
	if err := EnsureLayout(paths); err != nil {
		return Paths{}, Profile{}, err
	}
	profile, err := LoadProfile(paths)
	if err != nil {
		return Paths{}, Profile{}, err
	}
	return paths, profile, nil
}

func buildControlPlaneCodexPrompt(projectDir string, task TaskRecordV1) string {
	var b strings.Builder
	b.WriteString("You are executing a control-plane task.\n")
	fmt.Fprintf(&b, "Project: %s\n", projectDir)
	fmt.Fprintf(&b, "Task ID: %s\n", task.ID)
	fmt.Fprintf(&b, "Role: %s\n", task.Role)
	fmt.Fprintf(&b, "Title: %s\n", task.Title)
	fmt.Fprintf(&b, "Risk Level: %s\n", task.RiskLevel)
	if strings.TrimSpace(task.CodexObjective) != "" {
		fmt.Fprintf(&b, "Objective: %s\n", strings.TrimSpace(task.CodexObjective))
	}
	b.WriteString("\nAcceptance Criteria:\n")
	for _, item := range task.Acceptance {
		fmt.Fprintf(&b, "- %s\n", item)
	}
	b.WriteString("\nExecution rules:\n")
	b.WriteString("- Work only inside the project directory.\n")
	b.WriteString("- Keep changes focused on this task.\n")
	b.WriteString("- Run validation relevant to your edits.\n")
	return b.String()
}

type controlPlaneHeartbeat struct {
	TaskID     string
	LeaseOwner string
	AtUTC      string
}

type controlPlaneTaskExecutionOutcome struct {
	StartedAtUTC  string
	EndedAtUTC    string
	ExecArtifacts []string
	ExecError     string
	Verification  VerificationResultV1
	VerifyReason  string
}

type controlPlaneTaskWorkerResult struct {
	ClaimedTask TaskRecordV1
	Outcome     controlPlaneTaskExecutionOutcome
	Err         error
}

func runOneControlPlaneTask(db *ControlPlaneDBV1, cpPaths ControlPlanePaths, taskID string, workerNum int, opts ControlPlaneRunOptions) (string, error) {
	claimedTask, err := claimControlPlaneTask(db, cpPaths, taskID, workerNum, opts.LeaseSec)
	if err != nil {
		return "", err
	}
	outcome, err := executeClaimedControlPlaneTask(cpPaths, claimedTask, opts)
	if err != nil {
		return "", err
	}
	return applyClaimedTaskOutcome(db, cpPaths, claimedTask, outcome)
}

func claimNextReadyControlPlaneTask(db *ControlPlaneDBV1, cpPaths ControlPlanePaths, workerNum, leaseSec int) (TaskRecordV1, bool, error) {
	readyIDs := collectReadyTaskIDs(db.Tasks)
	if len(readyIDs) == 0 {
		return TaskRecordV1{}, false, nil
	}
	claimedTask, err := claimControlPlaneTask(db, cpPaths, readyIDs[0], workerNum, leaseSec)
	if err != nil {
		return TaskRecordV1{}, false, err
	}
	return claimedTask, true, nil
}

func claimControlPlaneTask(db *ControlPlaneDBV1, cpPaths ControlPlanePaths, taskID string, workerNum, leaseSec int) (TaskRecordV1, error) {
	task, ok := db.Tasks[taskID]
	if !ok {
		return TaskRecordV1{}, fmt.Errorf("task not found: %s", taskID)
	}
	if task.State != ControlPlaneTaskStateReady {
		return TaskRecordV1{}, fmt.Errorf("task %s is not ready: %s", taskID, task.State)
	}
	if leaseSec <= 0 {
		leaseSec = 120
	}
	task.Attempt++
	now := time.Now().UTC()
	task.LeaseOwner = fmt.Sprintf("worker-%d", workerNum)
	task.LeaseUntilUTC = now.Add(time.Duration(leaseSec) * time.Second).Format(time.RFC3339)
	task.HeartbeatUTC = now.Format(time.RFC3339)
	task.UpdatedAtUTC = now.Format(time.RFC3339)
	db.Tasks[taskID] = task
	if err := setControlPlaneTaskState(db, cpPaths, taskID, ControlPlaneTaskStateRunning, "task_state_changed", "task claimed", false); err != nil {
		return TaskRecordV1{}, err
	}
	return db.Tasks[taskID], nil
}

func runControlPlaneTaskWorker(cpPaths ControlPlanePaths, claimedTask TaskRecordV1, workerNum int, opts ControlPlaneRunOptions, heartbeats chan<- controlPlaneHeartbeat, runResults chan<- controlPlaneTaskWorkerResult) {
	done := make(chan struct{})
	heartbeatInterval := controlPlaneHeartbeatInterval(opts.LeaseSec)
	if heartbeatInterval > 0 {
		go func() {
			ticker := time.NewTicker(heartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case tick := <-ticker.C:
					select {
					case heartbeats <- controlPlaneHeartbeat{
						TaskID:     claimedTask.ID,
						LeaseOwner: claimedTask.LeaseOwner,
						AtUTC:      tick.UTC().Format(time.RFC3339),
					}:
					default:
					}
				}
			}
		}()
	}
	outcome, execErr := executeClaimedControlPlaneTask(cpPaths, claimedTask, opts)
	close(done)
	runResults <- controlPlaneTaskWorkerResult{
		ClaimedTask: claimedTask,
		Outcome:     outcome,
		Err:         execErr,
	}
}

func controlPlaneHeartbeatInterval(leaseSec int) time.Duration {
	intervalSec := leaseSec / 3
	if intervalSec < 5 {
		intervalSec = 5
	}
	if intervalSec > 30 {
		intervalSec = 30
	}
	return time.Duration(intervalSec) * time.Second
}

func applyControlPlaneHeartbeat(db *ControlPlaneDBV1, hb controlPlaneHeartbeat, leaseSec int) bool {
	taskID := strings.TrimSpace(hb.TaskID)
	if taskID == "" {
		return false
	}
	task, ok := db.Tasks[taskID]
	if !ok {
		return false
	}
	if task.State != ControlPlaneTaskStateRunning && task.State != ControlPlaneTaskStateVerifying {
		return false
	}
	owner := strings.TrimSpace(hb.LeaseOwner)
	if owner != "" && strings.TrimSpace(task.LeaseOwner) != "" && strings.TrimSpace(task.LeaseOwner) != owner {
		return false
	}
	heartbeatTime := parseRFC3339OrNow(hb.AtUTC)
	task.HeartbeatUTC = heartbeatTime.Format(time.RFC3339)
	if leaseSec <= 0 {
		leaseSec = 120
	}
	task.LeaseUntilUTC = heartbeatTime.Add(time.Duration(leaseSec) * time.Second).Format(time.RFC3339)
	task.UpdatedAtUTC = heartbeatTime.Format(time.RFC3339)
	db.Tasks[taskID] = task
	return true
}

func executeClaimedControlPlaneTask(cpPaths ControlPlanePaths, claimedTask TaskRecordV1, opts ControlPlaneRunOptions) (controlPlaneTaskExecutionOutcome, error) {
	started := time.Now().UTC()
	outcome := controlPlaneTaskExecutionOutcome{
		StartedAtUTC: started.Format(time.RFC3339),
	}
	execArtifacts, execErr := executeControlPlaneTask(cpPaths, claimedTask, opts)
	if execErr != nil {
		outcome.EndedAtUTC = time.Now().UTC().Format(time.RFC3339)
		outcome.ExecArtifacts = append([]string(nil), execArtifacts...)
		outcome.ExecError = compactLoopText(execErr.Error(), 220)
		return outcome, nil
	}
	verification, reason, err := verifyTaskCommand(cpPaths, claimedTask)
	if err != nil {
		return outcome, err
	}
	outcome.EndedAtUTC = time.Now().UTC().Format(time.RFC3339)
	outcome.ExecArtifacts = append([]string(nil), execArtifacts...)
	outcome.Verification = verification
	outcome.VerifyReason = compactLoopText(reason, 220)
	return outcome, nil
}

func applyClaimedTaskOutcome(db *ControlPlaneDBV1, cpPaths ControlPlanePaths, claimedTask TaskRecordV1, outcome controlPlaneTaskExecutionOutcome) (string, error) {
	taskID := claimedTask.ID
	task, ok := db.Tasks[taskID]
	if !ok {
		return "", fmt.Errorf("task not found: %s", taskID)
	}
	if task.State != ControlPlaneTaskStateRunning && task.State != ControlPlaneTaskStateVerifying {
		return task.State, nil
	}
	startedAt := parseRFC3339OrNow(outcome.StartedAtUTC).Format(time.RFC3339)
	endedAt := parseRFC3339OrNow(outcome.EndedAtUTC).Format(time.RFC3339)

	if strings.TrimSpace(outcome.ExecError) != "" {
		execError := compactLoopText(outcome.ExecError, 220)
		if err := setControlPlaneTaskState(db, cpPaths, taskID, ControlPlaneTaskStateBlocked, "execution_failed", execError, false); err != nil {
			return "", err
		}
		task = db.Tasks[taskID]
		failedAt := parseRFC3339OrNow(endedAt)
		policyTask, circuitOpened := applyBlockedRecoveryPolicy(task, failedAt)
		task = policyTask
		task.LeaseOwner = ""
		task.LeaseUntilUTC = ""
		task.HeartbeatUTC = ""
		task.LastError = execError
		task.UpdatedAtUTC = failedAt.Format(time.RFC3339)
		db.Tasks[taskID] = task
		if circuitOpened {
			_ = appendControlPlaneEvent(cpPaths, db, ControlPlaneEventV1{
				Type:   "circuit_opened",
				TaskID: taskID,
				Detail: fmt.Sprintf("blocked_count=%d until=%s", task.BlockedCount, task.CircuitOpenUntilUTC),
			})
		}
		db.TaskRuns = append(db.TaskRuns, TaskRunV1{
			TaskID:        taskID,
			Attempt:       task.Attempt,
			State:         ControlPlaneTaskStateBlocked,
			StartedAtUTC:  startedAt,
			EndedAtUTC:    endedAt,
			FailureReason: execError,
			Artifacts:     append([]string(nil), outcome.ExecArtifacts...),
		})
		if err := appendControlPlaneLearning(cpPaths, db, LearningEventV1{
			TimeUTC:    failedAt.Format(time.RFC3339),
			TaskID:     taskID,
			Category:   "execution_failure",
			Lesson:     compactLoopText(execError, 180),
			ActionItem: "Fix execution command/prompt and rerun cp recover + cp run.",
		}); err != nil {
			return "", err
		}
		return ControlPlaneTaskStateBlocked, nil
	}

	if err := setControlPlaneTaskState(db, cpPaths, taskID, ControlPlaneTaskStateVerifying, "task_state_changed", "execution finished; verifying", false); err != nil {
		return "", err
	}
	verification := outcome.Verification
	db.Verifications[taskID] = verification

	run := TaskRunV1{
		TaskID:       taskID,
		Attempt:      task.Attempt,
		StartedAtUTC: startedAt,
		EndedAtUTC:   endedAt,
		Artifacts:    append(append([]string(nil), outcome.ExecArtifacts...), verification.Evidence...),
	}
	if verification.Pass {
		run.State = ControlPlaneTaskStateDone
		if err := setControlPlaneTaskState(db, cpPaths, taskID, ControlPlaneTaskStateDone, "task_state_changed", "verification passed", false); err != nil {
			return "", err
		}
		task = db.Tasks[taskID]
		task.LeaseOwner = ""
		task.LeaseUntilUTC = ""
		task.HeartbeatUTC = ""
		task.LastError = ""
		task.BlockedCount = 0
		task.NextRecoverAtUTC = ""
		task.CircuitOpenUntilUTC = ""
		task.UpdatedAtUTC = time.Now().UTC().Format(time.RFC3339)
		db.Tasks[taskID] = task
		promotePlannedTasksToReady(db, cpPaths)
		db.TaskRuns = append(db.TaskRuns, run)
		return ControlPlaneTaskStateDone, nil
	}

	reason := compactLoopText(outcome.VerifyReason, 220)
	run.State = ControlPlaneTaskStateBlocked
	run.FailureReason = reason
	if strings.Contains(reason, "missing_evidence") {
		_ = appendControlPlaneEvent(cpPaths, db, ControlPlaneEventV1{
			Type:   "false_done_prevented",
			TaskID: taskID,
			Detail: reason,
		})
	}
	if err := setControlPlaneTaskState(db, cpPaths, taskID, ControlPlaneTaskStateBlocked, "verify_failed", reason, false); err != nil {
		return "", err
	}
	task = db.Tasks[taskID]
	failedAt := parseRFC3339OrNow(endedAt)
	policyTask, circuitOpened := applyBlockedRecoveryPolicy(task, failedAt)
	task = policyTask
	task.LeaseOwner = ""
	task.LeaseUntilUTC = ""
	task.HeartbeatUTC = ""
	task.LastError = reason
	task.UpdatedAtUTC = failedAt.Format(time.RFC3339)
	db.Tasks[taskID] = task
	if circuitOpened {
		_ = appendControlPlaneEvent(cpPaths, db, ControlPlaneEventV1{
			Type:   "circuit_opened",
			TaskID: taskID,
			Detail: fmt.Sprintf("blocked_count=%d until=%s", task.BlockedCount, task.CircuitOpenUntilUTC),
		})
	}
	db.TaskRuns = append(db.TaskRuns, run)
	if err := appendControlPlaneLearning(cpPaths, db, LearningEventV1{
		TimeUTC:    failedAt.Format(time.RFC3339),
		TaskID:     taskID,
		Category:   "verification_failure",
		Lesson:     compactLoopText(reason, 180),
		ActionItem: "Fix the failure reason and rerun cp recover + cp run.",
	}); err != nil {
		return "", err
	}
	return ControlPlaneTaskStateBlocked, nil
}

func parseRFC3339OrNow(raw string) time.Time {
	ts := strings.TrimSpace(raw)
	if ts == "" {
		return time.Now().UTC()
	}
	parsed, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Now().UTC()
	}
	return parsed.UTC()
}

func reclaimExpiredLeases(db *ControlPlaneDBV1, cpPaths ControlPlanePaths) int {
	now := time.Now().UTC()
	recovered := 0
	for _, id := range sortedTaskRecordIDs(db.Tasks) {
		task := db.Tasks[id]
		if task.State != ControlPlaneTaskStateRunning && task.State != ControlPlaneTaskStateVerifying {
			continue
		}
		if !leaseExpired(task, now) {
			continue
		}
		if err := setControlPlaneTaskState(db, cpPaths, id, ControlPlaneTaskStateReady, "recovered", "lease expired", true); err != nil {
			continue
		}
		task = db.Tasks[id]
		task.LeaseOwner = ""
		task.LeaseUntilUTC = ""
		task.HeartbeatUTC = ""
		task.UpdatedAtUTC = now.Format(time.RFC3339)
		db.Tasks[id] = task
		recovered++
	}
	return recovered
}

func promotePlannedTasksToReady(db *ControlPlaneDBV1, cpPaths ControlPlanePaths) int {
	promoted := 0
	ids := sortedTaskRecordIDs(db.Tasks)
	for _, id := range ids {
		task := db.Tasks[id]
		if task.State != ControlPlaneTaskStatePlanned {
			continue
		}
		if !depsSatisfied(task, db.Tasks) {
			continue
		}
		if err := setControlPlaneTaskState(db, cpPaths, id, ControlPlaneTaskStateReady, "task_state_changed", "dependencies satisfied", false); err != nil {
			continue
		}
		promoted++
	}
	return promoted
}

func setControlPlaneTaskState(db *ControlPlaneDBV1, cpPaths ControlPlanePaths, taskID, toState, eventType, detail string, force bool) error {
	toState = strings.TrimSpace(toState)
	if !isValidControlPlaneTaskState(toState) {
		return fmt.Errorf("invalid target state: %s", toState)
	}
	task, ok := db.Tasks[taskID]
	if !ok {
		return fmt.Errorf("task not found: %s", taskID)
	}
	from := task.State
	if from == toState {
		return nil
	}
	if !force && !isAllowedControlPlaneTransition(from, toState) {
		return fmt.Errorf("invalid state transition %s -> %s for task %s", from, toState, taskID)
	}
	task.State = toState
	task.UpdatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	db.Tasks[taskID] = task
	if strings.TrimSpace(eventType) == "" {
		eventType = "task_state_changed"
	}
	return appendControlPlaneEvent(cpPaths, db, ControlPlaneEventV1{
		Type:      eventType,
		IntentID:  task.IntentID,
		TaskID:    taskID,
		FromState: from,
		ToState:   toState,
		Attempt:   task.Attempt,
		Detail:    compactLoopText(detail, 220),
	})
}

func isAllowedControlPlaneTransition(from, to string) bool {
	allowed := map[string]map[string]struct{}{
		ControlPlaneTaskStateDraft: {
			ControlPlaneTaskStatePlanned: {},
		},
		ControlPlaneTaskStatePlanned: {
			ControlPlaneTaskStateReady: {},
		},
		ControlPlaneTaskStateReady: {
			ControlPlaneTaskStateRunning:   {},
			ControlPlaneTaskStateVerifying: {},
		},
		ControlPlaneTaskStateRunning: {
			ControlPlaneTaskStateVerifying: {},
			ControlPlaneTaskStateBlocked:   {},
			ControlPlaneTaskStateReady:     {},
		},
		ControlPlaneTaskStateVerifying: {
			ControlPlaneTaskStateDone:    {},
			ControlPlaneTaskStateBlocked: {},
			ControlPlaneTaskStateReady:   {},
		},
		ControlPlaneTaskStateBlocked: {
			ControlPlaneTaskStateReady: {},
		},
	}
	next, ok := allowed[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

func collectReadyTaskIDs(tasks map[string]TaskRecordV1) []string {
	ids := []string{}
	for id, task := range tasks {
		if task.State == ControlPlaneTaskStateReady {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a := tasks[ids[i]]
		b := tasks[ids[j]]
		if a.Priority == b.Priority {
			return a.ID < b.ID
		}
		return a.Priority < b.Priority
	})
	return ids
}

func collectTaskIDsByState(tasks map[string]TaskRecordV1, state string) []string {
	ids := []string{}
	for id, task := range tasks {
		if task.State == state {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func sortedTaskRecordIDs(tasks map[string]TaskRecordV1) []string {
	ids := make([]string, 0, len(tasks))
	for id := range tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func depsSatisfied(task TaskRecordV1, tasks map[string]TaskRecordV1) bool {
	for _, dep := range task.Deps {
		normalized := strings.TrimSpace(dep)
		if normalized == "" {
			continue
		}
		depTask, ok := tasks[normalized]
		if !ok {
			return false
		}
		if depTask.State != ControlPlaneTaskStateDone {
			return false
		}
	}
	return true
}

func leaseExpired(task TaskRecordV1, now time.Time) bool {
	until := strings.TrimSpace(task.LeaseUntilUTC)
	if until == "" {
		return false
	}
	ts, err := time.Parse(time.RFC3339, until)
	if err != nil {
		return true
	}
	return !now.Before(ts)
}

func recoverCooldownActive(task TaskRecordV1, now time.Time) bool {
	next := strings.TrimSpace(task.NextRecoverAtUTC)
	if next == "" {
		return false
	}
	ts, err := time.Parse(time.RFC3339, next)
	if err != nil {
		return false
	}
	return now.Before(ts)
}

func normalizeExpiredRecoveryWindows(db *ControlPlaneDBV1) (int, int) {
	now := time.Now().UTC()
	cooldownCleared := 0
	circuitCleared := 0
	for _, id := range sortedTaskRecordIDs(db.Tasks) {
		task := db.Tasks[id]
		if task.State != ControlPlaneTaskStateBlocked {
			continue
		}
		next := strings.TrimSpace(task.NextRecoverAtUTC)
		if next != "" {
			if ts, err := time.Parse(time.RFC3339, next); err != nil || !now.Before(ts) {
				task.NextRecoverAtUTC = ""
				cooldownCleared++
			}
		}
		circuit := strings.TrimSpace(task.CircuitOpenUntilUTC)
		if circuit != "" {
			if ts, err := time.Parse(time.RFC3339, circuit); err != nil || !now.Before(ts) {
				task.CircuitOpenUntilUTC = ""
				circuitCleared++
			}
		}
		db.Tasks[id] = task
	}
	return cooldownCleared, circuitCleared
}

func resetBlockedRecoveryPolicies(db *ControlPlaneDBV1, resetCircuit, resetRetryBudget bool) (int, int) {
	if !resetCircuit && !resetRetryBudget {
		return 0, 0
	}
	now := time.Now().UTC().Format(time.RFC3339)
	circuitResets := 0
	retryBudgetResets := 0
	for _, id := range sortedTaskRecordIDs(db.Tasks) {
		task := db.Tasks[id]
		if task.State != ControlPlaneTaskStateBlocked {
			continue
		}
		changed := false
		if resetCircuit {
			if strings.TrimSpace(task.CircuitOpenUntilUTC) != "" {
				circuitResets++
			}
			task.CircuitOpenUntilUTC = ""
			task.NextRecoverAtUTC = ""
			changed = true
		}
		if resetRetryBudget && task.Attempt >= controlPlaneRecoverMaxAttempts {
			task.Attempt = 0
			task.BlockedCount = 0
			task.NextRecoverAtUTC = ""
			retryBudgetResets++
			changed = true
		}
		if changed {
			task.UpdatedAtUTC = now
			db.Tasks[id] = task
		}
	}
	return circuitResets, retryBudgetResets
}

func circuitActive(task TaskRecordV1, now time.Time) bool {
	until := strings.TrimSpace(task.CircuitOpenUntilUTC)
	if until == "" {
		return false
	}
	ts, err := time.Parse(time.RFC3339, until)
	if err != nil {
		return false
	}
	return now.Before(ts)
}

func controlPlaneRecoveryBackoff(blockedCount int) time.Duration {
	// First blocked attempt can be retried immediately; backoff starts on consecutive failures.
	if blockedCount <= 1 {
		return 0
	}
	backoff := controlPlaneRecoverBackoffBaseSec
	exp := blockedCount - 2
	for i := 0; i < exp; i++ {
		backoff *= 2
		if backoff >= controlPlaneRecoverBackoffCapSec {
			backoff = controlPlaneRecoverBackoffCapSec
			break
		}
	}
	if backoff < 0 {
		backoff = 0
	}
	return time.Duration(backoff) * time.Second
}

func applyBlockedRecoveryPolicy(task TaskRecordV1, now time.Time) (TaskRecordV1, bool) {
	task.BlockedCount++
	backoff := controlPlaneRecoveryBackoff(task.BlockedCount)
	if backoff > 0 {
		task.NextRecoverAtUTC = now.Add(backoff).Format(time.RFC3339)
	} else {
		task.NextRecoverAtUTC = ""
	}

	circuitOpened := false
	if task.BlockedCount >= controlPlaneRecoverCircuitThreshold {
		wasActive := circuitActive(task, now)
		task.CircuitOpenUntilUTC = now.Add(time.Duration(controlPlaneRecoverCircuitCooldownSec) * time.Second).Format(time.RFC3339)
		if !wasActive {
			circuitOpened = true
		}
	}
	return task, circuitOpened
}

func isValidControlPlaneTaskState(state string) bool {
	_, ok := controlPlaneTaskStates[strings.TrimSpace(state)]
	return ok
}

func validateIntentSpec(spec IntentSpecV1) error {
	spec.ID = strings.TrimSpace(spec.ID)
	if spec.ID == "" {
		return fmt.Errorf("intent validation failed: id is required")
	}
	if spec.Version <= 0 {
		return fmt.Errorf("intent validation failed: version must be > 0")
	}
	if strings.TrimSpace(spec.Goal) == "" {
		return fmt.Errorf("intent validation failed: goal is required")
	}
	if len(normalizeStringList(spec.SuccessCriteria)) == 0 {
		return fmt.Errorf("intent validation failed: success_criteria must not be empty")
	}
	if len(spec.Epics) == 0 {
		return fmt.Errorf("intent validation failed: epics must not be empty")
	}
	epicSeen := map[string]struct{}{}
	for idx, epic := range spec.Epics {
		epicID := strings.TrimSpace(epic.ID)
		if epicID == "" {
			return fmt.Errorf("intent validation failed: epics[%d].id is required", idx)
		}
		if _, ok := epicSeen[epicID]; ok {
			return fmt.Errorf("intent validation failed: duplicate epic id %s", epicID)
		}
		epicSeen[epicID] = struct{}{}
		if len(epic.Tasks) == 0 {
			return fmt.Errorf("intent validation failed: epic %s must have tasks", epicID)
		}
	}
	return nil
}

func compileIntentToTaskNodes(spec IntentSpecV1) ([]TaskNodeV1, error) {
	nodes := []TaskNodeV1{}
	seenTaskID := map[string]struct{}{}
	for epicIdx, epic := range spec.Epics {
		epicID := strings.TrimSpace(epic.ID)
		for taskIdx, task := range epic.Tasks {
			taskID := strings.TrimSpace(task.ID)
			if taskID == "" {
				taskID = fmt.Sprintf("%s-task-%d", epicID, taskIdx+1)
			}
			if _, exists := seenTaskID[taskID]; exists {
				return nil, fmt.Errorf("plan validation failed: duplicate task id %s", taskID)
			}
			seenTaskID[taskID] = struct{}{}
			title := strings.TrimSpace(task.Title)
			if title == "" {
				return nil, fmt.Errorf("plan validation failed: task %s title is required", taskID)
			}
			role := strings.TrimSpace(task.Role)
			if !IsSupportedRole(role) {
				return nil, fmt.Errorf("plan validation failed: task %s has unsupported role %s", taskID, role)
			}
			priority := task.Priority
			if priority <= 0 {
				priority = defaultIssuePriority + epicIdx
			}
			acceptance := normalizeStringList(task.Acceptance)
			if len(acceptance) == 0 {
				return nil, fmt.Errorf("plan validation failed: task %s acceptance must not be empty", taskID)
			}
			verifyCmd := strings.TrimSpace(task.VerifyCmd)
			if verifyCmd == "" {
				return nil, fmt.Errorf("plan validation failed: task %s verify_cmd must not be empty", taskID)
			}
			executeCmd := strings.TrimSpace(task.ExecuteCmd)
			codexObjective := strings.TrimSpace(task.CodexObjective)
			risk := strings.TrimSpace(task.RiskLevel)
			if risk == "" {
				risk = "medium"
			}
			nodes = append(nodes, TaskNodeV1{
				ID:             taskID,
				EpicID:         epicID,
				Title:          title,
				Role:           role,
				Priority:       priority,
				Deps:           normalizeStringList(task.Deps),
				Acceptance:     acceptance,
				VerifyCmd:      verifyCmd,
				ExecuteCmd:     executeCmd,
				CodexObjective: codexObjective,
				RiskLevel:      risk,
			})
		}
	}
	stableSortTaskNodes(nodes)
	return nodes, nil
}

func stableSortTaskNodes(nodes []TaskNodeV1) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Priority == nodes[j].Priority {
			return nodes[i].ID < nodes[j].ID
		}
		return nodes[i].Priority < nodes[j].Priority
	})
}

func validateTaskNodeGraph(nodes []TaskNodeV1) error {
	if len(nodes) == 0 {
		return nil
	}
	index := map[string]TaskNodeV1{}
	for _, node := range nodes {
		index[node.ID] = node
	}
	for _, node := range nodes {
		for _, dep := range node.Deps {
			if _, ok := index[dep]; !ok {
				return fmt.Errorf("plan validation failed: task %s references missing dependency %s", node.ID, dep)
			}
		}
	}
	// DFS cycle detection.
	state := map[string]int{}
	var visit func(string) error
	visit = func(id string) error {
		switch state[id] {
		case 1:
			return fmt.Errorf("plan validation failed: cycle detected at task %s", id)
		case 2:
			return nil
		}
		state[id] = 1
		node := index[id]
		for _, dep := range node.Deps {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	for _, node := range nodes {
		if err := visit(node.ID); err != nil {
			return err
		}
	}
	return nil
}

func computeControlPlaneMetrics(db ControlPlaneDBV1) ControlPlaneMetrics {
	now := time.Now().UTC().Format(time.RFC3339)
	total := len(db.Tasks)
	done := 0
	blocked := 0
	for _, task := range db.Tasks {
		if task.State == ControlPlaneTaskStateDone {
			done++
		}
		if task.State == ControlPlaneTaskStateBlocked {
			blocked++
		}
	}
	blockedRate := 0.0
	if done+blocked > 0 {
		blockedRate = float64(blocked) / float64(done+blocked)
	}

	recoveryEvents := 0
	recoverySuccesses := 0
	falseDonePrevented := 0
	verificationFailures := 0
	blockedAt := map[string]time.Time{}
	totalRecoverySeconds := 0.0
	recoverySamples := 0

	for _, event := range db.Events {
		t := parseEventTime(event.TimeUTC)
		switch event.Type {
		case "recovered":
			recoveryEvents++
			if prev, ok := blockedAt[event.TaskID]; ok && !t.IsZero() {
				if t.After(prev) {
					totalRecoverySeconds += t.Sub(prev).Seconds()
					recoverySamples++
				}
			}
		case "false_done_prevented":
			falseDonePrevented++
		case "verify_failed":
			verificationFailures++
		}
		if event.ToState == ControlPlaneTaskStateBlocked && !t.IsZero() {
			blockedAt[event.TaskID] = t
		}
		if event.ToState == ControlPlaneTaskStateDone {
			if _, ok := blockedAt[event.TaskID]; ok {
				recoverySuccesses++
				delete(blockedAt, event.TaskID)
			}
		}
	}

	recoverySuccessRate := 0.0
	if recoveryEvents > 0 {
		recoverySuccessRate = float64(recoverySuccesses) / float64(recoveryEvents)
	}
	mttr := 0.0
	if recoverySamples > 0 {
		mttr = totalRecoverySeconds / float64(recoverySamples)
	}

	return ControlPlaneMetrics{
		UpdatedAtUTC:         now,
		TotalTasks:           total,
		DoneTasks:            done,
		BlockedTasks:         blocked,
		BlockedRate:          blockedRate,
		RecoveryEvents:       recoveryEvents,
		RecoverySuccesses:    recoverySuccesses,
		RecoverySuccessRate:  recoverySuccessRate,
		MeanTimeToRecovery:   mttr,
		FalseDonePrevented:   falseDonePrevented,
		VerificationFailures: verificationFailures,
	}
}

func parseEventTime(raw string) time.Time {
	ts := strings.TrimSpace(raw)
	if ts == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func replayTaskStatesFromEvents(events []ControlPlaneEventV1) map[string]string {
	replayed := map[string]string{}
	for _, event := range events {
		taskID := strings.TrimSpace(event.TaskID)
		toState := strings.TrimSpace(event.ToState)
		if taskID == "" || toState == "" {
			continue
		}
		replayed[taskID] = toState
	}
	return replayed
}

type controlPlaneTaskJSONConsistency struct {
	Missing       int
	Orphan        int
	StateMismatch int
	ParseErrors   int
}

func assessControlPlaneTaskJSONConsistency(cpPaths ControlPlanePaths, tasks map[string]TaskRecordV1) (controlPlaneTaskJSONConsistency, error) {
	stats := controlPlaneTaskJSONConsistency{}
	entries, err := os.ReadDir(cpPaths.TasksDir)
	if err != nil {
		return stats, err
	}
	fileTaskIDs := map[string]struct{}{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		taskID := strings.TrimSpace(strings.TrimSuffix(entry.Name(), ".json"))
		if taskID == "" {
			continue
		}
		fileTaskIDs[taskID] = struct{}{}
		raw, readErr := os.ReadFile(filepath.Join(cpPaths.TasksDir, entry.Name()))
		if readErr != nil {
			stats.ParseErrors++
			continue
		}
		record := TaskRecordV1{}
		if err := json.Unmarshal(bytes.TrimSpace(raw), &record); err != nil {
			stats.ParseErrors++
			continue
		}
		dbTask, ok := tasks[taskID]
		if !ok {
			stats.Orphan++
			continue
		}
		if strings.TrimSpace(record.State) != strings.TrimSpace(dbTask.State) {
			stats.StateMismatch++
		}
	}
	for taskID := range tasks {
		if _, ok := fileTaskIDs[taskID]; ok {
			continue
		}
		stats.Missing++
	}
	return stats, nil
}

type controlPlaneEventJournalConsistency struct {
	LineCount   int
	ParseErrors int
}

type controlPlaneSQLiteLedgerConsistency struct {
	RowCount    int
	ParseErrors int
}

func readControlPlaneEventJournalConsistency(path string) (controlPlaneEventJournalConsistency, error) {
	stats := controlPlaneEventJournalConsistency{}
	f, err := os.Open(path)
	if err != nil {
		return stats, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		stats.LineCount++
		entry := ControlPlaneEventV1{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			stats.ParseErrors++
		}
	}
	if err := scanner.Err(); err != nil {
		return stats, err
	}
	return stats, nil
}

type controlPlaneLearningJournalConsistency struct {
	LineCount   int
	ParseErrors int
}

func readControlPlaneLearningJournalConsistency(path string) (controlPlaneLearningJournalConsistency, error) {
	stats := controlPlaneLearningJournalConsistency{}
	f, err := os.Open(path)
	if err != nil {
		return stats, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		stats.LineCount++
		entry := LearningEventV1{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			stats.ParseErrors++
		}
	}
	if err := scanner.Err(); err != nil {
		return stats, err
	}
	return stats, nil
}

func readControlPlaneSQLiteLedgerConsistency(dbPath, tableName string, decode func(raw []byte) error) (controlPlaneSQLiteLedgerConsistency, error) {
	stats := controlPlaneSQLiteLedgerConsistency{}
	rows, err := runSQLiteJSONQuery(dbPath, fmt.Sprintf("SELECT event_json AS event_json FROM %s ORDER BY id;", tableName))
	if err != nil {
		return stats, err
	}
	stats.RowCount = len(rows)
	for _, row := range rows {
		raw := strings.TrimSpace(rowString(row, "event_json"))
		if raw == "" {
			stats.ParseErrors++
			continue
		}
		if err := decode([]byte(raw)); err != nil {
			stats.ParseErrors++
		}
	}
	return stats, nil
}

func defaultControlPlaneCutoverState() ControlPlaneCutoverState {
	now := time.Now().UTC().Format(time.RFC3339)
	return ControlPlaneCutoverState{
		Mode:          "v1",
		Canary:        false,
		UpdatedAtUTC:  now,
		LastSwitchUTC: now,
	}
}

func loadControlPlaneCutoverState(cpPaths ControlPlanePaths) (ControlPlaneCutoverState, error) {
	data, err := os.ReadFile(cpPaths.CutoverFile)
	if err != nil {
		if os.IsNotExist(err) {
			state := defaultControlPlaneCutoverState()
			if err := saveControlPlaneCutoverState(cpPaths, state); err != nil {
				return ControlPlaneCutoverState{}, err
			}
			return state, nil
		}
		return ControlPlaneCutoverState{}, fmt.Errorf("read cutover file: %w", err)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		state := defaultControlPlaneCutoverState()
		if err := saveControlPlaneCutoverState(cpPaths, state); err != nil {
			return ControlPlaneCutoverState{}, err
		}
		return state, nil
	}
	state := ControlPlaneCutoverState{}
	if err := json.Unmarshal(trimmed, &state); err != nil {
		return ControlPlaneCutoverState{}, fmt.Errorf("parse cutover file: %w", err)
	}
	mode := strings.TrimSpace(strings.ToLower(state.Mode))
	switch mode {
	case "v1", "v2":
		state.Mode = mode
	default:
		state.Mode = "v1"
	}
	if strings.TrimSpace(state.UpdatedAtUTC) == "" {
		state.UpdatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	}
	if strings.TrimSpace(state.LastSwitchUTC) == "" {
		state.LastSwitchUTC = state.UpdatedAtUTC
	}
	return state, nil
}

func saveControlPlaneCutoverState(cpPaths ControlPlanePaths, state ControlPlaneCutoverState) error {
	mode := strings.TrimSpace(strings.ToLower(state.Mode))
	if mode != "v1" && mode != "v2" {
		mode = "v1"
	}
	state.Mode = mode
	if strings.TrimSpace(state.UpdatedAtUTC) == "" {
		state.UpdatedAtUTC = time.Now().UTC().Format(time.RFC3339)
	}
	if strings.TrimSpace(state.LastSwitchUTC) == "" {
		state.LastSwitchUTC = state.UpdatedAtUTC
	}
	return writeJSONFile(cpPaths.CutoverFile, state)
}

func passOrFail(ok bool) string {
	if ok {
		return "pass"
	}
	return "fail"
}

func normalizeStringList(items []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, raw := range items {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func resolveInputPath(projectDir, raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("file is required")
	}
	candidate := trimmed
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(projectDir, candidate)
	}
	abs, err := filepath.Abs(candidate)
	if err != nil {
		return "", err
	}
	return abs, nil
}

func writeJSONFile(path string, payload any) error {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal json %s: %w", path, err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write json %s: %w", path, err)
	}
	return nil
}

func normalizeMigratedRole(role string) string {
	r := strings.TrimSpace(role)
	if IsSupportedRole(r) {
		return r
	}
	return "developer"
}

func normalizeMigratedPriority(priority int) int {
	if priority <= 0 {
		return defaultIssuePriority
	}
	return priority
}

func readIssueChecklist(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	out := []string{}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "- [ ] "):
			out = append(out, strings.TrimSpace(strings.TrimPrefix(trimmed, "- [ ] ")))
		case strings.HasPrefix(trimmed, "- [x] "):
			out = append(out, strings.TrimSpace(strings.TrimPrefix(trimmed, "- [x] ")))
		case strings.HasPrefix(trimmed, "- [X] "):
			out = append(out, strings.TrimSpace(strings.TrimPrefix(trimmed, "- [X] ")))
		}
	}
	return normalizeStringList(out)
}

func readIssueLastReason(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	reason := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "- reason:") {
			reason = strings.TrimSpace(strings.TrimPrefix(line, "- reason:"))
		}
	}
	return compactLoopText(reason, 220)
}

func ControlPlaneLoadDBForTest(projectDir string) (ControlPlaneDBV1, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return ControlPlaneDBV1{}, err
	}
	return loadControlPlaneDB(cpPaths)
}

func ControlPlaneWriteIntentFileForTest(projectDir string, spec IntentSpecV1) (string, error) {
	cpPaths, err := EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return "", err
	}
	path := filepath.Join(cpPaths.ProjectDir, "intent-test.json")
	if err := writeJSONFile(path, spec); err != nil {
		return "", err
	}
	return path, nil
}

var errControlPlaneNoEvidence = errors.New("missing evidence")

func ControlPlaneCheckNoEvidence(result VerificationResultV1) error {
	for _, c := range result.Checks {
		if c.Name == "evidence_present" && c.Status == "fail" {
			return errControlPlaneNoEvidence
		}
	}
	return nil
}
