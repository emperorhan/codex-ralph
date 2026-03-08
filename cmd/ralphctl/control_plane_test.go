package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codex-ralph/internal/ralph"
)

func TestRunControlPlaneCutoverAutoEnable(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	if err := runControlPlaneCutoverCommand(projectDir, []string{"auto"}); err != nil {
		t.Fatalf("cutover auto failed: %v", err)
	}
	state, err := ralph.ControlPlaneGetCutoverState(projectDir)
	if err != nil {
		t.Fatalf("get cutover state failed: %v", err)
	}
	if state.Mode != "v2" {
		t.Fatalf("cutover auto should enable v2, got mode=%s", state.Mode)
	}
	if !state.Canary {
		t.Fatalf("cutover auto should enable canary mode")
	}
}

func TestControlPlaneAPIMuxEndpoints(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	intentPath := filepath.Join(projectDir, "intent-api-test.json")
	writeJSONForTest(t, intentPath, map[string]any{
		"id":               "api-test-intent",
		"version":          1,
		"goal":             "exercise control plane api",
		"success_criteria": []string{"api endpoints respond"},
		"constraints":      []string{"single project"},
		"non_goals":        []string{"fleet"},
		"epics": []map[string]any{
			{
				"id":    "epic-1",
				"title": "Epic One",
				"tasks": []map[string]any{
					{
						"id":         "task-1",
						"title":      "Task One",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{},
						"acceptance": []string{"one"},
						"verify_cmd": "printf 'ok'",
						"risk_level": "low",
					},
				},
			},
		},
	})
	if _, err := ralph.ControlPlaneImportIntent(projectDir, intentPath); err != nil {
		t.Fatalf("import intent failed: %v", err)
	}
	if _, err := ralph.ControlPlanePlanIntent(projectDir, "api-test-intent", ralph.ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan intent failed: %v", err)
	}

	handler := newControlPlaneAPIMux(projectDir)

	for _, path := range []string{
		"/health",
		"/v2/status",
		"/v2/metrics",
		"/v2/metrics?with_baseline=true",
		"/v2/metrics/summary",
		"/v2/doctor",
		"/v2/cutover",
		"/v2/events?limit=1",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status mismatch: got=%d want=%d", path, rec.Code, http.StatusOK)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	streamReq := httptest.NewRequest(http.MethodGet, "/v2/events/stream?from=start", nil).WithContext(ctx)
	streamRec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(streamRec, streamReq)
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(streamRec.Body.String(), "data: ") {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel()
	<-done
	if streamRec.Code != http.StatusOK {
		t.Fatalf("stream status mismatch: got=%d want=%d", streamRec.Code, http.StatusOK)
	}
	body := streamRec.Body.String()
	if !strings.Contains(body, "data: ") {
		t.Fatalf("stream should contain at least one data frame, body=%q", body)
	}
}

func TestRunControlPlaneFaultInjectPermissionDenied(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	intentPath := filepath.Join(projectDir, "intent-fault-inject-cli.json")
	writeJSONForTest(t, intentPath, map[string]any{
		"id":               "fault-inject-cli-intent",
		"version":          1,
		"goal":             "exercise fault inject cli",
		"success_criteria": []string{"fault mode applied"},
		"constraints":      []string{"single project"},
		"non_goals":        []string{"fleet"},
		"epics": []map[string]any{
			{
				"id":    "epic-1",
				"title": "Epic One",
				"tasks": []map[string]any{
					{
						"id":         "task-1",
						"title":      "Task One",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{},
						"acceptance": []string{"one"},
						"verify_cmd": "printf 'ok'",
						"risk_level": "low",
					},
				},
			},
		},
	})
	if _, err := ralph.ControlPlaneImportIntent(projectDir, intentPath); err != nil {
		t.Fatalf("import intent failed: %v", err)
	}
	if _, err := ralph.ControlPlanePlanIntent(projectDir, "fault-inject-cli-intent", ralph.ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan intent failed: %v", err)
	}
	if err := runControlPlaneCommand("", projectDir, []string{"fault-inject", "--task-id", "task-1", "--mode", "permission-denied"}); err != nil {
		t.Fatalf("run fault-inject command failed: %v", err)
	}

	db, err := ralph.ControlPlaneLoadDBForTest(projectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	task := db.Tasks["task-1"]
	if !strings.Contains(task.ExecuteCmd, "fault-permission-denied.txt") {
		t.Fatalf("permission-denied mode should set execute_cmd fixture, got=%q", task.ExecuteCmd)
	}
}

func TestRunControlPlaneCutoverAutoDisableOnFailure(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	intentPath := filepath.Join(projectDir, "intent-cutover-fail.json")
	writeJSONForTest(t, intentPath, map[string]any{
		"id":               "cutover-fail-intent",
		"version":          1,
		"goal":             "make blocked task",
		"success_criteria": []string{"task done"},
		"constraints":      []string{"single project"},
		"non_goals":        []string{"fleet"},
		"epics": []map[string]any{
			{
				"id":    "epic-1",
				"title": "Epic One",
				"tasks": []map[string]any{
					{
						"id":         "task-1",
						"title":      "Task One",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{},
						"acceptance": []string{"one"},
						"verify_cmd": "false",
						"risk_level": "high",
					},
				},
			},
		},
	})
	if _, err := ralph.ControlPlaneImportIntent(projectDir, intentPath); err != nil {
		t.Fatalf("import intent failed: %v", err)
	}
	if _, err := ralph.ControlPlanePlanIntent(projectDir, "cutover-fail-intent", ralph.ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ralph.ControlPlaneRun(projectDir, ralph.ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before auto rollback"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	if err := runControlPlaneCutoverCommand(projectDir, []string{"auto", "--disable-on-fail=true"}); err != nil {
		t.Fatalf("cutover auto disable-on-fail failed: %v", err)
	}
	state, err := ralph.ControlPlaneGetCutoverState(projectDir)
	if err != nil {
		t.Fatalf("get cutover state failed: %v", err)
	}
	if state.Mode != "v1" {
		t.Fatalf("cutover auto should rollback to v1, got mode=%s", state.Mode)
	}
	if state.Canary {
		t.Fatalf("cutover rollback should disable canary")
	}
}

func TestRunControlPlaneCutoverAutoDryRunEnableNoMutation(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	before, err := ralph.ControlPlaneGetCutoverState(projectDir)
	if err != nil {
		t.Fatalf("get cutover state before failed: %v", err)
	}
	if err := runControlPlaneCutoverCommand(projectDir, []string{"auto", "--dry-run=true"}); err != nil {
		t.Fatalf("cutover auto dry-run failed: %v", err)
	}
	after, err := ralph.ControlPlaneGetCutoverState(projectDir)
	if err != nil {
		t.Fatalf("get cutover state after failed: %v", err)
	}
	if before.Mode != after.Mode || before.Canary != after.Canary {
		t.Fatalf("dry-run should not mutate cutover state: before=%+v after=%+v", before, after)
	}
}

func TestRunControlPlaneCutoverAutoDryRunRollbackNoMutation(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	prepareCutoverFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before dry-run rollback test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	if err := runControlPlaneCutoverCommand(projectDir, []string{"auto", "--dry-run=true", "--disable-on-fail=true", "--rollback-on=all"}); err != nil {
		t.Fatalf("cutover auto dry-run rollback failed: %v", err)
	}
	state, stateErr := ralph.ControlPlaneGetCutoverState(projectDir)
	if stateErr != nil {
		t.Fatalf("get cutover state failed: %v", stateErr)
	}
	if state.Mode != "v2" || !state.Canary {
		t.Fatalf("dry-run should not apply rollback: mode=%s canary=%t", state.Mode, state.Canary)
	}
}

func TestRunControlPlaneCutoverAutoDryRunKeepCurrentReturnsError(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	prepareCutoverFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before dry-run keep-current test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	err := runControlPlaneCutoverCommand(projectDir, []string{"auto", "--dry-run=true", "--disable-on-fail=true", "--rollback-on=doctor"})
	if err == nil {
		t.Fatalf("expected error when dry-run decision is keep-current")
	}
	state, stateErr := ralph.ControlPlaneGetCutoverState(projectDir)
	if stateErr != nil {
		t.Fatalf("get cutover state failed: %v", stateErr)
	}
	if state.Mode != "v2" {
		t.Fatalf("dry-run keep-current should not mutate mode, got=%s", state.Mode)
	}
}

func TestRunControlPlaneCutoverAutoDryRunWritesDecisionReport(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-auto-dry-run.json")
	if err := runControlPlaneCutoverCommand(projectDir, []string{
		"auto",
		"--dry-run=true",
		"--output", reportPath,
	}); err != nil {
		t.Fatalf("cutover auto dry-run with output failed: %v", err)
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read decision report failed: %v", readErr)
	}
	report := cutoverAutoDecisionReport{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse decision report failed: %v", err)
	}
	if report.Decision != "enable-v2" {
		t.Fatalf("decision mismatch: got=%s want=enable-v2", report.Decision)
	}
	if !report.DryRun || report.Applied {
		t.Fatalf("dry-run report mismatch: dry_run=%t applied=%t", report.DryRun, report.Applied)
	}
	if report.ResultMode != "v2" {
		t.Fatalf("result mode mismatch: got=%s want=v2", report.ResultMode)
	}
}

func TestRunControlPlaneCutoverEvaluateRequireBaselineWritesFailure(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-evaluate-require-baseline.json")
	if err := runControlPlaneCutoverCommand(projectDir, []string{
		"evaluate",
		"--require-baseline=true",
		"--output", reportPath,
	}); err != nil {
		t.Fatalf("cutover evaluate with require-baseline failed: %v", err)
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read evaluate report failed: %v", readErr)
	}
	report := ralph.ControlPlaneCutoverEvaluation{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse evaluate report failed: %v", err)
	}
	if report.Ready {
		t.Fatalf("evaluation should fail when baseline is required and missing")
	}
	found := false
	for _, failure := range report.Failures {
		if failure.Category == ralph.ControlPlaneCutoverFailureCategoryBaseline && failure.Code == "baseline.missing" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected baseline.missing failure, got=%+v", report.Failures)
	}
}

func TestRunControlPlaneCutoverAutoRequireBaselineRollsBack(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before require-baseline rollback test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-auto-require-baseline.json")
	if err := runControlPlaneCutoverCommand(projectDir, []string{
		"auto",
		"--require-baseline=true",
		"--disable-on-fail=true",
		"--rollback-on=baseline",
		"--output", reportPath,
	}); err != nil {
		t.Fatalf("cutover auto require-baseline rollback failed: %v", err)
	}
	state, stateErr := ralph.ControlPlaneGetCutoverState(projectDir)
	if stateErr != nil {
		t.Fatalf("get cutover state failed: %v", stateErr)
	}
	if state.Mode != "v1" {
		t.Fatalf("mode should rollback to v1 when baseline is missing, got=%s", state.Mode)
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read auto decision report failed: %v", readErr)
	}
	report := cutoverAutoDecisionReport{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse auto decision report failed: %v", err)
	}
	if report.Decision != "disable-v2" {
		t.Fatalf("decision mismatch: got=%s want=disable-v2", report.Decision)
	}
	if !report.RequireBaseline {
		t.Fatalf("report should include require_baseline=true")
	}
}

func TestRunControlPlaneCutoverEvaluateRequireSoakPassWritesFailure(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-evaluate-require-soak.json")
	if err := runControlPlaneCutoverCommand(projectDir, []string{
		"evaluate",
		"--require-soak-pass=true",
		"--output", reportPath,
	}); err != nil {
		t.Fatalf("cutover evaluate with require-soak-pass failed: %v", err)
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read evaluate report failed: %v", readErr)
	}
	report := ralph.ControlPlaneCutoverEvaluation{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse evaluate report failed: %v", err)
	}
	if report.Ready {
		t.Fatalf("evaluation should fail when soak pass is required and no soak report exists")
	}
	found := false
	for _, failure := range report.Failures {
		if failure.Category == ralph.ControlPlaneCutoverFailureCategorySoak && failure.Code == "soak.missing" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected soak.missing failure, got=%+v", report.Failures)
	}
}

func TestRunControlPlaneCutoverAutoRequireSoakRollsBack(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before require-soak rollback test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-auto-require-soak.json")
	if err := runControlPlaneCutoverCommand(projectDir, []string{
		"auto",
		"--require-soak-pass=true",
		"--disable-on-fail=true",
		"--rollback-on=soak",
		"--output", reportPath,
	}); err != nil {
		t.Fatalf("cutover auto require-soak rollback failed: %v", err)
	}
	state, stateErr := ralph.ControlPlaneGetCutoverState(projectDir)
	if stateErr != nil {
		t.Fatalf("get cutover state failed: %v", stateErr)
	}
	if state.Mode != "v1" {
		t.Fatalf("mode should rollback to v1 when soak is missing, got=%s", state.Mode)
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read auto decision report failed: %v", readErr)
	}
	report := cutoverAutoDecisionReport{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse auto decision report failed: %v", err)
	}
	if report.Decision != "disable-v2" {
		t.Fatalf("decision mismatch: got=%s want=disable-v2", report.Decision)
	}
	if !report.RequireSoakPass {
		t.Fatalf("report should include require_soak_pass=true")
	}
}

func TestRunControlPlaneCutoverAutoRequireSoakPassesWithValidReport(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	soakPath := writeSoakReportForTest(t, projectDir, "soak-20990101T000000Z.json", ralph.ControlPlaneSoakReport{
		StartedAtUTC:    "2026-03-06T00:00:00Z",
		FinishedAtUTC:   "2026-03-06T00:01:00Z",
		DurationSec:     60,
		IntervalSec:     30,
		Strict:          true,
		FailureDetected: false,
	})
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, false, false, "before require-soak pass test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-auto-require-soak-pass.json")
	if err := runControlPlaneCutoverCommand(projectDir, []string{
		"auto",
		"--require-soak-pass=true",
		"--soak-report", soakPath,
		"--max-soak-age-sec=9999999",
		"--disable-on-fail=true",
		"--rollback-on=soak",
		"--output", reportPath,
	}); err != nil {
		t.Fatalf("cutover auto require-soak pass failed: %v", err)
	}
	state, stateErr := ralph.ControlPlaneGetCutoverState(projectDir)
	if stateErr != nil {
		t.Fatalf("get cutover state failed: %v", stateErr)
	}
	if state.Mode != "v2" {
		t.Fatalf("mode should enable v2 when soak report is valid, got=%s", state.Mode)
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read auto decision report failed: %v", readErr)
	}
	report := cutoverAutoDecisionReport{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse auto decision report failed: %v", err)
	}
	if report.Decision != "enable-v2" {
		t.Fatalf("decision mismatch: got=%s want=enable-v2", report.Decision)
	}
	if strings.TrimSpace(report.SoakReportPath) == "" {
		t.Fatalf("report should include soak_report_path")
	}
}

func TestRunControlPlaneCutoverAutoRollbackWritesDecisionReport(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	prepareCutoverFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before rollback report test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-auto-rollback.json")
	if err := runControlPlaneCutoverCommand(projectDir, []string{
		"auto",
		"--disable-on-fail=true",
		"--rollback-on=all",
		"--output", reportPath,
	}); err != nil {
		t.Fatalf("cutover auto rollback with output failed: %v", err)
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read decision report failed: %v", readErr)
	}
	report := cutoverAutoDecisionReport{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse decision report failed: %v", err)
	}
	if report.Decision != "disable-v2" {
		t.Fatalf("decision mismatch: got=%s want=disable-v2", report.Decision)
	}
	if !report.Applied || report.ResultMode != "v1" {
		t.Fatalf("rollback report mismatch: applied=%t result_mode=%s", report.Applied, report.ResultMode)
	}
	if !report.ShouldRollback {
		t.Fatalf("should_rollback should be true")
	}
}

func TestRunControlPlaneCutoverAutoKeepCurrentErrorWritesDecisionReport(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	prepareCutoverFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before keep-current report test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-auto-keep-current.json")
	err := runControlPlaneCutoverCommand(projectDir, []string{
		"auto",
		"--disable-on-fail=true",
		"--rollback-on=doctor",
		"--output", reportPath,
	})
	if err == nil {
		t.Fatalf("expected keep-current error")
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read decision report failed: %v", readErr)
	}
	report := cutoverAutoDecisionReport{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse decision report failed: %v", err)
	}
	if report.Decision != "keep-current" {
		t.Fatalf("decision mismatch: got=%s want=keep-current", report.Decision)
	}
	if report.Applied {
		t.Fatalf("keep-current decision should not be applied")
	}
}

func TestRunControlPlaneCutoverAutoKeepCurrentAllowedReturnsNil(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	prepareCutoverFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before keep-current allowed test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	if err := runControlPlaneCutoverCommand(projectDir, []string{
		"auto",
		"--disable-on-fail=true",
		"--rollback-on=doctor",
		"--allow-keep-current=true",
	}); err != nil {
		t.Fatalf("cutover auto should succeed when keep-current is allowed: %v", err)
	}
	state, stateErr := ralph.ControlPlaneGetCutoverState(projectDir)
	if stateErr != nil {
		t.Fatalf("get cutover state failed: %v", stateErr)
	}
	if state.Mode != "v2" || !state.Canary {
		t.Fatalf("state should remain unchanged in keep-current: mode=%s canary=%t", state.Mode, state.Canary)
	}
}

func TestRunControlPlaneCutoverAutoPreRepairResetCircuitWritesDecisionReport(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	prepareCutoverCircuitFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before pre-repair reset-circuit report test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-auto-pre-repair-reset.json")
	if err := runControlPlaneCutoverCommand(projectDir, []string{
		"auto",
		"--disable-on-fail=true",
		"--rollback-on=doctor",
		"--allow-keep-current=true",
		"--pre-repair=true",
		"--pre-repair-reset-circuit=true",
		"--output", reportPath,
	}); err != nil {
		t.Fatalf("cutover auto with pre-repair reset-circuit failed: %v", err)
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("read decision report failed: %v", readErr)
	}
	report := cutoverAutoDecisionReport{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse decision report failed: %v", err)
	}
	if !report.PreRepairRequested || !report.PreRepairApplied {
		t.Fatalf("pre-repair report flags mismatch: requested=%t applied=%t", report.PreRepairRequested, report.PreRepairApplied)
	}
	foundReset := false
	for _, action := range report.PreRepairActions {
		if action.Name != "reset_recovery_policies" {
			continue
		}
		foundReset = true
		if !strings.Contains(action.Detail, "reset_circuit=true") {
			t.Fatalf("reset action detail should mention reset_circuit=true, got=%s", action.Detail)
		}
	}
	if !foundReset {
		t.Fatalf("expected reset_recovery_policies action in pre-repair report")
	}
}

func TestRunControlPlaneDoctorRepairResetFlags(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	if err := runControlPlaneCommand("", projectDir, []string{
		"doctor",
		"--repair=true",
		"--repair-reset-circuit=true",
		"--repair-reset-retry-budget=true",
	}); err != nil {
		t.Fatalf("cp doctor --repair with reset flags failed: %v", err)
	}
}

func TestRunControlPlaneCutoverAutoRollbackPolicyNoMatchKeepsV2(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	prepareCutoverFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before policy test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	err := runControlPlaneCutoverCommand(projectDir, []string{"auto", "--disable-on-fail=true", "--rollback-on=doctor"})
	if err == nil {
		t.Fatalf("expected evaluation error when rollback policy does not match failure categories")
	}
	state, stateErr := ralph.ControlPlaneGetCutoverState(projectDir)
	if stateErr != nil {
		t.Fatalf("get cutover state failed: %v", stateErr)
	}
	if state.Mode != "v2" {
		t.Fatalf("mode should remain v2 when policy does not match, got=%s", state.Mode)
	}
}

func TestRunControlPlaneCutoverAutoRollbackPolicyMatchRollsBack(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	prepareCutoverFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before policy match test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	if err := runControlPlaneCutoverCommand(projectDir, []string{"auto", "--disable-on-fail=true", "--rollback-on=kpi"}); err != nil {
		t.Fatalf("cutover auto with kpi policy failed: %v", err)
	}
	state, stateErr := ralph.ControlPlaneGetCutoverState(projectDir)
	if stateErr != nil {
		t.Fatalf("get cutover state failed: %v", stateErr)
	}
	if state.Mode != "v1" {
		t.Fatalf("mode should rollback to v1 when policy matches, got=%s", state.Mode)
	}
}

func TestRunControlPlaneCutoverAutoRollbackPolicyDataIntegrityMatchRollsBack(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	intentPath := filepath.Join(projectDir, "intent-integrity-rollback.json")
	writeJSONForTest(t, intentPath, map[string]any{
		"id":               "integrity-rollback-intent",
		"version":          1,
		"goal":             "trigger data integrity rollback policy",
		"success_criteria": []string{"task graph planned"},
		"constraints":      []string{"single project"},
		"non_goals":        []string{"fleet"},
		"epics": []map[string]any{
			{
				"id":    "epic-1",
				"title": "Epic One",
				"tasks": []map[string]any{
					{
						"id":         "task-1",
						"title":      "Task One",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{},
						"acceptance": []string{"one"},
						"verify_cmd": "printf 'ok-one'",
						"risk_level": "low",
					},
				},
			},
		},
	})
	if _, err := ralph.ControlPlaneImportIntent(projectDir, intentPath); err != nil {
		t.Fatalf("import intent failed: %v", err)
	}
	if _, err := ralph.ControlPlanePlanIntent(projectDir, "integrity-rollback-intent", ralph.ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before data_integrity rollback test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	cpPaths, err := ralph.NewControlPlanePaths(projectDir)
	if err != nil {
		t.Fatalf("new control plane paths failed: %v", err)
	}
	if err := os.Remove(filepath.Join(cpPaths.TasksDir, "task-1.json")); err != nil {
		t.Fatalf("remove task json failed: %v", err)
	}
	if err := runControlPlaneCutoverCommand(projectDir, []string{"auto", "--disable-on-fail=true", "--rollback-on=data_integrity"}); err != nil {
		t.Fatalf("cutover auto with data_integrity policy failed: %v", err)
	}
	state, stateErr := ralph.ControlPlaneGetCutoverState(projectDir)
	if stateErr != nil {
		t.Fatalf("get cutover state failed: %v", stateErr)
	}
	if state.Mode != "v1" {
		t.Fatalf("mode should rollback to v1 when data_integrity policy matches, got=%s", state.Mode)
	}
}

func TestRunControlPlaneCutoverAutoPreRepairForceRecoverExecutes(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	prepareCutoverCooldownFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, "before pre-repair force test"); err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	err := runControlPlaneCutoverCommand(projectDir, []string{
		"auto",
		"--disable-on-fail=false",
		"--rollback-on=all",
		"--pre-repair=true",
		"--pre-repair-force-recover=true",
	})
	if err == nil {
		t.Fatalf("expected evaluation failure due remaining KPI gates")
	}
	status, statusErr := ralph.ControlPlaneStatusReport(projectDir)
	if statusErr != nil {
		t.Fatalf("control plane status failed: %v", statusErr)
	}
	if status.StateCounts[ralph.ControlPlaneTaskStateBlocked] != 0 {
		t.Fatalf("blocked tasks should be recovered by pre-repair force, blocked=%d", status.StateCounts[ralph.ControlPlaneTaskStateBlocked])
	}
	if status.StateCounts[ralph.ControlPlaneTaskStateReady] < 1 {
		t.Fatalf("expected at least one ready task after pre-repair recover, ready=%d", status.StateCounts[ralph.ControlPlaneTaskStateReady])
	}
}

func TestRunControlPlaneBaselineCaptureAndShow(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	if err := runControlPlaneBaselineCommand(projectDir, []string{"capture", "--note", "baseline test"}); err != nil {
		t.Fatalf("baseline capture failed: %v", err)
	}
	baseline, ok, err := ralph.GetControlPlaneMetricsBaseline(projectDir)
	if err != nil {
		t.Fatalf("get baseline failed: %v", err)
	}
	if !ok {
		t.Fatalf("baseline should exist after capture")
	}
	if baseline.Note != "baseline test" {
		t.Fatalf("baseline note mismatch: got=%q", baseline.Note)
	}
	if err := runControlPlaneBaselineCommand(projectDir, []string{"show"}); err != nil {
		t.Fatalf("baseline show failed: %v", err)
	}
}

func TestRunControlPlaneMetricsWithBaseline(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	if err := runControlPlaneCommand("", projectDir, []string{"metrics", "--with-baseline=true", "--json=true"}); err != nil {
		t.Fatalf("metrics with baseline json failed: %v", err)
	}
	if err := runControlPlaneBaselineCommand(projectDir, []string{"capture", "--note", "metrics baseline"}); err != nil {
		t.Fatalf("baseline capture failed: %v", err)
	}
	if err := runControlPlaneCommand("", projectDir, []string{"metrics", "--with-baseline=true", "--json=false"}); err != nil {
		t.Fatalf("metrics with baseline text failed: %v", err)
	}
}

func TestRunControlPlaneRecoverForce(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	intentPath := filepath.Join(projectDir, "intent-recover-force.json")
	writeJSONForTest(t, intentPath, map[string]any{
		"id":               "recover-force-intent",
		"version":          1,
		"goal":             "exercise force recover",
		"success_criteria": []string{"recover"},
		"constraints":      []string{"single project"},
		"non_goals":        []string{"fleet"},
		"epics": []map[string]any{
			{
				"id":    "epic-1",
				"title": "Epic One",
				"tasks": []map[string]any{
					{
						"id":         "task-1",
						"title":      "Task One",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{},
						"acceptance": []string{"one"},
						"verify_cmd": "false",
						"risk_level": "high",
					},
				},
			},
		},
	})
	if _, err := ralph.ControlPlaneImportIntent(projectDir, intentPath); err != nil {
		t.Fatalf("import intent failed: %v", err)
	}
	if _, err := ralph.ControlPlanePlanIntent(projectDir, "recover-force-intent", ralph.ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ralph.ControlPlaneRun(projectDir, ralph.ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run #1 failed: %v", err)
	}
	if _, err := ralph.ControlPlaneRecover(projectDir, 1); err != nil {
		t.Fatalf("recover #1 failed: %v", err)
	}
	if _, err := ralph.ControlPlaneRun(projectDir, ralph.ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run #2 failed: %v", err)
	}
	if err := runControlPlaneCommand("", projectDir, []string{"recover", "--force=true"}); err != nil {
		t.Fatalf("cp recover --force failed: %v", err)
	}
}

func TestRunControlPlaneCutoverEvaluateWritesJSONReport(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "cutover-evaluate-test.json")
	if err := runControlPlaneCutoverCommand(projectDir, []string{"evaluate", "--output", reportPath}); err != nil {
		t.Fatalf("cutover evaluate output failed: %v", err)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read report failed: %v", err)
	}
	var eval ralph.ControlPlaneCutoverEvaluation
	if err := json.Unmarshal(data, &eval); err != nil {
		t.Fatalf("parse report failed: %v", err)
	}
	if strings.TrimSpace(eval.EvaluatedAtUTC) == "" {
		t.Fatalf("evaluated_at_utc should not be empty")
	}
}

func TestRunControlPlaneMigrateV1WritesJSONReport(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	paths, err := ralph.NewPaths(projectDir, projectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}
	if err := ralph.EnsureLayout(paths); err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	if _, _, err := ralph.CreateIssueWithOptions(paths, "developer", "legacy issue", ralph.IssueCreateOptions{Priority: 10}); err != nil {
		t.Fatalf("create issue failed: %v", err)
	}
	reportPath := filepath.Join(projectDir, "migrate-v1-report.json")
	if err := runControlPlaneCommand("", projectDir, []string{
		"migrate-v1",
		"--apply=true",
		"--verify=true",
		"--json=true",
		"--output", reportPath,
	}); err != nil {
		t.Fatalf("migrate-v1 command failed: %v", err)
	}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("read migrate report failed: %v", err)
	}
	report := controlPlaneMigrationReport{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse migrate report failed: %v", err)
	}
	if report.Migration.DryRun {
		t.Fatalf("expected applied migration in report")
	}
	if !report.VerifyEnabled || !report.VerifyExecuted {
		t.Fatalf("verify flags mismatch: enabled=%t executed=%t", report.VerifyEnabled, report.VerifyExecuted)
	}
	if report.Verify == nil {
		t.Fatalf("verify result should be present")
	}
}

func TestRunControlPlaneMigrateV1StrictVerifyFailureStillWritesReport(t *testing.T) {
	t.Parallel()

	projectDir := t.TempDir()
	paths, err := ralph.NewPaths(projectDir, projectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}
	if err := ralph.EnsureLayout(paths); err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	_, issueID, err := ralph.CreateIssueWithOptions(paths, "developer", "legacy issue", ralph.IssueCreateOptions{Priority: 10})
	if err != nil {
		t.Fatalf("create issue failed: %v", err)
	}
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	intentPath := filepath.Join(projectDir, "intent-conflict.json")
	writeJSONForTest(t, intentPath, map[string]any{
		"id":               "conflict-intent",
		"version":          1,
		"goal":             "conflict migrated task id",
		"success_criteria": []string{"planned"},
		"constraints":      []string{"single project"},
		"non_goals":        []string{"fleet"},
		"epics": []map[string]any{
			{
				"id":    "epic-1",
				"title": "Epic One",
				"tasks": []map[string]any{
					{
						"id":         issueID,
						"title":      "Conflicting task id",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{},
						"acceptance": []string{"ok"},
						"verify_cmd": "printf 'ok'",
						"risk_level": "low",
					},
				},
			},
		},
	})
	if _, err := ralph.ControlPlaneImportIntent(projectDir, intentPath); err != nil {
		t.Fatalf("import intent failed: %v", err)
	}
	if _, err := ralph.ControlPlanePlanIntent(projectDir, "conflict-intent", ralph.ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan intent failed: %v", err)
	}

	reportPath := filepath.Join(projectDir, "migrate-v1-strict-fail-report.json")
	err = runControlPlaneCommand("", projectDir, []string{
		"migrate-v1",
		"--apply=true",
		"--verify=true",
		"--strict-verify=true",
		"--output", reportPath,
	})
	if err == nil {
		t.Fatalf("expected strict verify failure")
	}
	if !strings.Contains(err.Error(), "migration parity check failed") {
		t.Fatalf("unexpected strict verify error: %v", err)
	}
	data, readErr := os.ReadFile(reportPath)
	if readErr != nil {
		t.Fatalf("strict verify failure should still write report: %v", readErr)
	}
	report := controlPlaneMigrationReport{}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("parse migrate report failed: %v", err)
	}
	if !report.VerifyEnabled || !report.VerifyExecuted || !report.VerifyStrict {
		t.Fatalf("strict verify flags mismatch: enabled=%t executed=%t strict=%t", report.VerifyEnabled, report.VerifyExecuted, report.VerifyStrict)
	}
	if report.Verify == nil {
		t.Fatalf("verify payload should be present")
	}
	if report.Verify.Matched {
		t.Fatalf("verify should be mismatched in strict failure scenario")
	}
}

func writeJSONForTest(t *testing.T, path string, payload any) {
	t.Helper()
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("marshal json %s: %v", path, err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write json %s: %v", path, err)
	}
}

func prepareCutoverFailureProject(t *testing.T, projectDir string) {
	t.Helper()
	if _, err := ralph.ControlPlaneInit(projectDir); err != nil {
		t.Fatalf("control plane init failed: %v", err)
	}
	intentPath := filepath.Join(projectDir, "intent-cutover-fail.json")
	writeJSONForTest(t, intentPath, map[string]any{
		"id":               "cutover-fail-intent",
		"version":          1,
		"goal":             "make blocked task",
		"success_criteria": []string{"task done"},
		"constraints":      []string{"single project"},
		"non_goals":        []string{"fleet"},
		"epics": []map[string]any{
			{
				"id":    "epic-1",
				"title": "Epic One",
				"tasks": []map[string]any{
					{
						"id":         "task-1",
						"title":      "Task One",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{},
						"acceptance": []string{"one"},
						"verify_cmd": "false",
						"risk_level": "high",
					},
				},
			},
		},
	})
	if _, err := ralph.ControlPlaneImportIntent(projectDir, intentPath); err != nil {
		t.Fatalf("import intent failed: %v", err)
	}
	if _, err := ralph.ControlPlanePlanIntent(projectDir, "cutover-fail-intent", ralph.ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ralph.ControlPlaneRun(projectDir, ralph.ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
}

func prepareCutoverCooldownFailureProject(t *testing.T, projectDir string) {
	t.Helper()
	prepareCutoverFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneRecover(projectDir, 1); err != nil {
		t.Fatalf("recover #1 failed: %v", err)
	}
	if _, err := ralph.ControlPlaneRun(projectDir, ralph.ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run #2 failed: %v", err)
	}
}

func prepareCutoverCircuitFailureProject(t *testing.T, projectDir string) {
	t.Helper()
	prepareCutoverCooldownFailureProject(t, projectDir)
	if _, err := ralph.ControlPlaneRecoverWithOptions(projectDir, ralph.ControlPlaneRecoverOptions{Limit: 1, Force: true}); err != nil {
		t.Fatalf("forced recover failed: %v", err)
	}
	if _, err := ralph.ControlPlaneRun(projectDir, ralph.ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run #3 failed: %v", err)
	}
}

func writeSoakReportForTest(t *testing.T, projectDir, fileName string, report ralph.ControlPlaneSoakReport) string {
	t.Helper()
	cpPaths, err := ralph.NewControlPlanePaths(projectDir)
	if err != nil {
		t.Fatalf("new cp paths failed: %v", err)
	}
	if err := os.MkdirAll(cpPaths.ReportsDir, 0o755); err != nil {
		t.Fatalf("mkdir reports dir failed: %v", err)
	}
	path := filepath.Join(cpPaths.ReportsDir, fileName)
	writeJSONForTest(t, path, report)
	return path
}
