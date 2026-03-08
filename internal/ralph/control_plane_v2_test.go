package ralph

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestControlPlaneInitCreatesLayout(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	res, err := ControlPlaneInit(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ControlPlaneInit failed: %v", err)
	}
	if res.Paths.DBFile == "" {
		t.Fatalf("expected db file path")
	}
	if _, err := os.Stat(res.Paths.DBFile); err != nil {
		t.Fatalf("db file should exist: %v", err)
	}
	if _, err := os.Stat(res.Paths.EventsFile); err != nil {
		t.Fatalf("events file should exist: %v", err)
	}
	header := make([]byte, 16)
	f, err := os.Open(res.Paths.DBFile)
	if err != nil {
		t.Fatalf("open db file failed: %v", err)
	}
	defer f.Close()
	n, err := f.Read(header)
	if err != nil {
		t.Fatalf("read db header failed: %v", err)
	}
	got := string(header[:n])
	if len(got) < len("SQLite format 3") || got[:len("SQLite format 3")] != "SQLite format 3" {
		t.Fatalf("controlplane.db is not sqlite format: %q", got)
	}
}

func TestControlPlaneLegacyJSONMigration(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	cpPaths, err := NewControlPlanePaths(paths.ProjectDir)
	if err != nil {
		t.Fatalf("NewControlPlanePaths failed: %v", err)
	}
	if err := os.MkdirAll(cpPaths.RootDir, 0o755); err != nil {
		t.Fatalf("mkdir cp root failed: %v", err)
	}

	legacy := newControlPlaneDB()
	legacy.Intents["legacy"] = IntentRecordV1{
		Spec: IntentSpecV1{
			ID:              "legacy",
			Version:         1,
			Goal:            "legacy goal",
			SuccessCriteria: []string{"legacy done"},
			Constraints:     []string{"none"},
			NonGoals:        []string{"none"},
			Epics: []IntentEpicV1{
				{ID: "epic-1", Title: "epic", Tasks: []IntentTaskSpecV1{}},
			},
		},
		SourcePath:    filepath.Join(paths.ProjectDir, "legacy.json"),
		ImportedAtUTC: "2026-03-06T00:00:00Z",
	}
	legacy.Tasks["legacy-task"] = TaskRecordV1{
		TaskNodeV1: TaskNodeV1{
			ID:         "legacy-task",
			EpicID:     "epic-1",
			Title:      "legacy task",
			Role:       "developer",
			Priority:   10,
			Deps:       []string{},
			Acceptance: []string{"done"},
			VerifyCmd:  "printf 'ok'",
			RiskLevel:  "low",
		},
		IntentID:     "legacy",
		State:        ControlPlaneTaskStateReady,
		Attempt:      0,
		UpdatedAtUTC: "2026-03-06T00:00:00Z",
	}
	raw, err := json.MarshalIndent(legacy, "", "  ")
	if err != nil {
		t.Fatalf("marshal legacy db failed: %v", err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(cpPaths.DBFile, raw, 0o644); err != nil {
		t.Fatalf("write legacy db failed: %v", err)
	}

	if _, err := EnsureControlPlaneLayout(paths.ProjectDir); err != nil {
		t.Fatalf("EnsureControlPlaneLayout failed: %v", err)
	}
	if _, err := os.Stat(cpPaths.DBFile + ".legacy-json.bak"); err != nil {
		t.Fatalf("legacy backup should exist: %v", err)
	}

	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load migrated db failed: %v", err)
	}
	if _, ok := db.Tasks["legacy-task"]; !ok {
		t.Fatalf("migrated task not found")
	}
}

func TestEnsureSQLiteSchemaBackfillsLedgerTablesWhenEmpty(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	intentPath := filepath.Join(paths.ProjectDir, "intent-ledger-backfill.json")
	writeJSON(t, intentPath, sampleIntentSpec("ledger-backfill-intent", "printf 'ok-one'", "printf 'ok-two'"))
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}

	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	db.Learnings = append(db.Learnings, LearningEventV1{
		TimeUTC:    "2026-03-06T00:00:00Z",
		TaskID:     "task-1",
		Category:   "diagnostic",
		Lesson:     "legacy learning row",
		ActionItem: "none",
	})
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		t.Fatalf("save db failed: %v", err)
	}
	if err := runSQLiteScript(cpPaths.DBFile, "DELETE FROM cp_event_ledger; DELETE FROM cp_learning_ledger;"); err != nil {
		t.Fatalf("clear ledger tables failed: %v", err)
	}

	if err := ensureSQLiteSchema(cpPaths); err != nil {
		t.Fatalf("ensure sqlite schema failed: %v", err)
	}
	eventRows, err := countSQLiteTableRows(cpPaths.DBFile, "cp_events")
	if err != nil {
		t.Fatalf("count cp_events failed: %v", err)
	}
	eventLedgerRows, err := countSQLiteTableRows(cpPaths.DBFile, "cp_event_ledger")
	if err != nil {
		t.Fatalf("count cp_event_ledger failed: %v", err)
	}
	if eventRows != eventLedgerRows {
		t.Fatalf("event ledger should be backfilled: cp_events=%d cp_event_ledger=%d", eventRows, eventLedgerRows)
	}
	learningRows, err := countSQLiteTableRows(cpPaths.DBFile, "cp_learnings")
	if err != nil {
		t.Fatalf("count cp_learnings failed: %v", err)
	}
	learningLedgerRows, err := countSQLiteTableRows(cpPaths.DBFile, "cp_learning_ledger")
	if err != nil {
		t.Fatalf("count cp_learning_ledger failed: %v", err)
	}
	if learningRows != learningLedgerRows {
		t.Fatalf("learning ledger should be backfilled: cp_learnings=%d cp_learning_ledger=%d", learningRows, learningLedgerRows)
	}
}

func TestControlPlaneImportIntentValidation(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-invalid.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "wallet",
		"version":          1,
		"goal":             "",
		"success_criteria": []string{"criterion"},
		"epics": []map[string]any{
			{"id": "epic-1", "title": "E1", "tasks": []map[string]any{}},
		},
	})

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestControlPlanePlanDetectsCycle(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-cycle.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "cycle-intent",
		"version":          1,
		"goal":             "test cycle",
		"success_criteria": []string{"done"},
		"constraints":      []string{"none"},
		"non_goals":        []string{"none"},
		"epics": []map[string]any{
			{
				"id":    "epic-a",
				"title": "A",
				"tasks": []map[string]any{
					{
						"id":         "task-a",
						"title":      "Task A",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{"task-b"},
						"acceptance": []string{"A done"},
						"verify_cmd": "printf 'a'",
					},
					{
						"id":         "task-b",
						"title":      "Task B",
						"role":       "qa",
						"priority":   20,
						"deps":       []string{"task-a"},
						"acceptance": []string{"B done"},
						"verify_cmd": "printf 'b'",
					},
				},
			},
		},
	})
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "cycle-intent", ControlPlanePlanOptions{}); err == nil {
		t.Fatalf("expected cycle detection error")
	}
}

func TestControlPlanePlanDetectsMissingDependency(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-missing-dep.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "missing-dep-intent",
		"version":          1,
		"goal":             "missing dependency should fail",
		"success_criteria": []string{"plan fails"},
		"constraints":      []string{"none"},
		"non_goals":        []string{"none"},
		"epics": []map[string]any{
			{
				"id":    "epic-a",
				"title": "A",
				"tasks": []map[string]any{
					{
						"id":         "task-a",
						"title":      "Task A",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{"task-missing"},
						"acceptance": []string{"A done"},
						"verify_cmd": "printf 'a'",
					},
				},
			},
		},
	})
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "missing-dep-intent", ControlPlanePlanOptions{}); err == nil {
		t.Fatalf("expected missing dependency error")
	}
}

func TestControlPlanePlanDeterministicSortSamePriority(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-order.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "order-intent",
		"version":          1,
		"goal":             "deterministic ordering",
		"success_criteria": []string{"stable ordering"},
		"constraints":      []string{"none"},
		"non_goals":        []string{"none"},
		"epics": []map[string]any{
			{
				"id":    "epic-a",
				"title": "A",
				"tasks": []map[string]any{
					{
						"id":         "task-b",
						"title":      "Task B",
						"role":       "developer",
						"priority":   10,
						"deps":       []string{},
						"acceptance": []string{"B done"},
						"verify_cmd": "printf 'b'",
					},
					{
						"id":         "task-a",
						"title":      "Task A",
						"role":       "qa",
						"priority":   10,
						"deps":       []string{},
						"acceptance": []string{"A done"},
						"verify_cmd": "printf 'a'",
					},
				},
			},
		},
	})
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "order-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	ready := collectReadyTaskIDs(db.Tasks)
	if len(ready) != 2 {
		t.Fatalf("ready tasks mismatch: got=%d want=2", len(ready))
	}
	if ready[0] != "task-a" || ready[1] != "task-b" {
		t.Fatalf("deterministic order mismatch: got=%v want=[task-a task-b]", ready)
	}
}

func TestControlPlaneRunCompletesLinearGraph(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-linear.json")
	writeJSON(t, intentPath, sampleIntentSpec("linear-intent", "printf 'ok-one'", "printf 'ok-two'"))

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "linear-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}

	runRes, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 2})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if runRes.Done != 2 {
		t.Fatalf("done count mismatch: got=%d want=2", runRes.Done)
	}

	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	if db.Tasks["task-1"].State != ControlPlaneTaskStateDone {
		t.Fatalf("task-1 should be done, got=%s", db.Tasks["task-1"].State)
	}
	if db.Tasks["task-2"].State != ControlPlaneTaskStateDone {
		t.Fatalf("task-2 should be done, got=%s", db.Tasks["task-2"].State)
	}
}

func TestControlPlaneRunRespectsMaxTasksWithMultipleWorkers(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-max-tasks-workers.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "max-tasks-workers-intent",
		"version":          1,
		"goal":             "respect max tasks with worker pool",
		"success_criteria": []string{"only max-tasks processed"},
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
					{
						"id":         "task-2",
						"title":      "Task Two",
						"role":       "developer",
						"priority":   20,
						"deps":       []string{},
						"acceptance": []string{"two"},
						"verify_cmd": "printf 'ok-two'",
						"risk_level": "low",
					},
					{
						"id":         "task-3",
						"title":      "Task Three",
						"role":       "developer",
						"priority":   30,
						"deps":       []string{},
						"acceptance": []string{"three"},
						"verify_cmd": "printf 'ok-three'",
						"risk_level": "low",
					},
				},
			},
		},
	})
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "max-tasks-workers-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	runRes, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 2, MaxTasks: 2})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if runRes.Processed != 2 {
		t.Fatalf("processed mismatch: got=%d want=2", runRes.Processed)
	}
	if runRes.RemainingReady != 1 {
		t.Fatalf("remaining_ready mismatch: got=%d want=1", runRes.RemainingReady)
	}
}

func TestControlPlaneFalseDonePrevention(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-no-evidence.json")
	writeJSON(t, intentPath, sampleIntentSpec("no-evidence-intent", "true", "printf 'ok-two'"))

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "no-evidence-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	metrics, err := ControlPlaneMetricsReport(paths.ProjectDir)
	if err != nil {
		t.Fatalf("metrics failed: %v", err)
	}
	if metrics.FalseDonePrevented < 1 {
		t.Fatalf("expected false_done_prevented >= 1, got=%d", metrics.FalseDonePrevented)
	}

	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	if db.Tasks["task-1"].State != ControlPlaneTaskStateBlocked {
		t.Fatalf("task-1 should be blocked due to missing evidence, got=%s", db.Tasks["task-1"].State)
	}
}

func TestControlPlaneVerifyPolicyTypedChecksPass(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-verify-policy-pass.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "verify-policy-pass-intent",
		"version":          1,
		"goal":             "run typed verification policy checks",
		"success_criteria": []string{"typed checks pass"},
		"constraints":      []string{"single project"},
		"non_goals":        []string{"fleet"},
		"epics": []map[string]any{
			{
				"id":    "epic-1",
				"title": "Epic One",
				"tasks": []map[string]any{
					{
						"id":       "task-1",
						"title":    "Task One",
						"role":     "developer",
						"priority": 10,
						"deps":     []string{},
						"acceptance": []string{
							"typed policy checks should run",
						},
						"verify_cmd": "lint: printf 'lint-pass'\nunit: printf 'unit-pass'\ncustom: printf 'custom-pass'",
						"risk_level": "medium",
					},
				},
			},
		},
	})

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "verify-policy-pass-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	task := db.Tasks["task-1"]
	if task.State != ControlPlaneTaskStateDone {
		t.Fatalf("task should be done, got=%s", task.State)
	}
	verification, ok := db.Verifications["task-1"]
	if !ok {
		t.Fatalf("verification result missing for task-1")
	}
	if !verification.Pass {
		t.Fatalf("verification should pass, failure_reason=%s", verification.FailureReason)
	}
	if !verificationHasCheckType(verification, "lint") {
		t.Fatalf("expected lint verification check")
	}
	if !verificationHasCheckType(verification, "unit") {
		t.Fatalf("expected unit verification check")
	}
	if !verificationHasCheckType(verification, "custom") {
		t.Fatalf("expected custom verification check")
	}
}

func TestControlPlaneVerifyPolicyFailureTaggedByKind(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-verify-policy-fail.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "verify-policy-fail-intent",
		"version":          1,
		"goal":             "unit verification failure should block task",
		"success_criteria": []string{"task blocked"},
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
						"acceptance": []string{"unit check fails"},
						"verify_cmd": "unit: false\ncustom: printf 'custom-pass'",
						"risk_level": "high",
					},
				},
			},
		},
	})

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "verify-policy-fail-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	runRes, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if runRes.Blocked != 1 {
		t.Fatalf("blocked mismatch: got=%d want=1", runRes.Blocked)
	}

	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	task := db.Tasks["task-1"]
	if task.State != ControlPlaneTaskStateBlocked {
		t.Fatalf("task should be blocked, got=%s", task.State)
	}
	if !strings.Contains(task.LastError, "verify_unit_failed") {
		t.Fatalf("expected verify_unit_failed in task last error, got=%s", task.LastError)
	}
	verification, ok := db.Verifications["task-1"]
	if !ok {
		t.Fatalf("verification result missing for task-1")
	}
	if verification.Pass {
		t.Fatalf("verification should fail")
	}
	if !strings.Contains(verification.FailureReason, "verify_unit_failed") {
		t.Fatalf("expected verify_unit_failed in failure reason, got=%s", verification.FailureReason)
	}
}

func TestControlPlaneRunExecuteCmdFailureBlocks(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-exec-fail.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "exec-fail-intent",
		"version":          1,
		"goal":             "run execute cmd before verify",
		"success_criteria": []string{"task should run"},
		"constraints":      []string{"single project"},
		"non_goals":        []string{"fleet"},
		"epics": []map[string]any{
			{
				"id":    "epic-1",
				"title": "Epic One",
				"tasks": []map[string]any{
					{
						"id":              "task-1",
						"title":           "Task One",
						"role":            "developer",
						"priority":        10,
						"deps":            []string{},
						"acceptance":      []string{"one"},
						"execute_cmd":     "exit 7",
						"verify_cmd":      "printf 'verify-one'",
						"codex_objective": "implement task one",
						"risk_level":      "medium",
					},
				},
			},
		},
	})

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "exec-fail-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	runRes, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if runRes.Blocked != 1 {
		t.Fatalf("blocked mismatch: got=%d want=1", runRes.Blocked)
	}
	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	if db.Tasks["task-1"].State != ControlPlaneTaskStateBlocked {
		t.Fatalf("task-1 should be blocked, got=%s", db.Tasks["task-1"].State)
	}
	if db.Tasks["task-1"].LastError == "" {
		t.Fatalf("task-1 last_error should not be empty")
	}
}

func TestControlPlaneRecoverMovesBlockedToReady(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-recover.json")
	writeJSON(t, intentPath, sampleIntentSpec("recover-intent", "true", "printf 'ok-two'"))

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "recover-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	rec, err := ControlPlaneRecover(paths.ProjectDir, 1)
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	if rec.Recovered != 1 {
		t.Fatalf("recovered mismatch: got=%d want=1", rec.Recovered)
	}

	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	if db.Tasks["task-1"].State != ControlPlaneTaskStateReady {
		t.Fatalf("task-1 should be ready after recover, got=%s", db.Tasks["task-1"].State)
	}
}

func TestControlPlaneRecoverRespectsCooldownPolicy(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-recover-cooldown.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "recover-cooldown-intent",
		"version":          1,
		"goal":             "exercise recover cooldown",
		"success_criteria": []string{"task recovers"},
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

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "recover-cooldown-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if _, err := ControlPlaneRecover(paths.ProjectDir, 1); err != nil {
		t.Fatalf("first recover failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("second run failed: %v", err)
	}

	rec, err := ControlPlaneRecover(paths.ProjectDir, 1)
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	if rec.Recovered != 0 {
		t.Fatalf("cooldown should block recover, got recovered=%d", rec.Recovered)
	}
	if rec.SkippedCooldown != 1 {
		t.Fatalf("expected cooldown skip count 1, got=%d", rec.SkippedCooldown)
	}

	forced, err := ControlPlaneRecoverWithOptions(paths.ProjectDir, ControlPlaneRecoverOptions{Limit: 1, Force: true})
	if err != nil {
		t.Fatalf("forced recover failed: %v", err)
	}
	if forced.Recovered != 1 {
		t.Fatalf("forced recover should succeed, got=%d", forced.Recovered)
	}
}

func TestControlPlaneRecoverCircuitOpenAndForceBypass(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-recover-circuit.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "recover-circuit-intent",
		"version":          1,
		"goal":             "exercise recover circuit",
		"success_criteria": []string{"task recovers"},
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

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "recover-circuit-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run #1 failed: %v", err)
	}
	if _, err := ControlPlaneRecover(paths.ProjectDir, 1); err != nil {
		t.Fatalf("recover #1 failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run #2 failed: %v", err)
	}
	if _, err := ControlPlaneRecoverWithOptions(paths.ProjectDir, ControlPlaneRecoverOptions{Limit: 1, Force: true}); err != nil {
		t.Fatalf("recover #2 forced failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run #3 failed: %v", err)
	}

	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	task := db.Tasks["task-1"]
	if task.BlockedCount < controlPlaneRecoverCircuitThreshold {
		t.Fatalf("blocked count should trigger circuit, got=%d", task.BlockedCount)
	}
	if strings.TrimSpace(task.CircuitOpenUntilUTC) == "" {
		t.Fatalf("circuit_open_until_utc should be set")
	}

	rec, err := ControlPlaneRecover(paths.ProjectDir, 1)
	if err != nil {
		t.Fatalf("recover under circuit failed: %v", err)
	}
	if rec.Recovered != 0 {
		t.Fatalf("circuit should prevent recover, got recovered=%d", rec.Recovered)
	}
	if rec.SkippedCircuitOpen != 1 {
		t.Fatalf("expected skipped_circuit_open=1, got=%d", rec.SkippedCircuitOpen)
	}

	forced, err := ControlPlaneRecoverWithOptions(paths.ProjectDir, ControlPlaneRecoverOptions{Limit: 1, Force: true})
	if err != nil {
		t.Fatalf("forced recover failed: %v", err)
	}
	if forced.Recovered != 1 {
		t.Fatalf("forced recover should bypass circuit, got=%d", forced.Recovered)
	}
}

func TestControlPlaneRecoverSkipsRetryBudget(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-recover-budget.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "recover-budget-intent",
		"version":          1,
		"goal":             "exercise recover retry budget",
		"success_criteria": []string{"task recovers"},
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

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "recover-budget-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	task := db.Tasks["task-1"]
	task.Attempt = controlPlaneRecoverMaxAttempts
	db.Tasks["task-1"] = task
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		t.Fatalf("save db failed: %v", err)
	}

	rec, err := ControlPlaneRecover(paths.ProjectDir, 1)
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
	if rec.Recovered != 0 {
		t.Fatalf("recover should skip budget exhausted task, got=%d", rec.Recovered)
	}
	if rec.SkippedRetryBudget != 1 {
		t.Fatalf("expected skipped_retry_budget=1, got=%d", rec.SkippedRetryBudget)
	}
}

func TestControlPlaneStateMachineRejectsInvalidTransition(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db := newControlPlaneDB()
	db.Tasks["task-1"] = TaskRecordV1{
		TaskNodeV1: TaskNodeV1{
			ID:         "task-1",
			EpicID:     "epic-1",
			Title:      "Task One",
			Role:       "developer",
			Priority:   10,
			Deps:       []string{},
			Acceptance: []string{"one"},
			VerifyCmd:  "printf 'ok'",
			RiskLevel:  "low",
		},
		IntentID:     "intent-1",
		State:        ControlPlaneTaskStateDraft,
		UpdatedAtUTC: "2026-03-06T00:00:00Z",
	}

	if err := setControlPlaneTaskState(&db, cpPaths, "task-1", ControlPlaneTaskStateDone, "task_state_changed", "invalid jump", false); err == nil {
		t.Fatalf("expected invalid transition error")
	}
	if err := setControlPlaneTaskState(&db, cpPaths, "task-1", ControlPlaneTaskStatePlanned, "task_state_changed", "valid transition", false); err != nil {
		t.Fatalf("expected valid transition, got error: %v", err)
	}
}

func TestRepairControlPlaneReclaimsExpiredLease(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	issuePath, issueID, err := CreateIssueWithOptions(paths, "developer", "legacy running issue", IssueCreateOptions{Priority: 10})
	if err != nil {
		t.Fatalf("create issue failed: %v", err)
	}
	inProgressPath := filepath.Join(paths.InProgressDir, issueID+".md")
	if err := os.Rename(issuePath, inProgressPath); err != nil {
		t.Fatalf("move issue to in-progress failed: %v", err)
	}
	if err := SetIssueStatus(inProgressPath, "in-progress"); err != nil {
		t.Fatalf("set issue status failed: %v", err)
	}

	if _, err := MigrateV1IssuesToControlPlane(paths.ProjectDir, false); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}
	dbBefore, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db before repair failed: %v", err)
	}
	if dbBefore.Tasks[issueID].State != ControlPlaneTaskStateRunning {
		t.Fatalf("expected migrated task to be running, got=%s", dbBefore.Tasks[issueID].State)
	}

	repairRes, err := RepairControlPlane(paths.ProjectDir)
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}
	if len(repairRes.Actions) == 0 {
		t.Fatalf("expected repair actions")
	}
	dbAfter, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db after repair failed: %v", err)
	}
	if dbAfter.Tasks[issueID].State != ControlPlaneTaskStateReady {
		t.Fatalf("expected repaired task to be ready, got=%s", dbAfter.Tasks[issueID].State)
	}
}

func TestRepairControlPlaneAutoRecoversBlocked(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-repair-auto-recover.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "repair-auto-recover-intent",
		"version":          1,
		"goal":             "repair should auto recover eligible blocked task",
		"success_criteria": []string{"task recovers"},
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
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "repair-auto-recover-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	repairRes, err := RepairControlPlaneWithOptions(paths.ProjectDir, ControlPlaneRepairOptions{
		AutoRecover: true,
	})
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}
	foundAutoRecover := false
	for _, action := range repairRes.Actions {
		if action.Name == "auto_recover_blocked" {
			foundAutoRecover = true
			break
		}
	}
	if !foundAutoRecover {
		t.Fatalf("repair actions should include auto_recover_blocked")
	}
	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	if db.Tasks["task-1"].State != ControlPlaneTaskStateReady {
		t.Fatalf("task should be ready after auto recover, got=%s", db.Tasks["task-1"].State)
	}
}

func TestRepairControlPlaneNormalizesExpiredRecoveryWindows(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	db.Tasks["task-1"] = TaskRecordV1{
		TaskNodeV1: TaskNodeV1{
			ID:         "task-1",
			EpicID:     "epic-1",
			Title:      "Task One",
			Role:       "developer",
			Priority:   10,
			Deps:       []string{},
			Acceptance: []string{"one"},
			VerifyCmd:  "printf 'ok'",
			RiskLevel:  "medium",
		},
		IntentID:            "intent-1",
		State:               ControlPlaneTaskStateBlocked,
		Attempt:             1,
		NextRecoverAtUTC:    "2020-01-01T00:00:00Z",
		CircuitOpenUntilUTC: "2020-01-01T00:00:00Z",
		UpdatedAtUTC:        "2026-03-06T00:00:00Z",
	}
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		t.Fatalf("save db failed: %v", err)
	}

	if _, err := RepairControlPlaneWithOptions(paths.ProjectDir, ControlPlaneRepairOptions{AutoRecover: false}); err != nil {
		t.Fatalf("repair failed: %v", err)
	}
	updated, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load updated db failed: %v", err)
	}
	task := updated.Tasks["task-1"]
	if strings.TrimSpace(task.NextRecoverAtUTC) != "" {
		t.Fatalf("next_recover_at_utc should be cleared, got=%q", task.NextRecoverAtUTC)
	}
	if strings.TrimSpace(task.CircuitOpenUntilUTC) != "" {
		t.Fatalf("circuit_open_until_utc should be cleared, got=%q", task.CircuitOpenUntilUTC)
	}
}

func TestRepairControlPlaneResetsRecoveryPolicies(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	db.Tasks["task-1"] = TaskRecordV1{
		TaskNodeV1: TaskNodeV1{
			ID:         "task-1",
			EpicID:     "epic-1",
			Title:      "Task One",
			Role:       "developer",
			Priority:   10,
			Deps:       []string{},
			Acceptance: []string{"one"},
			VerifyCmd:  "printf 'ok'",
			RiskLevel:  "medium",
		},
		IntentID:            "intent-1",
		State:               ControlPlaneTaskStateBlocked,
		Attempt:             controlPlaneRecoverMaxAttempts,
		BlockedCount:        7,
		NextRecoverAtUTC:    "2099-01-01T00:00:00Z",
		CircuitOpenUntilUTC: "2099-01-01T00:00:00Z",
		UpdatedAtUTC:        "2026-03-06T00:00:00Z",
	}
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		t.Fatalf("save db failed: %v", err)
	}

	repairRes, err := RepairControlPlaneWithOptions(paths.ProjectDir, ControlPlaneRepairOptions{
		AutoRecover:      false,
		ResetCircuit:     true,
		ResetRetryBudget: true,
	})
	if err != nil {
		t.Fatalf("repair failed: %v", err)
	}
	foundResetAction := false
	for _, action := range repairRes.Actions {
		if action.Name != "reset_recovery_policies" {
			continue
		}
		foundResetAction = true
		if !strings.Contains(action.Detail, "reset_circuit=true") || !strings.Contains(action.Detail, "reset_retry_budget=true") {
			t.Fatalf("unexpected reset action detail: %s", action.Detail)
		}
	}
	if !foundResetAction {
		t.Fatalf("expected reset_recovery_policies action")
	}

	updated, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load updated db failed: %v", err)
	}
	task := updated.Tasks["task-1"]
	if task.Attempt != 0 {
		t.Fatalf("attempt should be reset to 0, got=%d", task.Attempt)
	}
	if task.BlockedCount != 0 {
		t.Fatalf("blocked_count should be reset to 0, got=%d", task.BlockedCount)
	}
	if strings.TrimSpace(task.NextRecoverAtUTC) != "" {
		t.Fatalf("next_recover_at_utc should be cleared, got=%q", task.NextRecoverAtUTC)
	}
	if strings.TrimSpace(task.CircuitOpenUntilUTC) != "" {
		t.Fatalf("circuit_open_until_utc should be cleared, got=%q", task.CircuitOpenUntilUTC)
	}
}

func TestControlPlaneCutoverLifecycle(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	initial, err := ControlPlaneGetCutoverState(paths.ProjectDir)
	if err != nil {
		t.Fatalf("get cutover state failed: %v", err)
	}
	if initial.Mode != "v1" {
		t.Fatalf("initial mode mismatch: got=%s want=v1", initial.Mode)
	}

	updated, err := ControlPlaneSetCutoverMode(paths.ProjectDir, true, true, "canary rollout")
	if err != nil {
		t.Fatalf("set cutover mode failed: %v", err)
	}
	if updated.Mode != "v2" || !updated.Canary {
		t.Fatalf("cutover enable mismatch: mode=%s canary=%t", updated.Mode, updated.Canary)
	}

	eval, err := ControlPlaneEvaluateCutover(paths.ProjectDir)
	if err != nil {
		t.Fatalf("evaluate cutover failed: %v", err)
	}
	if eval.CurrentMode != "v2" {
		t.Fatalf("evaluation mode mismatch: got=%s", eval.CurrentMode)
	}

	rollback, err := ControlPlaneSetCutoverMode(paths.ProjectDir, false, false, "rollback")
	if err != nil {
		t.Fatalf("rollback cutover failed: %v", err)
	}
	if rollback.Mode != "v1" || rollback.Canary {
		t.Fatalf("rollback mismatch: mode=%s canary=%t", rollback.Mode, rollback.Canary)
	}
}

func TestControlPlaneBaselineCaptureAndLoad(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	baseline, err := CaptureControlPlaneMetricsBaseline(paths.ProjectDir, "baseline for test")
	if err != nil {
		t.Fatalf("capture baseline failed: %v", err)
	}
	if baseline.CapturedAtUTC == "" {
		t.Fatalf("captured_at_utc should not be empty")
	}
	loaded, ok, err := GetControlPlaneMetricsBaseline(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load baseline failed: %v", err)
	}
	if !ok {
		t.Fatalf("baseline should exist")
	}
	if loaded.CapturedAtUTC != baseline.CapturedAtUTC {
		t.Fatalf("baseline timestamp mismatch: got=%s want=%s", loaded.CapturedAtUTC, baseline.CapturedAtUTC)
	}
	if loaded.Note != "baseline for test" {
		t.Fatalf("baseline note mismatch: got=%q", loaded.Note)
	}
}

func TestControlPlaneMetricsSummaryReportWithBaseline(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	summary, err := ControlPlaneMetricsSummaryReport(paths.ProjectDir)
	if err != nil {
		t.Fatalf("metrics summary failed: %v", err)
	}
	if summary.BaselineAvailable {
		t.Fatalf("baseline should not exist yet")
	}
	if summary.Targets.BlockedRateMax != controlPlaneBlockedRateTarget {
		t.Fatalf("blocked target mismatch: got=%.4f want=%.4f", summary.Targets.BlockedRateMax, controlPlaneBlockedRateTarget)
	}
	if summary.Targets.RecoverySuccessRateMin != controlPlaneRecoverySuccessRateTarget {
		t.Fatalf("recovery target mismatch: got=%.4f want=%.4f", summary.Targets.RecoverySuccessRateMin, controlPlaneRecoverySuccessRateTarget)
	}
	if summary.Targets.MTTRSecondsMax != controlPlaneMTTRSecondsTarget {
		t.Fatalf("mttr target mismatch: got=%.2f want=%.2f", summary.Targets.MTTRSecondsMax, controlPlaneMTTRSecondsTarget)
	}

	cpPaths, err := NewControlPlanePaths(paths.ProjectDir)
	if err != nil {
		t.Fatalf("new cp paths failed: %v", err)
	}
	writeJSON(t, cpPaths.BaselineFile, ControlPlaneMetricsBaseline{
		CapturedAtUTC:       "2026-03-06T00:00:00Z",
		BlockedRate:         0.5,
		RecoverySuccessRate: 0.3,
		MeanTimeToRecovery:  900,
		Note:                "baseline summary test",
	})
	summaryWithBaseline, err := ControlPlaneMetricsSummaryReport(paths.ProjectDir)
	if err != nil {
		t.Fatalf("metrics summary with baseline failed: %v", err)
	}
	if !summaryWithBaseline.BaselineAvailable {
		t.Fatalf("baseline should be available")
	}
	if summaryWithBaseline.BaselineCapturedAtUTC != "2026-03-06T00:00:00Z" {
		t.Fatalf("baseline captured timestamp mismatch: got=%s", summaryWithBaseline.BaselineCapturedAtUTC)
	}
	if summaryWithBaseline.BlockedRateImprovementRatio <= 0 {
		t.Fatalf("expected positive blocked-rate improvement, got=%.4f", summaryWithBaseline.BlockedRateImprovementRatio)
	}
}

func TestControlPlaneEvaluateCutoverFailsWhenBaselineImprovementMissing(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	cpPaths, err := NewControlPlanePaths(paths.ProjectDir)
	if err != nil {
		t.Fatalf("new cp paths failed: %v", err)
	}
	writeJSON(t, cpPaths.BaselineFile, ControlPlaneMetricsBaseline{
		CapturedAtUTC:       "2026-03-06T00:00:00Z",
		BlockedRate:         0.20,
		RecoverySuccessRate: 1.0,
		MeanTimeToRecovery:  10,
		Note:                "test baseline",
	})
	intentPath := filepath.Join(paths.ProjectDir, "intent-cutover-baseline-fail.json")
	writeJSON(t, intentPath, map[string]any{
		"id":               "cutover-baseline-fail",
		"version":          1,
		"goal":             "force blocked state",
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
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import intent failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "cutover-baseline-fail", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	eval, err := ControlPlaneEvaluateCutover(paths.ProjectDir)
	if err != nil {
		t.Fatalf("evaluate cutover failed: %v", err)
	}
	if eval.Ready {
		t.Fatalf("evaluation should fail when baseline improvement is missing")
	}
	if !eval.BaselineAvailable {
		t.Fatalf("baseline should be available")
	}
	found := false
	for _, failure := range eval.FailureSummaries {
		if strings.Contains(failure, "blocked_rate_improvement insufficient") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected blocked rate improvement failure, got=%v", eval.FailureSummaries)
	}
	foundBaselineCategory := false
	for _, failure := range eval.Failures {
		if failure.Category == ControlPlaneCutoverFailureCategoryBaseline && strings.Contains(failure.Code, "blocked_rate_improvement") {
			foundBaselineCategory = true
			break
		}
	}
	if !foundBaselineCategory {
		t.Fatalf("expected baseline failure category, got=%+v", eval.Failures)
	}
}

func TestControlPlaneEvaluateCutoverRequireBaselineFailsWhenMissing(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	eval, err := ControlPlaneEvaluateCutoverWithOptions(paths.ProjectDir, ControlPlaneCutoverEvaluateOptions{
		RequireBaseline: true,
	})
	if err != nil {
		t.Fatalf("evaluate cutover failed: %v", err)
	}
	if eval.Ready {
		t.Fatalf("evaluation should fail when baseline is required but missing")
	}
	foundMissingBaseline := false
	for _, failure := range eval.Failures {
		if failure.Category == ControlPlaneCutoverFailureCategoryBaseline && failure.Code == "baseline.missing" {
			foundMissingBaseline = true
			break
		}
	}
	if !foundMissingBaseline {
		t.Fatalf("expected baseline.missing failure, got=%+v", eval.Failures)
	}
}

func TestControlPlaneEvaluateCutoverRequireSoakFailsWhenMissing(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	eval, err := ControlPlaneEvaluateCutoverWithOptions(paths.ProjectDir, ControlPlaneCutoverEvaluateOptions{
		RequireSoakPass: true,
	})
	if err != nil {
		t.Fatalf("evaluate cutover failed: %v", err)
	}
	if eval.Ready {
		t.Fatalf("evaluation should fail when soak pass is required but no soak report exists")
	}
	foundMissingSoak := false
	for _, failure := range eval.Failures {
		if failure.Category == ControlPlaneCutoverFailureCategorySoak && failure.Code == "soak.missing" {
			foundMissingSoak = true
			break
		}
	}
	if !foundMissingSoak {
		t.Fatalf("expected soak.missing failure, got=%+v", eval.Failures)
	}
}

func TestControlPlaneEvaluateCutoverRequireSoakFailsWhenSoakFailed(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	cpPaths, err := NewControlPlanePaths(paths.ProjectDir)
	if err != nil {
		t.Fatalf("new cp paths failed: %v", err)
	}
	soakPath := filepath.Join(cpPaths.ReportsDir, "soak-20990101T000000Z.json")
	writeJSON(t, soakPath, ControlPlaneSoakReport{
		StartedAtUTC:    "2026-03-06T00:00:00Z",
		FinishedAtUTC:   "2026-03-06T00:01:00Z",
		DurationSec:     60,
		IntervalSec:     30,
		Strict:          true,
		FailureDetected: true,
		FailureDetail:   "doctor check failed during soak",
	})

	eval, err := ControlPlaneEvaluateCutoverWithOptions(paths.ProjectDir, ControlPlaneCutoverEvaluateOptions{
		RequireSoakPass: true,
	})
	if err != nil {
		t.Fatalf("evaluate cutover failed: %v", err)
	}
	if eval.Ready {
		t.Fatalf("evaluation should fail when soak report indicates failure")
	}
	foundSoakFailed := false
	for _, failure := range eval.Failures {
		if failure.Category == ControlPlaneCutoverFailureCategorySoak && failure.Code == "soak.failed" {
			foundSoakFailed = true
			break
		}
	}
	if !foundSoakFailed {
		t.Fatalf("expected soak.failed failure, got=%+v", eval.Failures)
	}
}

func TestControlPlaneEvaluateCutoverSoakMaxAgeFailsWhenStale(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	cpPaths, err := NewControlPlanePaths(paths.ProjectDir)
	if err != nil {
		t.Fatalf("new cp paths failed: %v", err)
	}
	soakPath := filepath.Join(cpPaths.ReportsDir, "soak-20990101T000000Z.json")
	writeJSON(t, soakPath, ControlPlaneSoakReport{
		StartedAtUTC:    "2020-01-01T00:00:00Z",
		FinishedAtUTC:   "2020-01-01T00:05:00Z",
		DurationSec:     300,
		IntervalSec:     30,
		Strict:          true,
		FailureDetected: false,
	})

	eval, err := ControlPlaneEvaluateCutoverWithOptions(paths.ProjectDir, ControlPlaneCutoverEvaluateOptions{
		RequireSoakPass: true,
		MaxSoakAgeSec:   300,
	})
	if err != nil {
		t.Fatalf("evaluate cutover failed: %v", err)
	}
	if eval.Ready {
		t.Fatalf("evaluation should fail when soak report is stale")
	}
	foundSoakStale := false
	for _, failure := range eval.Failures {
		if failure.Category == ControlPlaneCutoverFailureCategorySoak && failure.Code == "soak.stale" {
			foundSoakStale = true
			break
		}
	}
	if !foundSoakStale {
		t.Fatalf("expected soak.stale failure, got=%+v", eval.Failures)
	}
}

func TestControlPlaneRunSoakCapturesSnapshots(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	report, err := ControlPlaneRunSoak(paths.ProjectDir, 0, 1, false)
	if err != nil {
		t.Fatalf("run soak failed: %v", err)
	}
	if len(report.Snapshots) < 1 {
		t.Fatalf("expected at least one snapshot")
	}
	if report.StartedAtUTC == "" || report.FinishedAtUTC == "" {
		t.Fatalf("soak timestamps should not be empty: %+v", report)
	}
}

func TestControlPlaneFaultInjectLeaseExpireAndRepair(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-fault-inject.json")
	writeJSON(t, intentPath, sampleIntentSpec("fault-inject-intent", "printf 'ok-one'", "printf 'ok-two'"))

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "fault-inject-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}

	injected, err := ControlPlaneFaultInject(paths.ProjectDir, "task-1", "lease-expire")
	if err != nil {
		t.Fatalf("fault inject failed: %v", err)
	}
	if !injected.Applied {
		t.Fatalf("fault inject should be applied")
	}
	if injected.State != ControlPlaneTaskStateRunning {
		t.Fatalf("fault inject state mismatch: got=%s want=running", injected.State)
	}

	if _, err := RepairControlPlane(paths.ProjectDir); err != nil {
		t.Fatalf("repair failed: %v", err)
	}
	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	if db.Tasks["task-1"].State != ControlPlaneTaskStateReady {
		t.Fatalf("expected repaired task to be ready, got=%s", db.Tasks["task-1"].State)
	}
}

func TestControlPlaneFaultInjectPermissionDeniedBlocksTask(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-fault-permission-denied.json")
	writeJSON(t, intentPath, sampleIntentSpec("fault-permission-denied-intent", "printf 'ok-one'", "printf 'ok-two'"))

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "fault-permission-denied-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	injected, err := ControlPlaneFaultInject(paths.ProjectDir, "task-1", "permission-denied")
	if err != nil {
		t.Fatalf("fault inject failed: %v", err)
	}
	if !injected.Applied {
		t.Fatalf("fault inject should be applied")
	}
	if !strings.Contains(injected.Detail, "permission denied fixture") {
		t.Fatalf("fault inject detail should mention permission denied fixture: %s", injected.Detail)
	}
	runRes, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{
		MaxWorkers: 1,
		MaxTasks:   1,
	})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if runRes.Blocked == 0 {
		t.Fatalf("expected blocked task after permission-denied execution fault")
	}

	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	if db.Tasks["task-1"].State != ControlPlaneTaskStateBlocked {
		t.Fatalf("task should be blocked after permission-denied fault, got=%s", db.Tasks["task-1"].State)
	}
}

func TestControlPlaneFaultInjectExecuteFailBlocksTask(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-fault-execute-fail.json")
	writeJSON(t, intentPath, sampleIntentSpec("fault-execute-fail-intent", "printf 'ok-one'", "printf 'ok-two'"))

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "fault-execute-fail-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneFaultInject(paths.ProjectDir, "task-1", "execute-fail"); err != nil {
		t.Fatalf("fault inject failed: %v", err)
	}
	runRes, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{
		MaxWorkers: 1,
		MaxTasks:   1,
	})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if runRes.Blocked == 0 {
		t.Fatalf("expected blocked task after execute-fail fault")
	}
	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	task := db.Tasks["task-1"]
	if task.State != ControlPlaneTaskStateBlocked {
		t.Fatalf("task should be blocked after execute-fail fault, got=%s", task.State)
	}
	if !strings.Contains(task.LastError, "execute_cmd_failed") {
		t.Fatalf("expected execute_cmd_failed error, got=%s", task.LastError)
	}
}

func TestControlPlaneFaultInjectVerifyFailBlocksTask(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-fault-verify-fail.json")
	writeJSON(t, intentPath, sampleIntentSpec("fault-verify-fail-intent", "printf 'ok-one'", "printf 'ok-two'"))

	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "fault-verify-fail-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneFaultInject(paths.ProjectDir, "task-1", "verify-fail"); err != nil {
		t.Fatalf("fault inject failed: %v", err)
	}
	runRes, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{
		MaxWorkers: 1,
		MaxTasks:   1,
	})
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if runRes.Blocked == 0 {
		t.Fatalf("expected blocked task after verify-fail fault")
	}
	db, err := ControlPlaneLoadDBForTest(paths.ProjectDir)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	task := db.Tasks["task-1"]
	if task.State != ControlPlaneTaskStateBlocked {
		t.Fatalf("task should be blocked after verify-fail fault, got=%s", task.State)
	}
	if !strings.Contains(task.LastError, "verify_custom_failed") {
		t.Fatalf("expected verify_custom_failed error, got=%s", task.LastError)
	}
}

func TestControlPlaneClaimTaskPreventsDuplicateRunningClaim(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-claim-duplicate.json")
	writeJSON(t, intentPath, sampleIntentSpec("claim-duplicate-intent", "printf 'ok-one'", "printf 'ok-two'"))
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "claim-duplicate-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}

	if _, err := claimControlPlaneTask(&db, cpPaths, "task-1", 1, 120); err != nil {
		t.Fatalf("first claim should succeed: %v", err)
	}
	if _, err := claimControlPlaneTask(&db, cpPaths, "task-1", 2, 120); err == nil {
		t.Fatalf("second claim should fail while task is running")
	}
	task := db.Tasks["task-1"]
	if task.LeaseOwner != "worker-1" {
		t.Fatalf("lease owner should remain worker-1, got=%s", task.LeaseOwner)
	}
	if task.Attempt != 1 {
		t.Fatalf("task attempt should remain 1 after duplicate claim, got=%d", task.Attempt)
	}
}

func TestControlPlaneReclaimExpiredLeaseAllowsReclaim(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-reclaim-expired-lease.json")
	writeJSON(t, intentPath, sampleIntentSpec("reclaim-expired-lease-intent", "printf 'ok-one'", "printf 'ok-two'"))
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "reclaim-expired-lease-intent", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	now := time.Now().UTC()
	if _, err := claimControlPlaneTask(&db, cpPaths, "task-1", 1, 120); err != nil {
		t.Fatalf("claim should succeed: %v", err)
	}
	task := db.Tasks["task-1"]
	task.LeaseUntilUTC = now.Add(-2 * time.Minute).Format(time.RFC3339)
	task.HeartbeatUTC = now.Add(-3 * time.Minute).Format(time.RFC3339)
	db.Tasks["task-1"] = task

	reclaimed := reclaimExpiredLeases(&db, cpPaths)
	if reclaimed != 1 {
		t.Fatalf("expected one reclaimed task, got=%d", reclaimed)
	}
	task = db.Tasks["task-1"]
	if task.State != ControlPlaneTaskStateReady {
		t.Fatalf("reclaimed task should return to ready, got=%s", task.State)
	}
	if strings.TrimSpace(task.LeaseOwner) != "" {
		t.Fatalf("lease owner should be cleared on reclaim, got=%s", task.LeaseOwner)
	}
	if _, err := claimControlPlaneTask(&db, cpPaths, "task-1", 2, 120); err != nil {
		t.Fatalf("reclaim after lease expiry should allow new claim: %v", err)
	}
}

func TestRunControlPlaneDoctorDetectsDoneVerificationInconsistency(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-doctor-done-verification.json")
	writeJSON(t, intentPath, sampleIntentSpec("doctor-done-verification", "printf 'ok-one'", "printf 'ok-two'"))
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "doctor-done-verification", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	delete(db.Verifications, "task-1")
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		t.Fatalf("save db failed: %v", err)
	}

	report, err := RunControlPlaneDoctor(paths.ProjectDir)
	if err != nil {
		t.Fatalf("doctor failed: %v", err)
	}
	check, ok := findDoctorCheck(report, "done_verification_consistency")
	if !ok {
		t.Fatalf("done_verification_consistency check not found")
	}
	if check.Status != "fail" {
		t.Fatalf("done_verification_consistency should fail, got=%s detail=%s", check.Status, check.Detail)
	}
}

func TestRunControlPlaneDoctorDetectsTaskJSONMismatch(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-doctor-task-json.json")
	writeJSON(t, intentPath, sampleIntentSpec("doctor-task-json", "printf 'ok-one'", "printf 'ok-two'"))
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "doctor-task-json", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	cpPaths, err := NewControlPlanePaths(paths.ProjectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}
	if err := os.Remove(filepath.Join(cpPaths.TasksDir, "task-1.json")); err != nil {
		t.Fatalf("remove task json failed: %v", err)
	}

	report, err := RunControlPlaneDoctor(paths.ProjectDir)
	if err != nil {
		t.Fatalf("doctor failed: %v", err)
	}
	check, ok := findDoctorCheck(report, "task_json_consistency")
	if !ok {
		t.Fatalf("task_json_consistency check not found")
	}
	if check.Status != "fail" {
		t.Fatalf("task_json_consistency should fail, got=%s detail=%s", check.Status, check.Detail)
	}
}

func TestRunControlPlaneDoctorDetectsEventJournalMismatch(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-doctor-event-journal.json")
	writeJSON(t, intentPath, sampleIntentSpec("doctor-event-journal", "printf 'ok-one'", "printf 'ok-two'"))
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	cpPaths, err := NewControlPlanePaths(paths.ProjectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}
	if err := os.WriteFile(cpPaths.EventsFile, []byte{}, 0o644); err != nil {
		t.Fatalf("truncate events file failed: %v", err)
	}

	report, err := RunControlPlaneDoctor(paths.ProjectDir)
	if err != nil {
		t.Fatalf("doctor failed: %v", err)
	}
	check, ok := findDoctorCheck(report, "event_journal_consistency")
	if !ok {
		t.Fatalf("event_journal_consistency check not found")
	}
	if check.Status != "fail" {
		t.Fatalf("event_journal_consistency should fail, got=%s detail=%s", check.Status, check.Detail)
	}
}

func TestRunControlPlaneDoctorDetectsEventLedgerMismatch(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-doctor-event-ledger.json")
	writeJSON(t, intentPath, sampleIntentSpec("doctor-event-ledger", "printf 'ok-one'", "printf 'ok-two'"))
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	if err := runSQLiteScript(
		cpPaths.DBFile,
		"INSERT INTO cp_event_ledger(recorded_at_utc,event_json) VALUES ('2026-03-06T00:00:00Z','{\"type\":\"task_state_changed\",\"task_id\":\"extra\"}');",
	); err != nil {
		t.Fatalf("inject event ledger mismatch failed: %v", err)
	}

	report, err := RunControlPlaneDoctor(paths.ProjectDir)
	if err != nil {
		t.Fatalf("doctor failed: %v", err)
	}
	check, ok := findDoctorCheck(report, "event_ledger_consistency")
	if !ok {
		t.Fatalf("event_ledger_consistency check not found")
	}
	if check.Status != "fail" {
		t.Fatalf("event_ledger_consistency should fail, got=%s detail=%s", check.Status, check.Detail)
	}
}

func TestRunControlPlaneDoctorDetectsLearningLedgerMismatch(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	db.Learnings = append(db.Learnings, LearningEventV1{
		TimeUTC:    "2026-03-06T00:00:00Z",
		TaskID:     "task-1",
		Category:   "diagnostic",
		Lesson:     "test-injected",
		ActionItem: "none",
	})
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		t.Fatalf("save db failed: %v", err)
	}
	if err := runSQLiteScript(
		cpPaths.DBFile,
		"INSERT INTO cp_learning_ledger(recorded_at_utc,event_json) VALUES ('2026-03-06T00:00:00Z','{\"time_utc\":\"2026-03-06T00:00:00Z\",\"task_id\":\"task-1\",\"category\":\"diagnostic\",\"lesson\":\"extra-one\",\"action_item\":\"none\"}');"+
			"INSERT INTO cp_learning_ledger(recorded_at_utc,event_json) VALUES ('2026-03-06T00:00:01Z','{\"time_utc\":\"2026-03-06T00:00:01Z\",\"task_id\":\"task-1\",\"category\":\"diagnostic\",\"lesson\":\"extra-two\",\"action_item\":\"none\"}');",
	); err != nil {
		t.Fatalf("inject learning ledger mismatch failed: %v", err)
	}

	report, err := RunControlPlaneDoctor(paths.ProjectDir)
	if err != nil {
		t.Fatalf("doctor failed: %v", err)
	}
	check, ok := findDoctorCheck(report, "learning_ledger_consistency")
	if !ok {
		t.Fatalf("learning_ledger_consistency check not found")
	}
	if check.Status != "fail" {
		t.Fatalf("learning_ledger_consistency should fail, got=%s detail=%s", check.Status, check.Detail)
	}
}

func TestRunControlPlaneDoctorDetectsLearningJournalMismatch(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, err := ControlPlaneInit(paths.ProjectDir); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	db.Learnings = append(db.Learnings, LearningEventV1{
		TimeUTC:    "2026-03-06T00:00:00Z",
		TaskID:     "task-1",
		Category:   "diagnostic",
		Lesson:     "journal mismatch injected",
		ActionItem: "none",
	})
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		t.Fatalf("save db failed: %v", err)
	}

	report, err := RunControlPlaneDoctor(paths.ProjectDir)
	if err != nil {
		t.Fatalf("doctor failed: %v", err)
	}
	check, ok := findDoctorCheck(report, "learning_journal_consistency")
	if !ok {
		t.Fatalf("learning_journal_consistency check not found")
	}
	if check.Status != "fail" {
		t.Fatalf("learning_journal_consistency should fail, got=%s detail=%s", check.Status, check.Detail)
	}
}

func TestRunControlPlaneDoctorDetectsEventTransitionInconsistency(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	intentPath := filepath.Join(paths.ProjectDir, "intent-doctor-event-transition.json")
	writeJSON(t, intentPath, sampleIntentSpec("doctor-event-transition", "printf 'ok-one'", "printf 'ok-two'"))
	if _, err := ControlPlaneImportIntent(paths.ProjectDir, intentPath); err != nil {
		t.Fatalf("import failed: %v", err)
	}
	if _, err := ControlPlanePlanIntent(paths.ProjectDir, "doctor-event-transition", ControlPlanePlanOptions{}); err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if _, err := ControlPlaneRun(paths.ProjectDir, ControlPlaneRunOptions{MaxWorkers: 1, MaxTasks: 1}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	cpPaths, err := EnsureControlPlaneLayout(paths.ProjectDir)
	if err != nil {
		t.Fatalf("ensure layout failed: %v", err)
	}
	db, err := loadControlPlaneDB(cpPaths)
	if err != nil {
		t.Fatalf("load db failed: %v", err)
	}
	db.Events = append(db.Events, ControlPlaneEventV1{
		TimeUTC:   "2026-03-06T00:00:00Z",
		Type:      "task_state_changed",
		IntentID:  "doctor-event-transition",
		TaskID:    "task-1",
		FromState: ControlPlaneTaskStateDone,
		ToState:   ControlPlaneTaskStateDraft,
		Attempt:   1,
		Detail:    "invalid transition injected by test",
	})
	if err := saveControlPlaneDB(cpPaths, db); err != nil {
		t.Fatalf("save db failed: %v", err)
	}

	report, err := RunControlPlaneDoctor(paths.ProjectDir)
	if err != nil {
		t.Fatalf("doctor failed: %v", err)
	}
	check, ok := findDoctorCheck(report, "event_transition_consistency")
	if !ok {
		t.Fatalf("event_transition_consistency check not found")
	}
	if check.Status != "fail" {
		t.Fatalf("event_transition_consistency should fail, got=%s detail=%s", check.Status, check.Detail)
	}
}

func TestNewCutoverFailureFromDoctorCheckClassifiesIntegrityChecksAsCritical(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"done_verification_consistency",
		"task_json_consistency",
		"event_journal_consistency",
		"learning_journal_consistency",
		"event_transition_consistency",
		"event_ledger_consistency",
		"learning_ledger_consistency",
	} {
		failure := newCutoverFailureFromDoctorCheck(ControlPlaneDoctorCheck{
			Name:   name,
			Status: "fail",
			Detail: "simulated",
		})
		if failure.Category != ControlPlaneCutoverFailureCategoryDataIntegrity {
			t.Fatalf("expected data_integrity category for %s, got=%s", name, failure.Category)
		}
		if !failure.Critical {
			t.Fatalf("expected critical=true for %s", name)
		}
	}
}

func TestControlPlaneMigrateV1DryRun(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	issuePath, _, err := CreateIssueWithOptions(paths, "developer", "legacy issue", IssueCreateOptions{Priority: 10})
	if err != nil {
		t.Fatalf("create issue failed: %v", err)
	}
	if issuePath == "" {
		t.Fatalf("issue path should not be empty")
	}

	res, err := MigrateV1IssuesToControlPlane(paths.ProjectDir, true)
	if err != nil {
		t.Fatalf("migrate dry-run failed: %v", err)
	}
	if !res.DryRun {
		t.Fatalf("dry-run should be true")
	}
	if res.Scanned < 1 {
		t.Fatalf("expected scanned >= 1, got=%d", res.Scanned)
	}
}

func TestVerifyV1ToV2MigrationCountsMatched(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if _, _, err := CreateIssueWithOptions(paths, "developer", "ready issue", IssueCreateOptions{Priority: 10}); err != nil {
		t.Fatalf("create issue failed: %v", err)
	}
	doneIssuePath, doneID, err := CreateIssueWithOptions(paths, "developer", "done issue", IssueCreateOptions{Priority: 20})
	if err != nil {
		t.Fatalf("create done issue failed: %v", err)
	}
	if err := SetIssueStatus(doneIssuePath, "done"); err != nil {
		t.Fatalf("set done issue status failed: %v", err)
	}
	if err := os.Rename(doneIssuePath, filepath.Join(paths.DoneDir, doneID+".md")); err != nil {
		t.Fatalf("move done issue failed: %v", err)
	}

	if _, err := MigrateV1IssuesToControlPlane(paths.ProjectDir, false); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}
	verify, err := VerifyV1ToV2Migration(paths.ProjectDir)
	if err != nil {
		t.Fatalf("verify migration failed: %v", err)
	}
	if !verify.Matched {
		t.Fatalf("migration counts should match, detail=%s", verify.Detail)
	}
	if verify.V1Counts["ready"] != verify.V2Counts["ready"] {
		t.Fatalf("ready count mismatch")
	}
	if verify.V1Counts["done"] != verify.V2Counts["done"] {
		t.Fatalf("done count mismatch")
	}
}

func sampleIntentSpec(id, verify1, verify2 string) map[string]any {
	return map[string]any{
		"id":               id,
		"version":          1,
		"goal":             "build a deterministic task graph",
		"success_criteria": []string{"tasks are done"},
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
						"verify_cmd": verify1,
						"risk_level": "medium",
					},
					{
						"id":         "task-2",
						"title":      "Task Two",
						"role":       "qa",
						"priority":   20,
						"deps":       []string{"task-1"},
						"acceptance": []string{"two"},
						"verify_cmd": verify2,
						"risk_level": "low",
					},
				},
			},
		},
	}
}

func findDoctorCheck(report ControlPlaneDoctorReport, name string) (ControlPlaneDoctorCheck, bool) {
	for _, check := range report.Checks {
		if check.Name == name {
			return check, true
		}
	}
	return ControlPlaneDoctorCheck{}, false
}

func verificationHasCheckType(result VerificationResultV1, checkType string) bool {
	target := strings.TrimSpace(checkType)
	if target == "" {
		return false
	}
	for _, check := range result.Checks {
		if strings.TrimSpace(check.Type) == target {
			return true
		}
	}
	return false
}
