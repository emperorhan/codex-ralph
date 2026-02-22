package ralph

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

var profileEnvKeysForTest = []string{
	"RALPH_PLUGIN_NAME",
	"RALPH_CODEX_MODEL",
	"RALPH_CODEX_MODEL_MANAGER",
	"RALPH_CODEX_MODEL_PLANNER",
	"RALPH_CODEX_MODEL_DEVELOPER",
	"RALPH_CODEX_MODEL_QA",
	"RALPH_CODEX_HOME",
	"RALPH_CODEX_SANDBOX",
	"RALPH_CODEX_APPROVAL",
	"RALPH_CODEX_EXEC_TIMEOUT_SEC",
	"RALPH_CODEX_RETRY_MAX_ATTEMPTS",
	"RALPH_CODEX_RETRY_BACKOFF_SEC",
	"RALPH_REQUIRE_CODEX",
	"RALPH_ROLE_RULES_ENABLED",
	"RALPH_HANDOFF_REQUIRED",
	"RALPH_HANDOFF_SCHEMA",
	"RALPH_IDLE_SLEEP_SEC",
	"RALPH_EXIT_ON_IDLE",
	"RALPH_NO_READY_MAX_LOOPS",
	"RALPH_VALIDATE_ROLES",
	"RALPH_VALIDATE_CMD",
	"RALPH_BUSYWAIT_DETECT_LOOPS",
	"RALPH_BUSYWAIT_SELF_HEAL_ENABLED",
	"RALPH_BUSYWAIT_DOCTOR_REPAIR_ENABLED",
	"RALPH_BUSYWAIT_SELF_HEAL_COOLDOWN_SEC",
	"RALPH_BUSYWAIT_SELF_HEAL_MAX_ATTEMPTS",
	"RALPH_BUSYWAIT_SELF_HEAL_CMD",
	"RALPH_INPROGRESS_WATCHDOG_ENABLED",
	"RALPH_INPROGRESS_WATCHDOG_STALE_SEC",
	"RALPH_INPROGRESS_WATCHDOG_SCAN_LOOPS",
	"RALPH_SUPERVISOR_ENABLED",
	"RALPH_SUPERVISOR_RESTART_DELAY_SEC",
}

func newTestPaths(t *testing.T) Paths {
	t.Helper()

	root := t.TempDir()
	controlDir := filepath.Join(root, "control")
	projectDir := filepath.Join(root, "project")

	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("create control dir: %v", err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("create project dir: %v", err)
	}

	paths, err := NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths: %v", err)
	}
	if err := EnsureLayout(paths); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}
	return paths
}

func resetProfileEnv(t *testing.T) {
	t.Helper()
	for _, key := range profileEnvKeysForTest {
		t.Setenv(key, "")
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
}

func writeJSON(t *testing.T, path string, payload any) {
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
