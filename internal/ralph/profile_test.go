package ralph

import "testing"

func TestLoadProfilePrecedence(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	writeFile(t, paths.ProfileYAMLFile, `
codex_exec_timeout_sec: 111
codex_retry_max_attempts: 2
validate_cmd: "base-yaml-cmd"
validate_roles: [manager, qa]
supervisor_enabled: false
`)

	writeFile(t, paths.ProfileLocalYAMLFile, `
codex_exec_timeout_sec: 222
validate_cmd: "local-yaml-cmd"
`)

	writeFile(t, paths.ProfileFile, `
RALPH_CODEX_EXEC_TIMEOUT_SEC=333
RALPH_VALIDATE_CMD=profile-env-cmd
RALPH_SUPERVISOR_ENABLED=false
`)

	writeFile(t, paths.ProfileLocalFile, `
RALPH_CODEX_EXEC_TIMEOUT_SEC=444
RALPH_VALIDATE_CMD=local-env-cmd
RALPH_VALIDATE_ROLES=developer,qa
`)

	t.Setenv("RALPH_CODEX_EXEC_TIMEOUT_SEC", "555")
	t.Setenv("RALPH_VALIDATE_CMD", "process-env-cmd")
	t.Setenv("RALPH_SUPERVISOR_ENABLED", "true")

	profile, err := LoadProfile(paths)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}

	if profile.CodexExecTimeoutSec != 555 {
		t.Fatalf("codex_exec_timeout_sec mismatch: got=%d want=555", profile.CodexExecTimeoutSec)
	}
	if profile.ValidateCmd != "process-env-cmd" {
		t.Fatalf("validate_cmd mismatch: got=%q want=%q", profile.ValidateCmd, "process-env-cmd")
	}
	if !profile.SupervisorEnabled {
		t.Fatalf("supervisor_enabled mismatch: got=false want=true")
	}
	if RoleSetCSV(profile.ValidateRoles) != "developer,qa" {
		t.Fatalf("validate_roles mismatch: got=%q want=%q", RoleSetCSV(profile.ValidateRoles), "developer,qa")
	}
	if profile.CodexRetryMaxAttempts != 2 {
		t.Fatalf("codex_retry_max_attempts mismatch: got=%d want=2", profile.CodexRetryMaxAttempts)
	}
}

func TestLoadProfileNestedYAMLKeys(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	writeFile(t, paths.ProfileYAMLFile, `
codex:
  exec_timeout_sec: 321
validation:
  roles:
    - manager
    - qa
supervisor:
  enabled: false
RALPH_NO_READY_MAX_LOOPS: 7
`)

	profile, err := LoadProfile(paths)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}

	if profile.CodexExecTimeoutSec != 321 {
		t.Fatalf("codex.exec_timeout_sec mismatch: got=%d want=321", profile.CodexExecTimeoutSec)
	}
	if RoleSetCSV(profile.ValidateRoles) != "manager,qa" {
		t.Fatalf("validation.roles mismatch: got=%q want=%q", RoleSetCSV(profile.ValidateRoles), "manager,qa")
	}
	if profile.SupervisorEnabled {
		t.Fatalf("supervisor.enabled mismatch: got=true want=false")
	}
	if profile.NoReadyMaxLoops != 7 {
		t.Fatalf("RALPH_NO_READY_MAX_LOOPS mismatch: got=%d want=7", profile.NoReadyMaxLoops)
	}
}

func TestLoadProfileRoleModelOverrides(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	writeFile(t, paths.ProfileYAMLFile, `
codex:
  model: gpt-global
  model_developer: dev-yaml
  model_qa: qa-yaml
`)

	writeFile(t, paths.ProfileLocalYAMLFile, `
codex:
  model_planner: planner-local-yaml
`)

	writeFile(t, paths.ProfileFile, `
RALPH_CODEX_MODEL_MANAGER=manager-env
RALPH_CODEX_MODEL_DEVELOPER=dev-env
`)

	writeFile(t, paths.ProfileLocalFile, `
RALPH_CODEX_MODEL_DEVELOPER=dev-local-env
`)

	t.Setenv("RALPH_CODEX_MODEL_DEVELOPER", "dev-process")
	t.Setenv("RALPH_CODEX_MODEL_QA", "qa-process")

	profile, err := LoadProfile(paths)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}

	if profile.CodexModel != "gpt-global" {
		t.Fatalf("codex_model mismatch: got=%q want=%q", profile.CodexModel, "gpt-global")
	}
	if profile.CodexModelManager != "manager-env" {
		t.Fatalf("codex_model_manager mismatch: got=%q want=%q", profile.CodexModelManager, "manager-env")
	}
	if profile.CodexModelPlanner != "planner-local-yaml" {
		t.Fatalf("codex_model_planner mismatch: got=%q want=%q", profile.CodexModelPlanner, "planner-local-yaml")
	}
	if profile.CodexModelDeveloper != "dev-process" {
		t.Fatalf("codex_model_developer mismatch: got=%q want=%q", profile.CodexModelDeveloper, "dev-process")
	}
	if profile.CodexModelQA != "qa-process" {
		t.Fatalf("codex_model_qa mismatch: got=%q want=%q", profile.CodexModelQA, "qa-process")
	}

	if got := profile.CodexModelForRole("manager"); got != "manager-env" {
		t.Fatalf("model manager mismatch: got=%q want=%q", got, "manager-env")
	}
	if got := profile.CodexModelForRole("planner"); got != "planner-local-yaml" {
		t.Fatalf("model planner mismatch: got=%q want=%q", got, "planner-local-yaml")
	}
	if got := profile.CodexModelForRole("developer"); got != "dev-process" {
		t.Fatalf("model developer mismatch: got=%q want=%q", got, "dev-process")
	}
	if got := profile.CodexModelForRole("qa"); got != "qa-process" {
		t.Fatalf("model qa mismatch: got=%q want=%q", got, "qa-process")
	}
	if got := profile.CodexModelForRole("unknown-role"); got != "gpt-global" {
		t.Fatalf("model fallback mismatch: got=%q want=%q", got, "gpt-global")
	}
}

func TestProfileToYAMLMapRoleModels(t *testing.T) {
	profile := DefaultProfile()
	profile.CodexModelManager = "manager-model"
	profile.CodexModelDeveloper = "developer-model"

	m := ProfileToYAMLMap(profile)
	if m["codex_model_manager"] != "manager-model" {
		t.Fatalf("codex_model_manager mismatch: got=%q want=%q", m["codex_model_manager"], "manager-model")
	}
	if m["codex_model_developer"] != "developer-model" {
		t.Fatalf("codex_model_developer mismatch: got=%q want=%q", m["codex_model_developer"], "developer-model")
	}
	if _, ok := m["codex_model_planner"]; ok {
		t.Fatalf("codex_model_planner should be omitted when empty")
	}
	if _, ok := m["codex_model_qa"]; ok {
		t.Fatalf("codex_model_qa should be omitted when empty")
	}
}

func TestLoadProfileCodexHome(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	writeFile(t, paths.ProfileYAMLFile, `
codex_home: .codex-home-yaml
`)
	writeFile(t, paths.ProfileLocalFile, `
RALPH_CODEX_HOME=.codex-home-local-env
`)
	t.Setenv("RALPH_CODEX_HOME", ".codex-home-process")

	profile, err := LoadProfile(paths)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	if profile.CodexHome != ".codex-home-process" {
		t.Fatalf("codex_home mismatch: got=%q want=%q", profile.CodexHome, ".codex-home-process")
	}

	m := ProfileToYAMLMap(profile)
	if m["codex_home"] != ".codex-home-process" {
		t.Fatalf("codex_home yaml map mismatch: got=%q want=%q", m["codex_home"], ".codex-home-process")
	}
}

func TestCodexModelForRoleAutoBehavior(t *testing.T) {
	profile := DefaultProfile()
	if got := profile.CodexModelForRole("developer"); got != "" {
		t.Fatalf("default auto model should resolve to empty exec model: got=%q", got)
	}

	profile.CodexModel = "gpt-5.3-codex"
	if got := profile.CodexModelForRole("manager"); got != "gpt-5.3-codex" {
		t.Fatalf("global model mismatch: got=%q want=%q", got, "gpt-5.3-codex")
	}

	profile.CodexModelDeveloper = "auto"
	if got := profile.CodexModelForRole("developer"); got != "" {
		t.Fatalf("role auto override should resolve to empty exec model: got=%q", got)
	}

	profile.CodexModelDeveloper = "gpt-5.3-codex-spark"
	if got := profile.CodexModelForRole("developer"); got != "gpt-5.3-codex-spark" {
		t.Fatalf("role explicit model mismatch: got=%q want=%q", got, "gpt-5.3-codex-spark")
	}
}

func TestLoadProfileCodexRuntimeControls(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	writeFile(t, paths.ProfileYAMLFile, `
codex_require_exit_signal: false
codex_exit_signal: CUSTOM_DONE
codex_context_summary_enabled: false
codex_context_summary_lines: 4
codex_circuit_breaker_enabled: false
codex_circuit_breaker_failures: 5
codex_circuit_breaker_cooldown_sec: 90
`)

	profile, err := LoadProfile(paths)
	if err != nil {
		t.Fatalf("load profile: %v", err)
	}
	if profile.CodexRequireExitSignal {
		t.Fatalf("codex_require_exit_signal mismatch: got=true want=false")
	}
	if profile.CodexExitSignal != "CUSTOM_DONE" {
		t.Fatalf("codex_exit_signal mismatch: got=%q want=%q", profile.CodexExitSignal, "CUSTOM_DONE")
	}
	if profile.CodexContextSummaryEnabled {
		t.Fatalf("codex_context_summary_enabled mismatch: got=true want=false")
	}
	if profile.CodexContextSummaryLines != 4 {
		t.Fatalf("codex_context_summary_lines mismatch: got=%d want=4", profile.CodexContextSummaryLines)
	}
	if profile.CodexCircuitBreakerEnabled {
		t.Fatalf("codex_circuit_breaker_enabled mismatch: got=true want=false")
	}
	if profile.CodexCircuitBreakerFailures != 5 {
		t.Fatalf("codex_circuit_breaker_failures mismatch: got=%d want=5", profile.CodexCircuitBreakerFailures)
	}
	if profile.CodexCircuitBreakerCooldownSec != 90 {
		t.Fatalf("codex_circuit_breaker_cooldown_sec mismatch: got=%d want=90", profile.CodexCircuitBreakerCooldownSec)
	}
}
