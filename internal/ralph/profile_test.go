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
