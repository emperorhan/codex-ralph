package ralph

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateAndVerifyPluginRegistry(t *testing.T) {
	paths := newTestPaths(t)
	writeTestPlugin(t, paths.ControlDir, "universal-default", "RALPH_CODEX_MODEL=gpt-5.3-codex\n")
	writeTestPlugin(t, paths.ControlDir, "go-default", "RALPH_VALIDATE_CMD=go test ./...\n")

	reg, err := GeneratePluginRegistry(paths.ControlDir)
	if err != nil {
		t.Fatalf("generate registry: %v", err)
	}
	if len(reg.Plugins) != 2 {
		t.Fatalf("registry plugin count mismatch: got=%d want=2", len(reg.Plugins))
	}
	if err := SavePluginRegistry(paths.ControlDir, reg); err != nil {
		t.Fatalf("save registry: %v", err)
	}

	checks, err := VerifyPluginRegistry(paths.ControlDir)
	if err != nil {
		t.Fatalf("verify registry: %v", err)
	}
	if got := RegistryFailureCount(checks); got != 0 {
		t.Fatalf("registry should have no failures: got=%d", got)
	}
	if err := VerifyPluginWithRegistry(paths.ControlDir, "universal-default"); err != nil {
		t.Fatalf("verify plugin with registry: %v", err)
	}
}

func TestVerifyPluginRegistryDetectsTamper(t *testing.T) {
	paths := newTestPaths(t)
	writeTestPlugin(t, paths.ControlDir, "universal-default", "RALPH_CODEX_MODEL=gpt-5.3-codex\n")

	reg, err := GeneratePluginRegistry(paths.ControlDir)
	if err != nil {
		t.Fatalf("generate registry: %v", err)
	}
	if err := SavePluginRegistry(paths.ControlDir, reg); err != nil {
		t.Fatalf("save registry: %v", err)
	}

	pluginPath := filepath.Join(paths.ControlDir, "plugins", "universal-default", "plugin.env")
	writeFile(t, pluginPath, "RALPH_CODEX_MODEL=gpt-5.3-codex\nRALPH_VALIDATE_CMD=echo tampered\n")

	err = VerifyPluginWithRegistry(paths.ControlDir, "universal-default")
	if err == nil {
		t.Fatalf("expected checksum mismatch error")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}

	checks, err := VerifyPluginRegistry(paths.ControlDir)
	if err != nil {
		t.Fatalf("verify registry: %v", err)
	}
	if got := RegistryFailureCount(checks); got == 0 {
		t.Fatalf("expected registry verification failure after tamper")
	}
}

func TestApplyStabilityDefaults(t *testing.T) {
	paths := newTestPaths(t)
	resetProfileEnv(t)

	if err := ApplyStabilityDefaults(paths); err != nil {
		t.Fatalf("apply stability defaults: %v", err)
	}
	values, err := ReadYAMLFlatMap(paths.ProfileLocalYAMLFile)
	if err != nil {
		t.Fatalf("read profile.local.yaml: %v", err)
	}
	assertMapValue(t, values, "codex_exec_timeout_sec", "900")
	assertMapValue(t, values, "codex_retry_max_attempts", "3")
	assertMapValue(t, values, "codex_retry_backoff_sec", "10")
	assertMapValue(t, values, "inprogress_watchdog_enabled", "true")
	assertMapValue(t, values, "supervisor_enabled", "true")
}

func writeTestPlugin(t *testing.T, controlDir, pluginName, body string) {
	t.Helper()
	pluginDir := filepath.Join(controlDir, "plugins", pluginName)
	if err := os.MkdirAll(pluginDir, 0o755); err != nil {
		t.Fatalf("create plugin dir: %v", err)
	}
	writeFile(t, filepath.Join(pluginDir, "plugin.env"), body)
}

func assertMapValue(t *testing.T, m map[string]string, key, want string) {
	t.Helper()
	got := strings.TrimSpace(m[key])
	if got != want {
		t.Fatalf("map[%q] mismatch: got=%q want=%q", key, got, want)
	}
}
