package ralph

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func EnsureDefaultControlAssets(controlDir string) error {
	controlDir = strings.TrimSpace(controlDir)
	if controlDir == "" {
		return fmt.Errorf("control-dir is required")
	}

	hasPlugin, err := hasAnyPlugin(controlDir)
	if err != nil {
		return err
	}
	if hasPlugin {
		return nil
	}

	seeded := false
	for name, validateCmd := range builtinPluginValidateCmd {
		pluginFile := pluginFilePath(controlDir, name)
		wrote, err := writeFileIfMissing(pluginFile, []byte(builtinPluginEnv(name, validateCmd)), 0o644)
		if err != nil {
			return err
		}
		if wrote {
			seeded = true
		}
	}
	if !seeded {
		return nil
	}

	registry, err := GeneratePluginRegistry(controlDir)
	if err != nil {
		return err
	}
	return SavePluginRegistry(controlDir, registry)
}

func hasAnyPlugin(controlDir string) (bool, error) {
	pluginRoot := filepath.Join(controlDir, "plugins")
	entries, err := os.ReadDir(pluginRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read plugins dir: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pluginFile := filepath.Join(pluginRoot, entry.Name(), "plugin.env")
		if _, err := os.Stat(pluginFile); err == nil {
			return true, nil
		} else if !os.IsNotExist(err) {
			return false, fmt.Errorf("check plugin file %s: %w", pluginFile, err)
		}
	}
	return false, nil
}

func writeFileIfMissing(path string, data []byte, perm os.FileMode) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, fmt.Errorf("create dir for %s: %w", path, err)
	}
	if err := os.WriteFile(path, data, perm); err != nil {
		return false, fmt.Errorf("write %s: %w", path, err)
	}
	return true, nil
}

var builtinPluginValidateCmd = map[string]string{
	"universal-default": `echo "validation not configured; set RALPH_VALIDATE_CMD for this project"`,
	"go-default":        "make test && make test-sidecar && make lint",
	"node-default":      "npm test && npm run lint",
}

func builtinPluginEnv(pluginName, validateCmd string) string {
	lines := []string{
		"RALPH_PLUGIN_NAME=" + pluginName,
		"RALPH_CODEX_MODEL=auto",
		"RALPH_CODEX_HOME=.codex-home",
		"RALPH_CODEX_SANDBOX=workspace-write",
		"RALPH_CODEX_APPROVAL=never",
		"RALPH_CODEX_EXEC_TIMEOUT_SEC=900",
		"RALPH_CODEX_RETRY_MAX_ATTEMPTS=3",
		"RALPH_CODEX_RETRY_BACKOFF_SEC=10",
		"RALPH_REQUIRE_CODEX=true",
		"RALPH_ROLE_RULES_ENABLED=true",
		"RALPH_HANDOFF_REQUIRED=true",
		"RALPH_HANDOFF_SCHEMA=universal",
		"RALPH_IDLE_SLEEP_SEC=20",
		"RALPH_EXIT_ON_IDLE=false",
		"RALPH_NO_READY_MAX_LOOPS=0",
		"RALPH_BUSYWAIT_DETECT_LOOPS=3",
		"RALPH_BUSYWAIT_SELF_HEAL_ENABLED=true",
		"RALPH_BUSYWAIT_DOCTOR_REPAIR_ENABLED=true",
		"RALPH_BUSYWAIT_SELF_HEAL_COOLDOWN_SEC=120",
		"RALPH_BUSYWAIT_SELF_HEAL_MAX_ATTEMPTS=20",
		"RALPH_INPROGRESS_WATCHDOG_ENABLED=true",
		"RALPH_INPROGRESS_WATCHDOG_STALE_SEC=1800",
		"RALPH_INPROGRESS_WATCHDOG_SCAN_LOOPS=1",
		"RALPH_SUPERVISOR_ENABLED=true",
		"RALPH_SUPERVISOR_RESTART_DELAY_SEC=5",
		"RALPH_VALIDATE_ROLES=developer,qa",
		"RALPH_VALIDATE_CMD='" + validateCmd + "'",
	}
	return strings.Join(lines, "\n") + "\n"
}
