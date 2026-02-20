package ralph

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Profile struct {
	PluginName                  string
	CodexModel                  string
	CodexSandbox                string
	CodexApproval               string
	CodexExecTimeoutSec         int
	CodexRetryMaxAttempts       int
	CodexRetryBackoffSec        int
	RequireCodex                bool
	RoleRulesEnabled            bool
	HandoffRequired             bool
	HandoffSchema               string
	IdleSleepSec                int
	ExitOnIdle                  bool
	NoReadyMaxLoops             int
	ValidateRoles               map[string]struct{}
	ValidateCmd                 string
	BusyWaitDetectLoops         int
	BusyWaitSelfHealEnabled     bool
	BusyWaitDoctorRepairEnabled bool
	BusyWaitSelfHealCooldownSec int
	BusyWaitSelfHealMaxAttempts int
	BusyWaitSelfHealCmd         string
	InProgressWatchdogEnabled   bool
	InProgressWatchdogStaleSec  int
	InProgressWatchdogScanLoops int
	SupervisorEnabled           bool
	SupervisorRestartDelaySec   int
}

func DefaultProfile() Profile {
	return Profile{
		PluginName:            "universal-default",
		CodexModel:            "gpt-5.3-codex",
		CodexSandbox:          "workspace-write",
		CodexApproval:         "never",
		CodexExecTimeoutSec:   900,
		CodexRetryMaxAttempts: 3,
		CodexRetryBackoffSec:  10,
		RequireCodex:          true,
		RoleRulesEnabled:      true,
		HandoffRequired:       true,
		HandoffSchema:         "universal",
		IdleSleepSec:          20,
		ExitOnIdle:            false,
		NoReadyMaxLoops:       0,
		ValidateRoles: map[string]struct{}{
			"developer": {},
			"qa":        {},
		},
		ValidateCmd:                 "echo \"skip validation\"",
		BusyWaitDetectLoops:         3,
		BusyWaitSelfHealEnabled:     true,
		BusyWaitDoctorRepairEnabled: true,
		BusyWaitSelfHealCooldownSec: 120,
		BusyWaitSelfHealMaxAttempts: 20,
		BusyWaitSelfHealCmd:         "",
		InProgressWatchdogEnabled:   true,
		InProgressWatchdogStaleSec:  1800,
		InProgressWatchdogScanLoops: 1,
		SupervisorEnabled:           true,
		SupervisorRestartDelaySec:   5,
	}
}

func LoadProfile(paths Paths) (Profile, error) {
	p := DefaultProfile()

	if err := loadProfileYAMLFile(paths.ProfileYAMLFile, "profile.yaml", &p); err != nil {
		return p, err
	}
	if err := loadProfileYAMLFile(paths.ProfileLocalYAMLFile, "profile.local.yaml", &p); err != nil {
		return p, err
	}
	if err := loadProfileEnvFile(paths.ProfileFile, "profile.env", &p); err != nil {
		return p, err
	}
	if err := loadProfileEnvFile(paths.ProfileLocalFile, "profile.local.env", &p); err != nil {
		return p, err
	}
	applyProcessEnvOverrides(&p)

	if p.IdleSleepSec <= 0 {
		p.IdleSleepSec = 20
	}
	if p.CodexModel == "" {
		p.CodexModel = "gpt-5.3-codex"
	}
	if p.CodexSandbox == "" {
		p.CodexSandbox = "workspace-write"
	}
	if p.CodexApproval == "" {
		p.CodexApproval = "never"
	}
	if p.CodexExecTimeoutSec < 0 {
		p.CodexExecTimeoutSec = 0
	}
	if p.CodexRetryMaxAttempts <= 0 {
		p.CodexRetryMaxAttempts = 1
	}
	if p.CodexRetryBackoffSec < 0 {
		p.CodexRetryBackoffSec = 0
	}
	p.HandoffSchema = normalizeHandoffSchema(p.HandoffSchema)
	if p.ValidateCmd == "" {
		p.ValidateCmd = "echo \"skip validation\""
	}
	if p.BusyWaitDetectLoops < 0 {
		p.BusyWaitDetectLoops = 0
	}
	if p.BusyWaitSelfHealCooldownSec < 0 {
		p.BusyWaitSelfHealCooldownSec = 0
	}
	if p.BusyWaitSelfHealMaxAttempts < 0 {
		p.BusyWaitSelfHealMaxAttempts = 0
	}
	if p.InProgressWatchdogStaleSec < 0 {
		p.InProgressWatchdogStaleSec = 0
	}
	if p.InProgressWatchdogScanLoops <= 0 {
		p.InProgressWatchdogScanLoops = 1
	}
	if p.SupervisorRestartDelaySec < 0 {
		p.SupervisorRestartDelaySec = 0
	}

	return p, nil
}

func loadProfileYAMLFile(path, displayName string, p *Profile) error {
	if _, err := os.Stat(path); err == nil {
		m, readErr := ReadYAMLFlatMap(path)
		if readErr != nil {
			return fmt.Errorf("read %s: %w", displayName, readErr)
		}
		applyProfileYAMLMap(p, m)
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat %s: %w", displayName, err)
	}
	return nil
}

func loadProfileEnvFile(path, displayName string, p *Profile) error {
	if _, err := os.Stat(path); err == nil {
		m, readErr := ReadEnvFile(path)
		if readErr != nil {
			return fmt.Errorf("read %s: %w", displayName, readErr)
		}
		applyProfileMap(p, m)
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat %s: %w", displayName, err)
	}
	return nil
}

func applyProcessEnvOverrides(p *Profile) {
	envMap := map[string]string{}
	for _, raw := range os.Environ() {
		i := strings.IndexByte(raw, '=')
		if i <= 0 {
			continue
		}
		key := raw[:i]
		if !strings.HasPrefix(key, "RALPH_") {
			continue
		}
		envMap[key] = raw[i+1:]
	}
	applyProfileMap(p, envMap)
}

func applyProfileYAMLMap(p *Profile, m map[string]string) {
	envMap := map[string]string{}
	for key, value := range m {
		envKey := profileConfigEnvKey(key)
		if envKey == "" {
			continue
		}
		envMap[envKey] = strings.TrimSpace(value)
	}
	applyProfileMap(p, envMap)
}

func profileConfigEnvKey(rawKey string) string {
	key := normalizeConfigKey(rawKey)
	if key == "" {
		return ""
	}
	if strings.HasPrefix(key, "ralph_") {
		return strings.ToUpper(strings.ReplaceAll(key, "-", "_"))
	}

	switch key {
	case "plugin", "plugin_name":
		return "RALPH_PLUGIN_NAME"
	case "codex_model", "codex.model":
		return "RALPH_CODEX_MODEL"
	case "codex_sandbox", "codex.sandbox":
		return "RALPH_CODEX_SANDBOX"
	case "codex_approval", "codex.approval":
		return "RALPH_CODEX_APPROVAL"
	case "codex_exec_timeout_sec", "codex.exec_timeout_sec":
		return "RALPH_CODEX_EXEC_TIMEOUT_SEC"
	case "codex_retry_max_attempts", "codex.retry_max_attempts":
		return "RALPH_CODEX_RETRY_MAX_ATTEMPTS"
	case "codex_retry_backoff_sec", "codex.retry_backoff_sec":
		return "RALPH_CODEX_RETRY_BACKOFF_SEC"
	case "require_codex":
		return "RALPH_REQUIRE_CODEX"
	case "role_rules_enabled":
		return "RALPH_ROLE_RULES_ENABLED"
	case "handoff_required", "handoff.required":
		return "RALPH_HANDOFF_REQUIRED"
	case "handoff_schema", "handoff.schema":
		return "RALPH_HANDOFF_SCHEMA"
	case "idle_sleep_sec":
		return "RALPH_IDLE_SLEEP_SEC"
	case "exit_on_idle":
		return "RALPH_EXIT_ON_IDLE"
	case "no_ready_max_loops":
		return "RALPH_NO_READY_MAX_LOOPS"
	case "validate_roles", "validation.roles":
		return "RALPH_VALIDATE_ROLES"
	case "validate_cmd", "validation.cmd":
		return "RALPH_VALIDATE_CMD"
	case "busywait_detect_loops", "busywait.detect_loops":
		return "RALPH_BUSYWAIT_DETECT_LOOPS"
	case "busywait_self_heal_enabled", "busywait.self_heal_enabled":
		return "RALPH_BUSYWAIT_SELF_HEAL_ENABLED"
	case "busywait_doctor_repair_enabled", "busywait.doctor_repair_enabled":
		return "RALPH_BUSYWAIT_DOCTOR_REPAIR_ENABLED"
	case "busywait_self_heal_cooldown_sec", "busywait.self_heal_cooldown_sec":
		return "RALPH_BUSYWAIT_SELF_HEAL_COOLDOWN_SEC"
	case "busywait_self_heal_max_attempts", "busywait.self_heal_max_attempts":
		return "RALPH_BUSYWAIT_SELF_HEAL_MAX_ATTEMPTS"
	case "busywait_self_heal_cmd", "busywait.self_heal_cmd":
		return "RALPH_BUSYWAIT_SELF_HEAL_CMD"
	case "inprogress_watchdog_enabled", "inprogress.watchdog_enabled":
		return "RALPH_INPROGRESS_WATCHDOG_ENABLED"
	case "inprogress_watchdog_stale_sec", "inprogress.watchdog_stale_sec":
		return "RALPH_INPROGRESS_WATCHDOG_STALE_SEC"
	case "inprogress_watchdog_scan_loops", "inprogress.watchdog_scan_loops":
		return "RALPH_INPROGRESS_WATCHDOG_SCAN_LOOPS"
	case "supervisor_enabled", "supervisor.enabled":
		return "RALPH_SUPERVISOR_ENABLED"
	case "supervisor_restart_delay_sec", "supervisor.restart_delay_sec":
		return "RALPH_SUPERVISOR_RESTART_DELAY_SEC"
	default:
		return ""
	}
}

func normalizeConfigKey(raw string) string {
	key := strings.TrimSpace(strings.ToLower(raw))
	key = strings.ReplaceAll(key, "-", "_")
	key = strings.ReplaceAll(key, " ", "_")
	return key
}

func ProfileToYAMLMap(p Profile) map[string]string {
	return map[string]string{
		"plugin_name":                     p.PluginName,
		"codex_model":                     p.CodexModel,
		"codex_sandbox":                   p.CodexSandbox,
		"codex_approval":                  p.CodexApproval,
		"codex_exec_timeout_sec":          strconv.Itoa(p.CodexExecTimeoutSec),
		"codex_retry_max_attempts":        strconv.Itoa(p.CodexRetryMaxAttempts),
		"codex_retry_backoff_sec":         strconv.Itoa(p.CodexRetryBackoffSec),
		"require_codex":                   boolToEnv(p.RequireCodex),
		"role_rules_enabled":              boolToEnv(p.RoleRulesEnabled),
		"handoff_required":                boolToEnv(p.HandoffRequired),
		"handoff_schema":                  normalizeHandoffSchema(p.HandoffSchema),
		"idle_sleep_sec":                  strconv.Itoa(p.IdleSleepSec),
		"exit_on_idle":                    boolToEnv(p.ExitOnIdle),
		"no_ready_max_loops":              strconv.Itoa(p.NoReadyMaxLoops),
		"validate_roles":                  RoleSetCSV(p.ValidateRoles),
		"validate_cmd":                    p.ValidateCmd,
		"busywait_detect_loops":           strconv.Itoa(p.BusyWaitDetectLoops),
		"busywait_self_heal_enabled":      boolToEnv(p.BusyWaitSelfHealEnabled),
		"busywait_doctor_repair_enabled":  boolToEnv(p.BusyWaitDoctorRepairEnabled),
		"busywait_self_heal_cooldown_sec": strconv.Itoa(p.BusyWaitSelfHealCooldownSec),
		"busywait_self_heal_max_attempts": strconv.Itoa(p.BusyWaitSelfHealMaxAttempts),
		"busywait_self_heal_cmd":          p.BusyWaitSelfHealCmd,
		"inprogress_watchdog_enabled":     boolToEnv(p.InProgressWatchdogEnabled),
		"inprogress_watchdog_stale_sec":   strconv.Itoa(p.InProgressWatchdogStaleSec),
		"inprogress_watchdog_scan_loops":  strconv.Itoa(p.InProgressWatchdogScanLoops),
		"supervisor_enabled":              boolToEnv(p.SupervisorEnabled),
		"supervisor_restart_delay_sec":    strconv.Itoa(p.SupervisorRestartDelaySec),
	}
}

func applyProfileMap(p *Profile, m map[string]string) {
	if v := m["RALPH_PLUGIN_NAME"]; v != "" {
		p.PluginName = v
	}
	if v := m["RALPH_CODEX_MODEL"]; v != "" {
		p.CodexModel = v
	}
	if v := m["RALPH_CODEX_SANDBOX"]; v != "" {
		p.CodexSandbox = v
	}
	if v := m["RALPH_CODEX_APPROVAL"]; v != "" {
		p.CodexApproval = v
	}
	if v, ok := parseInt(m["RALPH_CODEX_EXEC_TIMEOUT_SEC"]); ok {
		p.CodexExecTimeoutSec = v
	}
	if v, ok := parseInt(m["RALPH_CODEX_RETRY_MAX_ATTEMPTS"]); ok {
		p.CodexRetryMaxAttempts = v
	}
	if v, ok := parseInt(m["RALPH_CODEX_RETRY_BACKOFF_SEC"]); ok {
		p.CodexRetryBackoffSec = v
	}
	if v, ok := parseBool(m["RALPH_REQUIRE_CODEX"]); ok {
		p.RequireCodex = v
	}
	if v, ok := parseBool(m["RALPH_ROLE_RULES_ENABLED"]); ok {
		p.RoleRulesEnabled = v
	}
	if v, ok := parseBool(m["RALPH_HANDOFF_REQUIRED"]); ok {
		p.HandoffRequired = v
	}
	if v := m["RALPH_HANDOFF_SCHEMA"]; v != "" {
		p.HandoffSchema = v
	}
	if v, ok := parseInt(m["RALPH_IDLE_SLEEP_SEC"]); ok {
		p.IdleSleepSec = v
	}
	if v, ok := parseBool(m["RALPH_EXIT_ON_IDLE"]); ok {
		p.ExitOnIdle = v
	}
	if v, ok := parseInt(m["RALPH_NO_READY_MAX_LOOPS"]); ok {
		p.NoReadyMaxLoops = v
	}
	if v := m["RALPH_VALIDATE_CMD"]; v != "" {
		p.ValidateCmd = v
	}
	if v := m["RALPH_VALIDATE_ROLES"]; v != "" {
		p.ValidateRoles = parseRoleSet(v)
	}
	if v, ok := parseInt(m["RALPH_BUSYWAIT_DETECT_LOOPS"]); ok {
		p.BusyWaitDetectLoops = v
	}
	if v, ok := parseBool(m["RALPH_BUSYWAIT_SELF_HEAL_ENABLED"]); ok {
		p.BusyWaitSelfHealEnabled = v
	}
	if v, ok := parseBool(m["RALPH_BUSYWAIT_DOCTOR_REPAIR_ENABLED"]); ok {
		p.BusyWaitDoctorRepairEnabled = v
	}
	if v, ok := parseInt(m["RALPH_BUSYWAIT_SELF_HEAL_COOLDOWN_SEC"]); ok {
		p.BusyWaitSelfHealCooldownSec = v
	}
	if v, ok := parseInt(m["RALPH_BUSYWAIT_SELF_HEAL_MAX_ATTEMPTS"]); ok {
		p.BusyWaitSelfHealMaxAttempts = v
	}
	if v := m["RALPH_BUSYWAIT_SELF_HEAL_CMD"]; v != "" {
		p.BusyWaitSelfHealCmd = v
	}
	if v, ok := parseBool(m["RALPH_INPROGRESS_WATCHDOG_ENABLED"]); ok {
		p.InProgressWatchdogEnabled = v
	}
	if v, ok := parseInt(m["RALPH_INPROGRESS_WATCHDOG_STALE_SEC"]); ok {
		p.InProgressWatchdogStaleSec = v
	}
	if v, ok := parseInt(m["RALPH_INPROGRESS_WATCHDOG_SCAN_LOOPS"]); ok {
		p.InProgressWatchdogScanLoops = v
	}
	if v, ok := parseBool(m["RALPH_SUPERVISOR_ENABLED"]); ok {
		p.SupervisorEnabled = v
	}
	if v, ok := parseInt(m["RALPH_SUPERVISOR_RESTART_DELAY_SEC"]); ok {
		p.SupervisorRestartDelaySec = v
	}
}

func parseRoleSet(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		role := strings.TrimSpace(part)
		if role == "" {
			continue
		}
		out[role] = struct{}{}
	}
	if len(out) == 0 {
		out["developer"] = struct{}{}
		out["qa"] = struct{}{}
	}
	return out
}

func parseBool(raw string) (bool, bool) {
	if raw == "" {
		return false, false
	}
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "1", "true", "yes", "y", "on":
		return true, true
	case "0", "false", "no", "n", "off":
		return false, true
	}
	v, err := strconv.ParseBool(s)
	if err != nil {
		return false, false
	}
	return v, true
}

func parseInt(raw string) (int, bool) {
	if raw == "" {
		return 0, false
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, false
	}
	return v, true
}

func normalizeHandoffSchema(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "strict":
		return "strict"
	case "universal", "":
		return "universal"
	default:
		return "universal"
	}
}
