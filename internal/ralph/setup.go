package ralph

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const validationSkipCommand = `echo "validation skipped by setup"`

type SetupMode string

const (
	SetupModePluginDefault SetupMode = "plugin-default"
	SetupModeSkip          SetupMode = "skip"
	SetupModeCustom        SetupMode = "custom"
)

type SetupSelections struct {
	Plugin           string
	RoleRulesEnabled bool
	HandoffRequired  bool
	HandoffSchema    string
	DoctorAutoRepair bool
	ValidationMode   SetupMode
	ValidateCmd      string
}

func DefaultSetupSelections(preferredPlugin string) SetupSelections {
	return SetupSelections{
		Plugin:           strings.TrimSpace(preferredPlugin),
		RoleRulesEnabled: true,
		HandoffRequired:  true,
		HandoffSchema:    "universal",
		DoctorAutoRepair: true,
		ValidationMode:   SetupModePluginDefault,
	}
}

func RunSetupWizard(paths Paths, executablePath, preferredPlugin string, in io.Reader, out io.Writer) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}

	profile, err := LoadProfile(paths)
	if err != nil {
		return err
	}
	plugins, err := ListPlugins(paths.ControlDir)
	if err != nil {
		return err
	}
	if len(plugins) == 0 {
		return fmt.Errorf("no plugins found in %s/plugins", paths.ControlDir)
	}

	reader := bufio.NewReader(in)
	fmt.Fprintln(out, "## Ralph Setup Wizard")
	fmt.Fprintf(out, "- project_dir: %s\n", paths.ProjectDir)
	fmt.Fprintf(out, "- control_dir: %s\n\n", paths.ControlDir)
	fmt.Fprintf(out, "- codex: %s\n\n", codexConnectionSummary())

	pluginDefault := pickDefaultPlugin(plugins, profile.PluginName)
	if preferred := strings.TrimSpace(preferredPlugin); preferred != "" && containsString(plugins, preferred) {
		pluginDefault = preferred
	}
	plugin, err := promptChoice(reader, out, "Select plugin", plugins, pluginDefault)
	if err != nil {
		return err
	}

	roleRulesEnabled, err := promptBool(reader, out, "Enable role rule files?", profile.RoleRulesEnabled)
	if err != nil {
		return err
	}
	handoffRequired, err := promptBool(reader, out, "Require handoff JSON for completion?", profile.HandoffRequired)
	if err != nil {
		return err
	}
	handoffSchema, err := promptChoice(reader, out, "Handoff schema", []string{"universal", "strict"}, normalizeHandoffSchema(profile.HandoffSchema))
	if err != nil {
		return err
	}
	doctorAutoRepair, err := promptBool(reader, out, "Enable busy-wait auto doctor repair?", profile.BusyWaitDoctorRepairEnabled)
	if err != nil {
		return err
	}

	fmt.Fprintln(out, "\nValidation mode")
	fmt.Fprintln(out, "1) plugin-default")
	fmt.Fprintln(out, "2) skip (quick setup)")
	fmt.Fprintln(out, "3) custom command")
	modeInput, err := promptInput(reader, out, "Choose", "1")
	if err != nil {
		return err
	}
	mode := SetupModePluginDefault
	validateCmd := ""
	switch strings.TrimSpace(modeInput) {
	case "2":
		mode = SetupModeSkip
		validateCmd = validationSkipCommand
	case "3":
		mode = SetupModeCustom
		cmd, err := promptInput(reader, out, "Validation command", profile.ValidateCmd)
		if err != nil {
			return err
		}
		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			return fmt.Errorf("custom validation command cannot be empty")
		}
		validateCmd = cmd
	default:
		mode = SetupModePluginDefault
	}

	selections := SetupSelections{
		Plugin:           plugin,
		RoleRulesEnabled: roleRulesEnabled,
		HandoffRequired:  handoffRequired,
		HandoffSchema:    handoffSchema,
		DoctorAutoRepair: doctorAutoRepair,
		ValidationMode:   mode,
		ValidateCmd:      validateCmd,
	}

	fmt.Fprintln(out, "\n## Setup Summary")
	fmt.Fprintf(out, "- plugin: %s\n", selections.Plugin)
	fmt.Fprintf(out, "- role_rules_enabled: %t\n", selections.RoleRulesEnabled)
	fmt.Fprintf(out, "- handoff_required: %t\n", selections.HandoffRequired)
	fmt.Fprintf(out, "- handoff_schema: %s\n", selections.HandoffSchema)
	fmt.Fprintf(out, "- busywait_auto_doctor_repair: %t\n", selections.DoctorAutoRepair)
	switch selections.ValidationMode {
	case SetupModePluginDefault:
		fmt.Fprintln(out, "- validate_cmd: plugin default")
	case SetupModeSkip:
		fmt.Fprintf(out, "- validate_cmd: %s\n", validationSkipCommand)
	case SetupModeCustom:
		fmt.Fprintf(out, "- validate_cmd: %s\n", selections.ValidateCmd)
	}

	confirm, err := promptBool(reader, out, "Apply these settings?", true)
	if err != nil {
		return err
	}
	if !confirm {
		return fmt.Errorf("setup canceled")
	}

	if err := ApplySetupSelections(paths, executablePath, selections); err != nil {
		return err
	}
	fmt.Fprintln(out, "\nsetup complete")
	fmt.Fprintf(out, "- helper: %s\n", filepath.Join(paths.ProjectDir, "ralph"))
	fmt.Fprintf(out, "- profile_yaml: %s\n", paths.ProfileYAMLFile)
	fmt.Fprintf(out, "- profile_local_yaml: %s\n", paths.ProfileLocalYAMLFile)
	fmt.Fprintf(out, "- profile_env_override: %s\n", paths.ProfileLocalFile)
	return nil
}

func ApplySetupSelections(paths Paths, executablePath string, selections SetupSelections) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}

	plugin := strings.TrimSpace(selections.Plugin)
	if plugin == "" {
		plugin = "universal-default"
	}
	if _, err := os.Stat(pluginFilePath(paths.ControlDir, plugin)); err != nil {
		return fmt.Errorf("plugin not found: %s", plugin)
	}

	wrapperPath := filepath.Join(paths.ProjectDir, "ralph")
	if _, err := os.Stat(wrapperPath); os.IsNotExist(err) {
		if err := Install(paths, plugin, executablePath); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		if err := ApplyPlugin(paths, plugin); err != nil {
			return err
		}
	}

	existing := map[string]string{}
	if _, err := os.Stat(paths.ProfileLocalYAMLFile); err == nil {
		m, readErr := ReadYAMLFlatMap(paths.ProfileLocalYAMLFile)
		if readErr != nil {
			return fmt.Errorf("read profile.local.yaml: %w", readErr)
		}
		existing = m
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat profile.local.yaml: %w", err)
	}

	setProfileConfigValue(existing, "role_rules_enabled", boolToEnv(selections.RoleRulesEnabled), "RALPH_ROLE_RULES_ENABLED")
	setProfileConfigValue(existing, "handoff_required", boolToEnv(selections.HandoffRequired), "RALPH_HANDOFF_REQUIRED")
	setProfileConfigValue(existing, "handoff_schema", normalizeHandoffSchema(selections.HandoffSchema), "RALPH_HANDOFF_SCHEMA")
	setProfileConfigValue(existing, "busywait_doctor_repair_enabled", boolToEnv(selections.DoctorAutoRepair), "RALPH_BUSYWAIT_DOCTOR_REPAIR_ENABLED")

	switch selections.ValidationMode {
	case SetupModePluginDefault:
		delete(existing, "validate_cmd")
		delete(existing, "RALPH_VALIDATE_CMD")
	case SetupModeSkip:
		setProfileConfigValue(existing, "validate_cmd", validationSkipCommand, "RALPH_VALIDATE_CMD")
	case SetupModeCustom:
		cmd := strings.TrimSpace(selections.ValidateCmd)
		if cmd == "" {
			return fmt.Errorf("custom validation command cannot be empty")
		}
		setProfileConfigValue(existing, "validate_cmd", cmd, "RALPH_VALIDATE_CMD")
	}

	if err := WriteYAMLFlatMap(paths.ProfileLocalYAMLFile, existing); err != nil {
		return fmt.Errorf("write profile.local.yaml: %w", err)
	}
	if err := pruneLegacySetupEnvOverrides(paths.ProfileLocalFile); err != nil {
		return err
	}
	return EnsureRoleRuleFiles(paths)
}

func ApplyStabilityDefaults(paths Paths) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}

	existing := map[string]string{}
	if _, err := os.Stat(paths.ProfileLocalYAMLFile); err == nil {
		m, readErr := ReadYAMLFlatMap(paths.ProfileLocalYAMLFile)
		if readErr != nil {
			return fmt.Errorf("read profile.local.yaml: %w", readErr)
		}
		existing = m
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat profile.local.yaml: %w", err)
	}

	setProfileConfigValue(existing, "codex_exec_timeout_sec", "900", "RALPH_CODEX_EXEC_TIMEOUT_SEC")
	setProfileConfigValue(existing, "codex_retry_max_attempts", "3", "RALPH_CODEX_RETRY_MAX_ATTEMPTS")
	setProfileConfigValue(existing, "codex_retry_backoff_sec", "10", "RALPH_CODEX_RETRY_BACKOFF_SEC")
	setProfileConfigValue(existing, "codex_skip_git_repo_check", "true", "RALPH_CODEX_SKIP_GIT_REPO_CHECK")
	setProfileConfigValue(existing, "codex_output_last_message_enabled", "true", "RALPH_CODEX_OUTPUT_LAST_MESSAGE_ENABLED")
	setProfileConfigValue(existing, "inprogress_watchdog_enabled", "true", "RALPH_INPROGRESS_WATCHDOG_ENABLED")
	setProfileConfigValue(existing, "inprogress_watchdog_stale_sec", "1800", "RALPH_INPROGRESS_WATCHDOG_STALE_SEC")
	setProfileConfigValue(existing, "inprogress_watchdog_scan_loops", "1", "RALPH_INPROGRESS_WATCHDOG_SCAN_LOOPS")
	setProfileConfigValue(existing, "supervisor_enabled", "true", "RALPH_SUPERVISOR_ENABLED")
	setProfileConfigValue(existing, "supervisor_restart_delay_sec", "5", "RALPH_SUPERVISOR_RESTART_DELAY_SEC")

	if err := WriteYAMLFlatMap(paths.ProfileLocalYAMLFile, existing); err != nil {
		return fmt.Errorf("write profile.local.yaml: %w", err)
	}
	return pruneLegacySetupEnvOverrides(paths.ProfileLocalFile)
}

// ApplyRemoteProfilePreset is kept for backward compatibility.
func ApplyRemoteProfilePreset(paths Paths) error {
	return ApplyStabilityDefaults(paths)
}

func pickDefaultPlugin(plugins []string, current string) string {
	if containsString(plugins, strings.TrimSpace(current)) {
		return strings.TrimSpace(current)
	}
	if containsString(plugins, "universal-default") {
		return "universal-default"
	}
	if containsString(plugins, "go-default") {
		return "go-default"
	}
	return plugins[0]
}

func promptChoice(reader *bufio.Reader, out io.Writer, label string, options []string, defaultValue string) (string, error) {
	fmt.Fprintf(out, "%s\n", label)
	for idx, opt := range options {
		defaultMark := ""
		if opt == defaultValue {
			defaultMark = " (default)"
		}
		fmt.Fprintf(out, "%d) %s%s\n", idx+1, opt, defaultMark)
	}

	defaultIdx := 1
	for idx, opt := range options {
		if opt == defaultValue {
			defaultIdx = idx + 1
			break
		}
	}
	answer, err := promptInput(reader, out, "Choose", fmt.Sprintf("%d", defaultIdx))
	if err != nil {
		return "", err
	}
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return defaultValue, nil
	}
	if i := parseChoice(answer, len(options)); i >= 0 {
		return options[i], nil
	}
	if containsString(options, answer) {
		return answer, nil
	}
	return "", fmt.Errorf("invalid choice: %s", answer)
}

func parseChoice(raw string, total int) int {
	n := 0
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return -1
		}
		n = n*10 + int(ch-'0')
	}
	if n < 1 || n > total {
		return -1
	}
	return n - 1
}

func promptBool(reader *bufio.Reader, out io.Writer, label string, defaultValue bool) (bool, error) {
	def := "y"
	if !defaultValue {
		def = "n"
	}
	answer, err := promptInput(reader, out, fmt.Sprintf("%s (y/n)", label), def)
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "", "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid yes/no input: %s", answer)
	}
}

func promptInput(reader *bufio.Reader, out io.Writer, label, defaultValue string) (string, error) {
	if strings.TrimSpace(defaultValue) == "" {
		fmt.Fprintf(out, "%s: ", label)
	} else {
		fmt.Fprintf(out, "%s [%s]: ", label, defaultValue)
	}
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return defaultValue, nil
	}
	return line, nil
}

func boolToEnv(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func codexConnectionSummary() string {
	if _, err := exec.LookPath("codex"); err != nil {
		return "CLI not found (install Codex CLI first)"
	}
	out, err := exec.Command("codex", "login", "status").CombinedOutput()
	summary := firstNonEmptyLine(string(out))
	if strings.TrimSpace(summary) == "" {
		summary = "status unavailable"
	}
	if err != nil {
		return summary + " (run: codex login)"
	}
	return summary
}

func firstNonEmptyLine(raw string) string {
	for _, line := range strings.Split(raw, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "WARNING:") {
			continue
		}
		return trimmed
	}
	return ""
}

func setProfileConfigValue(m map[string]string, canonicalKey, value string, aliasKeys ...string) {
	delete(m, canonicalKey)
	for _, alias := range aliasKeys {
		delete(m, alias)
	}
	m[canonicalKey] = value
}

func pruneLegacySetupEnvOverrides(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat profile.local.env: %w", err)
	}
	values, err := ReadEnvFile(path)
	if err != nil {
		return fmt.Errorf("read profile.local.env: %w", err)
	}
	delete(values, "RALPH_ROLE_RULES_ENABLED")
	delete(values, "RALPH_HANDOFF_REQUIRED")
	delete(values, "RALPH_HANDOFF_SCHEMA")
	delete(values, "RALPH_BUSYWAIT_DOCTOR_REPAIR_ENABLED")
	delete(values, "RALPH_VALIDATE_CMD")

	if len(values) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove profile.local.env: %w", err)
		}
		return nil
	}
	return writeProfileLocalEnv(path, values)
}

func writeProfileLocalEnv(path string, values map[string]string) error {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ordered := []string{
		"RALPH_ROLE_RULES_ENABLED",
		"RALPH_HANDOFF_REQUIRED",
		"RALPH_HANDOFF_SCHEMA",
		"RALPH_BUSYWAIT_DOCTOR_REPAIR_ENABLED",
		"RALPH_VALIDATE_CMD",
	}

	buf := strings.Builder{}
	buf.WriteString("# Ralph local overrides (generated by `ralphctl setup`)\n")
	seen := map[string]struct{}{}
	for _, key := range ordered {
		val, ok := values[key]
		if !ok {
			continue
		}
		if strings.TrimSpace(val) == "" {
			continue
		}
		seen[key] = struct{}{}
		fmt.Fprintf(&buf, "%s=%s\n", key, envQuote(val))
	}
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		val := strings.TrimSpace(values[key])
		if val == "" {
			continue
		}
		fmt.Fprintf(&buf, "%s=%s\n", key, envQuote(val))
	}

	return os.WriteFile(path, []byte(buf.String()), 0o644)
}

func envQuote(v string) string {
	if v == "" {
		return "''"
	}
	if strings.ContainsAny(v, " \t#'\"") {
		return "'" + strings.ReplaceAll(v, "'", "'\"'\"'") + "'"
	}
	return v
}
