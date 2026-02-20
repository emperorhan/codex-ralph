package ralph

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	doctorStatusPass = "pass"
	doctorStatusWarn = "warn"
	doctorStatusFail = "fail"
)

type DoctorCheck struct {
	Name   string
	Status string
	Detail string
}

type DoctorReport struct {
	UpdatedUTC time.Time
	ProjectDir string
	Checks     []DoctorCheck
}

type DoctorRepairAction struct {
	Name   string
	Status string
	Detail string
}

func (r *DoctorReport) add(name, status, detail string) {
	r.Checks = append(r.Checks, DoctorCheck{
		Name:   name,
		Status: status,
		Detail: detail,
	})
}

func (r DoctorReport) count(status string) int {
	n := 0
	for _, c := range r.Checks {
		if c.Status == status {
			n++
		}
	}
	return n
}

func (r DoctorReport) HasFailures() bool {
	return r.count(doctorStatusFail) > 0
}

func (r DoctorReport) Print(w io.Writer) {
	fmt.Fprintln(w, "## Ralph Doctor")
	fmt.Fprintf(w, "- updated_utc: %s\n", r.UpdatedUTC.Format(time.RFC3339))
	fmt.Fprintf(w, "- project: %s\n", r.ProjectDir)
	fmt.Fprintf(
		w,
		"- summary: pass=%d warn=%d fail=%d\n",
		r.count(doctorStatusPass),
		r.count(doctorStatusWarn),
		r.count(doctorStatusFail),
	)
	for _, check := range r.Checks {
		fmt.Fprintf(w, "- [%s] %s: %s\n", check.Status, check.Name, check.Detail)
	}
}

func RunDoctor(paths Paths) (DoctorReport, error) {
	report := DoctorReport{
		UpdatedUTC: time.Now().UTC(),
		ProjectDir: paths.ProjectDir,
		Checks:     []DoctorCheck{},
	}

	if err := EnsureLayout(paths); err != nil {
		report.add("layout", doctorStatusFail, err.Error())
		return report, nil
	}
	report.add("layout", doctorStatusPass, ".ralph layout is available")

	enabled, err := IsEnabled(paths)
	if err != nil {
		report.add("state", doctorStatusFail, err.Error())
	} else {
		report.add("state", doctorStatusPass, fmt.Sprintf("RALPH_LOCAL_ENABLED=%t", enabled))
	}

	profile, err := LoadProfile(paths)
	if err != nil {
		report.add("profile", doctorStatusFail, err.Error())
		return report, nil
	}
	report.add("profile", doctorStatusPass, fmt.Sprintf("plugin=%s model=%s", profile.PluginName, profile.CodexModel))
	report.add("handoff-schema", doctorStatusPass, profile.HandoffSchema)

	if profile.RoleRulesEnabled {
		status, detail := checkNonEmptyFile(paths.CommonRulesFile)
		report.add("rules:common", status, detail)
		for _, role := range RequiredAgentRoles {
			status, detail := checkNonEmptyFile(paths.RoleRulesFile(role))
			report.add("rules:"+role, status, detail)
		}
	} else {
		report.add("rules", doctorStatusWarn, "role rules disabled (RALPH_ROLE_RULES_ENABLED=false)")
	}

	if strings.TrimSpace(profile.PluginName) == "" {
		report.add("plugin", doctorStatusWarn, "plugin name is empty in profile")
	} else if _, err := os.Stat(pluginFilePath(paths.ControlDir, profile.PluginName)); err != nil {
		report.add("plugin", doctorStatusWarn, fmt.Sprintf("plugin file not found: %s", pluginFilePath(paths.ControlDir, profile.PluginName)))
	} else {
		report.add("plugin", doctorStatusPass, fmt.Sprintf("plugin file found: %s", profile.PluginName))
	}
	appendPluginRegistryChecks(&report, paths.ControlDir)
	appendSecurityChecks(&report, paths, profile)

	if _, err := exec.LookPath("bash"); err != nil {
		report.add("command:bash", doctorStatusFail, "bash command not found")
	} else {
		report.add("command:bash", doctorStatusPass, "bash command available")
	}

	if profile.RequireCodex {
		if _, err := exec.LookPath("codex"); err != nil {
			report.add("command:codex", doctorStatusFail, "codex command required but not found")
		} else {
			report.add("command:codex", doctorStatusPass, "codex command available")
			authOut, authErr := exec.Command("codex", "login", "status").CombinedOutput()
			authSummary := firstNonEmptyLine(string(authOut))
			if strings.TrimSpace(authSummary) == "" {
				authSummary = "status unavailable"
			}
			if authErr != nil {
				report.add("auth:codex", doctorStatusWarn, authSummary+" (run: codex login)")
			} else {
				report.add("auth:codex", doctorStatusPass, authSummary)
			}
		}
	} else {
		report.add("command:codex", doctorStatusWarn, "RALPH_REQUIRE_CODEX=false (codex execution disabled)")
	}

	if len(profile.ValidateRoles) > 0 && strings.TrimSpace(profile.ValidateCmd) == "" {
		report.add("validation", doctorStatusWarn, "validation roles configured but RALPH_VALIDATE_CMD is empty")
	} else {
		report.add("validation", doctorStatusPass, fmt.Sprintf("validate_roles=%s", RoleSetCSV(profile.ValidateRoles)))
	}

	status, detail := evaluatePIDFile(paths.PIDFile)
	report.add("daemon:primary", status, detail)
	for _, role := range RequiredAgentRoles {
		status, detail := evaluatePIDFile(paths.RolePIDFile(role))
		report.add("daemon:"+role, status, detail)
	}

	inProgressCount, inProgressErr := CountIssueFiles(paths.InProgressDir)
	if inProgressErr != nil {
		report.add("queue:in-progress", doctorStatusFail, inProgressErr.Error())
	} else {
		_, primaryRunning := daemonPID(paths)
		roleRunning, _ := RunningRoleDaemons(paths)
		if inProgressCount > 0 && !primaryRunning && len(roleRunning) == 0 {
			report.add("queue:in-progress", doctorStatusWarn, fmt.Sprintf("%d issues in in-progress with no active daemon (run: ralphctl recover)", inProgressCount))
		} else {
			report.add("queue:in-progress", doctorStatusPass, fmt.Sprintf("%d issues in in-progress", inProgressCount))
		}
	}

	blockedCount, blockedErr := CountIssueFiles(paths.BlockedDir)
	if blockedErr != nil {
		report.add("queue:blocked", doctorStatusFail, blockedErr.Error())
	} else if blockedCount > 0 {
		report.add("queue:blocked", doctorStatusWarn, fmt.Sprintf("%d blocked issues require manual triage", blockedCount))
	} else {
		report.add("queue:blocked", doctorStatusPass, "no blocked issues")
	}

	if profile.HandoffRequired {
		if info, err := os.Stat(paths.HandoffsDir); err != nil {
			report.add("handoff-dir", doctorStatusFail, err.Error())
		} else if !info.IsDir() {
			report.add("handoff-dir", doctorStatusFail, "handoff path is not a directory")
		} else {
			report.add("handoff-dir", doctorStatusPass, paths.HandoffsDir)
		}
	} else {
		report.add("handoff", doctorStatusWarn, "handoff validation disabled (RALPH_HANDOFF_REQUIRED=false)")
	}

	if _, err := os.Stat(paths.ProgressJournal); err != nil {
		if os.IsNotExist(err) {
			report.add("progress-journal", doctorStatusWarn, "progress journal not created yet")
		} else {
			report.add("progress-journal", doctorStatusFail, err.Error())
		}
	} else {
		report.add("progress-journal", doctorStatusPass, paths.ProgressJournal)
	}

	if _, err := LoadBusyWaitState(paths); err != nil {
		report.add("busywait-state", doctorStatusFail, err.Error())
	} else {
		report.add("busywait-state", doctorStatusPass, "busywait state is readable")
	}

	return report, nil
}

func RepairProject(paths Paths) ([]DoctorRepairAction, error) {
	actions := []DoctorRepairAction{}
	if err := EnsureLayout(paths); err != nil {
		return actions, err
	}
	actions = append(actions, DoctorRepairAction{
		Name:   "layout",
		Status: doctorStatusPass,
		Detail: "layout ensured",
	})

	if err := EnsureRoleRuleFiles(paths); err != nil {
		actions = append(actions, DoctorRepairAction{
			Name:   "rules",
			Status: doctorStatusFail,
			Detail: err.Error(),
		})
	} else {
		actions = append(actions, DoctorRepairAction{
			Name:   "rules",
			Status: doctorStatusPass,
			Detail: "role rule files ensured",
		})
	}

	pidFiles := []string{paths.PIDFile}
	for _, role := range RequiredAgentRoles {
		pidFiles = append(pidFiles, paths.RolePIDFile(role))
	}
	removedCount := 0
	for _, pidFile := range pidFiles {
		removed, err := removeStalePIDFile(pidFile)
		if err != nil {
			actions = append(actions, DoctorRepairAction{
				Name:   "stale-pid",
				Status: doctorStatusFail,
				Detail: fmt.Sprintf("%s: %v", pidFile, err),
			})
			continue
		}
		if removed {
			removedCount++
		}
	}
	actions = append(actions, DoctorRepairAction{
		Name:   "stale-pid",
		Status: doctorStatusPass,
		Detail: fmt.Sprintf("removed %d stale pid file(s)", removedCount),
	})

	_, primaryRunning := daemonPID(paths)
	roleRunning, _ := RunningRoleDaemons(paths)
	if !primaryRunning && len(roleRunning) == 0 {
		recovered, err := RecoverInProgressWithCount(paths)
		if err != nil {
			actions = append(actions, DoctorRepairAction{
				Name:   "recover-in-progress",
				Status: doctorStatusFail,
				Detail: err.Error(),
			})
		} else {
			actions = append(actions, DoctorRepairAction{
				Name:   "recover-in-progress",
				Status: doctorStatusPass,
				Detail: fmt.Sprintf("recovered %d issue(s)", recovered),
			})
		}
	} else {
		actions = append(actions, DoctorRepairAction{
			Name:   "recover-in-progress",
			Status: doctorStatusWarn,
			Detail: "skipped because daemon is running",
		})
	}

	return actions, nil
}

func evaluatePIDFile(path string) (string, string) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return doctorStatusPass, "not running"
		}
		return doctorStatusFail, fmt.Sprintf("read pid file: %v", err)
	}

	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return doctorStatusWarn, "pid file is empty"
	}
	pid, convErr := strconv.Atoi(raw)
	if convErr != nil || pid <= 0 {
		return doctorStatusWarn, "pid file has invalid value"
	}
	if isPIDRunning(pid) {
		return doctorStatusPass, fmt.Sprintf("running (pid=%d)", pid)
	}
	return doctorStatusWarn, fmt.Sprintf("stale pid file (pid=%d not running)", pid)
}

func checkNonEmptyFile(path string) (string, string) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return doctorStatusWarn, fmt.Sprintf("missing: %s", path)
		}
		return doctorStatusFail, fmt.Sprintf("read failed: %v", err)
	}
	if strings.TrimSpace(string(data)) == "" {
		return doctorStatusWarn, fmt.Sprintf("empty: %s", path)
	}
	return doctorStatusPass, path
}

func removeStalePIDFile(pidFile string) (bool, error) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	raw := strings.TrimSpace(string(data))
	if raw == "" {
		if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		return true, nil
	}

	pid, convErr := strconv.Atoi(raw)
	if convErr != nil || pid <= 0 || !isPIDRunning(pid) {
		if err := os.Remove(pidFile); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func appendPluginRegistryChecks(report *DoctorReport, controlDir string) {
	checks, err := VerifyPluginRegistry(controlDir)
	if err != nil {
		if os.IsNotExist(err) {
			report.add("plugin-registry", doctorStatusWarn, "registry not found (run: ralphctl --control-dir DIR registry generate)")
			return
		}
		report.add("plugin-registry", doctorStatusFail, err.Error())
		return
	}

	passCount := 0
	warnCount := 0
	failCount := 0
	for _, check := range checks {
		switch check.Status {
		case doctorStatusPass:
			passCount++
		case doctorStatusWarn:
			warnCount++
		case doctorStatusFail:
			failCount++
		default:
			warnCount++
		}
	}
	if failCount > 0 {
		report.add("plugin-registry", doctorStatusFail, fmt.Sprintf("pass=%d warn=%d fail=%d", passCount, warnCount, failCount))
		return
	}
	if warnCount > 0 {
		report.add("plugin-registry", doctorStatusWarn, fmt.Sprintf("pass=%d warn=%d fail=%d", passCount, warnCount, failCount))
		return
	}
	report.add("plugin-registry", doctorStatusPass, fmt.Sprintf("pass=%d warn=%d fail=%d", passCount, warnCount, failCount))
}

func appendSecurityChecks(report *DoctorReport, paths Paths, profile Profile) {
	switch strings.TrimSpace(profile.CodexSandbox) {
	case "danger-full-access":
		report.add("security:codex-sandbox", doctorStatusFail, "danger-full-access is risky for unattended automation")
	case "":
		report.add("security:codex-sandbox", doctorStatusWarn, "empty codex sandbox; expected workspace-write")
	default:
		report.add("security:codex-sandbox", doctorStatusPass, profile.CodexSandbox)
	}

	if strings.TrimSpace(strings.ToLower(profile.CodexApproval)) != "never" {
		report.add("security:codex-approval", doctorStatusWarn, fmt.Sprintf("codex approval=%s (recommended: never for autonomous loop)", profile.CodexApproval))
	} else {
		report.add("security:codex-approval", doctorStatusPass, "never")
	}
	if profile.CodexSkipGitRepoCheck {
		report.add("security:codex-git-repo-check", doctorStatusPass, "skip-git-repo-check enabled")
	} else {
		report.add("security:codex-git-repo-check", doctorStatusWarn, "skip-git-repo-check disabled (non-git project may fail)")
	}

	validateCmdLower := strings.ToLower(strings.TrimSpace(profile.ValidateCmd))
	if validateCmdLower == "" || strings.Contains(validateCmdLower, "validation skipped by setup") || strings.Contains(validateCmdLower, "skip validation") {
		report.add("security:validation", doctorStatusWarn, "validation command looks disabled")
	} else {
		report.add("security:validation", doctorStatusPass, "validation command configured")
	}

	if strings.TrimSpace(profile.BusyWaitSelfHealCmd) != "" {
		report.add("security:self-heal-cmd", doctorStatusWarn, "busywait self-heal command is enabled; review command safety")
	} else {
		report.add("security:self-heal-cmd", doctorStatusPass, "disabled")
	}

	checkFileWritePermissions(report, "security:file-perm:profile.yaml", paths.ProfileYAMLFile)
	checkFileWritePermissions(report, "security:file-perm:profile.local.yaml", paths.ProfileLocalYAMLFile)
	checkFileWritePermissions(report, "security:file-perm:profile.env", paths.ProfileFile)
	checkFileWritePermissions(report, "security:file-perm:profile.local.env", paths.ProfileLocalFile)
	checkDirectoryWritable(report, "security:write-check:project-dir", paths.ProjectDir)
	checkDirectoryWritable(report, "security:write-check:control-dir", paths.ControlDir)
}

func checkFileWritePermissions(report *DoctorReport, checkName, path string) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			report.add(checkName, doctorStatusWarn, "file not found")
			return
		}
		report.add(checkName, doctorStatusFail, err.Error())
		return
	}
	perm := info.Mode().Perm()
	if perm&0o022 != 0 {
		report.add(checkName, doctorStatusFail, fmt.Sprintf("permissions too broad: %#o", perm))
		return
	}
	report.add(checkName, doctorStatusPass, fmt.Sprintf("%#o", perm))
}

func checkDirectoryWritable(report *DoctorReport, checkName, dir string) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		report.add(checkName, doctorStatusFail, fmt.Sprintf("mkdir failed: %v", err))
		return
	}
	f, err := os.CreateTemp(dir, ".ralph-write-check-*")
	if err != nil {
		report.add(checkName, doctorStatusFail, fmt.Sprintf("write failed: %v", err))
		return
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	report.add(checkName, doctorStatusPass, "writable")
}
