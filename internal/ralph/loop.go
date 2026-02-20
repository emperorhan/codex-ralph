package ralph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type RunOptions struct {
	MaxLoops     int
	Stdout       io.Writer
	AllowedRoles map[string]struct{}
}

type BusyWaitHealResult struct {
	ReadyBefore      int
	ReadyAfter       int
	InProgressBefore int
	RecoveredCount   int
	CmdRan           bool
	CmdExitCode      int
	CmdLogFile       string
	DoctorRepairRan  bool
	DoctorRepairNote string
	Result           string
	Err              error
}

func RunLoop(ctx context.Context, paths Paths, profile Profile, opts RunOptions) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	if err := preflightLoopPermissions(paths); err != nil {
		return err
	}

	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}
	if opts.MaxLoops < 0 {
		opts.MaxLoops = 0
	}

	if profile.RequireCodex {
		if _, err := exec.LookPath("codex"); err != nil {
			return fmt.Errorf("codex command not found")
		}
	}
	if _, err := exec.LookPath("bash"); err != nil {
		return fmt.Errorf("bash command not found")
	}

	busyState, err := LoadBusyWaitState(paths)
	if err != nil {
		return err
	}
	profileReloadState, err := LoadProfileReloadState(paths)
	if err != nil {
		fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: failed to load profile reload state: %v\n", err)
		profileReloadState = ProfileReloadState{}
	}

	roleScope := RoleSetCSV(opts.AllowedRoles)
	busyWaitOwner := len(opts.AllowedRoles) == 0
	if !busyWaitOwner {
		_, busyWaitOwner = opts.AllowedRoles["manager"]
	}

	if busyWaitOwner {
		recoveredOnBoot, err := RecoverInProgressWithCount(paths)
		if err != nil {
			return fmt.Errorf("recover in-progress: %w", err)
		}
		if recoveredOnBoot > 0 {
			fmt.Fprintf(opts.Stdout, "[ralph-loop] recovered %d stale in-progress issue(s) on startup\n", recoveredOnBoot)
			_ = AppendBusyWaitEvent(paths, BusyWaitEvent{
				Type:           "startup_recover_in_progress",
				RecoveredCount: recoveredOnBoot,
				Result:         "recovered",
				Detail:         "role_scope=" + roleScopeOrAll(roleScope),
			})
		}
	}

	loopCount := 0
	idleCount := 0
	tickCount := 0
	permissionErrStreak := 0
	activeProfile := profile

	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(opts.Stdout, "[ralph-loop] interrupted; stopping")
			return nil
		default:
		}
		tickCount++

		enabled, err := IsEnabled(paths)
		if err != nil {
			return err
		}
		if !enabled {
			fmt.Fprintln(opts.Stdout, "[ralph-loop] disabled; stopping")
			return nil
		}
		reloadedProfile, changed, reloadErr := reloadLoopProfile(paths, activeProfile)
		if reloadErr != nil {
			fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: profile reload failed; using previous settings: %v\n", reloadErr)
		} else {
			if changed {
				summary := profileReloadSummary(reloadedProfile)
				fmt.Fprintf(opts.Stdout, "[ralph-loop] profile reloaded: %s\n", summary)
				profileReloadState.LastReloadAt = time.Now().UTC()
				profileReloadState.ReloadCount++
				profileReloadState.LastSummary = summary
				if err := SaveProfileReloadState(paths, profileReloadState); err != nil {
					fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: failed to save profile reload state: %v\n", err)
				}
			}
			activeProfile = reloadedProfile
		}
		if busyWaitOwner && activeProfile.InProgressWatchdogEnabled && shouldRunWatchdogScan(tickCount, activeProfile.InProgressWatchdogScanLoops) {
			recovered, watchdogErr := RecoverStaleInProgressWithCount(paths, time.Duration(activeProfile.InProgressWatchdogStaleSec)*time.Second)
			if watchdogErr != nil {
				fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: in-progress watchdog failed: %v\n", watchdogErr)
				_ = AppendBusyWaitEvent(paths, BusyWaitEvent{
					Type:      "watchdog_recover_failed",
					LoopCount: loopCount,
					Result:    "error",
					Error:     watchdogErr.Error(),
					Detail:    "role_scope=" + roleScopeOrAll(roleScope),
				})
			} else if recovered > 0 {
				fmt.Fprintf(opts.Stdout, "[ralph-loop] watchdog recovered %d stale in-progress issue(s)\n", recovered)
				_ = AppendBusyWaitEvent(paths, BusyWaitEvent{
					Type:           "watchdog_recover_in_progress",
					LoopCount:      loopCount,
					RecoveredCount: recovered,
					Result:         "recovered",
					Detail:         fmt.Sprintf("stale_sec=%d; role_scope=%s", activeProfile.InProgressWatchdogStaleSec, roleScopeOrAll(roleScope)),
				})
			}
		}

		if opts.MaxLoops > 0 && loopCount >= opts.MaxLoops {
			fmt.Fprintf(opts.Stdout, "[ralph-loop] max loops reached (%d)\n", opts.MaxLoops)
			return nil
		}

		issuePath, meta, err := PickNextReadyIssueForRoles(paths, opts.AllowedRoles)
		if err != nil {
			return err
		}
		if issuePath == "" {
			if len(opts.AllowedRoles) > 0 {
				globalReady, _ := CountReadyIssues(paths)
				if globalReady > 0 {
					fmt.Fprintf(opts.Stdout, "[ralph-loop] no ready issues for roles=%s; global_ready=%d; sleeping %ds\n", roleScope, globalReady, activeProfile.IdleSleepSec)
					if err := sleepOrCancel(ctx, time.Duration(activeProfile.IdleSleepSec)*time.Second); err != nil {
						return nil
					}
					continue
				}
			}

			idleCount++

			readyBefore, _ := CountReadyIssues(paths)
			inProgressBefore, _ := CountIssueFiles(paths.InProgressDir)
			blockedBefore, _ := CountIssueFiles(paths.BlockedDir)
			if idleCount == 1 && readyBefore == 0 && inProgressBefore == 0 && blockedBefore == 0 {
				fmt.Fprintln(opts.Stdout, "[ralph-loop] input required: no queued work. add issue (`./ralph new ...`) or import PRD (`./ralph import-prd --file prd.json`)")
			}
			if shouldDetectBusyWait(busyWaitOwner, activeProfile.BusyWaitDetectLoops, idleCount, readyBefore, inProgressBefore) {
				now := time.Now().UTC()
				fmt.Fprintf(opts.Stdout, "[ralph-loop] busy-wait detected (idle_count=%d, ready=%d, in_progress=%d, role_scope=%s)\n", idleCount, readyBefore, inProgressBefore, roleScopeOrAll(roleScope))

				busyState.LastDetectedAt = now
				busyState.LastIdleCount = idleCount
				if err := SaveBusyWaitState(paths, busyState); err != nil {
					fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: failed to save busywait state: %v\n", err)
				}
				if err := AppendBusyWaitEvent(paths, BusyWaitEvent{
					Type:             "busy_wait_detected",
					IdleCount:        idleCount,
					LoopCount:        loopCount,
					ReadyBefore:      readyBefore,
					InProgressBefore: inProgressBefore,
					Result:           "detected",
					Detail:           "role_scope=" + roleScopeOrAll(roleScope),
				}); err != nil {
					fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: failed to append busywait event: %v\n", err)
				}

				if activeProfile.BusyWaitSelfHealEnabled {
					canHeal, skipReason := canRunBusyWaitSelfHeal(now, busyState, activeProfile)
					if canHeal {
						heal := executeBusyWaitSelfHeal(ctx, paths, activeProfile)
						busyState.LastSelfHealAt = now
						busyState.SelfHealAttempts++
						busyState.LastSelfHealResult = heal.Result
						busyState.LastSelfHealLog = heal.CmdLogFile
						busyState.LastRecoveredCount = heal.RecoveredCount
						busyState.LastReadyAfter = heal.ReadyAfter
						if heal.Err != nil {
							busyState.LastSelfHealError = heal.Err.Error()
						} else {
							busyState.LastSelfHealError = ""
						}

						if err := SaveBusyWaitState(paths, busyState); err != nil {
							fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: failed to save busywait state after self-heal: %v\n", err)
						}

						event := BusyWaitEvent{
							Type:             "busy_wait_self_heal",
							IdleCount:        idleCount,
							LoopCount:        loopCount,
							ReadyBefore:      heal.ReadyBefore,
							ReadyAfter:       heal.ReadyAfter,
							InProgressBefore: heal.InProgressBefore,
							RecoveredCount:   heal.RecoveredCount,
							SelfHealAttempt:  busyState.SelfHealAttempts,
							SelfHealApplied:  true,
							Result:           heal.Result,
							LogFile:          heal.CmdLogFile,
							Detail:           "role_scope=" + roleScopeOrAll(roleScope),
						}
						if heal.Err != nil {
							event.Error = heal.Err.Error()
						}
						if err := AppendBusyWaitEvent(paths, event); err != nil {
							fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: failed to append self-heal event: %v\n", err)
						}

						if heal.Err != nil {
							fmt.Fprintf(opts.Stdout, "[ralph-loop] busy-wait self-heal finished with warning: %v\n", heal.Err)
						} else {
							fmt.Fprintf(opts.Stdout, "[ralph-loop] busy-wait self-heal finished: %s\n", heal.Result)
						}

						if heal.ReadyAfter > 0 {
							fmt.Fprintln(opts.Stdout, "[ralph-loop] self-heal produced ready work; retrying immediately")
							continue
						}
					} else {
						if err := AppendBusyWaitEvent(paths, BusyWaitEvent{
							Type:            "busy_wait_self_heal_skipped",
							IdleCount:       idleCount,
							LoopCount:       loopCount,
							SelfHealApplied: false,
							Detail:          skipReason + "; role_scope=" + roleScopeOrAll(roleScope),
							Result:          "skipped",
						}); err != nil {
							fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: failed to append self-heal-skip event: %v\n", err)
						}
						fmt.Fprintf(opts.Stdout, "[ralph-loop] busy-wait self-heal skipped: %s\n", skipReason)
					}
				}
			}

			if activeProfile.ExitOnIdle {
				fmt.Fprintln(opts.Stdout, "[ralph-loop] no ready issues; exit_on_idle=true")
				return nil
			}
			if activeProfile.NoReadyMaxLoops > 0 && idleCount >= activeProfile.NoReadyMaxLoops {
				fmt.Fprintf(opts.Stdout, "[ralph-loop] no ready issues; reached no_ready_max_loops=%d\n", activeProfile.NoReadyMaxLoops)
				return nil
			}
			fmt.Fprintf(opts.Stdout, "[ralph-loop] no ready issues; sleeping %ds\n", activeProfile.IdleSleepSec)
			if err := sleepOrCancel(ctx, time.Duration(activeProfile.IdleSleepSec)*time.Second); err != nil {
				return nil
			}
			continue
		}
		idleCount = 0

		if err := processIssue(ctx, paths, activeProfile, issuePath, meta, opts.Stdout); err != nil {
			fmt.Fprintf(opts.Stdout, "[ralph-loop] issue processing error: %v\n", err)
			if isLikelyPermissionErr(err) {
				permissionErrStreak++
				waitSec := permissionErrorBackoffSec(activeProfile.IdleSleepSec, permissionErrStreak)
				if appendErr := AppendBusyWaitEvent(paths, BusyWaitEvent{
					Type:      "process_permission_error",
					LoopCount: loopCount,
					Result:    "detected",
					Error:     err.Error(),
					Detail:    fmt.Sprintf("streak=%d; wait_sec=%d; role_scope=%s", permissionErrStreak, waitSec, roleScopeOrAll(roleScope)),
				}); appendErr != nil {
					fmt.Fprintf(opts.Stdout, "[ralph-loop] warning: failed to append permission-error event: %v\n", appendErr)
				}
				fmt.Fprintf(opts.Stdout, "[ralph-loop] permission-related failure detected (streak=%d); sleeping %ds and retrying. hint: ralphctl --control-dir %s --project-dir %s doctor --repair\n", permissionErrStreak, waitSec, paths.ControlDir, paths.ProjectDir)
				if err := sleepOrCancel(ctx, time.Duration(waitSec)*time.Second); err != nil {
					return nil
				}
			} else {
				permissionErrStreak = 0
			}
		} else {
			permissionErrStreak = 0
		}
		loopCount++
	}
}

func reloadLoopProfile(paths Paths, current Profile) (Profile, bool, error) {
	next, err := LoadProfile(paths)
	if err != nil {
		return current, false, err
	}
	if reflect.DeepEqual(current, next) {
		return current, false, nil
	}
	return next, true, nil
}

func profileReloadSummary(p Profile) string {
	globalModel := strings.TrimSpace(p.CodexModel)
	if globalModel == "" || normalizeCodexModelForExec(globalModel) == "" {
		globalModel = "auto"
	}
	developerModel := strings.TrimSpace(p.CodexModelDeveloper)
	if developerModel == "" {
		developerModel = "(inherit)"
	}
	return fmt.Sprintf(
		"plugin=%s codex_model=%s codex_model_developer=%s idle_sleep_sec=%d retry=%d timeout=%ds",
		p.PluginName,
		globalModel,
		developerModel,
		p.IdleSleepSec,
		p.CodexRetryMaxAttempts,
		p.CodexExecTimeoutSec,
	)
}

func roleScopeOrAll(scope string) string {
	if strings.TrimSpace(scope) == "" {
		return "all"
	}
	return scope
}

func sleepOrCancel(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func processIssue(ctx context.Context, paths Paths, profile Profile, issuePath string, meta IssueMeta, stdout io.Writer) error {
	inProgressPath := filepath.Join(paths.InProgressDir, meta.ID+".md")
	if err := os.Rename(issuePath, inProgressPath); err != nil {
		return fmt.Errorf("move to in-progress: %w", err)
	}
	if err := SetIssueStatus(inProgressPath, "in-progress"); err != nil {
		return err
	}

	logPath := filepath.Join(paths.LogsDir, fmt.Sprintf("%s-%s.log", meta.ID, time.Now().UTC().Format("20060102T150405Z")))
	handoffPath := HandoffFilePath(paths, meta)
	if err := runCodexAndValidate(ctx, paths, profile, inProgressPath, meta, logPath, handoffPath); err != nil {
		_ = SetIssueStatus(inProgressPath, "blocked")
		_ = AppendIssueResult(inProgressPath, "blocked", err.Error(), logPath)
		blockedPath := filepath.Join(paths.BlockedDir, meta.ID+".md")
		if renameErr := os.Rename(inProgressPath, blockedPath); renameErr != nil {
			return fmt.Errorf("move blocked failed (%v), root cause: %w", renameErr, err)
		}
		if progressErr := AppendProgressEntry(paths, meta, "blocked", err.Error(), logPath); progressErr != nil {
			fmt.Fprintf(stdout, "[ralph-loop] warning: progress journal append failed: %v\n", progressErr)
		}
		fmt.Fprintf(stdout, "[ralph-loop] blocked %s: %v\n", meta.ID, err)
		return nil
	}

	if err := SetIssueStatus(inProgressPath, "done"); err != nil {
		return err
	}
	if err := AppendIssueResult(inProgressPath, "done", "completed", logPath); err != nil {
		return err
	}
	donePath := filepath.Join(paths.DoneDir, meta.ID+".md")
	if err := os.Rename(inProgressPath, donePath); err != nil {
		return fmt.Errorf("move done: %w", err)
	}
	if progressErr := AppendProgressEntry(paths, meta, "done", "completed", logPath); progressErr != nil {
		fmt.Fprintf(stdout, "[ralph-loop] warning: progress journal append failed: %v\n", progressErr)
	}
	fmt.Fprintf(stdout, "[ralph-loop] done %s (%s)\n", meta.ID, meta.Title)
	return nil
}

func runCodexAndValidate(ctx context.Context, paths Paths, profile Profile, inProgressPath string, meta IssueMeta, logPath, handoffPath string) error {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer logFile.Close()

	issueBytes, err := os.ReadFile(inProgressPath)
	if err != nil {
		return fmt.Errorf("read issue: %w", err)
	}

	ruleBundle := RoleRuleBundle{}
	if profile.RoleRulesEnabled {
		ruleBundle, err = LoadRoleRuleBundle(paths, meta.Role)
		if err != nil {
			return fmt.Errorf("load role rules: %w", err)
		}
	}

	requireHandoff := profile.HandoffRequired && profile.RequireCodex
	prompt := buildCodexPrompt(paths.ProjectDir, string(issueBytes), meta, handoffPath, ruleBundle, profile.RoleRulesEnabled, requireHandoff, profile.HandoffSchema)
	lastMessagePath := ""
	if profile.CodexOutputLastMessage {
		lastMessagePath = codexLastMessagePath(logPath)
	}

	if profile.RequireCodex {
		model := profile.CodexModelForRole(meta.Role)
		modelLabel := model
		if strings.TrimSpace(modelLabel) == "" {
			modelLabel = "auto(codex default)"
		}
		_, _ = fmt.Fprintf(logFile, "[ralph] codex role=%s model=%s\n", meta.Role, modelLabel)
		if err := runCodexWithRetries(ctx, paths, profile, model, prompt, logFile, lastMessagePath); err != nil {
			return err
		}
		if lastMessagePath != "" {
			if _, err := os.Stat(lastMessagePath); err == nil {
				_, _ = fmt.Fprintf(logFile, "[ralph] codex last message saved: %s\n", lastMessagePath)
			} else {
				_, _ = fmt.Fprintf(logFile, "[ralph] warning: codex last message file not found: %s\n", lastMessagePath)
			}
		}
	} else {
		_, _ = fmt.Fprintln(logFile, "codex execution skipped (RALPH_REQUIRE_CODEX=false)")
	}

	if shouldValidate(profile, meta.Role) {
		validateCmd := exec.CommandContext(ctx, "bash", "-lc", profile.ValidateCmd)
		validateCmd.Dir = paths.ProjectDir
		validateCmd.Stdout = logFile
		validateCmd.Stderr = logFile
		if err := validateCmd.Run(); err != nil {
			return fmt.Errorf("validate_exit_%d", exitCode(err))
		}
	}
	if requireHandoff {
		if err := ValidateRoleHandoff(meta, handoffPath, profile.HandoffSchema); err != nil {
			return fmt.Errorf("handoff_invalid: %w", err)
		}
	}

	return nil
}

func buildCodexPrompt(projectDir, issueText string, meta IssueMeta, handoffPath string, rules RoleRuleBundle, includeRules, requireHandoff bool, handoffSchema string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are executing a local Ralph issue in project %s.\n\nIssue:\n%s\n\n", projectDir, issueText)
	b.WriteString("Execution rules:\n")
	b.WriteString("- Keep edits inside project root.\n")
	b.WriteString("- Follow acceptance criteria.\n")
	b.WriteString("- Do not open PR or remote automation.\n")

	if includeRules {
		b.WriteString("\nRole contract (common):\n")
		b.WriteString(rules.Common)
		b.WriteString("\n\nRole contract (")
		b.WriteString(meta.Role)
		b.WriteString("):\n")
		b.WriteString(rules.Role)
		b.WriteString("\n")
	}

	if requireHandoff {
		b.WriteString("\nHandoff contract:\n")
		b.WriteString(HandoffInstruction(meta, handoffPath, handoffSchema))
		b.WriteString("\n")
	}

	return b.String()
}

func shouldValidate(profile Profile, role string) bool {
	_, ok := profile.ValidateRoles[role]
	return ok
}

func runCodexWithRetries(ctx context.Context, paths Paths, profile Profile, model, prompt string, logFile *os.File, lastMessagePath string) error {
	attempts := profile.CodexRetryMaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	backoffSec := profile.CodexRetryBackoffSec
	if backoffSec < 0 {
		backoffSec = 0
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		_, _ = fmt.Fprintf(logFile, "[ralph] codex attempt %d/%d\n", attempt, attempts)
		err, retryable := runSingleCodexAttempt(ctx, paths, profile, model, prompt, logFile, lastMessagePath)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable || attempt >= attempts {
			break
		}

		waitSec := codexRetryBackoff(backoffSec, attempt)
		if waitSec > 0 {
			_, _ = fmt.Fprintf(logFile, "[ralph] codex attempt %d failed (%v); retrying in %ds\n", attempt, err, waitSec)
			if err := sleepOrCancel(ctx, time.Duration(waitSec)*time.Second); err != nil {
				return fmt.Errorf("codex_retry_canceled")
			}
		} else {
			_, _ = fmt.Fprintf(logFile, "[ralph] codex attempt %d failed (%v); retrying immediately\n", attempt, err)
		}
	}

	if attempts > 1 {
		return fmt.Errorf("codex_failed_after_%d_attempts: %w", attempts, lastErr)
	}
	return lastErr
}

func runSingleCodexAttempt(ctx context.Context, paths Paths, profile Profile, model, prompt string, logFile *os.File, lastMessagePath string) (error, bool) {
	cmdCtx := ctx
	cancel := func() {}
	if profile.CodexExecTimeoutSec > 0 {
		cmdCtx, cancel = context.WithTimeout(ctx, time.Duration(profile.CodexExecTimeoutSec)*time.Second)
	}
	defer cancel()

	args := []string{
		"--ask-for-approval", profile.CodexApproval,
		"exec",
		"--sandbox", profile.CodexSandbox,
		"--cd", paths.ProjectDir,
	}
	if strings.TrimSpace(model) != "" {
		args = append(args, "--model", model)
	}
	if profile.CodexSkipGitRepoCheck {
		args = append(args, "--skip-git-repo-check")
	}
	if strings.TrimSpace(lastMessagePath) != "" {
		args = append(args, "--output-last-message", lastMessagePath)
	}
	// Use stdin prompt to avoid argv length limits for large issue/rule payloads.
	args = append(args, "-")

	codexCmd := exec.CommandContext(cmdCtx, "codex", args...)
	tail := newTailBuffer(64 * 1024)
	codexCmd.Stdout = io.MultiWriter(logFile, tail)
	codexCmd.Stderr = io.MultiWriter(logFile, tail)
	codexCmd.Stdin = strings.NewReader(prompt)
	runErr := codexCmd.Run()
	if runErr == nil {
		return nil, false
	}
	if profile.CodexExecTimeoutSec > 0 && errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
		_, _ = fmt.Fprintf(logFile, "[ralph] codex timeout after %ds\n", profile.CodexExecTimeoutSec)
		return fmt.Errorf("codex_timeout_%ds", profile.CodexExecTimeoutSec), true
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("codex_canceled"), false
	}

	code := exitCode(runErr)
	if reason, retryable := classifyCodexFailure(code, strings.ToLower(tail.String())); !retryable {
		_, _ = fmt.Fprintf(logFile, "[ralph] codex non-retryable failure: %s (rc=%d)\n", reason, code)
		return fmt.Errorf("%s", reason), false
	}
	return fmt.Errorf("codex_exit_%d", code), code != 130
}

func codexLastMessagePath(logPath string) string {
	base := strings.TrimSuffix(logPath, filepath.Ext(logPath))
	if base == "" {
		base = logPath
	}
	return base + ".last.txt"
}

func preflightLoopPermissions(paths Paths) error {
	dirs := []struct {
		name string
		path string
	}{
		{name: "project-dir", path: paths.ProjectDir},
		{name: "control-dir", path: paths.ControlDir},
		{name: "issues-dir", path: paths.IssuesDir},
		{name: "in-progress-dir", path: paths.InProgressDir},
		{name: "blocked-dir", path: paths.BlockedDir},
		{name: "done-dir", path: paths.DoneDir},
		{name: "logs-dir", path: paths.LogsDir},
	}
	for _, d := range dirs {
		if err := ensureDirWritable(d.path); err != nil {
			return fmt.Errorf("permission preflight failed for %s (%s): %w", d.name, d.path, err)
		}
	}
	return nil
}

func ensureDirWritable(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return fmt.Errorf("empty path")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".ralph-loop-write-check-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}

func classifyCodexFailure(exitCode int, outputLower string) (string, bool) {
	if exitCode == 130 {
		return "codex_canceled", false
	}

	authMarkers := []string{
		"not logged in",
		"run: codex login",
		"authentication",
		"unauthorized",
		"forbidden",
		"invalid api key",
	}
	if hasAnySubstring(outputLower, authMarkers...) {
		return "codex_auth_error", false
	}

	argMarkers := []string{
		"unknown option",
		"unknown argument",
		"invalid value",
		"error parsing",
	}
	if hasAnySubstring(outputLower, argMarkers...) {
		return "codex_invalid_args", false
	}

	modelMarkers := []string{
		"unknown model",
		"model not found",
		"model does not exist",
		"invalid model",
	}
	if hasAnySubstring(outputLower, modelMarkers...) {
		return "codex_model_error", false
	}

	permissionMarkers := []string{
		"permission denied",
		"operation not permitted",
		"approval required",
		"sandbox blocked",
	}
	if hasAnySubstring(outputLower, permissionMarkers...) {
		return "codex_permission_denied", false
	}

	return "", true
}

func hasAnySubstring(s string, patterns ...string) bool {
	for _, p := range patterns {
		if p != "" && strings.Contains(s, p) {
			return true
		}
	}
	return false
}

func isLikelyPermissionErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrPermission) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return hasAnySubstring(msg,
		"permission denied",
		"operation not permitted",
		"read-only file system",
		"sandbox blocked",
		"approval required",
	)
}

func permissionErrorBackoffSec(idleSleepSec, streak int) int {
	base := idleSleepSec
	if base < 5 {
		base = 5
	}
	if streak <= 1 {
		return base
	}
	wait := base
	for i := 1; i < streak; i++ {
		if wait >= 300 {
			return 300
		}
		wait *= 2
	}
	if wait > 300 {
		return 300
	}
	return wait
}

type tailBuffer struct {
	max  int
	data []byte
}

func newTailBuffer(max int) *tailBuffer {
	if max <= 0 {
		max = 4096
	}
	return &tailBuffer{max: max}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	if len(p) >= b.max {
		b.data = append(b.data[:0], p[len(p)-b.max:]...)
		return len(p), nil
	}

	overflow := len(b.data) + len(p) - b.max
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *tailBuffer) String() string {
	return string(b.data)
}

func codexRetryBackoff(baseSec, attempt int) int {
	if baseSec <= 0 {
		return 0
	}
	wait := baseSec
	for i := 1; i < attempt; i++ {
		if wait >= 300 {
			return 300
		}
		wait *= 2
	}
	if wait > 300 {
		return 300
	}
	return wait
}

func shouldRunWatchdogScan(tickCount, scanLoops int) bool {
	if scanLoops <= 1 {
		return true
	}
	return tickCount%scanLoops == 0
}

func shouldDetectBusyWait(owner bool, detectLoops, idleCount, readyCount, inProgressCount int) bool {
	if !owner || detectLoops <= 0 {
		return false
	}
	if idleCount < detectLoops || idleCount%detectLoops != 0 {
		return false
	}
	// Empty queue without any active work is an idle state, not a stuck state.
	return readyCount > 0 || inProgressCount > 0
}

func canRunBusyWaitSelfHeal(now time.Time, state BusyWaitState, profile Profile) (bool, string) {
	if !profile.BusyWaitSelfHealEnabled {
		return false, "self-heal disabled"
	}
	if profile.BusyWaitSelfHealMaxAttempts > 0 && state.SelfHealAttempts >= profile.BusyWaitSelfHealMaxAttempts {
		return false, fmt.Sprintf("max attempts reached (%d)", profile.BusyWaitSelfHealMaxAttempts)
	}
	if profile.BusyWaitSelfHealCooldownSec > 0 && !state.LastSelfHealAt.IsZero() {
		nextAllowed := state.LastSelfHealAt.Add(time.Duration(profile.BusyWaitSelfHealCooldownSec) * time.Second)
		if now.Before(nextAllowed) {
			remaining := int(nextAllowed.Sub(now).Seconds())
			if remaining < 1 {
				remaining = 1
			}
			return false, fmt.Sprintf("cooldown active (%ds remaining)", remaining)
		}
	}
	return true, ""
}

func executeBusyWaitSelfHeal(ctx context.Context, paths Paths, profile Profile) BusyWaitHealResult {
	res := BusyWaitHealResult{}
	res.ReadyBefore, _ = CountReadyIssues(paths)
	res.InProgressBefore, _ = CountIssueFiles(paths.InProgressDir)

	recovered, recoverErr := RecoverInProgressWithCount(paths)
	res.RecoveredCount = recovered
	if recoverErr != nil {
		res.Err = fmt.Errorf("recover in-progress failed: %w", recoverErr)
	}

	cmdState := "no_cmd"
	if strings.TrimSpace(profile.BusyWaitSelfHealCmd) != "" {
		res.CmdRan = true
		res.CmdLogFile = filepath.Join(paths.LogsDir, fmt.Sprintf("busywait-self-heal-%s.log", time.Now().UTC().Format("20060102T150405Z")))
		logFile, err := os.OpenFile(res.CmdLogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			if res.Err == nil {
				res.Err = fmt.Errorf("open self-heal log file: %w", err)
			} else {
				res.Err = fmt.Errorf("%v; open self-heal log file: %w", res.Err, err)
			}
			cmdState = "cmd_log_open_failed"
		} else {
			cmd := exec.CommandContext(ctx, "bash", "-lc", profile.BusyWaitSelfHealCmd)
			cmd.Dir = paths.ProjectDir
			cmd.Stdout = logFile
			cmd.Stderr = logFile
			runErr := cmd.Run()
			_ = logFile.Close()
			if runErr != nil {
				res.CmdExitCode = exitCode(runErr)
				if res.Err == nil {
					res.Err = fmt.Errorf("self-heal cmd exit_%d", res.CmdExitCode)
				} else {
					res.Err = fmt.Errorf("%v; self-heal cmd exit_%d", res.Err, res.CmdExitCode)
				}
				cmdState = fmt.Sprintf("cmd_exit_%d", res.CmdExitCode)
			} else {
				cmdState = "cmd_ok"
			}
		}
	}

	doctorState := "doctor_skip"
	if profile.BusyWaitDoctorRepairEnabled {
		res.DoctorRepairRan = true
		actions, doctorErr := RepairProject(paths)
		doctorState = summarizeDoctorRepairActions(actions, doctorErr)
		res.DoctorRepairNote = doctorState
		if doctorErr != nil {
			if res.Err == nil {
				res.Err = fmt.Errorf("doctor repair failed: %w", doctorErr)
			} else {
				res.Err = fmt.Errorf("%v; doctor repair failed: %w", res.Err, doctorErr)
			}
		}
	}

	res.ReadyAfter, _ = CountReadyIssues(paths)
	res.Result = fmt.Sprintf("recovered=%d cmd=%s doctor=%s ready_before=%d ready_after=%d", res.RecoveredCount, cmdState, doctorState, res.ReadyBefore, res.ReadyAfter)
	return res
}

func summarizeDoctorRepairActions(actions []DoctorRepairAction, doctorErr error) string {
	if doctorErr != nil {
		return "doctor_error"
	}
	passCount := 0
	warnCount := 0
	failCount := 0
	for _, action := range actions {
		switch action.Status {
		case doctorStatusPass:
			passCount++
		case doctorStatusWarn:
			warnCount++
		case doctorStatusFail:
			failCount++
		}
	}
	return fmt.Sprintf("doctor_ok(pass=%d,warn=%d,fail=%d)", passCount, warnCount, failCount)
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if exitErr.ExitCode() >= 0 {
			return exitErr.ExitCode()
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return 130
	}
	msg := err.Error()
	for _, token := range strings.Fields(msg) {
		if n, convErr := strconv.Atoi(token); convErr == nil {
			return n
		}
	}
	return 1
}
