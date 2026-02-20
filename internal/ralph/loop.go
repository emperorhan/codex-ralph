package ralph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
		if busyWaitOwner && profile.InProgressWatchdogEnabled && shouldRunWatchdogScan(tickCount, profile.InProgressWatchdogScanLoops) {
			recovered, watchdogErr := RecoverStaleInProgressWithCount(paths, time.Duration(profile.InProgressWatchdogStaleSec)*time.Second)
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
					Detail:         fmt.Sprintf("stale_sec=%d; role_scope=%s", profile.InProgressWatchdogStaleSec, roleScopeOrAll(roleScope)),
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
					fmt.Fprintf(opts.Stdout, "[ralph-loop] no ready issues for roles=%s; global_ready=%d; sleeping %ds\n", roleScope, globalReady, profile.IdleSleepSec)
					if err := sleepOrCancel(ctx, time.Duration(profile.IdleSleepSec)*time.Second); err != nil {
						return nil
					}
					continue
				}
			}

			idleCount++

			if busyWaitOwner && profile.BusyWaitDetectLoops > 0 && idleCount >= profile.BusyWaitDetectLoops && idleCount%profile.BusyWaitDetectLoops == 0 {
				now := time.Now().UTC()
				readyBefore, _ := CountReadyIssues(paths)
				inProgressBefore, _ := CountIssueFiles(paths.InProgressDir)
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

				if profile.BusyWaitSelfHealEnabled {
					canHeal, skipReason := canRunBusyWaitSelfHeal(now, busyState, profile)
					if canHeal {
						heal := executeBusyWaitSelfHeal(ctx, paths, profile)
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

			if profile.ExitOnIdle {
				fmt.Fprintln(opts.Stdout, "[ralph-loop] no ready issues; exit_on_idle=true")
				return nil
			}
			if profile.NoReadyMaxLoops > 0 && idleCount >= profile.NoReadyMaxLoops {
				fmt.Fprintf(opts.Stdout, "[ralph-loop] no ready issues; reached no_ready_max_loops=%d\n", profile.NoReadyMaxLoops)
				return nil
			}
			fmt.Fprintf(opts.Stdout, "[ralph-loop] no ready issues; sleeping %ds\n", profile.IdleSleepSec)
			if err := sleepOrCancel(ctx, time.Duration(profile.IdleSleepSec)*time.Second); err != nil {
				return nil
			}
			continue
		}
		idleCount = 0

		if err := processIssue(ctx, paths, profile, issuePath, meta, opts.Stdout); err != nil {
			fmt.Fprintf(opts.Stdout, "[ralph-loop] issue processing error: %v\n", err)
		}
		loopCount++
	}
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

	if profile.RequireCodex {
		if err := runCodexWithRetries(ctx, paths, profile, prompt, logFile); err != nil {
			return err
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

func runCodexWithRetries(ctx context.Context, paths Paths, profile Profile, prompt string, logFile *os.File) error {
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
		err, retryable := runSingleCodexAttempt(ctx, paths, profile, prompt, logFile)
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

func runSingleCodexAttempt(ctx context.Context, paths Paths, profile Profile, prompt string, logFile *os.File) (error, bool) {
	cmdCtx := ctx
	cancel := func() {}
	if profile.CodexExecTimeoutSec > 0 {
		cmdCtx, cancel = context.WithTimeout(ctx, time.Duration(profile.CodexExecTimeoutSec)*time.Second)
	}
	defer cancel()

	codexCmd := exec.CommandContext(cmdCtx,
		"codex",
		"--ask-for-approval", profile.CodexApproval,
		"exec",
		"--model", profile.CodexModel,
		"--sandbox", profile.CodexSandbox,
		"--cd", paths.ProjectDir,
		prompt,
	)
	codexCmd.Stdout = logFile
	codexCmd.Stderr = logFile
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
	return fmt.Errorf("codex_exit_%d", code), code != 130
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
