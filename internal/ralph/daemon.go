package ralph

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func StartDaemon(paths Paths) (int, bool, error) {
	return startDaemonWithRoleScope(paths, paths.PIDFile, paths.RunnerLogFile, nil)
}

func StartRoleDaemon(paths Paths, role string) (int, bool, error) {
	role = strings.TrimSpace(role)
	if !IsSupportedRole(role) {
		return 0, false, fmt.Errorf("unsupported role: %s", role)
	}
	roleSet := map[string]struct{}{role: {}}
	return startDaemonWithRoleScope(paths, paths.RolePIDFile(role), paths.RoleRunnerLogFile(role), roleSet)
}

func StopDaemon(paths Paths) error {
	if err := SetEnabled(paths, false); err != nil {
		return err
	}
	if err := stopDaemonByPIDFile(paths.PIDFile); err != nil {
		return err
	}
	for _, role := range RequiredAgentRoles {
		if err := stopDaemonByPIDFile(paths.RolePIDFile(role)); err != nil {
			return err
		}
	}
	return RecoverInProgress(paths)
}

func StopPrimaryDaemon(paths Paths) error {
	return stopDaemonByPIDFile(paths.PIDFile)
}

func StopRoleDaemon(paths Paths, role string) error {
	role = strings.TrimSpace(role)
	if !IsSupportedRole(role) {
		return fmt.Errorf("unsupported role: %s", role)
	}
	return stopDaemonByPIDFile(paths.RolePIDFile(role))
}

func RunningRoleDaemons(paths Paths) ([]string, map[string]int) {
	running := []string{}
	pids := map[string]int{}
	for _, role := range RequiredAgentRoles {
		pid, ok := daemonPIDFromFile(paths.RolePIDFile(role))
		if ok {
			running = append(running, role)
			pids[role] = pid
		}
	}
	return running, pids
}

func RunSupervisor(ctx context.Context, paths Paths, profile Profile, allowedRoles map[string]struct{}, stdout io.Writer) error {
	if stdout == nil {
		stdout = os.Stdout
	}
	if err := EnsureLayout(paths); err != nil {
		return err
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}

	roleScope := RoleSetCSV(allowedRoles)
	restartDelaySec := profile.SupervisorRestartDelaySec
	if restartDelaySec < 0 {
		restartDelaySec = 0
	}

	for {
		if err := ctx.Err(); err != nil {
			fmt.Fprintln(stdout, "[ralph-supervisor] interrupted; stopping")
			return nil
		}
		enabled, err := IsEnabled(paths)
		if err != nil {
			fmt.Fprintf(stdout, "[ralph-supervisor] warning: read enabled state failed: %v\n", err)
			if err := sleepOrCancel(ctx, 2*time.Second); err != nil {
				return nil
			}
			continue
		}
		if !enabled {
			fmt.Fprintln(stdout, "[ralph-supervisor] disabled; stopping")
			return nil
		}

		args := []string{
			"--control-dir", paths.ControlDir,
			"--project-dir", paths.ProjectDir,
			"run",
			"--max-loops", "0",
		}
		if roleScope != "" {
			args = append(args, "--roles", roleScope)
		}

		fmt.Fprintf(stdout, "[ralph-supervisor] starting worker (roles=%s)\n", roleScopeOrAll(roleScope))
		worker := exec.CommandContext(ctx, exe, args...)
		worker.Stdout = stdout
		worker.Stderr = stdout
		runErr := worker.Run()
		if ctx.Err() != nil {
			fmt.Fprintln(stdout, "[ralph-supervisor] interrupted; stopping")
			return nil
		}

		enabledAfter, enabledErr := IsEnabled(paths)
		if enabledErr == nil && !enabledAfter {
			fmt.Fprintln(stdout, "[ralph-supervisor] disabled; stopping")
			return nil
		}
		if runErr == nil {
			fmt.Fprintln(stdout, "[ralph-supervisor] worker exited; restarting")
		} else {
			fmt.Fprintf(stdout, "[ralph-supervisor] worker exited (rc=%d); restarting\n", exitCode(runErr))
		}
		if restartDelaySec > 0 {
			fmt.Fprintf(stdout, "[ralph-supervisor] restart delay: %ds\n", restartDelaySec)
			if err := sleepOrCancel(ctx, time.Duration(restartDelaySec)*time.Second); err != nil {
				return nil
			}
		}
	}
}

func startDaemonWithRoleScope(paths Paths, pidFile, logFile string, allowedRoles map[string]struct{}) (int, bool, error) {
	if err := EnsureLayout(paths); err != nil {
		return 0, false, err
	}
	if err := SetEnabled(paths, true); err != nil {
		return 0, false, err
	}

	if pid, running := daemonPIDFromFile(pidFile); running {
		return pid, true, nil
	}
	profile, err := LoadProfile(paths)
	if err != nil {
		return 0, false, err
	}

	exe, err := os.Executable()
	if err != nil {
		return 0, false, fmt.Errorf("resolve executable: %w", err)
	}

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, false, fmt.Errorf("open daemon log: %w", err)
	}
	defer f.Close()

	args := []string{
		"--control-dir", paths.ControlDir,
		"--project-dir", paths.ProjectDir,
	}
	roleScope := RoleSetCSV(allowedRoles)
	if profile.SupervisorEnabled {
		args = append(args, "supervise")
		if roleScope != "" {
			args = append(args, "--roles", roleScope)
		}
	} else {
		args = append(args, "run", "--max-loops", "0")
		if roleScope != "" {
			args = append(args, "--roles", roleScope)
		}
	}

	cmd := exec.Command(exe, args...)
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		return 0, false, fmt.Errorf("start daemon: %w", err)
	}
	pid := cmd.Process.Pid
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
		return 0, false, fmt.Errorf("write pid file: %w", err)
	}
	_ = cmd.Process.Release()
	return pid, false, nil
}

func stopDaemonByPIDFile(pidFile string) error {
	pid, running := daemonPIDFromFile(pidFile)
	if !running {
		_ = os.Remove(pidFile)
		return nil
	}

	proc, err := os.FindProcess(pid)
	if err == nil {
		_ = proc.Signal(syscall.SIGTERM)
	}
	for i := 0; i < 30; i++ {
		if !isPIDRunning(pid) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if isPIDRunning(pid) {
		if proc, findErr := os.FindProcess(pid); findErr == nil {
			_ = proc.Signal(syscall.SIGKILL)
		}
	}
	_ = os.Remove(pidFile)
	return nil
}

func daemonPID(paths Paths) (int, bool) {
	return daemonPIDFromFile(paths.PIDFile)
}

func daemonPIDFromFile(pidFile string) (int, bool) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, false
	}
	raw := strings.TrimSpace(string(data))
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		return 0, false
	}
	if !isPIDRunning(pid) {
		return pid, false
	}
	return pid, true
}

func isPIDRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = proc.Signal(syscall.Signal(0))
	return err == nil
}

func TailRunner(paths Paths, lines int, follow bool) error {
	if lines <= 0 {
		lines = 120
	}
	if err := EnsureLayout(paths); err != nil {
		return err
	}

	args := []string{"-n", strconv.Itoa(lines)}
	if follow {
		args = append(args, "-f")
	}
	args = append(args, paths.RunnerLogFile)

	cmd := exec.Command("tail", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}
