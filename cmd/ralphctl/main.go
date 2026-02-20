package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"codex-ralph/internal/ralph"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	defaultControl := defaultControlDir(cwd)
	global := flag.NewFlagSet("ralphctl", flag.ContinueOnError)
	global.SetOutput(os.Stderr)
	controlDir := global.String("control-dir", defaultControl, "directory that stores shared plugins and fleet config")
	projectDir := global.String("project-dir", cwd, "target project directory (.ralph lives here)")

	global.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl [--control-dir DIR] [--project-dir DIR] <command> [args]")
		fmt.Fprintln(os.Stderr, "Commands: list-plugins, install, apply-plugin, registry, setup, reload, init, on, off, new, import-prd, recover, doctor, run, supervise, start, stop, restart, status, tail, service, fleet, telegram")
	}

	if err := global.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	args := global.Args()
	if len(args) == 0 {
		global.Usage()
		return fmt.Errorf("command is required")
	}

	cmd := args[0]
	cmdArgs := args[1:]

	if commandNeedsControlAssets(cmd) {
		if err := ralph.EnsureDefaultControlAssets(*controlDir); err != nil {
			return err
		}
	}

	if cmd == "fleet" {
		return runFleetCommand(*controlDir, cmdArgs)
	}
	if cmd == "registry" {
		return runRegistryCommand(*controlDir, cmdArgs)
	}
	if cmd == "service" {
		paths, err := ralph.NewPaths(*controlDir, *projectDir)
		if err != nil {
			return err
		}
		return runServiceCommand(paths, cmdArgs)
	}
	if cmd == "telegram" {
		paths, err := ralph.NewPaths(*controlDir, *projectDir)
		if err != nil {
			return err
		}
		return runTelegramCommand(*controlDir, paths, cmdArgs)
	}

	paths, err := ralph.NewPaths(*controlDir, *projectDir)
	if err != nil {
		return err
	}

	switch cmd {
	case "list-plugins":
		plugins, err := ralph.ListPlugins(paths.ControlDir)
		if err != nil {
			return err
		}
		for _, p := range plugins {
			fmt.Println(p)
		}
		return nil

	case "install":
		fs := flag.NewFlagSet("install", flag.ContinueOnError)
		plugin := fs.String("plugin", "universal-default", "plugin name")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		exe, err := executablePath()
		if err != nil {
			return err
		}
		if err := ralph.Install(paths, *plugin, exe); err != nil {
			return err
		}
		fmt.Println("Ralph Runtime Installed")
		fmt.Println("=======================")
		fmt.Printf("Control Dir:  %s\n", paths.ControlDir)
		fmt.Printf("Project Dir:  %s\n", paths.ProjectDir)
		fmt.Printf("Plugin:       %s\n", *plugin)
		fmt.Printf("Helper:       %s\n", filepath.Join(paths.ProjectDir, "ralph"))
		fmt.Printf("Profile YAML: %s\n", paths.ProfileYAMLFile)
		return nil

	case "apply-plugin":
		fs := flag.NewFlagSet("apply-plugin", flag.ContinueOnError)
		plugin := fs.String("plugin", "", "plugin name")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		if strings.TrimSpace(*plugin) == "" {
			return fmt.Errorf("--plugin is required")
		}
		if err := ralph.ApplyPlugin(paths, *plugin); err != nil {
			return err
		}
		fmt.Println("Plugin Applied")
		fmt.Println("==============")
		fmt.Printf("Plugin:            %s\n", *plugin)
		fmt.Printf("Profile YAML:      %s\n", paths.ProfileYAMLFile)
		fmt.Printf("Profile Override:  %s\n", paths.ProfileFile)
		return nil

	case "setup":
		fs := flag.NewFlagSet("setup", flag.ContinueOnError)
		plugin := fs.String("plugin", "", "preferred default plugin in wizard")
		nonInteractive := fs.Bool("non-interactive", false, "apply defaults without prompts")
		advanced := fs.Bool("advanced", false, "run interactive setup wizard")
		modeRaw := fs.String("mode", "", "deprecated: use --advanced")
		startAfter := fs.Bool("start", true, "start daemon after setup completes")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		exe, err := executablePath()
		if err != nil {
			return err
		}
		legacyMode := strings.ToLower(strings.TrimSpace(*modeRaw))
		if legacyMode != "" {
			switch legacyMode {
			case "advanced":
				*advanced = true
			case "quickstart", "remote":
				*advanced = false
			default:
				return fmt.Errorf("invalid --mode: %s (expected quickstart|advanced|remote)", legacyMode)
			}
		}
		if *nonInteractive {
			*advanced = false
		}

		if *advanced {
			if err := ralph.RunSetupWizard(paths, exe, *plugin, os.Stdin, os.Stdout); err != nil {
				return err
			}
		} else {
			selection := ralph.DefaultSetupSelections(strings.TrimSpace(*plugin))
			if err := ralph.ApplySetupSelections(paths, exe, selection); err != nil {
				return err
			}
		}
		if err := ralph.ApplyStabilityDefaults(paths); err != nil {
			return err
		}
		fmt.Println("Setup Complete")
		fmt.Println("==============")
		fmt.Printf("Helper:                %s\n", filepath.Join(paths.ProjectDir, "ralph"))
		fmt.Printf("Profile YAML:          %s\n", paths.ProfileYAMLFile)
		fmt.Printf("Profile Local YAML:    %s\n", paths.ProfileLocalYAMLFile)
		fmt.Printf("Profile Env Override:  %s\n", paths.ProfileLocalFile)
		fmt.Println()
		fmt.Println("Defaults")
		fmt.Println("- timeout/retry + watchdog + supervisor: enabled")
		fmt.Println("- runtime profile reload: automatic (loop boundary)")
		fmt.Println("- supervisor settings changes: daemon restart required")
		if *startAfter {
			startResult, err := startProjectDaemon(paths, startOptions{
				DoctorRepair: true,
				FixPerms:     true,
				Out:          os.Stdout,
			})
			if err != nil {
				return err
			}
			fmt.Printf("Daemon: %s\n", startResult)
		}
		return nil

	case "reload":
		fs := flag.NewFlagSet("reload", flag.ContinueOnError)
		restartRunning := fs.Bool("restart-running", true, "restart loop/telegram daemons that were running before reload")
		telegram := fs.Bool("telegram", true, "reload telegram daemon when it is running")
		currentOnly := fs.Bool("current-only", false, "reload only current project")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		exe, err := executablePath()
		if err != nil {
			return err
		}
		results, err := reloadConnectedProjects(*controlDir, paths, exe, reloadOptions{
			RestartRunning: *restartRunning,
			ReloadTelegram: *telegram,
			CurrentOnly:    *currentOnly,
		})
		if err != nil {
			return err
		}
		printReloadSummary(os.Stdout, exe, *controlDir, results)
		return nil

	case "init":
		if err := ralph.EnsureLayout(paths); err != nil {
			return err
		}
		fmt.Printf("initialized: %s\n", paths.RalphDir)
		return nil

	case "on":
		if err := ralph.SetEnabled(paths, true); err != nil {
			return err
		}
		fmt.Println("ralph_local_enabled=true")
		return nil

	case "off":
		if err := ralph.SetEnabled(paths, false); err != nil {
			return err
		}
		fmt.Println("ralph_local_enabled=false")
		return nil

	case "new":
		fs := flag.NewFlagSet("new", flag.ContinueOnError)
		priority := fs.Int("priority", 0, "optional priority (lower value runs first)")
		storyID := fs.String("story-id", "", "optional external story id")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		args := fs.Args()
		if len(args) < 2 {
			return fmt.Errorf("usage: new [--priority N] [--story-id ID] <manager|planner|developer|qa> <title>")
		}
		role := args[0]
		title := strings.Join(args[1:], " ")
		path, _, err := ralph.CreateIssueWithOptions(paths, role, title, ralph.IssueCreateOptions{
			Priority: *priority,
			StoryID:  *storyID,
		})
		if err != nil {
			return err
		}
		fmt.Printf("created: %s\n", path)
		return nil

	case "import-prd":
		fs := flag.NewFlagSet("import-prd", flag.ContinueOnError)
		file := fs.String("file", "prd.json", "path to prd json file")
		defaultRole := fs.String("default-role", "developer", "fallback role for stories with missing/invalid role")
		dryRun := fs.Bool("dry-run", false, "preview without creating issues")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		result, err := ralph.ImportPRDStories(paths, *file, *defaultRole, *dryRun)
		if err != nil {
			return err
		}
		fmt.Println("prd import summary")
		fmt.Printf("- source: %s\n", result.SourcePath)
		fmt.Printf("- dry_run: %t\n", result.DryRun)
		fmt.Printf("- stories_total: %d\n", result.StoriesTotal)
		fmt.Printf("- imported: %d\n", result.Imported)
		fmt.Printf("- skipped_passed: %d\n", result.SkippedPassed)
		fmt.Printf("- skipped_existing: %d\n", result.SkippedExisting)
		fmt.Printf("- skipped_invalid: %d\n", result.SkippedInvalid)
		for _, createdPath := range result.CreatedPaths {
			fmt.Printf("- created: %s\n", createdPath)
		}
		return nil

	case "recover":
		recovered, err := ralph.RecoverInProgressWithCount(paths)
		if err != nil {
			return err
		}
		fmt.Printf("recovered in-progress issues: %d\n", recovered)
		return nil

	case "doctor":
		fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
		strict := fs.Bool("strict", false, "exit with error when failing checks are found")
		repair := fs.Bool("repair", false, "run safe repair actions before checks")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		if *repair {
			actions, err := ralph.RepairProject(paths)
			if err != nil {
				return err
			}
			fmt.Println("## Ralph Doctor Repair")
			for _, action := range actions {
				fmt.Printf("- [%s] %s: %s\n", action.Status, action.Name, action.Detail)
			}
		}
		report, err := ralph.RunDoctor(paths)
		if err != nil {
			return err
		}
		report.Print(os.Stdout)
		if *strict && report.HasFailures() {
			return fmt.Errorf("doctor reported failing checks")
		}
		return nil

	case "run":
		fs := flag.NewFlagSet("run", flag.ContinueOnError)
		maxLoops := fs.Int("max-loops", 1, "0 means infinite")
		rolesRaw := fs.String("roles", "", "comma-separated role scope (manager,planner,developer,qa)")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		allowedRoles, err := ralph.ParseRolesCSV(*rolesRaw)
		if err != nil {
			return err
		}
		profile, err := ralph.LoadProfile(paths)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return ralph.RunLoop(ctx, paths, profile, ralph.RunOptions{MaxLoops: *maxLoops, Stdout: os.Stdout, AllowedRoles: allowedRoles})

	case "supervise":
		fs := flag.NewFlagSet("supervise", flag.ContinueOnError)
		rolesRaw := fs.String("roles", "", "comma-separated role scope (manager,planner,developer,qa)")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		allowedRoles, err := ralph.ParseRolesCSV(*rolesRaw)
		if err != nil {
			return err
		}
		profile, err := ralph.LoadProfile(paths)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return ralph.RunSupervisor(ctx, paths, profile, allowedRoles, os.Stdout)

	case "start":
		fs := flag.NewFlagSet("start", flag.ContinueOnError)
		doctorRepair := fs.Bool("doctor-repair", true, "run doctor --repair before start")
		fixPerms := fs.Bool("fix-perms", false, "normalize project/control permissions before repair/start")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		startResult, err := startProjectDaemon(paths, startOptions{
			DoctorRepair: *doctorRepair,
			FixPerms:     *fixPerms,
			Out:          os.Stdout,
		})
		if err != nil {
			return err
		}
		fmt.Println("Ralph Loop")
		fmt.Println("==========")
		fmt.Println(startResult)
		return nil

	case "stop":
		if err := ralph.StopDaemon(paths); err != nil {
			return err
		}
		fmt.Println("Ralph Loop")
		fmt.Println("==========")
		fmt.Println("ralph-loop stopped")
		return nil

	case "restart":
		if err := ralph.StopDaemon(paths); err != nil {
			return err
		}
		pid, _, err := ralph.StartDaemon(paths)
		if err != nil {
			return err
		}
		fmt.Println("Ralph Loop")
		fmt.Println("==========")
		fmt.Printf("ralph-loop restarted (pid=%d)\n", pid)
		return nil

	case "status":
		st, err := ralph.GetStatus(paths)
		if err != nil {
			return err
		}
		st.Print(os.Stdout)
		return nil

	case "tail":
		fs := flag.NewFlagSet("tail", flag.ContinueOnError)
		lines := fs.Int("lines", 120, "number of lines")
		follow := fs.Bool("follow", true, "follow appended lines")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		return ralph.TailRunner(paths, *lines, *follow)

	default:
		global.Usage()
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func runRegistryCommand(controlDir string, args []string) error {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --control-dir DIR registry <subcommand>")
		fmt.Fprintln(os.Stderr, "Subcommands: generate, list, verify")
	}
	if len(args) == 0 {
		usage()
		return fmt.Errorf("registry subcommand is required")
	}

	switch args[0] {
	case "generate":
		reg, err := ralph.GeneratePluginRegistry(controlDir)
		if err != nil {
			return err
		}
		if err := ralph.SavePluginRegistry(controlDir, reg); err != nil {
			return err
		}
		fmt.Println("plugin registry generated")
		fmt.Printf("- path: %s\n", ralph.PluginRegistryPath(controlDir))
		fmt.Printf("- version: %d\n", reg.Version)
		fmt.Printf("- generated_at_utc: %s\n", reg.GeneratedAtUTC)
		fmt.Printf("- plugins: %d\n", len(reg.Plugins))
		return nil

	case "list":
		reg, err := ralph.LoadPluginRegistry(controlDir)
		if err != nil {
			return err
		}
		if len(reg.Plugins) == 0 {
			fmt.Println("plugin registry is empty")
			return nil
		}
		fmt.Println("## Plugin Registry")
		fmt.Printf("- path: %s\n", ralph.PluginRegistryPath(controlDir))
		fmt.Printf("- generated_at_utc: %s\n", reg.GeneratedAtUTC)
		for _, entry := range reg.Plugins {
			fmt.Printf("- name=%s file=%s sha256=%s\n", entry.Name, entry.File, entry.SHA256)
		}
		return nil

	case "verify":
		checks, err := ralph.VerifyPluginRegistry(controlDir)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("plugin registry not found (%s); run: ralphctl --control-dir %s registry generate", ralph.PluginRegistryPath(controlDir), controlDir)
			}
			return err
		}
		failures := ralph.RegistryFailureCount(checks)
		fmt.Println("## Plugin Registry Verify")
		fmt.Printf("- path: %s\n", ralph.PluginRegistryPath(controlDir))
		for _, check := range checks {
			fmt.Printf("- [%s] %s: %s\n", check.Status, check.Name, check.Detail)
		}
		if failures > 0 {
			return fmt.Errorf("plugin registry verification failed: %d issue(s)", failures)
		}
		fmt.Println("plugin registry verification passed")
		return nil

	default:
		usage()
		return fmt.Errorf("unknown registry subcommand: %s", args[0])
	}
}

type startOptions struct {
	DoctorRepair bool
	FixPerms     bool
	Out          io.Writer
}

type reloadOptions struct {
	RestartRunning bool
	ReloadTelegram bool
	CurrentOnly    bool
}

type reloadTarget struct {
	ID        string
	Paths     ralph.Paths
	Source    string
	IsCurrent bool
}

type reloadProjectResult struct {
	ID                 string
	ProjectDir         string
	Source             string
	WrapperUpdated     bool
	PrimaryWasRunning  bool
	PrimaryPID         int
	PrimaryRestarted   bool
	RoleWorkers        []string
	TelegramWasRunning bool
	TelegramPID        int
	TelegramRestarted  bool
}

func startProjectDaemon(paths ralph.Paths, opts startOptions) (string, error) {
	out := opts.Out
	if out == nil {
		out = os.Stdout
	}

	if opts.FixPerms {
		fixResult, err := ralph.AutoFixPermissions(paths)
		if err != nil {
			return "", err
		}
		if len(fixResult.UpdatedPaths) > 0 {
			fmt.Fprintf(out, "permission fix: updated %d path(s)\n", len(fixResult.UpdatedPaths))
		} else {
			fmt.Fprintln(out, "permission fix: no changes")
		}
	}

	if opts.DoctorRepair {
		actions, err := ralph.RepairProject(paths)
		if err != nil {
			return "", err
		}
		fmt.Fprintln(out, "## Start Preflight (doctor --repair)")
		for _, action := range actions {
			fmt.Fprintf(out, "- [%s] %s: %s\n", action.Status, action.Name, action.Detail)
		}
	}

	pid, already, err := ralph.StartDaemon(paths)
	if err != nil {
		return "", err
	}
	if already {
		return fmt.Sprintf("ralph-loop already running (pid=%d)", pid), nil
	}
	return fmt.Sprintf("ralph-loop started (pid=%d)", pid), nil
}

func reloadConnectedProjects(controlDir string, currentPaths ralph.Paths, executable string, opts reloadOptions) ([]reloadProjectResult, error) {
	targets, err := resolveReloadTargets(controlDir, currentPaths, opts.CurrentOnly)
	if err != nil {
		return nil, err
	}
	results := make([]reloadProjectResult, 0, len(targets))
	for _, target := range targets {
		res, err := reloadSingleProject(target, executable, opts)
		if err != nil {
			return nil, fmt.Errorf("reload project %s (%s): %w", target.ID, target.Paths.ProjectDir, err)
		}
		results = append(results, res)
	}
	return results, nil
}

func resolveReloadTargets(controlDir string, currentPaths ralph.Paths, currentOnly bool) ([]reloadTarget, error) {
	targetByDir := map[string]reloadTarget{}
	add := func(t reloadTarget) {
		key := t.Paths.ProjectDir
		if key == "" {
			return
		}
		existing, ok := targetByDir[key]
		if !ok {
			targetByDir[key] = t
			return
		}
		if existing.Source == "current" && t.Source == "fleet" {
			t.IsCurrent = existing.IsCurrent
			targetByDir[key] = t
			return
		}
		if t.IsCurrent {
			existing.IsCurrent = true
			targetByDir[key] = existing
		}
	}

	includeCurrent := projectLooksManaged(currentPaths)
	if currentOnly {
		if !includeCurrent {
			return nil, fmt.Errorf("current project is not managed yet (run setup first or omit --current-only)")
		}
		add(reloadTarget{
			ID:        "current",
			Paths:     currentPaths,
			Source:    "current",
			IsCurrent: true,
		})
	} else {
		if includeCurrent {
			add(reloadTarget{
				ID:        "current",
				Paths:     currentPaths,
				Source:    "current",
				IsCurrent: true,
			})
		}

		cfg, err := ralph.LoadFleetConfig(controlDir)
		if err != nil {
			return nil, err
		}
		for _, p := range cfg.Projects {
			paths, err := ralph.NewPaths(controlDir, p.ProjectDir)
			if err != nil {
				return nil, err
			}
			add(reloadTarget{
				ID:        p.ID,
				Paths:     paths,
				Source:    "fleet",
				IsCurrent: filepath.Clean(paths.ProjectDir) == filepath.Clean(currentPaths.ProjectDir),
			})
		}
	}

	if len(targetByDir) == 0 {
		return nil, fmt.Errorf("no connected projects found (fleet empty and current project unmanaged)")
	}

	targets := make([]reloadTarget, 0, len(targetByDir))
	for _, t := range targetByDir {
		targets = append(targets, t)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].IsCurrent != targets[j].IsCurrent {
			return targets[i].IsCurrent
		}
		return targets[i].ID < targets[j].ID
	})
	return targets, nil
}

func projectLooksManaged(paths ralph.Paths) bool {
	if _, err := os.Stat(filepath.Join(paths.ProjectDir, "ralph")); err == nil {
		return true
	}
	if _, err := os.Stat(paths.RalphDir); err == nil {
		return true
	}
	if _, running, _ := telegramPIDState(paths.PIDFile); running {
		return true
	}
	_, rolePIDs := ralph.RunningRoleDaemons(paths)
	return len(rolePIDs) > 0
}

func reloadSingleProject(target reloadTarget, executable string, opts reloadOptions) (reloadProjectResult, error) {
	paths := target.Paths
	if err := ralph.EnsureLayout(paths); err != nil {
		return reloadProjectResult{}, err
	}

	primaryPID, primaryRunning, _ := telegramPIDState(paths.PIDFile)
	roleWorkers, _ := ralph.RunningRoleDaemons(paths)
	sort.Strings(roleWorkers)
	telegramPID, telegramRunning, _ := telegramPIDState(paths.TelegramPIDFile())

	res := reloadProjectResult{
		ID:                 target.ID,
		ProjectDir:         paths.ProjectDir,
		Source:             target.Source,
		PrimaryWasRunning:  primaryRunning,
		PrimaryPID:         primaryPID,
		RoleWorkers:        append([]string(nil), roleWorkers...),
		TelegramWasRunning: telegramRunning,
		TelegramPID:        telegramPID,
	}

	if opts.RestartRunning {
		for _, role := range roleWorkers {
			if err := ralph.StopRoleDaemon(paths, role); err != nil {
				return res, err
			}
		}
		if primaryRunning {
			if err := ralph.StopPrimaryDaemon(paths); err != nil {
				return res, err
			}
		}
		if opts.ReloadTelegram && telegramRunning {
			if _, err := stopTelegramDaemon(paths); err != nil {
				return res, err
			}
		}
	}

	if err := ralph.WriteProjectWrapper(paths, executable); err != nil {
		return res, err
	}
	res.WrapperUpdated = true

	if !opts.RestartRunning {
		return res, nil
	}

	for _, role := range roleWorkers {
		_, _, err := ralph.StartRoleDaemon(paths, role)
		if err != nil {
			return res, err
		}
	}
	if len(roleWorkers) > 0 {
		res.PrimaryRestarted = true
	}
	if primaryRunning {
		_, _, err := ralph.StartDaemon(paths)
		if err != nil {
			return res, err
		}
		res.PrimaryRestarted = true
	}
	if opts.ReloadTelegram && telegramRunning {
		runArgs := ensureTelegramForegroundArg([]string{"--config-file", telegramConfigFileFromArgs(paths.ControlDir, nil)})
		if _, err := startTelegramDaemon(paths, runArgs); err != nil {
			return res, err
		}
		res.TelegramRestarted = true
	}
	return res, nil
}

func printReloadSummary(out io.Writer, executable, controlDir string, results []reloadProjectResult) {
	fmt.Fprintln(out, "Ralph Reload")
	fmt.Fprintln(out, "============")
	fmt.Fprintf(out, "- control_dir: %s\n", controlDir)
	fmt.Fprintf(out, "- binary: %s\n", executable)
	fmt.Fprintf(out, "- projects: %d\n", len(results))
	for _, res := range results {
		fmt.Fprintf(out, "\n[%s] %s\n", res.ID, res.ProjectDir)
		fmt.Fprintf(out, "- source: %s\n", res.Source)
		fmt.Fprintf(out, "- wrapper: updated\n")
		fmt.Fprintf(out, "- daemon_primary: %s\n", reloadRunStateLabel(res.PrimaryWasRunning, res.PrimaryRestarted, res.PrimaryPID))
		if len(res.RoleWorkers) == 0 {
			fmt.Fprintf(out, "- daemon_roles: none\n")
		} else {
			fmt.Fprintf(out, "- daemon_roles: %s\n", strings.Join(res.RoleWorkers, ","))
		}
		fmt.Fprintf(out, "- telegram: %s\n", reloadRunStateLabel(res.TelegramWasRunning, res.TelegramRestarted, res.TelegramPID))
	}
}

func reloadRunStateLabel(wasRunning, restarted bool, pid int) string {
	if !wasRunning {
		return "not-running"
	}
	if restarted {
		return fmt.Sprintf("restarted(previous_pid=%d)", pid)
	}
	return fmt.Sprintf("running(previous_pid=%d)", pid)
}

func runServiceCommand(paths ralph.Paths, args []string) error {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --control-dir DIR --project-dir DIR service <subcommand> [args]")
		fmt.Fprintln(os.Stderr, "Subcommands: install, uninstall, status")
	}
	if len(args) == 0 {
		usage()
		return fmt.Errorf("service subcommand is required")
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "install":
		fs := flag.NewFlagSet("service install", flag.ContinueOnError)
		name := fs.String("name", "", "service name (default: ralph-<project-dir>)")
		startNow := fs.Bool("start", true, "enable/start service immediately")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		exe, err := executablePath()
		if err != nil {
			return err
		}
		result, err := ralph.InstallService(paths, exe, *name, *startNow)
		if err != nil {
			return err
		}
		fmt.Println("service installed")
		fmt.Printf("- platform: %s\n", result.Platform)
		fmt.Printf("- service: %s\n", result.ServiceName)
		fmt.Printf("- unit_path: %s\n", result.UnitPath)
		fmt.Printf("- activated: %t\n", result.Activated)
		hint := ralph.ServiceInstallHint(result.Platform)
		if hint != "" {
			fmt.Printf("- hint: %s\n", hint)
		}
		for _, warn := range result.Warnings {
			fmt.Printf("- warning: %s\n", warn)
		}
		return nil

	case "uninstall":
		fs := flag.NewFlagSet("service uninstall", flag.ContinueOnError)
		name := fs.String("name", "", "service name (default: ralph-<project-dir>)")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		result, err := ralph.UninstallService(paths, *name)
		if err != nil {
			return err
		}
		fmt.Println("service uninstalled")
		fmt.Printf("- platform: %s\n", result.Platform)
		fmt.Printf("- service: %s\n", result.ServiceName)
		fmt.Printf("- unit_path: %s\n", result.UnitPath)
		for _, warn := range result.Warnings {
			fmt.Printf("- warning: %s\n", warn)
		}
		return nil

	case "status":
		fs := flag.NewFlagSet("service status", flag.ContinueOnError)
		name := fs.String("name", "", "service name (default: ralph-<project-dir>)")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		status, err := ralph.GetServiceStatus(paths, *name)
		if err != nil {
			return err
		}
		fmt.Println("## Service Status")
		fmt.Printf("- platform: %s\n", status.Platform)
		fmt.Printf("- service: %s\n", status.ServiceName)
		fmt.Printf("- installed: %t\n", status.Installed)
		fmt.Printf("- active: %t\n", status.Active)
		if strings.TrimSpace(status.Detail) != "" {
			fmt.Printf("- detail: %s\n", status.Detail)
		}
		return nil

	default:
		usage()
		return fmt.Errorf("unknown service subcommand: %s", sub)
	}
}

func renderFleetDashboard(controlDir, projectID string, all bool, out io.Writer) error {
	projects, err := ralph.ResolveFleetProjects(controlDir, projectID, all)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "## Fleet Dashboard")
	fmt.Fprintf(out, "- updated_utc: %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(out, "- control_dir: %s\n", controlDir)
	fmt.Fprintf(out, "- projects: %d\n", len(projects))
	for _, p := range projects {
		paths, err := ralph.NewPaths(controlDir, p.ProjectDir)
		if err != nil {
			return err
		}
		st, err := ralph.GetStatus(paths)
		if err != nil {
			return err
		}
		roles, rolePIDs := ralph.RunningRoleDaemons(paths)
		fmt.Fprintf(
			out,
			"- project=%s plugin=%s daemon=%s ready=%d in_progress=%d done=%d blocked=%d\n",
			p.ID,
			p.Plugin,
			st.Daemon,
			st.QueueReady,
			st.InProgress,
			st.Done,
			st.Blocked,
		)
		if len(roles) > 0 {
			roleLine := []string{}
			for _, role := range ralph.RequiredAgentRoles {
				pid, ok := rolePIDs[role]
				if !ok {
					continue
				}
				roleLine = append(roleLine, fmt.Sprintf("%s:%d", role, pid))
			}
			if len(roleLine) > 0 {
				fmt.Fprintf(out, "  workers=%s\n", strings.Join(roleLine, ","))
			}
		}
		if st.LastProfileReloadAt != "" || st.ProfileReloadCount > 0 {
			fmt.Fprintf(
				out,
				"  profile_reload_at=%s | profile_reload_count=%d\n",
				valueOrDash(st.LastProfileReloadAt),
				st.ProfileReloadCount,
			)
		}
		if st.LastFailureCause != "" || st.LastCodexRetryCount > 0 || st.LastPermissionStreak > 0 {
			fmt.Fprintf(
				out,
				"  last_failure=%s | codex_retries=%d | perm_streak=%d\n",
				compactSingleLine(st.LastFailureCause, 120),
				st.LastCodexRetryCount,
				st.LastPermissionStreak,
			)
		}
	}
	return nil
}

func sleepOrInterrupt(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func runFleetCommand(controlDir string, args []string) error {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --control-dir DIR fleet <subcommand> [args]")
		fmt.Fprintln(os.Stderr, "Subcommands: interactive, register, unregister, list, start, stop, status, dashboard, apply-plugin, bootstrap")
	}
	if len(args) == 0 {
		return runFleetInteractive(controlDir)
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "interactive", "ui":
		return runFleetInteractive(controlDir)

	case "register":
		fs := flag.NewFlagSet("fleet register", flag.ContinueOnError)
		id := fs.String("id", "", "fleet project id")
		projectDir := fs.String("project-dir", "", "project directory")
		plugin := fs.String("plugin", "universal-default", "plugin name")
		prdPath := fs.String("prd", "PRD.md", "project PRD path")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		fp, err := ralph.RegisterFleetProject(controlDir, *id, *projectDir, *plugin, *prdPath)
		if err != nil {
			return err
		}

		paths, err := ralph.NewPaths(controlDir, fp.ProjectDir)
		if err != nil {
			return err
		}
		exe, err := executablePath()
		if err != nil {
			return err
		}
		if err := ralph.EnsureFleetProjectInstalled(paths, fp.Plugin, exe); err != nil {
			return err
		}
		if err := ralph.EnsureFleetAgentSetFile(paths, fp); err != nil {
			return err
		}
		created, err := ralph.EnsureRoleBootstrapIssues(paths, fp.PRDPath)
		if err != nil {
			return err
		}
		fmt.Println("fleet project registered")
		fmt.Printf("- id: %s\n", fp.ID)
		fmt.Printf("- project_dir: %s\n", fp.ProjectDir)
		fmt.Printf("- plugin: %s\n", fp.Plugin)
		fmt.Printf("- assigned_roles: %s\n", strings.Join(fp.AssignedRoles, ","))
		fmt.Printf("- bootstrap_created: %d\n", len(created))
		return nil

	case "unregister":
		fs := flag.NewFlagSet("fleet unregister", flag.ContinueOnError)
		id := fs.String("id", "", "fleet project id")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		if strings.TrimSpace(*id) == "" {
			return fmt.Errorf("--id is required")
		}

		cfg, err := ralph.LoadFleetConfig(controlDir)
		if err != nil {
			return err
		}
		if fp, ok := ralph.FindFleetProject(cfg, *id); ok {
			paths, pathErr := ralph.NewPaths(controlDir, fp.ProjectDir)
			if pathErr == nil {
				_ = ralph.SetEnabled(paths, false)
				_ = ralph.StopPrimaryDaemon(paths)
				for _, role := range fp.AssignedRoles {
					_ = ralph.StopRoleDaemon(paths, role)
				}
				_ = ralph.RecoverInProgress(paths)
			}
		}

		if err := ralph.UnregisterFleetProject(controlDir, *id); err != nil {
			return err
		}
		fmt.Printf("fleet project unregistered: %s\n", *id)
		return nil

	case "list":
		cfg, err := ralph.LoadFleetConfig(controlDir)
		if err != nil {
			return err
		}
		if len(cfg.Projects) == 0 {
			fmt.Println("fleet is empty")
			return nil
		}
		fmt.Println("## Fleet Projects")
		for _, p := range cfg.Projects {
			fmt.Printf("- id=%s project_dir=%s plugin=%s roles=%s prd=%s\n", p.ID, p.ProjectDir, p.Plugin, strings.Join(p.AssignedRoles, ","), p.PRDPath)
		}
		return nil

	case "start":
		fs := flag.NewFlagSet("fleet start", flag.ContinueOnError)
		id := fs.String("id", "", "fleet project id")
		all := fs.Bool("all", false, "start all projects")
		bootstrap := fs.Bool("bootstrap", true, "ensure bootstrap issues for role set")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		projects, err := ralph.ResolveFleetProjects(controlDir, *id, *all)
		if err != nil {
			return err
		}
		exe, err := executablePath()
		if err != nil {
			return err
		}
		for _, p := range projects {
			paths, err := ralph.NewPaths(controlDir, p.ProjectDir)
			if err != nil {
				return err
			}
			if err := ralph.EnsureFleetProjectInstalled(paths, p.Plugin, exe); err != nil {
				return err
			}
			if err := ralph.EnsureFleetAgentSetFile(paths, p); err != nil {
				return err
			}
			if *bootstrap {
				if _, err := ralph.EnsureRoleBootstrapIssues(paths, p.PRDPath); err != nil {
					return err
				}
			}
			if err := ralph.StopPrimaryDaemon(paths); err != nil {
				return err
			}
			if err := ralph.SetEnabled(paths, true); err != nil {
				return err
			}
			fmt.Printf("[fleet] project=%s\n", p.ID)
			for _, role := range p.AssignedRoles {
				pid, already, err := ralph.StartRoleDaemon(paths, role)
				if err != nil {
					return err
				}
				if already {
					fmt.Printf("  - %s: already running (pid=%d)\n", role, pid)
				} else {
					fmt.Printf("  - %s: started (pid=%d)\n", role, pid)
				}
			}
		}
		return nil

	case "stop":
		fs := flag.NewFlagSet("fleet stop", flag.ContinueOnError)
		id := fs.String("id", "", "fleet project id")
		all := fs.Bool("all", false, "stop all projects")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		projects, err := ralph.ResolveFleetProjects(controlDir, *id, *all)
		if err != nil {
			return err
		}
		for _, p := range projects {
			paths, err := ralph.NewPaths(controlDir, p.ProjectDir)
			if err != nil {
				return err
			}
			if err := ralph.SetEnabled(paths, false); err != nil {
				return err
			}
			if err := ralph.StopPrimaryDaemon(paths); err != nil {
				return err
			}
			for _, role := range p.AssignedRoles {
				if err := ralph.StopRoleDaemon(paths, role); err != nil {
					return err
				}
			}
			if err := ralph.RecoverInProgress(paths); err != nil {
				return err
			}
			fmt.Printf("[fleet] stopped project=%s\n", p.ID)
		}
		return nil

	case "status":
		fs := flag.NewFlagSet("fleet status", flag.ContinueOnError)
		id := fs.String("id", "", "fleet project id")
		all := fs.Bool("all", false, "show all projects")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		projects, err := ralph.ResolveFleetProjects(controlDir, *id, *all)
		if err != nil {
			return err
		}
		fmt.Println("## Fleet Status")
		for _, p := range projects {
			paths, err := ralph.NewPaths(controlDir, p.ProjectDir)
			if err != nil {
				return err
			}
			st, err := ralph.GetStatus(paths)
			if err != nil {
				return err
			}
			roles, rolePIDs := ralph.RunningRoleDaemons(paths)
			fmt.Printf("- project=%s dir=%s plugin=%s roles=%s daemon=%s ready=%d in_progress=%d done=%d blocked=%d\n", p.ID, p.ProjectDir, p.Plugin, strings.Join(p.AssignedRoles, ","), st.Daemon, st.QueueReady, st.InProgress, st.Done, st.Blocked)
			if len(roles) > 0 {
				for _, role := range roles {
					fmt.Printf("  - worker[%s]=running pid=%d\n", role, rolePIDs[role])
				}
			}
			if st.LastSelfHealAt != "" {
				fmt.Printf("  - busywait_last_detected=%s self_heal_attempts=%d\n", st.LastBusyWaitDetectedAt, st.SelfHealAttempts)
			}
			if st.LastProfileReloadAt != "" || st.ProfileReloadCount > 0 {
				fmt.Printf(
					"  - profile_reload_at=%s profile_reload_count=%d\n",
					valueOrDash(st.LastProfileReloadAt),
					st.ProfileReloadCount,
				)
			}
			if st.LastFailureCause != "" || st.LastCodexRetryCount > 0 || st.LastPermissionStreak > 0 {
				fmt.Printf(
					"  - last_failure=%s codex_retries=%d perm_streak=%d\n",
					compactSingleLine(st.LastFailureCause, 120),
					st.LastCodexRetryCount,
					st.LastPermissionStreak,
				)
			}
		}
		return nil

	case "dashboard":
		fs := flag.NewFlagSet("fleet dashboard", flag.ContinueOnError)
		id := fs.String("id", "", "fleet project id")
		all := fs.Bool("all", true, "show all projects")
		watch := fs.Bool("watch", false, "refresh continuously")
		intervalSec := fs.Int("interval-sec", 5, "refresh interval seconds when --watch is enabled")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		if *intervalSec <= 0 {
			return fmt.Errorf("--interval-sec must be > 0")
		}
		if *watch {
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			for {
				select {
				case <-ctx.Done():
					fmt.Println("[fleet-dashboard] interrupted")
					return nil
				default:
				}
				fmt.Print("\033[H\033[2J")
				if err := renderFleetDashboard(controlDir, *id, *all, os.Stdout); err != nil {
					return err
				}
				if err := sleepOrInterrupt(ctx, time.Duration(*intervalSec)*time.Second); err != nil {
					return nil
				}
			}
		}
		return renderFleetDashboard(controlDir, *id, *all, os.Stdout)

	case "apply-plugin":
		fs := flag.NewFlagSet("fleet apply-plugin", flag.ContinueOnError)
		id := fs.String("id", "", "fleet project id")
		all := fs.Bool("all", false, "apply to all projects")
		plugin := fs.String("plugin", "", "plugin name (optional: use registered plugin)")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		projects, err := ralph.ResolveFleetProjects(controlDir, *id, *all)
		if err != nil {
			return err
		}
		for _, p := range projects {
			paths, err := ralph.NewPaths(controlDir, p.ProjectDir)
			if err != nil {
				return err
			}
			targetPlugin := p.Plugin
			if strings.TrimSpace(*plugin) != "" {
				targetPlugin = *plugin
			}
			if err := ralph.ApplyPlugin(paths, targetPlugin); err != nil {
				return err
			}
			fmt.Printf("[fleet] applied plugin project=%s plugin=%s\n", p.ID, targetPlugin)
		}
		return nil

	case "bootstrap":
		fs := flag.NewFlagSet("fleet bootstrap", flag.ContinueOnError)
		id := fs.String("id", "", "fleet project id")
		all := fs.Bool("all", false, "bootstrap all projects")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		projects, err := ralph.ResolveFleetProjects(controlDir, *id, *all)
		if err != nil {
			return err
		}
		for _, p := range projects {
			paths, err := ralph.NewPaths(controlDir, p.ProjectDir)
			if err != nil {
				return err
			}
			created, err := ralph.EnsureRoleBootstrapIssues(paths, p.PRDPath)
			if err != nil {
				return err
			}
			fmt.Printf("[fleet] bootstrap project=%s created=%d\n", p.ID, len(created))
		}
		return nil

	default:
		usage()
		return fmt.Errorf("unknown fleet subcommand: %s", sub)
	}
}

func runFleetInteractive(controlDir string) error {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("## Fleet Interactive")
		fmt.Printf("- control_dir: %s\n", controlDir)
		fmt.Println("1) List projects")
		fmt.Println("2) Register project")
		fmt.Println("3) Unregister project")
		fmt.Println("4) Start project")
		fmt.Println("5) Stop project")
		fmt.Println("6) Start all projects")
		fmt.Println("7) Stop all projects")
		fmt.Println("8) Fleet status")
		fmt.Println("9) Fleet dashboard")
		fmt.Println("10) Quit")

		choiceRaw, err := promptFleetInput(reader, "Choose", "1")
		if err != nil {
			return err
		}
		choice := strings.TrimSpace(choiceRaw)
		switch choice {
		case "1":
			if err := runFleetCommand(controlDir, []string{"list"}); err != nil {
				fmt.Fprintf(os.Stderr, "[fleet] %v\n", err)
			}
		case "2":
			if err := runFleetInteractiveRegister(controlDir, reader); err != nil {
				fmt.Fprintf(os.Stderr, "[fleet] %v\n", err)
			}
		case "3":
			if err := runFleetInteractiveUnregister(controlDir, reader); err != nil {
				fmt.Fprintf(os.Stderr, "[fleet] %v\n", err)
			}
		case "4":
			if err := runFleetInteractiveStart(controlDir, reader); err != nil {
				fmt.Fprintf(os.Stderr, "[fleet] %v\n", err)
			}
		case "5":
			if err := runFleetInteractiveStop(controlDir, reader); err != nil {
				fmt.Fprintf(os.Stderr, "[fleet] %v\n", err)
			}
		case "6":
			if err := runFleetCommand(controlDir, []string{"start", "--all"}); err != nil {
				fmt.Fprintf(os.Stderr, "[fleet] %v\n", err)
			}
		case "7":
			if err := runFleetCommand(controlDir, []string{"stop", "--all"}); err != nil {
				fmt.Fprintf(os.Stderr, "[fleet] %v\n", err)
			}
		case "8":
			if err := runFleetCommand(controlDir, []string{"status", "--all"}); err != nil {
				fmt.Fprintf(os.Stderr, "[fleet] %v\n", err)
			}
		case "9":
			if err := runFleetCommand(controlDir, []string{"dashboard", "--all"}); err != nil {
				fmt.Fprintf(os.Stderr, "[fleet] %v\n", err)
			}
		case "10", "q", "quit", "exit":
			fmt.Println("fleet interactive closed")
			return nil
		default:
			fmt.Fprintf(os.Stderr, "[fleet] invalid choice: %s\n", choice)
		}
		fmt.Println("")
	}
}

func runFleetInteractiveRegister(controlDir string, reader *bufio.Reader) error {
	plugins, err := ralph.ListPlugins(controlDir)
	if err != nil {
		return err
	}
	if len(plugins) == 0 {
		return fmt.Errorf("no plugins found in %s/plugins", controlDir)
	}

	id, err := promptFleetInput(reader, "Project id", "")
	if err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("project id is required")
	}

	projectDir, err := promptFleetInput(reader, "Project dir", "")
	if err != nil {
		return err
	}
	projectDir = strings.TrimSpace(projectDir)
	if projectDir == "" {
		return fmt.Errorf("project dir is required")
	}

	plugin, err := promptFleetChoice(reader, "Select plugin", plugins, "universal-default")
	if err != nil {
		return err
	}

	prdPath, err := promptFleetInput(reader, "PRD path", "PRD.md")
	if err != nil {
		return err
	}
	prdPath = strings.TrimSpace(prdPath)
	if prdPath == "" {
		prdPath = "PRD.md"
	}

	if err := runFleetCommand(controlDir, []string{
		"register",
		"--id", id,
		"--project-dir", projectDir,
		"--plugin", plugin,
		"--prd", prdPath,
	}); err != nil {
		return err
	}

	startNow, err := promptFleetBool(reader, "Start this project now?", true)
	if err != nil {
		return err
	}
	if startNow {
		return runFleetCommand(controlDir, []string{"start", "--id", id})
	}
	return nil
}

func runFleetInteractiveUnregister(controlDir string, reader *bufio.Reader) error {
	cfg, err := ralph.LoadFleetConfig(controlDir)
	if err != nil {
		return err
	}
	id, err := promptFleetProjectID(reader, cfg, "Select project to unregister")
	if err != nil {
		return err
	}
	confirm, err := promptFleetBool(reader, fmt.Sprintf("Unregister project %s?", id), false)
	if err != nil {
		return err
	}
	if !confirm {
		return nil
	}
	return runFleetCommand(controlDir, []string{"unregister", "--id", id})
}

func runFleetInteractiveStart(controlDir string, reader *bufio.Reader) error {
	cfg, err := ralph.LoadFleetConfig(controlDir)
	if err != nil {
		return err
	}
	id, err := promptFleetProjectID(reader, cfg, "Select project to start")
	if err != nil {
		return err
	}
	return runFleetCommand(controlDir, []string{"start", "--id", id})
}

func runFleetInteractiveStop(controlDir string, reader *bufio.Reader) error {
	cfg, err := ralph.LoadFleetConfig(controlDir)
	if err != nil {
		return err
	}
	id, err := promptFleetProjectID(reader, cfg, "Select project to stop")
	if err != nil {
		return err
	}
	return runFleetCommand(controlDir, []string{"stop", "--id", id})
}

func promptFleetInput(reader *bufio.Reader, label, defaultValue string) (string, error) {
	if strings.TrimSpace(defaultValue) == "" {
		fmt.Printf("%s: ", label)
	} else {
		fmt.Printf("%s [%s]: ", label, defaultValue)
	}
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return defaultValue, nil
	}
	return line, nil
}

func promptFleetBool(reader *bufio.Reader, label string, defaultValue bool) (bool, error) {
	def := "y"
	if !defaultValue {
		def = "n"
	}
	raw, err := promptFleetInput(reader, label+" (y/n)", def)
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid yes/no input: %s", raw)
	}
}

func promptFleetChoice(reader *bufio.Reader, label string, options []string, defaultValue string) (string, error) {
	if len(options) == 0 {
		return "", fmt.Errorf("no options available")
	}
	fmt.Println(label)
	defaultIndex := 0
	for i, opt := range options {
		if opt == defaultValue {
			defaultIndex = i
			break
		}
	}
	for i, opt := range options {
		defMark := ""
		if i == defaultIndex {
			defMark = " (default)"
		}
		fmt.Printf("%d) %s%s\n", i+1, opt, defMark)
	}
	raw, err := promptFleetInput(reader, "Choose", strconv.Itoa(defaultIndex+1))
	if err != nil {
		return "", err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return options[defaultIndex], nil
	}
	if idx, convErr := strconv.Atoi(raw); convErr == nil {
		if idx >= 1 && idx <= len(options) {
			return options[idx-1], nil
		}
	}
	for _, opt := range options {
		if raw == opt {
			return opt, nil
		}
	}
	return "", fmt.Errorf("invalid choice: %s", raw)
}

func promptFleetProjectID(reader *bufio.Reader, cfg ralph.FleetConfig, label string) (string, error) {
	if len(cfg.Projects) == 0 {
		return "", fmt.Errorf("fleet is empty. register project first")
	}
	options := make([]string, 0, len(cfg.Projects))
	for _, p := range cfg.Projects {
		options = append(options, p.ID)
	}
	return promptFleetChoice(reader, label, options, options[0])
}

func compactSingleLine(raw string, maxLen int) string {
	v := strings.TrimSpace(raw)
	v = strings.ReplaceAll(v, "\n", " ")
	v = strings.ReplaceAll(v, "\r", " ")
	v = strings.Join(strings.Fields(v), " ")
	if !utf8.ValidString(v) {
		v = string(bytes.ToValidUTF8([]byte(v), []byte("?")))
	}
	if maxLen <= 0 {
		return v
	}
	runes := []rune(v)
	if len(runes) <= maxLen {
		return v
	}
	if maxLen <= 3 {
		return string(runes[:maxLen])
	}
	return string(runes[:maxLen-3]) + "..."
}

func valueOrDash(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "-"
	}
	return raw
}

func defaultControlDir(cwd string) string {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return cwd
	}
	return filepath.Join(home, ".ralph-control")
}

func commandNeedsControlAssets(cmd string) bool {
	switch cmd {
	case "list-plugins", "install", "apply-plugin", "setup", "reload", "fleet", "registry", "service", "telegram":
		return true
	default:
		return false
	}
}

func executablePath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Abs(exe)
}
