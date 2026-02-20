package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

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

	global := flag.NewFlagSet("ralphctl", flag.ContinueOnError)
	global.SetOutput(os.Stderr)
	controlDir := global.String("control-dir", cwd, "directory that stores shared plugins and fleet config")
	projectDir := global.String("project-dir", cwd, "target project directory (.ralph lives here)")

	global.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl [--control-dir DIR] [--project-dir DIR] <command> [args]")
		fmt.Fprintln(os.Stderr, "Commands: list-plugins, install, apply-plugin, setup, init, on, off, new, import-prd, recover, doctor, run, supervise, start, stop, restart, status, tail, fleet")
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

	if cmd == "fleet" {
		return runFleetCommand(*controlDir, cmdArgs)
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
		fmt.Println("installed ralph runtime")
		fmt.Printf("- control_dir: %s\n", paths.ControlDir)
		fmt.Printf("- project_dir: %s\n", paths.ProjectDir)
		fmt.Printf("- plugin: %s\n", *plugin)
		fmt.Printf("- helper: %s\n", filepath.Join(paths.ProjectDir, "ralph"))
		fmt.Printf("- profile_yaml: %s\n", paths.ProfileYAMLFile)
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
		fmt.Printf("applied plugin: %s\n", *plugin)
		fmt.Printf("profile_yaml: %s\n", paths.ProfileYAMLFile)
		fmt.Printf("profile_env_override: %s\n", paths.ProfileFile)
		return nil

	case "setup":
		fs := flag.NewFlagSet("setup", flag.ContinueOnError)
		plugin := fs.String("plugin", "", "preferred default plugin in wizard")
		nonInteractive := fs.Bool("non-interactive", false, "apply defaults without prompts")
		if err := fs.Parse(cmdArgs); err != nil {
			return err
		}
		exe, err := executablePath()
		if err != nil {
			return err
		}
		if *nonInteractive {
			selection := ralph.SetupSelections{
				Plugin:           strings.TrimSpace(*plugin),
				RoleRulesEnabled: true,
				HandoffRequired:  true,
				HandoffSchema:    "universal",
				DoctorAutoRepair: true,
				ValidationMode:   ralph.SetupModePluginDefault,
			}
			if err := ralph.ApplySetupSelections(paths, exe, selection); err != nil {
				return err
			}
			fmt.Println("setup complete (non-interactive)")
			fmt.Printf("- helper: %s\n", filepath.Join(paths.ProjectDir, "ralph"))
			fmt.Printf("- profile_yaml: %s\n", paths.ProfileYAMLFile)
			fmt.Printf("- profile_local_yaml: %s\n", paths.ProfileLocalYAMLFile)
			fmt.Printf("- profile_env_override: %s\n", paths.ProfileLocalFile)
			return nil
		}
		return ralph.RunSetupWizard(paths, exe, *plugin, os.Stdin, os.Stdout)

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
		pid, already, err := ralph.StartDaemon(paths)
		if err != nil {
			return err
		}
		if already {
			fmt.Printf("ralph-loop already running (pid=%d)\n", pid)
		} else {
			fmt.Printf("ralph-loop started (pid=%d)\n", pid)
		}
		return nil

	case "stop":
		if err := ralph.StopDaemon(paths); err != nil {
			return err
		}
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

func runFleetCommand(controlDir string, args []string) error {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --control-dir DIR fleet <subcommand> [args]")
		fmt.Fprintln(os.Stderr, "Subcommands: interactive, register, unregister, list, start, stop, status, apply-plugin, bootstrap")
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
		}
		return nil

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
		fmt.Println("9) Quit")

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
		case "9", "q", "quit", "exit":
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

func executablePath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Abs(exe)
}
