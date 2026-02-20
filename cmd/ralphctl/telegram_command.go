package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"codex-ralph/internal/ralph"
)

func runTelegramCommand(controlDir string, paths ralph.Paths, args []string) error {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --control-dir DIR --project-dir DIR telegram <run|setup|stop|status|tail> [flags]")
		fmt.Fprintln(os.Stderr, "Env: RALPH_TELEGRAM_BOT_TOKEN, RALPH_TELEGRAM_CHAT_IDS, RALPH_TELEGRAM_USER_IDS, RALPH_TELEGRAM_ALLOW_CONTROL, RALPH_TELEGRAM_NOTIFY, RALPH_TELEGRAM_NOTIFY_SCOPE")
	}
	if len(args) == 0 {
		usage()
		return fmt.Errorf("telegram subcommand is required")
	}

	switch args[0] {
	case "run":
		return runTelegramRunCommand(controlDir, paths, args[1:])
	case "setup":
		return runTelegramSetupCommand(controlDir, args[1:])
	case "stop":
		return runTelegramStopCommand(paths, args[1:])
	case "status":
		return runTelegramStatusCommand(controlDir, paths, args[1:])
	case "tail":
		return runTelegramTailCommand(paths, args[1:])
	default:
		usage()
		return fmt.Errorf("unknown telegram subcommand: %s", args[0])
	}
}

func runTelegramRunCommand(controlDir string, paths ralph.Paths, args []string) error {
	configFile := telegramConfigFileFromArgs(controlDir, args)
	cfg, err := loadTelegramCLIConfig(configFile)
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("telegram run", flag.ContinueOnError)
	configFileFlag := fs.String("config-file", configFile, "telegram config file path")
	foreground := fs.Bool("foreground", false, "run in foreground (default: start daemon and return)")
	token := fs.String("token", firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_BOT_TOKEN")), cfg.Token), "telegram bot token")
	chatIDsRaw := fs.String("chat-ids", firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_CHAT_IDS")), cfg.ChatIDs), "allowed chat IDs CSV (required)")
	userIDsRaw := fs.String("user-ids", firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_USER_IDS")), cfg.UserIDs), "allowed user IDs CSV (optional; recommended for group chats)")
	allowControl := fs.Bool("allow-control", envBoolDefault("RALPH_TELEGRAM_ALLOW_CONTROL", cfg.AllowControl), "allow control commands (/start,/stop,/restart,/doctor_repair,/recover)")
	enableNotify := fs.Bool("notify", envBoolDefault("RALPH_TELEGRAM_NOTIFY", cfg.Notify), "push alerts for blocked/retry/stuck")
	notifyScope := fs.String("notify-scope", firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_NOTIFY_SCOPE")), cfg.NotifyScope), "notify scope: project|fleet|auto")
	notifyIntervalSec := fs.Int("notify-interval-sec", envIntDefault("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", cfg.NotifyIntervalSec), "status poll interval for notify alerts")
	notifyRetryThreshold := fs.Int("notify-retry-threshold", envIntDefault("RALPH_TELEGRAM_NOTIFY_RETRY_THRESHOLD", cfg.NotifyRetryThreshold), "codex retry alert threshold")
	notifyPermStreakThreshold := fs.Int("notify-perm-streak-threshold", envIntDefault("RALPH_TELEGRAM_NOTIFY_PERM_STREAK_THRESHOLD", cfg.NotifyPermStreakThreshold), "permission streak alert threshold")
	pollTimeoutSec := fs.Int("poll-timeout-sec", 30, "telegram getUpdates timeout (seconds)")
	offsetFile := fs.String("offset-file", filepath.Join(controlDir, "telegram.offset"), "telegram update offset file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	configFile = strings.TrimSpace(*configFileFlag)

	if strings.TrimSpace(*token) == "" {
		return fmt.Errorf("--token is required (or run `ralphctl telegram setup`)")
	}
	allowedChatIDs, err := ralph.ParseTelegramChatIDs(*chatIDsRaw)
	if err != nil {
		return err
	}
	if len(allowedChatIDs) == 0 {
		return fmt.Errorf("--chat-ids is required (or run `ralphctl telegram setup`)")
	}
	allowedUserIDs := map[int64]struct{}{}
	if strings.TrimSpace(*userIDsRaw) != "" {
		allowedUserIDs, err = ralph.ParseTelegramUserIDs(*userIDsRaw)
		if err != nil {
			return err
		}
	}
	if *allowControl && len(allowedUserIDs) == 0 && requiresUserAllowlistForControl(allowedChatIDs) {
		return fmt.Errorf("--allow-control with group/supergroup chat requires --user-ids (or set RALPH_TELEGRAM_USER_IDS)")
	}
	if *pollTimeoutSec <= 0 {
		return fmt.Errorf("--poll-timeout-sec must be > 0")
	}
	if *notifyIntervalSec <= 0 {
		return fmt.Errorf("--notify-interval-sec must be > 0")
	}
	resolvedNotifyScope, err := normalizeNotifyScope(*notifyScope)
	if err != nil {
		return fmt.Errorf("invalid --notify-scope: %w", err)
	}
	if !*foreground {
		msg, err := startTelegramDaemon(paths, ensureTelegramForegroundArg(args))
		if err != nil {
			return err
		}
		fmt.Println(msg)
		fmt.Printf("- control_dir: %s\n", controlDir)
		fmt.Printf("- project_dir: %s\n", paths.ProjectDir)
		fmt.Printf("- config_file: %s\n", configFile)
		fmt.Printf("- pid_file: %s\n", paths.TelegramPIDFile())
		fmt.Printf("- log_file: %s\n", paths.TelegramLogFile())
		fmt.Println("- mode: daemon")
		fmt.Println("- hint: stop with `ralphctl telegram stop`, logs with `ralphctl telegram tail`")
		return nil
	}

	fmt.Println("telegram bot started")
	fmt.Printf("- control_dir: %s\n", controlDir)
	fmt.Printf("- project_dir: %s\n", paths.ProjectDir)
	fmt.Printf("- config_file: %s\n", configFile)
	fmt.Println("- mode: foreground")
	fmt.Printf("- allow_control: %t\n", *allowControl)
	fmt.Printf("- notify: %t\n", *enableNotify)
	fmt.Printf("- notify_scope: %s\n", resolvedNotifyScope)
	fmt.Printf("- notify_interval_sec: %d\n", *notifyIntervalSec)
	fmt.Printf("- notify_retry_threshold: %d\n", *notifyRetryThreshold)
	fmt.Printf("- notify_perm_streak_threshold: %d\n", *notifyPermStreakThreshold)
	fmt.Printf("- allowed_chats: %d\n", len(allowedChatIDs))
	if len(allowedUserIDs) > 0 {
		fmt.Printf("- allowed_users: %d\n", len(allowedUserIDs))
	} else {
		fmt.Printf("- allowed_users: any (chat allowlist only)\n")
	}
	fmt.Printf("- offset_file: %s\n", *offsetFile)

	notifyHandler := ralph.TelegramNotifyHandler(nil)
	if *enableNotify {
		notifyHandler = newScopedStatusNotifyHandler(controlDir, paths, resolvedNotifyScope, *notifyRetryThreshold, *notifyPermStreakThreshold)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return ralph.RunTelegramBot(ctx, ralph.TelegramBotOptions{
		Token:             *token,
		AllowedChatIDs:    allowedChatIDs,
		AllowedUserIDs:    allowedUserIDs,
		PollTimeoutSec:    *pollTimeoutSec,
		NotifyIntervalSec: *notifyIntervalSec,
		OffsetFile:        *offsetFile,
		Out:               os.Stdout,
		OnCommand:         telegramCommandHandler(controlDir, paths, *allowControl),
		OnNotifyTick:      notifyHandler,
	})
}

func runTelegramStopCommand(paths ralph.Paths, args []string) error {
	fs := flag.NewFlagSet("telegram stop", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	msg, err := stopTelegramDaemon(paths)
	if err != nil {
		return err
	}
	fmt.Println(msg)
	return nil
}

func runTelegramStatusCommand(controlDir string, paths ralph.Paths, args []string) error {
	fs := flag.NewFlagSet("telegram status", flag.ContinueOnError)
	offsetFile := fs.String("offset-file", filepath.Join(controlDir, "telegram.offset"), "telegram update offset file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := ralph.EnsureLayout(paths); err != nil {
		return err
	}

	pid, running, stale := telegramPIDState(paths.TelegramPIDFile())
	fmt.Println("## Telegram Bot Status")
	fmt.Printf("- control_dir: %s\n", controlDir)
	fmt.Printf("- project_dir: %s\n", paths.ProjectDir)
	fmt.Printf("- pid_file: %s\n", paths.TelegramPIDFile())
	fmt.Printf("- log_file: %s\n", paths.TelegramLogFile())
	fmt.Printf("- offset_file: %s\n", strings.TrimSpace(*offsetFile))
	switch {
	case running:
		fmt.Printf("- daemon: running(pid=%d)\n", pid)
	case stale:
		fmt.Printf("- daemon: stopped(stale_pid=%d)\n", pid)
	default:
		fmt.Println("- daemon: stopped")
	}
	return nil
}

func runTelegramTailCommand(paths ralph.Paths, args []string) error {
	fs := flag.NewFlagSet("telegram tail", flag.ContinueOnError)
	lines := fs.Int("lines", 120, "number of lines")
	follow := fs.Bool("follow", true, "follow appended lines")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := ralph.EnsureLayout(paths); err != nil {
		return err
	}
	return tailFile(paths.TelegramLogFile(), *lines, *follow)
}

func runTelegramSetupCommand(controlDir string, args []string) error {
	configFile := telegramConfigFileFromArgs(controlDir, args)
	cfg, err := loadTelegramCLIConfig(configFile)
	if err != nil {
		return err
	}

	defaultToken := firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_BOT_TOKEN")), cfg.Token)
	defaultChatIDs := firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_CHAT_IDS")), cfg.ChatIDs)
	defaultUserIDs := firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_USER_IDS")), cfg.UserIDs)
	defaultAllowControl := envBoolDefault("RALPH_TELEGRAM_ALLOW_CONTROL", cfg.AllowControl)
	defaultNotify := envBoolDefault("RALPH_TELEGRAM_NOTIFY", cfg.Notify)
	defaultNotifyScope := firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_NOTIFY_SCOPE")), cfg.NotifyScope)
	if strings.TrimSpace(defaultNotifyScope) == "" {
		defaultNotifyScope = "auto"
	}
	defaultNotifyInterval := envIntDefault("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", cfg.NotifyIntervalSec)
	defaultNotifyRetry := envIntDefault("RALPH_TELEGRAM_NOTIFY_RETRY_THRESHOLD", cfg.NotifyRetryThreshold)
	defaultNotifyPerm := envIntDefault("RALPH_TELEGRAM_NOTIFY_PERM_STREAK_THRESHOLD", cfg.NotifyPermStreakThreshold)

	fs := flag.NewFlagSet("telegram setup", flag.ContinueOnError)
	configFileFlag := fs.String("config-file", configFile, "telegram config file path")
	nonInteractive := fs.Bool("non-interactive", false, "save config without interactive prompts")
	tokenFlag := fs.String("token", defaultToken, "telegram bot token")
	chatIDsFlag := fs.String("chat-ids", defaultChatIDs, "allowed chat IDs CSV")
	userIDsFlag := fs.String("user-ids", defaultUserIDs, "allowed user IDs CSV (optional)")
	allowControlFlag := fs.Bool("allow-control", defaultAllowControl, "allow control commands")
	notifyFlag := fs.Bool("notify", defaultNotify, "enable notify alerts")
	notifyScopeFlag := fs.String("notify-scope", defaultNotifyScope, "notify scope: project|fleet|auto")
	notifyIntervalFlag := fs.Int("notify-interval-sec", defaultNotifyInterval, "notify interval seconds")
	notifyRetryFlag := fs.Int("notify-retry-threshold", defaultNotifyRetry, "notify retry threshold")
	notifyPermFlag := fs.Int("notify-perm-streak-threshold", defaultNotifyPerm, "notify permission streak threshold")
	if err := fs.Parse(args); err != nil {
		return err
	}

	final := telegramCLIConfig{
		Token:                     strings.TrimSpace(*tokenFlag),
		ChatIDs:                   strings.TrimSpace(*chatIDsFlag),
		UserIDs:                   strings.TrimSpace(*userIDsFlag),
		AllowControl:              *allowControlFlag,
		Notify:                    *notifyFlag,
		NotifyScope:               strings.TrimSpace(*notifyScopeFlag),
		NotifyIntervalSec:         *notifyIntervalFlag,
		NotifyRetryThreshold:      *notifyRetryFlag,
		NotifyPermStreakThreshold: *notifyPermFlag,
	}
	configFile = strings.TrimSpace(*configFileFlag)

	if !*nonInteractive {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("## Telegram Setup")
		fmt.Printf("- control_dir: %s\n", controlDir)
		fmt.Printf("- config_file: %s\n\n", configFile)

		tokenInput, err := promptFleetInput(reader, "Bot token", final.Token)
		if err != nil {
			return err
		}
		final.Token = strings.TrimSpace(tokenInput)

		chatInput, err := promptFleetInput(reader, "Allowed chat IDs (CSV)", final.ChatIDs)
		if err != nil {
			return err
		}
		final.ChatIDs = strings.TrimSpace(chatInput)

		userInput, err := promptFleetInput(reader, "Allowed user IDs (CSV, optional)", final.UserIDs)
		if err != nil {
			return err
		}
		final.UserIDs = strings.TrimSpace(userInput)

		allowControlInput, err := promptFleetBool(reader, "Allow control commands?", final.AllowControl)
		if err != nil {
			return err
		}
		final.AllowControl = allowControlInput

		notifyInput, err := promptFleetBool(reader, "Enable notify alerts?", final.Notify)
		if err != nil {
			return err
		}
		final.Notify = notifyInput

		scopeInput, err := promptFleetChoice(reader, "Notify scope", []string{"auto", "project", "fleet"}, firstNonEmpty(final.NotifyScope, "auto"))
		if err != nil {
			return err
		}
		final.NotifyScope = strings.TrimSpace(scopeInput)

		intervalInput, err := promptFleetInput(reader, "Notify interval sec", strconv.Itoa(final.NotifyIntervalSec))
		if err != nil {
			return err
		}
		if v, convErr := strconv.Atoi(strings.TrimSpace(intervalInput)); convErr == nil {
			final.NotifyIntervalSec = v
		}

		retryInput, err := promptFleetInput(reader, "Retry alert threshold", strconv.Itoa(final.NotifyRetryThreshold))
		if err != nil {
			return err
		}
		if v, convErr := strconv.Atoi(strings.TrimSpace(retryInput)); convErr == nil {
			final.NotifyRetryThreshold = v
		}

		permInput, err := promptFleetInput(reader, "Permission streak threshold", strconv.Itoa(final.NotifyPermStreakThreshold))
		if err != nil {
			return err
		}
		if v, convErr := strconv.Atoi(strings.TrimSpace(permInput)); convErr == nil {
			final.NotifyPermStreakThreshold = v
		}
	}

	if strings.TrimSpace(final.Token) == "" {
		return fmt.Errorf("token is required")
	}
	if strings.TrimSpace(final.ChatIDs) == "" {
		return fmt.Errorf("chat-ids is required")
	}
	allowedChatIDs, err := ralph.ParseTelegramChatIDs(final.ChatIDs)
	if err != nil {
		return err
	}
	allowedUserIDs := map[int64]struct{}{}
	if strings.TrimSpace(final.UserIDs) != "" {
		allowedUserIDs, err = ralph.ParseTelegramUserIDs(final.UserIDs)
		if err != nil {
			return err
		}
	}
	if final.AllowControl && len(allowedUserIDs) == 0 && requiresUserAllowlistForControl(allowedChatIDs) {
		return fmt.Errorf("allow-control with group/supergroup chat requires user-ids")
	}
	if final.NotifyIntervalSec <= 0 {
		return fmt.Errorf("notify-interval-sec must be > 0")
	}
	scope, err := normalizeNotifyScope(final.NotifyScope)
	if err != nil {
		return fmt.Errorf("notify-scope: %w", err)
	}
	final.NotifyScope = scope
	if err := saveTelegramCLIConfig(configFile, final); err != nil {
		return err
	}

	fmt.Println("telegram setup complete")
	fmt.Printf("- config_file: %s\n", configFile)
	fmt.Printf("- allow_control: %t\n", final.AllowControl)
	fmt.Printf("- notify: %t\n", final.Notify)
	fmt.Printf("- notify_scope: %s\n", final.NotifyScope)
	fmt.Printf("- run: ralphctl --project-dir \"$PWD\" telegram run --config-file %s\n", configFile)
	fmt.Printf("- status: ralphctl --project-dir \"$PWD\" telegram status\n")
	fmt.Printf("- stop: ralphctl --project-dir \"$PWD\" telegram stop\n")
	return nil
}

type telegramCLIConfig struct {
	Token                     string
	ChatIDs                   string
	UserIDs                   string
	AllowControl              bool
	Notify                    bool
	NotifyScope               string
	NotifyIntervalSec         int
	NotifyRetryThreshold      int
	NotifyPermStreakThreshold int
}

func defaultTelegramCLIConfig() telegramCLIConfig {
	return telegramCLIConfig{
		AllowControl:              false,
		Notify:                    true,
		NotifyScope:               "auto",
		NotifyIntervalSec:         30,
		NotifyRetryThreshold:      2,
		NotifyPermStreakThreshold: 3,
	}
}

func telegramConfigFileFromArgs(controlDir string, args []string) string {
	defaultPath := filepath.Join(controlDir, "telegram.env")
	for i := 0; i < len(args); i++ {
		raw := strings.TrimSpace(args[i])
		if strings.HasPrefix(raw, "--config-file=") {
			v := strings.TrimSpace(strings.TrimPrefix(raw, "--config-file="))
			if v != "" {
				return v
			}
			continue
		}
		if raw == "--config-file" && i+1 < len(args) {
			v := strings.TrimSpace(args[i+1])
			if v != "" {
				return v
			}
		}
	}
	return defaultPath
}

func loadTelegramCLIConfig(path string) (telegramCLIConfig, error) {
	cfg := defaultTelegramCLIConfig()
	path = strings.TrimSpace(path)
	if path == "" {
		return cfg, nil
	}
	values, err := ralph.ReadEnvFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("read telegram config: %w", err)
	}
	if v := strings.TrimSpace(values["RALPH_TELEGRAM_BOT_TOKEN"]); v != "" {
		cfg.Token = v
	}
	if v := strings.TrimSpace(values["RALPH_TELEGRAM_CHAT_IDS"]); v != "" {
		cfg.ChatIDs = v
	}
	if v := strings.TrimSpace(values["RALPH_TELEGRAM_USER_IDS"]); v != "" {
		cfg.UserIDs = v
	}
	if v, ok := parseBoolRaw(values["RALPH_TELEGRAM_ALLOW_CONTROL"]); ok {
		cfg.AllowControl = v
	}
	if v, ok := parseBoolRaw(values["RALPH_TELEGRAM_NOTIFY"]); ok {
		cfg.Notify = v
	}
	if v := strings.TrimSpace(values["RALPH_TELEGRAM_NOTIFY_SCOPE"]); v != "" {
		cfg.NotifyScope = v
	}
	if v, ok := parseIntRaw(values["RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC"]); ok {
		cfg.NotifyIntervalSec = v
	}
	if v, ok := parseIntRaw(values["RALPH_TELEGRAM_NOTIFY_RETRY_THRESHOLD"]); ok {
		cfg.NotifyRetryThreshold = v
	}
	if v, ok := parseIntRaw(values["RALPH_TELEGRAM_NOTIFY_PERM_STREAK_THRESHOLD"]); ok {
		cfg.NotifyPermStreakThreshold = v
	}
	return cfg, nil
}

func saveTelegramCLIConfig(path string, cfg telegramCLIConfig) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("config file path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create telegram config dir: %w", err)
	}
	var b strings.Builder
	b.WriteString("# Ralph Telegram config\n")
	b.WriteString("RALPH_TELEGRAM_BOT_TOKEN=" + envQuoteValue(cfg.Token) + "\n")
	b.WriteString("RALPH_TELEGRAM_CHAT_IDS=" + envQuoteValue(cfg.ChatIDs) + "\n")
	b.WriteString("RALPH_TELEGRAM_USER_IDS=" + envQuoteValue(cfg.UserIDs) + "\n")
	b.WriteString("RALPH_TELEGRAM_ALLOW_CONTROL=" + strconv.FormatBool(cfg.AllowControl) + "\n")
	b.WriteString("RALPH_TELEGRAM_NOTIFY=" + strconv.FormatBool(cfg.Notify) + "\n")
	b.WriteString("RALPH_TELEGRAM_NOTIFY_SCOPE=" + cfg.NotifyScope + "\n")
	b.WriteString("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC=" + strconv.Itoa(cfg.NotifyIntervalSec) + "\n")
	b.WriteString("RALPH_TELEGRAM_NOTIFY_RETRY_THRESHOLD=" + strconv.Itoa(cfg.NotifyRetryThreshold) + "\n")
	b.WriteString("RALPH_TELEGRAM_NOTIFY_PERM_STREAK_THRESHOLD=" + strconv.Itoa(cfg.NotifyPermStreakThreshold) + "\n")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("set telegram config permissions: %w", err)
	}
	return nil
}

func telegramCommandHandler(controlDir string, paths ralph.Paths, allowControl bool) ralph.TelegramCommandHandler {
	return func(ctx context.Context, chatID int64, text string) (string, error) {
		_ = ctx
		text = strings.TrimSpace(text)
		if text == "" {
			return "", nil
		}

		if allowControl && !strings.HasPrefix(text, "/") {
			hasSession, err := telegramHasActivePRDSession(controlDir, chatID)
			if err != nil {
				return "", err
			}
			if hasSession {
				return telegramPRDHandleInput(controlDir, paths, chatID, text)
			}
		}

		cmd, cmdArgs := parseTelegramCommandLine(text)
		switch cmd {
		case "", "/help":
			return buildTelegramHelp(allowControl), nil

		case "/ping":
			return "pong " + time.Now().UTC().Format(time.RFC3339), nil

		case "/status":
			return telegramStatusCommand(controlDir, paths, cmdArgs)

		case "/fleet", "/fleet_status", "/dashboard":
			return telegramFleetDashboardCommand(controlDir, cmdArgs)

		case "/doctor":
			return telegramDoctorCommand(controlDir, paths, cmdArgs)

		case "/start":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			return telegramStartCommand(controlDir, paths, cmdArgs)

		case "/stop":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			return telegramStopCommand(controlDir, paths, cmdArgs)

		case "/restart":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			return telegramRestartCommand(controlDir, paths, cmdArgs)

		case "/doctor_repair":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			return telegramDoctorRepairCommand(controlDir, paths, cmdArgs)

		case "/recover":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			return telegramRecoverCommand(controlDir, paths, cmdArgs)

		case "/new", "/issue":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			return telegramNewIssueCommand(paths, cmdArgs)

		case "/prd":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			return telegramPRDCommand(controlDir, paths, chatID, cmdArgs)

		default:
			return "unknown command\n\n" + buildTelegramHelp(allowControl), nil
		}
	}
}

type telegramTargetSpec struct {
	All       bool
	ProjectID string
}

func (s telegramTargetSpec) HasTarget() bool {
	return s.All || strings.TrimSpace(s.ProjectID) != ""
}

func (s telegramTargetSpec) Label() string {
	if s.All {
		return "all"
	}
	if strings.TrimSpace(s.ProjectID) == "" {
		return "current"
	}
	return s.ProjectID
}

func parseTelegramTargetSpec(raw string) (telegramTargetSpec, error) {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return telegramTargetSpec{}, nil
	}
	if len(fields) > 1 {
		return telegramTargetSpec{}, fmt.Errorf("invalid target: use one value ('all' or project id)")
	}
	target := strings.TrimSpace(fields[0])
	if target == "" {
		return telegramTargetSpec{}, nil
	}
	switch strings.ToLower(target) {
	case "all", "*":
		return telegramTargetSpec{All: true}, nil
	default:
		return telegramTargetSpec{ProjectID: target}, nil
	}
}

func resolveTelegramFleetPaths(controlDir string, spec telegramTargetSpec) ([]ralph.FleetProject, map[string]ralph.Paths, error) {
	if !spec.HasTarget() {
		return nil, nil, fmt.Errorf("fleet target is required")
	}
	projects, err := ralph.ResolveFleetProjects(controlDir, spec.ProjectID, spec.All)
	if err != nil {
		return nil, nil, err
	}
	pathsByID := make(map[string]ralph.Paths, len(projects))
	for _, p := range projects {
		paths, err := ralph.NewPaths(controlDir, p.ProjectDir)
		if err != nil {
			return nil, nil, err
		}
		pathsByID[p.ID] = paths
	}
	return projects, pathsByID, nil
}

func telegramStatusCommand(controlDir string, paths ralph.Paths, rawArgs string) (string, error) {
	spec, err := parseTelegramTargetSpec(rawArgs)
	if err != nil {
		return "", err
	}
	if !spec.HasTarget() {
		st, err := ralph.GetStatus(paths)
		if err != nil {
			return "", err
		}
		return formatStatusForTelegram(st), nil
	}
	var b bytes.Buffer
	if err := renderFleetDashboard(controlDir, spec.ProjectID, spec.All, &b); err != nil {
		return "", err
	}
	return b.String(), nil
}

func telegramFleetDashboardCommand(controlDir, rawArgs string) (string, error) {
	spec, err := parseTelegramTargetSpec(rawArgs)
	if err != nil {
		return "", err
	}
	projectID := ""
	all := true
	if spec.HasTarget() {
		projectID = spec.ProjectID
		all = spec.All
	}
	var b bytes.Buffer
	if err := renderFleetDashboard(controlDir, projectID, all, &b); err != nil {
		return "", err
	}
	return b.String(), nil
}

func telegramDoctorCommand(controlDir string, paths ralph.Paths, rawArgs string) (string, error) {
	spec, err := parseTelegramTargetSpec(rawArgs)
	if err != nil {
		return "", err
	}
	if !spec.HasTarget() {
		report, err := ralph.RunDoctor(paths)
		if err != nil {
			return "", err
		}
		return formatDoctorReportForTelegram(report), nil
	}
	return runFleetDoctorReports(controlDir, spec)
}

func telegramStartCommand(controlDir string, paths ralph.Paths, rawArgs string) (string, error) {
	spec, err := parseTelegramTargetSpec(rawArgs)
	if err != nil {
		return "", err
	}
	if !spec.HasTarget() {
		res, err := startProjectDaemon(paths, startOptions{
			DoctorRepair: true,
			FixPerms:     false,
			Out:          io.Discard,
		})
		if err != nil {
			return "", err
		}
		return res, nil
	}
	if err := runFleetCommand(controlDir, buildFleetTargetArgs("start", spec)); err != nil {
		return "", err
	}
	return fmt.Sprintf("fleet start completed (target=%s)", spec.Label()), nil
}

func telegramStopCommand(controlDir string, paths ralph.Paths, rawArgs string) (string, error) {
	spec, err := parseTelegramTargetSpec(rawArgs)
	if err != nil {
		return "", err
	}
	if !spec.HasTarget() {
		if err := ralph.StopDaemon(paths); err != nil {
			return "", err
		}
		return "ralph-loop stopped", nil
	}
	if err := runFleetCommand(controlDir, buildFleetTargetArgs("stop", spec)); err != nil {
		return "", err
	}
	return fmt.Sprintf("fleet stop completed (target=%s)", spec.Label()), nil
}

func telegramRestartCommand(controlDir string, paths ralph.Paths, rawArgs string) (string, error) {
	spec, err := parseTelegramTargetSpec(rawArgs)
	if err != nil {
		return "", err
	}
	if !spec.HasTarget() {
		if err := ralph.StopDaemon(paths); err != nil {
			return "", err
		}
		pid, _, err := ralph.StartDaemon(paths)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("ralph-loop restarted (pid=%d)", pid), nil
	}
	if err := runFleetCommand(controlDir, buildFleetTargetArgs("stop", spec)); err != nil {
		return "", err
	}
	if err := runFleetCommand(controlDir, buildFleetTargetArgs("start", spec)); err != nil {
		return "", err
	}
	return fmt.Sprintf("fleet restart completed (target=%s)", spec.Label()), nil
}

func telegramDoctorRepairCommand(controlDir string, paths ralph.Paths, rawArgs string) (string, error) {
	spec, err := parseTelegramTargetSpec(rawArgs)
	if err != nil {
		return "", err
	}
	if !spec.HasTarget() {
		actions, err := ralph.RepairProject(paths)
		if err != nil {
			return "", err
		}
		return formatDoctorRepairActions(actions), nil
	}
	projects, pathsByID, err := resolveTelegramFleetPaths(controlDir, spec)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "fleet doctor repair completed (target=%s)\n", spec.Label())
	for _, p := range projects {
		actions, err := ralph.RepairProject(pathsByID[p.ID])
		if err != nil {
			fmt.Fprintf(&b, "- project=%s status=fail detail=%s\n", p.ID, compactSingleLine(err.Error(), 160))
			continue
		}
		pass, warn, fail := countDoctorRepairActions(actions)
		fmt.Fprintf(&b, "- project=%s pass=%d warn=%d fail=%d\n", p.ID, pass, warn, fail)
	}
	return b.String(), nil
}

func telegramRecoverCommand(controlDir string, paths ralph.Paths, rawArgs string) (string, error) {
	spec, err := parseTelegramTargetSpec(rawArgs)
	if err != nil {
		return "", err
	}
	if !spec.HasTarget() {
		recovered, err := ralph.RecoverInProgressWithCount(paths)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("recovered in-progress issues: %d", recovered), nil
	}
	projects, pathsByID, err := resolveTelegramFleetPaths(controlDir, spec)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	total := 0
	for _, p := range projects {
		recovered, err := ralph.RecoverInProgressWithCount(pathsByID[p.ID])
		if err != nil {
			fmt.Fprintf(&b, "- project=%s status=fail detail=%s\n", p.ID, compactSingleLine(err.Error(), 160))
			continue
		}
		total += recovered
		fmt.Fprintf(&b, "- project=%s recovered=%d\n", p.ID, recovered)
	}
	fmt.Fprintf(&b, "fleet recover completed (target=%s total=%d)", spec.Label(), total)
	return b.String(), nil
}

func telegramNewIssueCommand(paths ralph.Paths, rawArgs string) (string, error) {
	role, title, err := parseTelegramNewIssueArgs(rawArgs)
	if err != nil {
		return "", err
	}
	issuePath, issueID, err := ralph.CreateIssue(paths, role, title)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"issue created\n- id: %s\n- role: %s\n- title: %s\n- path: %s",
		issueID,
		role,
		title,
		issuePath,
	), nil
}

func parseTelegramNewIssueArgs(raw string) (string, string, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return "", "", fmt.Errorf("usage: /new [manager|planner|developer|qa] <title>")
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return "", "", fmt.Errorf("usage: /new [manager|planner|developer|qa] <title>")
	}

	first := strings.ToLower(strings.TrimSpace(fields[0]))
	if ralph.IsSupportedRole(first) {
		if len(fields) < 2 {
			return "", "", fmt.Errorf("usage: /new %s <title>", first)
		}
		return first, strings.TrimSpace(strings.Join(fields[1:], " ")), nil
	}
	return "developer", text, nil
}

const (
	telegramPRDStageAwaitProduct      = "await_product"
	telegramPRDStageAwaitStoryTitle   = "await_story_title"
	telegramPRDStageAwaitStoryDesc    = "await_story_desc"
	telegramPRDStageAwaitStoryRole    = "await_story_role"
	telegramPRDStageAwaitStoryPrio    = "await_story_priority"
	telegramPRDDefaultPriority        = 1000
	telegramPRDDefaultProductFallback = "Telegram PRD"
)

type telegramPRDStory struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Role        string `json:"role"`
	Priority    int    `json:"priority"`
}

type telegramPRDDocument struct {
	UserStories []telegramPRDStory `json:"userStories"`
}

type telegramPRDSession struct {
	ChatID          int64              `json:"chat_id"`
	Stage           string             `json:"stage"`
	ProductName     string             `json:"product_name"`
	Stories         []telegramPRDStory `json:"stories"`
	DraftTitle      string             `json:"draft_title,omitempty"`
	DraftDesc       string             `json:"draft_desc,omitempty"`
	DraftRole       string             `json:"draft_role,omitempty"`
	CreatedAtUTC    string             `json:"created_at_utc,omitempty"`
	LastUpdatedAtUT string             `json:"last_updated_at_utc,omitempty"`
}

type telegramPRDSessionStore struct {
	Sessions map[string]telegramPRDSession `json:"sessions"`
}

func telegramPRDCommand(controlDir string, paths ralph.Paths, chatID int64, rawArgs string) (string, error) {
	fields := strings.Fields(strings.TrimSpace(rawArgs))
	if len(fields) == 0 {
		return telegramPRDHelp(), nil
	}
	sub := strings.ToLower(strings.TrimSpace(fields[0]))
	arg := strings.TrimSpace(strings.Join(fields[1:], " "))

	switch sub {
	case "help":
		return telegramPRDHelp(), nil
	case "start":
		return telegramPRDStartSession(controlDir, chatID, arg)
	case "preview", "status":
		return telegramPRDPreviewSession(controlDir, chatID)
	case "save":
		return telegramPRDSaveSession(controlDir, paths, chatID, arg)
	case "apply":
		return telegramPRDApplySession(controlDir, paths, chatID, arg)
	case "cancel", "stop":
		return telegramPRDCancelSession(controlDir, chatID)
	default:
		return "unknown /prd subcommand\n\n" + telegramPRDHelp(), nil
	}
}

func telegramPRDHelp() string {
	return strings.Join([]string{
		"Ralph PRD Wizard",
		"- /prd start [product_name]",
		"- /prd preview",
		"- /prd save [file]",
		"- /prd apply [file]",
		"- /prd cancel",
		"",
		"Flow",
		"1) /prd start",
		"2) answer prompts (product -> title -> description -> role -> priority)",
		"3) repeat story title or run /prd preview, /prd save, /prd apply",
	}, "\n")
}

func telegramPRDStartSession(controlDir string, chatID int64, productName string) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	session := telegramPRDSession{
		ChatID:          chatID,
		Stage:           telegramPRDStageAwaitProduct,
		ProductName:     "",
		Stories:         []telegramPRDStory{},
		CreatedAtUTC:    now,
		LastUpdatedAtUT: now,
	}
	productName = strings.TrimSpace(productName)
	if productName != "" {
		session.ProductName = productName
		session.Stage = telegramPRDStageAwaitStoryTitle
	}
	if err := telegramUpsertPRDSession(controlDir, session); err != nil {
		return "", err
	}
	if session.Stage == telegramPRDStageAwaitStoryTitle {
		return fmt.Sprintf("PRD wizard started\n- product: %s\n- next: 첫 user story 제목을 입력하세요", session.ProductName), nil
	}
	return "PRD wizard started\n- next: 제품/프로젝트 이름을 입력하세요", nil
}

func telegramPRDPreviewSession(controlDir string, chatID int64) (string, error) {
	session, found, err := telegramLoadPRDSession(controlDir, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}
	var b strings.Builder
	fmt.Fprintln(&b, "PRD session")
	fmt.Fprintf(&b, "- product: %s\n", valueOrDash(strings.TrimSpace(session.ProductName)))
	fmt.Fprintf(&b, "- stage: %s\n", session.Stage)
	fmt.Fprintf(&b, "- stories: %d\n", len(session.Stories))
	maxRows := len(session.Stories)
	if maxRows > 10 {
		maxRows = 10
	}
	for i := 0; i < maxRows; i++ {
		s := session.Stories[i]
		fmt.Fprintf(&b, "- [%d] %s | role=%s | priority=%d\n", i+1, compactSingleLine(s.Title, 70), s.Role, s.Priority)
	}
	if len(session.Stories) > maxRows {
		fmt.Fprintf(&b, "- ... and %d more\n", len(session.Stories)-maxRows)
	}
	fmt.Fprintf(&b, "- next: %s\n", telegramPRDStagePrompt(session.Stage))
	return b.String(), nil
}

func telegramPRDSaveSession(controlDir string, paths ralph.Paths, chatID int64, rawPath string) (string, error) {
	session, found, err := telegramLoadPRDSession(controlDir, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("no active PRD session (run: /prd start)")
	}
	if len(session.Stories) == 0 {
		return "", fmt.Errorf("no stories in session yet")
	}
	targetPath, err := resolveTelegramPRDFilePath(paths, chatID, rawPath)
	if err != nil {
		return "", err
	}
	if err := writeTelegramPRDFile(targetPath, session); err != nil {
		return "", err
	}
	return fmt.Sprintf("prd saved\n- file: %s\n- stories: %d", targetPath, len(session.Stories)), nil
}

func telegramPRDApplySession(controlDir string, paths ralph.Paths, chatID int64, rawPath string) (string, error) {
	session, found, err := telegramLoadPRDSession(controlDir, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("no active PRD session (run: /prd start)")
	}
	if len(session.Stories) == 0 {
		return "", fmt.Errorf("no stories in session yet")
	}
	targetPath, err := resolveTelegramPRDFilePath(paths, chatID, rawPath)
	if err != nil {
		return "", err
	}
	if err := writeTelegramPRDFile(targetPath, session); err != nil {
		return "", err
	}
	result, err := ralph.ImportPRDStories(paths, targetPath, "developer", false)
	if err != nil {
		return "", err
	}
	if err := telegramDeletePRDSession(controlDir, chatID); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"prd applied\n- file: %s\n- stories_total: %d\n- imported: %d\n- skipped_existing: %d\n- skipped_invalid: %d\n- next: /status",
		targetPath,
		result.StoriesTotal,
		result.Imported,
		result.SkippedExisting,
		result.SkippedInvalid,
	), nil
}

func telegramPRDCancelSession(controlDir string, chatID int64) (string, error) {
	if err := telegramDeletePRDSession(controlDir, chatID); err != nil {
		return "", err
	}
	return "PRD session canceled", nil
}

func telegramPRDHandleInput(controlDir string, paths ralph.Paths, chatID int64, input string) (string, error) {
	session, found, err := telegramLoadPRDSession(controlDir, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("no active PRD session")
	}

	updated, reply, err := advanceTelegramPRDSession(session, input)
	if err != nil {
		return "", err
	}
	if err := telegramUpsertPRDSession(controlDir, updated); err != nil {
		return "", err
	}
	_ = paths
	return reply, nil
}

func advanceTelegramPRDSession(session telegramPRDSession, input string) (telegramPRDSession, string, error) {
	session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
	input = strings.TrimSpace(input)
	if input == "" {
		return session, telegramPRDStagePrompt(session.Stage), nil
	}

	switch session.Stage {
	case telegramPRDStageAwaitProduct:
		session.ProductName = input
		session.Stage = telegramPRDStageAwaitStoryTitle
		return session, fmt.Sprintf("product set: %s\n- next: 첫 user story 제목을 입력하세요", session.ProductName), nil

	case telegramPRDStageAwaitStoryTitle:
		session.DraftTitle = input
		session.Stage = telegramPRDStageAwaitStoryDesc
		return session, "story title saved\n- next: 설명을 입력하세요", nil

	case telegramPRDStageAwaitStoryDesc:
		session.DraftDesc = input
		session.Stage = telegramPRDStageAwaitStoryRole
		return session, "story description saved\n- next: role 입력 (manager|planner|developer|qa)", nil

	case telegramPRDStageAwaitStoryRole:
		role, err := parseTelegramPRDStoryRole(input)
		if err != nil {
			return session, "", err
		}
		session.DraftRole = role
		session.Stage = telegramPRDStageAwaitStoryPrio
		return session, fmt.Sprintf("role saved: %s\n- next: priority 입력 (숫자, 기본값=%d)", role, telegramPRDDefaultPriority), nil

	case telegramPRDStageAwaitStoryPrio:
		priority, err := parseTelegramPRDStoryPriority(input)
		if err != nil {
			return session, "", err
		}
		idx := len(session.Stories) + 1
		story := telegramPRDStory{
			ID:          telegramPRDStoryID(session, idx),
			Title:       strings.TrimSpace(session.DraftTitle),
			Description: strings.TrimSpace(session.DraftDesc),
			Role:        strings.TrimSpace(session.DraftRole),
			Priority:    priority,
		}
		if strings.TrimSpace(story.Title) == "" || strings.TrimSpace(story.Description) == "" || strings.TrimSpace(story.Role) == "" {
			return session, "", fmt.Errorf("incomplete story draft; run /prd cancel then /prd start")
		}
		session.Stories = append(session.Stories, story)
		session.DraftTitle = ""
		session.DraftDesc = ""
		session.DraftRole = ""
		session.Stage = telegramPRDStageAwaitStoryTitle
		return session, fmt.Sprintf(
			"story added\n- id: %s\n- title: %s\n- role: %s\n- priority: %d\n- stories_total: %d\n- next: 다음 story 제목 입력 또는 /prd preview /prd save /prd apply",
			story.ID,
			compactSingleLine(story.Title, 90),
			story.Role,
			story.Priority,
			len(session.Stories),
		), nil

	default:
		session.Stage = telegramPRDStageAwaitProduct
		return session, "session stage reset\n- next: 제품/프로젝트 이름을 입력하세요", nil
	}
}

func parseTelegramPRDStoryRole(input string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(input))
	switch v {
	case "1":
		v = "manager"
	case "2":
		v = "planner"
	case "3":
		v = "developer"
	case "4":
		v = "qa"
	}
	if !ralph.IsSupportedRole(v) {
		return "", fmt.Errorf("invalid role: %q (use manager|planner|developer|qa)", input)
	}
	return v, nil
}

func parseTelegramPRDStoryPriority(input string) (int, error) {
	v := strings.TrimSpace(strings.ToLower(input))
	if v == "" || v == "default" || v == "skip" {
		return telegramPRDDefaultPriority, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid priority: %q (use positive number)", input)
	}
	return n, nil
}

func telegramPRDStagePrompt(stage string) string {
	switch stage {
	case telegramPRDStageAwaitProduct:
		return "제품/프로젝트 이름을 입력하세요"
	case telegramPRDStageAwaitStoryTitle:
		return "story 제목을 입력하세요"
	case telegramPRDStageAwaitStoryDesc:
		return "story 설명을 입력하세요"
	case telegramPRDStageAwaitStoryRole:
		return "role 입력 (manager|planner|developer|qa)"
	case telegramPRDStageAwaitStoryPrio:
		return fmt.Sprintf("priority 입력 (숫자, 기본값=%d)", telegramPRDDefaultPriority)
	default:
		return "unknown stage"
	}
}

func telegramHasActivePRDSession(controlDir string, chatID int64) (bool, error) {
	_, found, err := telegramLoadPRDSession(controlDir, chatID)
	return found, err
}

func telegramPRDSessionFile(controlDir string) string {
	return filepath.Join(controlDir, "telegram-prd-sessions.json")
}

func telegramSessionKey(chatID int64) string {
	return strconv.FormatInt(chatID, 10)
}

func loadTelegramPRDSessionStore(controlDir string) (telegramPRDSessionStore, error) {
	path := telegramPRDSessionFile(controlDir)
	store := telegramPRDSessionStore{Sessions: map[string]telegramPRDSession{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return store, nil
		}
		return store, fmt.Errorf("read prd session store: %w", err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return store, nil
	}
	if err := json.Unmarshal(data, &store); err != nil {
		return store, fmt.Errorf("parse prd session store: %w", err)
	}
	if store.Sessions == nil {
		store.Sessions = map[string]telegramPRDSession{}
	}
	return store, nil
}

func saveTelegramPRDSessionStore(controlDir string, store telegramPRDSessionStore) error {
	path := telegramPRDSessionFile(controlDir)
	if store.Sessions == nil {
		store.Sessions = map[string]telegramPRDSession{}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create prd session dir: %w", err)
	}
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal prd session store: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write prd session store: %w", err)
	}
	return nil
}

func telegramLoadPRDSession(controlDir string, chatID int64) (telegramPRDSession, bool, error) {
	store, err := loadTelegramPRDSessionStore(controlDir)
	if err != nil {
		return telegramPRDSession{}, false, err
	}
	key := telegramSessionKey(chatID)
	session, ok := store.Sessions[key]
	return session, ok, nil
}

func telegramUpsertPRDSession(controlDir string, session telegramPRDSession) error {
	store, err := loadTelegramPRDSessionStore(controlDir)
	if err != nil {
		return err
	}
	key := telegramSessionKey(session.ChatID)
	store.Sessions[key] = session
	return saveTelegramPRDSessionStore(controlDir, store)
}

func telegramDeletePRDSession(controlDir string, chatID int64) error {
	store, err := loadTelegramPRDSessionStore(controlDir)
	if err != nil {
		return err
	}
	delete(store.Sessions, telegramSessionKey(chatID))
	return saveTelegramPRDSessionStore(controlDir, store)
}

func resolveTelegramPRDFilePath(paths ralph.Paths, chatID int64, raw string) (string, error) {
	if err := ralph.EnsureLayout(paths); err != nil {
		return "", err
	}
	target := strings.TrimSpace(raw)
	if target == "" {
		target = filepath.Join(paths.ReportsDir, fmt.Sprintf("telegram-prd-%d.json", chatID))
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(paths.ProjectDir, target)
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolve prd path: %w", err)
	}
	return absTarget, nil
}

func writeTelegramPRDFile(path string, session telegramPRDSession) error {
	product := strings.TrimSpace(session.ProductName)
	if product == "" {
		product = telegramPRDDefaultProductFallback
	}
	stories := make([]telegramPRDStory, 0, len(session.Stories))
	for _, story := range session.Stories {
		s := story
		if strings.TrimSpace(s.ID) == "" {
			s.ID = telegramPRDStoryID(session, len(stories)+1)
		}
		if strings.TrimSpace(s.Role) == "" {
			s.Role = "developer"
		}
		if s.Priority <= 0 {
			s.Priority = telegramPRDDefaultPriority
		}
		stories = append(stories, s)
	}
	doc := map[string]any{
		"metadata": map[string]any{
			"product":          product,
			"source":           "telegram-prd-wizard",
			"generated_at_utc": time.Now().UTC().Format(time.RFC3339),
		},
		"userStories": telegramPRDDocument{
			UserStories: stories,
		}.UserStories,
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal prd json: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create prd dir: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write prd json: %w", err)
	}
	return nil
}

func telegramPRDStoryID(session telegramPRDSession, idx int) string {
	prefixTime := time.Now().UTC()
	if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(session.CreatedAtUTC)); err == nil {
		prefixTime = parsed.UTC()
	}
	if idx <= 0 {
		idx = 1
	}
	return fmt.Sprintf("TG-%s-%03d", prefixTime.Format("20060102T150405Z"), idx)
}

func buildFleetTargetArgs(sub string, spec telegramTargetSpec) []string {
	args := []string{sub}
	if spec.All {
		return append(args, "--all")
	}
	if strings.TrimSpace(spec.ProjectID) != "" {
		return append(args, "--id", spec.ProjectID)
	}
	return args
}

func runFleetDoctorReports(controlDir string, spec telegramTargetSpec) (string, error) {
	projects, pathsByID, err := resolveTelegramFleetPaths(controlDir, spec)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Ralph Fleet Doctor\n")
	fmt.Fprintf(&b, "- target: %s\n", spec.Label())
	fmt.Fprintf(&b, "- projects: %d\n", len(projects))
	for _, p := range projects {
		report, err := ralph.RunDoctor(pathsByID[p.ID])
		if err != nil {
			fmt.Fprintf(&b, "- project=%s status=fail detail=%s\n", p.ID, compactSingleLine(err.Error(), 160))
			continue
		}
		pass, warn, fail := countDoctorChecks(report)
		fmt.Fprintf(&b, "- project=%s pass=%d warn=%d fail=%d\n", p.ID, pass, warn, fail)
	}
	return b.String(), nil
}

func parseTelegramCommandLine(raw string) (string, string) {
	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) == 0 {
		return "", ""
	}
	cmd := strings.ToLower(strings.TrimSpace(fields[0]))
	if i := strings.IndexByte(cmd, '@'); i > 0 {
		cmd = cmd[:i]
	}
	if !strings.HasPrefix(cmd, "/") {
		cmd = "/" + cmd
	}
	args := strings.TrimSpace(strings.Join(fields[1:], " "))
	return cmd, args
}

func buildTelegramHelp(allowControl bool) string {
	lines := []string{
		"Ralph Telegram commands",
		"- /help",
		"- /ping",
		"- /status [all|<project_id>]",
		"- /doctor [all|<project_id>]",
		"- /fleet [all|<project_id>]",
	}
	if allowControl {
		lines = append(lines,
			"- /start [all|<project_id>]",
			"- /stop [all|<project_id>]",
			"- /restart [all|<project_id>]",
			"- /doctor_repair [all|<project_id>]",
			"- /recover [all|<project_id>]",
			"- /new [role] <title> (default role: developer)",
			"- /prd help",
		)
	} else {
		lines = append(lines, "- control commands disabled (--allow-control)")
	}
	return strings.Join(lines, "\n")
}

func formatStatusForTelegram(st ralph.Status) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Ralph Status\n")
	fmt.Fprintf(&b, "- project: %s\n", st.ProjectDir)
	fmt.Fprintf(&b, "- plugin: %s\n", st.PluginName)
	fmt.Fprintf(&b, "- daemon: %s\n", st.Daemon)
	fmt.Fprintf(&b, "- queue: ready=%d in_progress=%d done=%d blocked=%d\n", st.QueueReady, st.InProgress, st.Done, st.Blocked)
	fmt.Fprintf(&b, "- next_ready: %s\n", st.NextReady)
	if ralph.IsInputRequiredStatus(st) {
		fmt.Fprintf(&b, "- input_required: true\n")
		fmt.Fprintf(&b, "- input_hint: add issue (`./ralph new ...`) or import PRD (`./ralph import-prd --file prd.json`)\n")
	}
	if st.LastProfileReloadAt != "" || st.ProfileReloadCount > 0 {
		fmt.Fprintf(&b, "- profile_reload_at: %s\n", valueOrDash(st.LastProfileReloadAt))
		fmt.Fprintf(&b, "- profile_reload_count: %d\n", st.ProfileReloadCount)
	}
	if st.LastFailureCause != "" || st.LastCodexRetryCount > 0 || st.LastPermissionStreak > 0 {
		fmt.Fprintf(
			&b,
			"- last_failure: %s | codex_retries=%d | perm_streak=%d\n",
			compactSingleLine(st.LastFailureCause, 120),
			st.LastCodexRetryCount,
			st.LastPermissionStreak,
		)
	}
	return b.String()
}

func formatDoctorReportForTelegram(report ralph.DoctorReport) string {
	pass, warn, fail := countDoctorChecks(report)

	var b strings.Builder
	fmt.Fprintln(&b, "Ralph Doctor")
	fmt.Fprintf(&b, "- updated_utc: %s\n", report.UpdatedUTC.Format(time.RFC3339))
	fmt.Fprintf(&b, "- summary: pass=%d warn=%d fail=%d\n", pass, warn, fail)
	if fail > 0 || warn > 0 {
		printed := 0
		for _, c := range report.Checks {
			if c.Status == "pass" {
				continue
			}
			fmt.Fprintf(&b, "- [%s] %s: %s\n", c.Status, c.Name, compactSingleLine(c.Detail, 140))
			printed++
			if printed >= 12 {
				break
			}
		}
	}
	return b.String()
}

func formatDoctorRepairActions(actions []ralph.DoctorRepairAction) string {
	pass, warn, fail := countDoctorRepairActions(actions)
	var b strings.Builder
	fmt.Fprintf(&b, "doctor repair completed\n")
	fmt.Fprintf(&b, "- summary: pass=%d warn=%d fail=%d\n", pass, warn, fail)
	for _, a := range actions {
		if a.Status == "pass" {
			continue
		}
		fmt.Fprintf(&b, "- [%s] %s: %s\n", a.Status, a.Name, compactSingleLine(a.Detail, 120))
	}
	return b.String()
}

func countDoctorChecks(report ralph.DoctorReport) (int, int, int) {
	pass, warn, fail := 0, 0, 0
	for _, c := range report.Checks {
		switch c.Status {
		case "pass":
			pass++
		case "warn":
			warn++
		case "fail":
			fail++
		}
	}
	return pass, warn, fail
}

func countDoctorRepairActions(actions []ralph.DoctorRepairAction) (int, int, int) {
	pass, warn, fail := 0, 0, 0
	for _, a := range actions {
		switch a.Status {
		case "pass":
			pass++
		case "warn":
			warn++
		case "fail":
			fail++
		}
	}
	return pass, warn, fail
}

func normalizeNotifyScope(raw string) (string, error) {
	scope := strings.ToLower(strings.TrimSpace(raw))
	if scope == "" {
		scope = "auto"
	}
	switch scope {
	case "auto", "project", "fleet":
		return scope, nil
	default:
		return "", fmt.Errorf("expected one of auto|project|fleet")
	}
}

func requiresUserAllowlistForControl(allowedChatIDs map[int64]struct{}) bool {
	for chatID := range allowedChatIDs {
		if chatID < 0 {
			return true
		}
	}
	return false
}

func newScopedStatusNotifyHandler(controlDir string, paths ralph.Paths, scope string, retryThreshold, permThreshold int) ralph.TelegramNotifyHandler {
	switch scope {
	case "fleet":
		return newFleetStatusNotifyHandler(controlDir, paths, retryThreshold, permThreshold)
	case "auto":
		enabled, err := hasFleetProjects(controlDir)
		if err != nil || !enabled {
			return newStatusNotifyHandler(paths, retryThreshold, permThreshold)
		}
		return newFleetStatusNotifyHandler(controlDir, paths, retryThreshold, permThreshold)
	default:
		return newStatusNotifyHandler(paths, retryThreshold, permThreshold)
	}
}

func hasFleetProjects(controlDir string) (bool, error) {
	cfg, err := ralph.LoadFleetConfig(controlDir)
	if err != nil {
		return false, err
	}
	return len(cfg.Projects) > 0, nil
}

func newFleetStatusNotifyHandler(controlDir string, defaultPaths ralph.Paths, retryThreshold, permThreshold int) ralph.TelegramNotifyHandler {
	initialized := false
	prevByProject := map[string]ralph.Status{}
	return func(ctx context.Context) ([]string, error) {
		_ = ctx

		cfg, err := ralph.LoadFleetConfig(controlDir)
		if err != nil {
			return nil, err
		}

		type notifyProject struct {
			ID       string
			Paths    ralph.Paths
			FullName string
		}
		targets := make([]notifyProject, 0, len(cfg.Projects))
		if len(cfg.Projects) == 0 {
			targets = append(targets, notifyProject{
				ID:       "current",
				Paths:    defaultPaths,
				FullName: defaultPaths.ProjectDir,
			})
		} else {
			for _, p := range cfg.Projects {
				projectPaths, err := ralph.NewPaths(controlDir, p.ProjectDir)
				if err != nil {
					return nil, err
				}
				targets = append(targets, notifyProject{
					ID:       p.ID,
					Paths:    projectPaths,
					FullName: p.ProjectDir,
				})
			}
		}

		alerts := []string{}
		currByProject := make(map[string]ralph.Status, len(targets))
		for _, target := range targets {
			current, err := ralph.GetStatus(target.Paths)
			if err != nil {
				return nil, err
			}
			current.ProjectDir = fmt.Sprintf("%s (%s)", target.ID, target.FullName)
			currByProject[target.ID] = current
			if !initialized {
				continue
			}
			prev, ok := prevByProject[target.ID]
			if !ok {
				continue
			}
			alerts = append(alerts, buildStatusAlerts(prev, current, retryThreshold, permThreshold)...)
		}

		prevByProject = currByProject
		if !initialized {
			initialized = true
			return nil, nil
		}
		return alerts, nil
	}
}

func newStatusNotifyHandler(paths ralph.Paths, retryThreshold, permThreshold int) ralph.TelegramNotifyHandler {
	initialized := false
	prev := ralph.Status{}
	return func(ctx context.Context) ([]string, error) {
		_ = ctx
		current, err := ralph.GetStatus(paths)
		if err != nil {
			return nil, err
		}
		if !initialized {
			initialized = true
			prev = current
			return nil, nil
		}
		alerts := buildStatusAlerts(prev, current, retryThreshold, permThreshold)
		prev = current
		return alerts, nil
	}
}

func buildStatusAlerts(prev, current ralph.Status, retryThreshold, permThreshold int) []string {
	out := []string{}
	project := current.ProjectDir
	if strings.TrimSpace(project) == "" {
		project = "(unknown-project)"
	}

	if current.Blocked > prev.Blocked {
		out = append(out, fmt.Sprintf(
			"[ralph alert][blocked]\n- project: %s\n- blocked: %d (+%d)\n- reason: %s\n- updated_at: %s",
			project,
			current.Blocked,
			current.Blocked-prev.Blocked,
			valueOrDash(compactSingleLine(current.LastFailureCause, 160)),
			valueOrDash(current.LastFailureUpdatedAt),
		))
	} else if current.LastFailureUpdatedAt != "" && current.LastFailureUpdatedAt != prev.LastFailureUpdatedAt {
		out = append(out, fmt.Sprintf(
			"[ralph alert][failure]\n- project: %s\n- reason: %s\n- updated_at: %s",
			project,
			valueOrDash(compactSingleLine(current.LastFailureCause, 160)),
			current.LastFailureUpdatedAt,
		))
	}

	if retryThreshold > 0 && current.LastCodexRetryCount >= retryThreshold && current.LastFailureUpdatedAt != "" && current.LastFailureUpdatedAt != prev.LastFailureUpdatedAt {
		out = append(out, fmt.Sprintf(
			"[ralph alert][retry]\n- project: %s\n- codex_retries: %d (threshold=%d)\n- reason: %s",
			project,
			current.LastCodexRetryCount,
			retryThreshold,
			valueOrDash(compactSingleLine(current.LastFailureCause, 160)),
		))
	}

	if current.LastBusyWaitDetectedAt != "" &&
		current.LastBusyWaitDetectedAt != prev.LastBusyWaitDetectedAt &&
		(current.QueueReady > 0 || current.InProgress > 0) {
		out = append(out, fmt.Sprintf(
			"[ralph alert][stuck]\n- project: %s\n- busywait_detected_at: %s\n- idle_count: %d",
			project,
			current.LastBusyWaitDetectedAt,
			current.LastBusyWaitIdleCount,
		))
	}

	if permThreshold > 0 && current.LastPermissionStreak >= permThreshold && current.LastPermissionStreak > prev.LastPermissionStreak {
		out = append(out, fmt.Sprintf(
			"[ralph alert][permission]\n- project: %s\n- permission_streak: %d (threshold=%d)\n- last_failure: %s",
			project,
			current.LastPermissionStreak,
			permThreshold,
			valueOrDash(compactSingleLine(current.LastFailureCause, 160)),
		))
	}
	if ralph.IsInputRequiredStatus(current) && !ralph.IsInputRequiredStatus(prev) {
		out = append(out, fmt.Sprintf(
			"[ralph alert][input_required]\n- project: %s\n- message: no queued work. add issue (`./ralph new ...`) or import PRD (`./ralph import-prd --file prd.json`)",
			project,
		))
	}

	return out
}

func startTelegramDaemon(paths ralph.Paths, runArgs []string) (string, error) {
	if err := ralph.EnsureLayout(paths); err != nil {
		return "", err
	}

	pidFile := paths.TelegramPIDFile()
	pid, running, stale := telegramPIDState(pidFile)
	if running {
		return fmt.Sprintf("telegram bot already running (pid=%d)", pid), nil
	}
	if stale {
		_ = os.Remove(pidFile)
	}

	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	logFile := paths.TelegramLogFile()
	logHandle, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return "", fmt.Errorf("open telegram log: %w", err)
	}
	defer logHandle.Close()

	args := []string{
		"--control-dir", paths.ControlDir,
		"--project-dir", paths.ProjectDir,
		"telegram",
		"run",
	}
	args = append(args, runArgs...)

	cmd := exec.Command(exe, args...)
	cmd.Stdout = logHandle
	cmd.Stderr = logHandle
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start telegram daemon: %w", err)
	}
	pid = cmd.Process.Pid
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
		return "", fmt.Errorf("write telegram pid file: %w", err)
	}
	_ = cmd.Process.Release()
	return fmt.Sprintf("telegram bot started (pid=%d)", pid), nil
}

func stopTelegramDaemon(paths ralph.Paths) (string, error) {
	if err := ralph.EnsureLayout(paths); err != nil {
		return "", err
	}

	pidFile := paths.TelegramPIDFile()
	pid, running, stale := telegramPIDState(pidFile)
	if !running {
		_ = os.Remove(pidFile)
		if stale {
			return fmt.Sprintf("telegram bot stopped (stale pid removed: %d)", pid), nil
		}
		return "telegram bot is not running", nil
	}

	proc, err := os.FindProcess(pid)
	if err == nil {
		_ = proc.Signal(syscall.SIGTERM)
	}
	for i := 0; i < 30; i++ {
		if !isTelegramPIDRunning(pid) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if isTelegramPIDRunning(pid) {
		if proc, findErr := os.FindProcess(pid); findErr == nil {
			_ = proc.Signal(syscall.SIGKILL)
		}
	}
	_ = os.Remove(pidFile)
	return fmt.Sprintf("telegram bot stopped (pid=%d)", pid), nil
}

func telegramPIDState(pidFile string) (int, bool, bool) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, false, false
	}
	raw := strings.TrimSpace(string(data))
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		return 0, false, true
	}
	if isTelegramPIDRunning(pid) {
		return pid, true, false
	}
	return pid, false, true
}

func isTelegramPIDRunning(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

func ensureTelegramForegroundArg(args []string) []string {
	out := append([]string{}, args...)
	out = append(out, "--foreground")
	return out
}

func tailFile(path string, lines int, follow bool) error {
	if lines <= 0 {
		lines = 120
	}
	tailArgs := []string{"-n", strconv.Itoa(lines)}
	if follow {
		tailArgs = append(tailArgs, "-f")
	}
	tailArgs = append(tailArgs, path)

	cmd := exec.Command("tail", tailArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func envBoolDefault(key string, defaultValue bool) bool {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return defaultValue
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return defaultValue
	}
}

func envIntDefault(key string, defaultValue int) int {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return defaultValue
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return defaultValue
	}
	return v
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func parseBoolRaw(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		return true, true
	case "0", "false", "no", "n", "off":
		return false, true
	default:
		return false, false
	}
}

func parseIntRaw(raw string) (int, bool) {
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, false
	}
	return v, true
}

func envQuoteValue(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return `""`
	}
	if strings.ContainsAny(raw, " \t#") {
		if !strings.Contains(raw, `"`) {
			return `"` + raw + `"`
		}
		if !strings.Contains(raw, "'") {
			return "'" + raw + "'"
		}
	}
	return raw
}
