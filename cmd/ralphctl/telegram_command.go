package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"codex-ralph/internal/ralph"
)

func runTelegramCommand(controlDir string, paths ralph.Paths, args []string) error {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --control-dir DIR --project-dir DIR telegram <run|setup|stop|status|tail> [flags]")
		fmt.Fprintln(os.Stderr, "Env: RALPH_TELEGRAM_BOT_TOKEN, RALPH_TELEGRAM_CHAT_IDS, RALPH_TELEGRAM_USER_IDS, RALPH_TELEGRAM_ALLOW_CONTROL, RALPH_TELEGRAM_NOTIFY, RALPH_TELEGRAM_NOTIFY_SCOPE, RALPH_TELEGRAM_COMMAND_TIMEOUT_SEC, RALPH_TELEGRAM_COMMAND_CONCURRENCY")
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
	commandTimeoutSec := fs.Int("command-timeout-sec", envIntDefault("RALPH_TELEGRAM_COMMAND_TIMEOUT_SEC", cfg.CommandTimeoutSec), "timeout seconds per telegram command")
	commandConcurrency := fs.Int("command-concurrency", envIntDefault("RALPH_TELEGRAM_COMMAND_CONCURRENCY", cfg.CommandConcurrency), "max concurrent command workers across chats")
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
	if *commandTimeoutSec <= 0 {
		return fmt.Errorf("--command-timeout-sec must be > 0")
	}
	if *commandConcurrency <= 0 {
		return fmt.Errorf("--command-concurrency must be > 0")
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
		fmt.Println("Telegram Daemon")
		fmt.Println("===============")
		fmt.Println(msg)
		fmt.Println()
		fmt.Printf("Control Dir: %s\n", controlDir)
		fmt.Printf("Project Dir: %s\n", paths.ProjectDir)
		fmt.Printf("Config:      %s\n", configFile)
		fmt.Printf("PID File:    %s\n", paths.TelegramPIDFile())
		fmt.Printf("Log File:    %s\n", paths.TelegramLogFile())
		fmt.Println("Mode:        daemon")
		fmt.Println()
		fmt.Println("Quick Commands")
		fmt.Println("- stop:   ralphctl telegram stop")
		fmt.Println("- status: ralphctl telegram status")
		fmt.Println("- logs:   ralphctl telegram tail")
		return nil
	}

	fmt.Println("Telegram Bot")
	fmt.Println("============")
	fmt.Println("Started in foreground mode")
	fmt.Println()
	fmt.Printf("Control Dir:   %s\n", controlDir)
	fmt.Printf("Project Dir:   %s\n", paths.ProjectDir)
	fmt.Printf("Config:        %s\n", configFile)
	fmt.Printf("Allow Control: %t\n", *allowControl)
	fmt.Printf("Notify:        %t\n", *enableNotify)
	fmt.Printf("Notify Scope:  %s\n", resolvedNotifyScope)
	fmt.Printf("Notify Every:  %ds\n", *notifyIntervalSec)
	fmt.Printf("Retry Alert:   %d\n", *notifyRetryThreshold)
	fmt.Printf("Perm Alert:    %d\n", *notifyPermStreakThreshold)
	fmt.Printf("Cmd Timeout:   %ds\n", *commandTimeoutSec)
	fmt.Printf("Cmd Workers:   %d\n", *commandConcurrency)
	fmt.Printf("Allowed Chats: %d\n", len(allowedChatIDs))
	if len(allowedUserIDs) > 0 {
		fmt.Printf("Allowed Users: %d\n", len(allowedUserIDs))
	} else {
		fmt.Printf("Allowed Users: any (chat allowlist only)\n")
	}
	fmt.Printf("Offset File:   %s\n", *offsetFile)

	notifyHandler := ralph.TelegramNotifyHandler(nil)
	if *enableNotify {
		notifyHandler = newScopedStatusNotifyHandler(controlDir, paths, resolvedNotifyScope, *notifyRetryThreshold, *notifyPermStreakThreshold)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return ralph.RunTelegramBot(ctx, ralph.TelegramBotOptions{
		Token:              *token,
		AllowedChatIDs:     allowedChatIDs,
		AllowedUserIDs:     allowedUserIDs,
		PollTimeoutSec:     *pollTimeoutSec,
		NotifyIntervalSec:  *notifyIntervalSec,
		CommandTimeoutSec:  *commandTimeoutSec,
		CommandConcurrency: *commandConcurrency,
		OffsetFile:         *offsetFile,
		Out:                os.Stdout,
		OnCommand:          telegramCommandHandler(controlDir, paths, *allowControl),
		OnNotifyTick:       notifyHandler,
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
	fmt.Println("Telegram Status")
	fmt.Println("===============")
	fmt.Printf("Control Dir: %s\n", controlDir)
	fmt.Printf("Project Dir: %s\n", paths.ProjectDir)
	fmt.Printf("PID File:    %s\n", paths.TelegramPIDFile())
	fmt.Printf("Log File:    %s\n", paths.TelegramLogFile())
	fmt.Printf("Offset File: %s\n", strings.TrimSpace(*offsetFile))
	switch {
	case running:
		fmt.Printf("Daemon:      running (pid=%d)\n", pid)
	case stale:
		fmt.Printf("Daemon:      stopped (stale pid=%d)\n", pid)
	default:
		fmt.Println("Daemon:      stopped")
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
	defaultCommandTimeout := envIntDefault("RALPH_TELEGRAM_COMMAND_TIMEOUT_SEC", cfg.CommandTimeoutSec)
	defaultCommandConcurrency := envIntDefault("RALPH_TELEGRAM_COMMAND_CONCURRENCY", cfg.CommandConcurrency)

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
	commandTimeoutFlag := fs.Int("command-timeout-sec", defaultCommandTimeout, "timeout seconds per telegram command")
	commandConcurrencyFlag := fs.Int("command-concurrency", defaultCommandConcurrency, "max concurrent command workers across chats")
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
		CommandTimeoutSec:         *commandTimeoutFlag,
		CommandConcurrency:        *commandConcurrencyFlag,
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

		timeoutInput, err := promptFleetInput(reader, "Command timeout sec", strconv.Itoa(final.CommandTimeoutSec))
		if err != nil {
			return err
		}
		if v, convErr := strconv.Atoi(strings.TrimSpace(timeoutInput)); convErr == nil {
			final.CommandTimeoutSec = v
		}

		workersInput, err := promptFleetInput(reader, "Command concurrency", strconv.Itoa(final.CommandConcurrency))
		if err != nil {
			return err
		}
		if v, convErr := strconv.Atoi(strings.TrimSpace(workersInput)); convErr == nil {
			final.CommandConcurrency = v
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
	if final.CommandTimeoutSec <= 0 {
		return fmt.Errorf("command-timeout-sec must be > 0")
	}
	if final.CommandConcurrency <= 0 {
		return fmt.Errorf("command-concurrency must be > 0")
	}
	scope, err := normalizeNotifyScope(final.NotifyScope)
	if err != nil {
		return fmt.Errorf("notify-scope: %w", err)
	}
	final.NotifyScope = scope
	if err := saveTelegramCLIConfig(configFile, final); err != nil {
		return err
	}

	fmt.Println("Telegram Setup Complete")
	fmt.Println("======================")
	fmt.Printf("Config:        %s\n", configFile)
	fmt.Printf("Allow Control: %t\n", final.AllowControl)
	fmt.Printf("Notify:        %t\n", final.Notify)
	fmt.Printf("Notify Scope:  %s\n", final.NotifyScope)
	fmt.Printf("Cmd Timeout:   %ds\n", final.CommandTimeoutSec)
	fmt.Printf("Cmd Workers:   %d\n", final.CommandConcurrency)
	fmt.Println()
	fmt.Println("Next Commands")
	fmt.Printf("- run:    ralphctl --project-dir \"$PWD\" telegram run --config-file %s\n", configFile)
	fmt.Printf("- status: ralphctl --project-dir \"$PWD\" telegram status\n")
	fmt.Printf("- stop:   ralphctl --project-dir \"$PWD\" telegram stop\n")
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
	CommandTimeoutSec         int
	CommandConcurrency        int
}

func defaultTelegramCLIConfig() telegramCLIConfig {
	return telegramCLIConfig{
		AllowControl:              false,
		Notify:                    true,
		NotifyScope:               "auto",
		NotifyIntervalSec:         30,
		NotifyRetryThreshold:      2,
		NotifyPermStreakThreshold: 3,
		CommandTimeoutSec:         300,
		CommandConcurrency:        4,
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
	if v, ok := parseIntRaw(values["RALPH_TELEGRAM_COMMAND_TIMEOUT_SEC"]); ok {
		cfg.CommandTimeoutSec = v
	}
	if v, ok := parseIntRaw(values["RALPH_TELEGRAM_COMMAND_CONCURRENCY"]); ok {
		cfg.CommandConcurrency = v
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
	b.WriteString("RALPH_TELEGRAM_COMMAND_TIMEOUT_SEC=" + strconv.Itoa(cfg.CommandTimeoutSec) + "\n")
	b.WriteString("RALPH_TELEGRAM_COMMAND_CONCURRENCY=" + strconv.Itoa(cfg.CommandConcurrency) + "\n")
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
			hasSession, err := telegramHasActivePRDSession(paths, chatID)
			if err != nil {
				return "", err
			}
			if hasSession {
				return telegramPRDHandleInput(paths, chatID, text)
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
			return telegramPRDCommand(paths, chatID, cmdArgs)

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
	telegramPRDStageAwaitProblem      = "await_problem"
	telegramPRDStageAwaitGoal         = "await_goal"
	telegramPRDStageAwaitInScope      = "await_in_scope"
	telegramPRDStageAwaitOutOfScope   = "await_out_of_scope"
	telegramPRDStageAwaitAcceptance   = "await_acceptance"
	telegramPRDStageAwaitConstraints  = "await_constraints"
	telegramPRDDefaultPriority        = 1000
	telegramPRDDefaultProductFallback = "Telegram PRD"
	telegramPRDClarityMinScore        = 80
	telegramPRDAssumedPrefix          = "[assumed]"
	telegramPRDCodexAssistTimeoutSec  = 45
)

var telegramPRDRoleOrder = []string{"manager", "planner", "developer", "qa"}

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

type telegramPRDContext struct {
	Problem       string         `json:"problem,omitempty"`
	Goal          string         `json:"goal,omitempty"`
	InScope       string         `json:"in_scope,omitempty"`
	OutOfScope    string         `json:"out_of_scope,omitempty"`
	Acceptance    string         `json:"acceptance,omitempty"`
	Constraints   string         `json:"constraints,omitempty"`
	Assumptions   []string       `json:"assumptions,omitempty"`
	AgentPriority map[string]int `json:"agent_priority,omitempty"`
}

type telegramPRDSession struct {
	ChatID          int64              `json:"chat_id"`
	Stage           string             `json:"stage"`
	ProductName     string             `json:"product_name"`
	Stories         []telegramPRDStory `json:"stories"`
	Context         telegramPRDContext `json:"context,omitempty"`
	DraftTitle      string             `json:"draft_title,omitempty"`
	DraftDesc       string             `json:"draft_desc,omitempty"`
	DraftRole       string             `json:"draft_role,omitempty"`
	CodexScore      int                `json:"codex_score,omitempty"`
	CodexReady      bool               `json:"codex_ready,omitempty"`
	CodexMissing    []string           `json:"codex_missing,omitempty"`
	CodexSummary    string             `json:"codex_summary,omitempty"`
	CodexScoredAtUT string             `json:"codex_scored_at_utc,omitempty"`
	Approved        bool               `json:"approved,omitempty"`
	CreatedAtUTC    string             `json:"created_at_utc,omitempty"`
	LastUpdatedAtUT string             `json:"last_updated_at_utc,omitempty"`
}

type telegramPRDClarityStatus struct {
	Score         int
	RequiredTotal int
	RequiredReady int
	ReadyToApply  bool
	Missing       []string
	NextStage     string
	NextPrompt    string
}

type telegramPRDCodexAssistResponse struct {
	Intent           string `json:"intent"`
	Reply            string `json:"reply"`
	NormalizedAnswer string `json:"normalized_answer"`
}

type telegramPRDInputAssistResult struct {
	Handled       bool
	Reply         string
	InputOverride string
}

type telegramPRDCodexScoreResponse struct {
	Score        int      `json:"score"`
	ReadyToApply bool     `json:"ready_to_apply"`
	Missing      []string `json:"missing"`
	Summary      string   `json:"summary"`
}

type telegramPRDCodexStoryPriorityResponse struct {
	Priority int    `json:"priority"`
	Reason   string `json:"reason"`
}

type telegramPRDCodexRefineResponse struct {
	Score          int      `json:"score"`
	ReadyToApply   bool     `json:"ready_to_apply"`
	Ask            string   `json:"ask"`
	Missing        []string `json:"missing"`
	SuggestedStage string   `json:"suggested_stage"`
	Reason         string   `json:"reason"`
}

type telegramPRDSessionStore struct {
	Sessions map[string]telegramPRDSession `json:"sessions"`
}

var telegramPRDSessionStoreMu sync.Mutex
var telegramPRDCodexAssistAnalyzer = analyzeTelegramPRDInputWithCodex
var telegramPRDStoryPriorityEstimator = estimateTelegramPRDStoryPriorityWithCodex
var telegramPRDRefineAnalyzer = analyzeTelegramPRDRefineWithCodex
var telegramPRDScoreAnalyzer = analyzeTelegramPRDScoreWithCodex

func telegramPRDCommand(paths ralph.Paths, chatID int64, rawArgs string) (string, error) {
	fields := strings.Fields(strings.TrimSpace(rawArgs))
	if len(fields) == 0 {
		return telegramPRDHelp(), nil
	}
	sub := strings.ToLower(strings.TrimSpace(fields[0]))
	arg := strings.TrimSpace(strings.Join(fields[1:], " "))

	var (
		reply string
		err   error
	)
	switch sub {
	case "help":
		return telegramPRDHelp(), nil
	case "start":
		reply, err = telegramPRDStartSession(paths, chatID, arg)
	case "refine":
		reply, err = telegramPRDRefineSession(paths, chatID)
	case "score":
		reply, err = telegramPRDScoreSession(paths, chatID)
	case "approve":
		reply, err = telegramPRDApproveSession(paths, chatID)
	case "preview", "status":
		reply, err = telegramPRDPreviewSession(paths, chatID)
	case "priority":
		reply, err = telegramPRDPrioritySession(paths, chatID, arg)
	case "save":
		reply, err = telegramPRDSaveSession(paths, chatID, arg)
	case "apply":
		reply, err = telegramPRDApplySession(paths, chatID, arg)
	case "cancel", "stop":
		reply, err = telegramPRDCancelSession(paths, chatID)
	default:
		return "unknown /prd subcommand\n\n" + telegramPRDHelp(), nil
	}
	if err != nil {
		return "", err
	}
	commandText := "/prd " + sub
	if strings.TrimSpace(arg) != "" {
		commandText += " " + strings.TrimSpace(arg)
	}
	logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "user", commandText))
	logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "assistant", reply))
	return reply, nil
}

func telegramPRDHelp() string {
	return strings.Join([]string{
		"Ralph PRD Wizard",
		"================",
		"",
		"Commands",
		"- /prd start [product_name]",
		"- /prd refine",
		"- /prd score",
		"- /prd approve",
		"- /prd preview",
		"- /prd priority [manager=900 planner=950 developer=1000 qa=1100|default]",
		"- /prd save [file]",
		"- /prd apply [file]",
		"- /prd cancel",
		"",
		"Flow",
		"1) /prd start",
		"2) /prd refine (Codex가 부족한 컨텍스트를 동적으로 질문)",
		"3) (optional) /prd priority 로 에이전트별 기본 priority 조정",
		"4) answer prompts, then add stories",
		"   - 기본: title -> description -> role(선택: priority)",
		"   - 빠른 입력: title | description | role [priority]",
		"5) /prd score or /prd preview",
		"6) /prd apply",
	}, "\n")
}

func telegramPRDStartSession(paths ralph.Paths, chatID int64, productName string) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	session := telegramPRDSession{
		ChatID:      chatID,
		Stage:       telegramPRDStageAwaitProduct,
		ProductName: "",
		Stories:     []telegramPRDStory{},
		Context: telegramPRDContext{
			AgentPriority: telegramPRDDefaultAgentPriorityMap(),
		},
		Approved:        false,
		CreatedAtUTC:    now,
		LastUpdatedAtUT: now,
	}
	productName = strings.TrimSpace(productName)
	if productName != "" {
		session.ProductName = productName
		session.Stage = telegramPRDStageAwaitProblem
	}
	if err := clearTelegramPRDConversation(paths, chatID); err != nil {
		return "", err
	}
	if err := telegramUpsertPRDSession(paths, session); err != nil {
		return "", err
	}
	if session.Stage == telegramPRDStageAwaitProblem {
		return fmt.Sprintf("PRD wizard started\n- product: %s\n- next: /prd refine", session.ProductName), nil
	}
	return "PRD wizard started\n- next: 제품/프로젝트 이름을 입력하세요", nil
}

func telegramPRDDefaultPriorityForRole(role string) int {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "manager":
		return 900
	case "planner":
		return 950
	case "developer":
		return 1000
	case "qa":
		return 1100
	default:
		return telegramPRDDefaultPriority
	}
}

func telegramPRDDefaultAgentPriorityMap() map[string]int {
	out := make(map[string]int, len(telegramPRDRoleOrder))
	for _, role := range telegramPRDRoleOrder {
		out[role] = telegramPRDDefaultPriorityForRole(role)
	}
	return out
}

func copyTelegramPRDAgentPriorityMap(src map[string]int) map[string]int {
	if len(src) == 0 {
		return map[string]int{}
	}
	out := make(map[string]int, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func normalizeTelegramPRDAgentPriorityMap(src map[string]int) map[string]int {
	out := telegramPRDDefaultAgentPriorityMap()
	for _, role := range telegramPRDRoleOrder {
		if src == nil {
			continue
		}
		if v := src[role]; v > 0 {
			out[role] = v
		}
	}
	return out
}

func formatTelegramPRDAgentPriorityInline(priorityMap map[string]int) string {
	normalized := normalizeTelegramPRDAgentPriorityMap(priorityMap)
	parts := make([]string, 0, len(telegramPRDRoleOrder))
	for _, role := range telegramPRDRoleOrder {
		parts = append(parts, fmt.Sprintf("%s=%d", role, normalized[role]))
	}
	return strings.Join(parts, " ")
}

func parseTelegramPRDAgentPriorityArgs(raw string) (map[string]int, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return nil, fmt.Errorf("usage: /prd priority manager=900 planner=950 developer=1000 qa=1100")
	}
	text = strings.ReplaceAll(text, ",", " ")
	fields := strings.Fields(text)
	out := map[string]int{}
	for _, field := range fields {
		token := strings.TrimSpace(field)
		if token == "" {
			continue
		}
		sep := ""
		if strings.Contains(token, "=") {
			sep = "="
		} else if strings.Contains(token, ":") {
			sep = ":"
		}
		if sep == "" {
			return nil, fmt.Errorf("invalid token: %q (expected role=priority)", token)
		}
		parts := strings.SplitN(token, sep, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid token: %q", token)
		}
		role := strings.ToLower(strings.TrimSpace(parts[0]))
		if !ralph.IsSupportedRole(role) {
			return nil, fmt.Errorf("invalid role: %q", role)
		}
		n, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid priority for %s: %q", role, parts[1])
		}
		out[role] = n
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one role priority is required")
	}
	return out, nil
}

func telegramPRDPrioritySession(paths ralph.Paths, chatID int64, raw string) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}

	current := normalizeTelegramPRDAgentPriorityMap(session.Context.AgentPriority)
	arg := strings.TrimSpace(raw)
	if arg == "" {
		return strings.Join([]string{
			"agent priority profile",
			fmt.Sprintf("- current: %s", formatTelegramPRDAgentPriorityInline(current)),
			"- update: /prd priority manager=900 planner=950 developer=1000 qa=1100",
			"- reset: /prd priority default",
		}, "\n"), nil
	}

	if strings.EqualFold(arg, "default") || strings.EqualFold(arg, "reset") {
		session.Context.AgentPriority = telegramPRDDefaultAgentPriorityMap()
		session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
		if err := telegramUpsertPRDSession(paths, session); err != nil {
			return "", err
		}
		return fmt.Sprintf("agent priorities reset\n- current: %s", formatTelegramPRDAgentPriorityInline(session.Context.AgentPriority)), nil
	}

	updates, err := parseTelegramPRDAgentPriorityArgs(arg)
	if err != nil {
		return "", err
	}
	merged := copyTelegramPRDAgentPriorityMap(current)
	for role, priority := range updates {
		merged[role] = priority
	}
	session.Context.AgentPriority = normalizeTelegramPRDAgentPriorityMap(merged)
	session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
	if err := telegramUpsertPRDSession(paths, session); err != nil {
		return "", err
	}
	return fmt.Sprintf("agent priorities updated\n- current: %s", formatTelegramPRDAgentPriorityInline(session.Context.AgentPriority)), nil
}

func telegramPRDStoryPriorityForRole(session telegramPRDSession, role string) int {
	role = strings.ToLower(strings.TrimSpace(role))
	if v := session.Context.AgentPriority[role]; v > 0 {
		return v
	}
	return telegramPRDDefaultPriorityForRole(role)
}

func resolveTelegramPRDStoryPriority(paths ralph.Paths, session telegramPRDSession, story telegramPRDStory) (int, string) {
	fallback := telegramPRDStoryPriorityForRole(session, story.Role)
	priority, source, err := telegramPRDStoryPriorityEstimator(paths, session, story)
	if err != nil || priority <= 0 {
		return fallback, "fallback_role_profile"
	}
	return priority, source
}

func telegramPRDRefineSession(paths ralph.Paths, chatID int64) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}
	session, codexRefine, usedCodexRefine, codexRefineErr := refreshTelegramPRDRefineWithCodex(paths, session)
	if usedCodexRefine && codexRefineErr == nil {
		if codexRefine.ReadyToApply {
			session.Stage = telegramPRDStageAwaitStoryTitle
		} else if stage, ok := normalizeTelegramPRDRefineSuggestedStage(codexRefine.SuggestedStage); ok {
			session.Stage = stage
		} else {
			status := evaluateTelegramPRDClarity(session)
			if status.NextStage != "" {
				session.Stage = status.NextStage
			}
		}
		session.Approved = false
		session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
		if err := telegramUpsertPRDSession(paths, session); err != nil {
			return "", err
		}
		return formatTelegramPRDCodexRefineQuestion(codexRefine), nil
	}

	status := evaluateTelegramPRDClarity(session)
	if codexRefineErr != nil {
		fmt.Fprintf(os.Stderr, "[telegram] prd refine codex fallback: %v\n", codexRefineErr)
	}
	return formatTelegramPRDRefineUnavailable(session.Stage, status.Score, codexRefineErr), nil
}

func telegramPRDScoreSession(paths ralph.Paths, chatID int64) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}

	updated, usedCodex, scoreErr := refreshTelegramPRDScoreWithCodex(paths, session)
	if scoreErr == nil && usedCodex {
		if err := telegramUpsertPRDSession(paths, updated); err != nil {
			return "", err
		}
		return formatTelegramPRDCodexScore(updated), nil
	}

	status := evaluateTelegramPRDClarity(session)
	reply := formatTelegramPRDScore(status)
	if scoreErr != nil {
		reply += "\n- note: codex scoring unavailable, fallback heuristic used"
	}
	return reply, nil
}

func telegramPRDApproveSession(paths ralph.Paths, chatID int64) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}
	session.Approved = true
	session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
	if err := telegramUpsertPRDSession(paths, session); err != nil {
		return "", err
	}
	status := evaluateTelegramPRDClarity(session)
	return fmt.Sprintf(
		"prd apply override enabled\n- score: %d/100\n- note: clarity gate bypassed for this session\n- next: /prd apply",
		status.Score,
	), nil
}

func telegramPRDPreviewSession(paths ralph.Paths, chatID int64) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}
	var b strings.Builder
	clarity := evaluateTelegramPRDClarity(session)
	displayScore := clarity.Score
	displayReady := clarity.ReadyToApply
	displayMissing := clarity.Missing
	scoringMode := "heuristic"
	if session.CodexScore > 0 || session.CodexScoredAtUT != "" {
		displayScore = session.CodexScore
		displayReady = session.CodexReady
		if len(session.CodexMissing) > 0 {
			displayMissing = session.CodexMissing
		}
		scoringMode = "codex"
	}
	fmt.Fprintln(&b, "PRD session")
	fmt.Fprintf(&b, "- product: %s\n", valueOrDash(strings.TrimSpace(session.ProductName)))
	fmt.Fprintf(&b, "- stage: %s\n", session.Stage)
	fmt.Fprintf(&b, "- approved_override: %t\n", session.Approved)
	fmt.Fprintf(&b, "- clarity_score: %d/100\n", displayScore)
	fmt.Fprintf(&b, "- clarity_gate: %d\n", telegramPRDClarityMinScore)
	fmt.Fprintf(&b, "- scoring_mode: %s\n", scoringMode)
	if displayReady {
		fmt.Fprintf(&b, "- clarity_status: ready\n")
	} else {
		fmt.Fprintf(&b, "- clarity_status: needs_input (%d/%d required)\n", clarity.RequiredReady, clarity.RequiredTotal)
	}
	fmt.Fprintf(&b, "- stories: %d\n", len(session.Stories))
	if strings.TrimSpace(session.Context.Problem) != "" {
		fmt.Fprintf(&b, "- problem: %s\n", compactSingleLine(session.Context.Problem, 120))
	}
	if strings.TrimSpace(session.Context.Goal) != "" {
		fmt.Fprintf(&b, "- goal: %s\n", compactSingleLine(session.Context.Goal, 120))
	}
	if strings.TrimSpace(session.Context.InScope) != "" {
		fmt.Fprintf(&b, "- in_scope: %s\n", compactSingleLine(session.Context.InScope, 120))
	}
	if strings.TrimSpace(session.Context.OutOfScope) != "" {
		fmt.Fprintf(&b, "- out_of_scope: %s\n", compactSingleLine(session.Context.OutOfScope, 120))
	}
	if strings.TrimSpace(session.Context.Acceptance) != "" {
		fmt.Fprintf(&b, "- acceptance: %s\n", compactSingleLine(session.Context.Acceptance, 120))
	}
	if strings.TrimSpace(session.Context.Constraints) != "" {
		fmt.Fprintf(&b, "- constraints: %s\n", compactSingleLine(session.Context.Constraints, 120))
	}
	fmt.Fprintf(&b, "- agent_priorities: %s\n", formatTelegramPRDAgentPriorityInline(session.Context.AgentPriority))
	if len(session.Context.Assumptions) > 0 {
		fmt.Fprintf(&b, "- assumptions: %d\n", len(session.Context.Assumptions))
	}
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
	if len(displayMissing) > 0 {
		fmt.Fprintln(&b, "- missing:")
		for i, m := range displayMissing {
			if i >= 5 {
				fmt.Fprintf(&b, "  - ... and %d more\n", len(displayMissing)-i)
				break
			}
			fmt.Fprintf(&b, "  - %s\n", m)
		}
	}
	fmt.Fprintf(&b, "- next: %s\n", telegramPRDStagePrompt(session.Stage))
	return b.String(), nil
}

func telegramPRDSaveSession(paths ralph.Paths, chatID int64, rawPath string) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
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

func telegramPRDApplySession(paths ralph.Paths, chatID int64, rawPath string) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("no active PRD session (run: /prd start)")
	}
	if len(session.Stories) == 0 {
		return "", fmt.Errorf("no stories in session yet")
	}

	// Prefer codex-based scoring when available.
	sessionForGate, usedCodexGate, codexScoreErr := refreshTelegramPRDScoreWithCodex(paths, session)
	if codexScoreErr == nil && usedCodexGate {
		session = sessionForGate
		if err := telegramUpsertPRDSession(paths, session); err != nil {
			return "", err
		}
	}

	clarity := evaluateTelegramPRDClarity(session)
	readyToApply := clarity.ReadyToApply
	scoreForReply := clarity.Score
	missingForReply := append([]string(nil), clarity.Missing...)
	if usedCodexGate && codexScoreErr == nil {
		readyToApply = session.CodexReady && session.CodexScore >= telegramPRDClarityMinScore
		scoreForReply = session.CodexScore
		if len(session.CodexMissing) > 0 {
			missingForReply = append([]string(nil), session.CodexMissing...)
		}
	}

	if !readyToApply && !session.Approved {
		missingPreview := "-"
		if len(missingForReply) > 0 {
			missingPreview = compactSingleLine(strings.Join(missingForReply, ", "), 180)
		}
		scoringMode := "heuristic"
		if usedCodexGate && codexScoreErr == nil {
			scoringMode = "codex"
		}
		return strings.Join([]string{
			"prd apply blocked",
			fmt.Sprintf("- clarity_score: %d/100", scoreForReply),
			fmt.Sprintf("- clarity_gate: %d", telegramPRDClarityMinScore),
			fmt.Sprintf("- scoring_mode: %s", scoringMode),
			"- reason: missing required context",
			fmt.Sprintf("- missing: %s", missingPreview),
			"- next: /prd refine",
			"- optional_override: /prd approve",
		}, "\n"), nil
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
	if err := telegramDeletePRDSession(paths, chatID); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"prd applied\n- file: %s\n- stories_total: %d\n- imported: %d\n- skipped_existing: %d\n- skipped_invalid: %d\n- clarity_score: %d/100\n- override_used: %t\n- next: /status",
		targetPath,
		result.StoriesTotal,
		result.Imported,
		result.SkippedExisting,
		result.SkippedInvalid,
		scoreForReply,
		session.Approved && !readyToApply,
	), nil
}

func telegramPRDCancelSession(paths ralph.Paths, chatID int64) (string, error) {
	if err := telegramDeletePRDSession(paths, chatID); err != nil {
		return "", err
	}
	logTelegramPRDConversationWarning(clearTelegramPRDConversation(paths, chatID))
	return "PRD session canceled", nil
}

func telegramPRDHandleInput(paths ralph.Paths, chatID int64, input string) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("no active PRD session")
	}

	assist, err := telegramPRDAssistInput(paths, session, input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[telegram] prd assist fallback: %v\n", err)
	}
	if assist.Handled {
		session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
		if err := telegramUpsertPRDSession(paths, session); err != nil {
			return "", err
		}
		logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "user", input))
		logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "assistant", assist.Reply))
		return assist.Reply, nil
	}
	if strings.TrimSpace(assist.InputOverride) != "" {
		input = assist.InputOverride
	}

	updated, reply, err := advanceTelegramPRDSession(paths, session, input)
	if err != nil {
		return "", err
	}
	if err := telegramUpsertPRDSession(paths, updated); err != nil {
		return "", err
	}
	logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "user", input))
	logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "assistant", reply))
	return reply, nil
}

func advanceTelegramPRDSession(paths ralph.Paths, session telegramPRDSession, input string) (telegramPRDSession, string, error) {
	session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
	session.Approved = false
	input = strings.TrimSpace(input)
	if input == "" {
		return session, telegramPRDStagePrompt(session.Stage), nil
	}

	switch session.Stage {
	case telegramPRDStageAwaitProduct:
		session.ProductName = input
		status := evaluateTelegramPRDClarity(session)
		session.Stage = status.NextStage
		if session.Stage == "" {
			session.Stage = telegramPRDStageAwaitStoryTitle
		}
		return session, fmt.Sprintf("product set: %s\n- next: /prd refine", session.ProductName), nil

	case telegramPRDStageAwaitProblem:
		session.Context.Problem = normalizeTelegramPRDContextAnswer(input, "현재 기능/운영상 pain point는 명시되지 않음")
		recordTelegramPRDAssumption(&session.Context, "problem", session.Context.Problem)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitGoal:
		session.Context.Goal = normalizeTelegramPRDContextAnswer(input, "단기 목표는 첫 동작 가능한 자동화 루프 확보")
		recordTelegramPRDAssumption(&session.Context, "goal", session.Context.Goal)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitInScope:
		session.Context.InScope = normalizeTelegramPRDContextAnswer(input, "초기 릴리즈에서는 핵심 사용자 흐름만 포함")
		recordTelegramPRDAssumption(&session.Context, "in_scope", session.Context.InScope)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitOutOfScope:
		session.Context.OutOfScope = normalizeTelegramPRDContextAnswer(input, "대규모 리팩터/새 인프라 구축은 제외")
		recordTelegramPRDAssumption(&session.Context, "out_of_scope", session.Context.OutOfScope)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitAcceptance:
		session.Context.Acceptance = normalizeTelegramPRDContextAnswer(input, "주요 시나리오 성공 + 실패 시 복구 경로 확인")
		recordTelegramPRDAssumption(&session.Context, "acceptance", session.Context.Acceptance)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitConstraints:
		session.Context.Constraints = normalizeTelegramPRDContextAnswer(input, "시간/리소스 제약은 일반적인 단일 개발자 환경 가정")
		recordTelegramPRDAssumption(&session.Context, "constraints", session.Context.Constraints)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitStoryTitle:
		if story, quick, err := parseTelegramPRDQuickStoryInput(session, input); err != nil {
			if quick {
				return session, "", err
			}
		} else if quick {
			updated, reply, err := telegramPRDAppendStoryFromQuick(paths, session, story)
			return updated, reply, err
		}
		session.DraftTitle = input
		session.Stage = telegramPRDStageAwaitStoryDesc
		return session, "story title saved\n- next: 설명을 입력하세요 (quick: 제목 | 설명 | role [priority])", nil

	case telegramPRDStageAwaitStoryDesc:
		session.DraftDesc = input
		session.Stage = telegramPRDStageAwaitStoryRole
		return session, "story description saved\n- next: role 입력 (manager|planner|developer|qa, optional: role priority)", nil

	case telegramPRDStageAwaitStoryRole:
		role, priority, explicitPriority, err := parseTelegramPRDStoryRoleAndPriorityInput(session, input, "")
		if err != nil {
			return session, "", err
		}
		updated, story, source, err := telegramPRDAppendStoryFromDraft(paths, session, role, priority, explicitPriority)
		if err != nil {
			return session, "", err
		}
		return updated, telegramPRDStoryAddedReply(updated, story, source), nil

	case telegramPRDStageAwaitStoryPrio:
		priority, err := parseTelegramPRDStoryPriority(input)
		if err != nil {
			return session, "", err
		}
		rawPriority := strings.TrimSpace(strings.ToLower(input))
		explicitPriority := !(rawPriority == "" || rawPriority == "default" || rawPriority == "skip")
		updated, story, source, err := telegramPRDAppendStoryFromDraft(paths, session, strings.TrimSpace(session.DraftRole), priority, explicitPriority)
		if err != nil {
			return session, "", err
		}
		return updated, telegramPRDStoryAddedReply(updated, story, source), nil

	default:
		status := evaluateTelegramPRDClarity(session)
		session.Stage = status.NextStage
		if session.Stage == "" {
			session.Stage = telegramPRDStageAwaitProduct
		}
		return session, "session stage reset\n- next: /prd refine", nil
	}
}

func advanceTelegramPRDRefineFlow(paths ralph.Paths, session telegramPRDSession) (telegramPRDSession, string, error) {
	sessionForCodex, codexRefine, usedCodexRefine, codexRefineErr := refreshTelegramPRDRefineWithCodex(paths, session)
	if usedCodexRefine && codexRefineErr == nil {
		session = sessionForCodex
		if codexRefine.ReadyToApply {
			session.Stage = telegramPRDStageAwaitStoryTitle
			return session, formatTelegramPRDCodexRefineQuestion(codexRefine), nil
		}
		if stage, ok := normalizeTelegramPRDRefineSuggestedStage(codexRefine.SuggestedStage); ok {
			session.Stage = stage
		}
		if strings.TrimSpace(session.Stage) == "" {
			session.Stage = telegramPRDStageAwaitStoryTitle
		}
		return session, formatTelegramPRDCodexRefineQuestion(codexRefine), nil
	}

	status := evaluateTelegramPRDClarity(session)
	if codexRefineErr != nil {
		fmt.Fprintf(os.Stderr, "[telegram] prd refine codex fallback: %v\n", codexRefineErr)
	}
	return session, formatTelegramPRDRefineUnavailable(session.Stage, status.Score, codexRefineErr), nil
}

func telegramPRDAssistInput(paths ralph.Paths, session telegramPRDSession, input string) (telegramPRDInputAssistResult, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return telegramPRDInputAssistResult{}, nil
	}

	assist, err := telegramPRDCodexAssistAnalyzer(paths, session, input)
	if err != nil {
		return telegramPRDInputAssistResult{}, err
	}

	intent := strings.ToLower(strings.TrimSpace(assist.Intent))
	switch intent {
	case "answer":
		v := strings.TrimSpace(assist.NormalizedAnswer)
		if v != "" {
			return telegramPRDInputAssistResult{
				InputOverride: v,
			}, nil
		}
		return telegramPRDInputAssistResult{}, nil
	case "clarify", "recommend":
		reply := strings.TrimSpace(assist.Reply)
		if reply == "" {
			reply = telegramPRDStageAssistFallback(session.Stage)
		}
		if strings.TrimSpace(reply) == "" {
			return telegramPRDInputAssistResult{}, nil
		}
		return telegramPRDInputAssistResult{
			Handled: true,
			Reply:   reply,
		}, nil
	default:
		return telegramPRDInputAssistResult{}, nil
	}
}

func analyzeTelegramPRDInputWithCodex(paths ralph.Paths, session telegramPRDSession, input string) (telegramPRDCodexAssistResponse, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return telegramPRDCodexAssistResponse{}, fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return telegramPRDCodexAssistResponse{}, err
	}
	if !profile.RequireCodex {
		return telegramPRDCodexAssistResponse{}, fmt.Errorf("codex assist disabled (require_codex=false)")
	}
	timeoutSec := profile.CodexExecTimeoutSec
	if timeoutSec <= 0 || timeoutSec > 120 {
		timeoutSec = telegramPRDCodexAssistTimeoutSec
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	tmpDir, err := os.MkdirTemp("", "ralph-telegram-prd-assist-*")
	if err != nil {
		return telegramPRDCodexAssistResponse{}, err
	}
	defer os.RemoveAll(tmpDir)
	outPath := filepath.Join(tmpDir, "assistant-last-message.txt")

	model := strings.TrimSpace(profile.CodexModelForRole("planner"))
	args := []string{
		"--ask-for-approval", profile.CodexApproval,
		"exec",
		"--sandbox", profile.CodexSandbox,
	}
	if model != "" {
		args = append(args, "--model", model)
	}
	args = append(args,
		"--cd", paths.ProjectDir,
		"--skip-git-repo-check",
		"--output-last-message", outPath,
		"-",
	)

	conversationTail := readTelegramPRDConversationTail(paths, session.ChatID, 6000)
	prompt := buildTelegramPRDAssistPrompt(session, input, conversationTail)
	cmd := exec.CommandContext(ctx, "codex", args...)
	cmd.Stdin = strings.NewReader(prompt)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return telegramPRDCodexAssistResponse{}, err
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		return telegramPRDCodexAssistResponse{}, fmt.Errorf("read codex assist output: %w", err)
	}
	parsed, err := parseTelegramPRDCodexAssistResponse(string(raw))
	if err != nil {
		return telegramPRDCodexAssistResponse{}, err
	}
	return parsed, nil
}

func estimateTelegramPRDStoryPriorityWithCodex(paths ralph.Paths, session telegramPRDSession, story telegramPRDStory) (int, string, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return 0, "", fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return 0, "", err
	}
	if !profile.RequireCodex {
		return 0, "", fmt.Errorf("codex priority disabled (require_codex=false)")
	}
	timeoutSec := profile.CodexExecTimeoutSec
	if timeoutSec <= 0 || timeoutSec > 120 {
		timeoutSec = telegramPRDCodexAssistTimeoutSec
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	tmpDir, err := os.MkdirTemp("", "ralph-telegram-prd-priority-*")
	if err != nil {
		return 0, "", err
	}
	defer os.RemoveAll(tmpDir)
	outPath := filepath.Join(tmpDir, "assistant-last-message.txt")

	model := strings.TrimSpace(profile.CodexModelForRole("planner"))
	args := []string{
		"--ask-for-approval", profile.CodexApproval,
		"exec",
		"--sandbox", profile.CodexSandbox,
	}
	if model != "" {
		args = append(args, "--model", model)
	}
	args = append(args,
		"--cd", paths.ProjectDir,
		"--skip-git-repo-check",
		"--output-last-message", outPath,
		"-",
	)

	conversationTail := readTelegramPRDConversationTail(paths, session.ChatID, 5000)
	prompt := buildTelegramPRDStoryPriorityPrompt(session, story, conversationTail)
	cmd := exec.CommandContext(ctx, "codex", args...)
	cmd.Stdin = strings.NewReader(prompt)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return 0, "", err
	}

	raw, err := os.ReadFile(outPath)
	if err != nil {
		return 0, "", fmt.Errorf("read codex priority output: %w", err)
	}
	parsed, err := parseTelegramPRDCodexStoryPriorityResponse(string(raw))
	if err != nil {
		return 0, "", err
	}
	priority := parsed.Priority
	if priority <= 0 {
		return 0, "", fmt.Errorf("invalid codex priority: %d", parsed.Priority)
	}
	return priority, "codex_auto", nil
}

func buildTelegramPRDStoryPriorityPrompt(session telegramPRDSession, story telegramPRDStory, conversationTail string) string {
	payload, _ := json.MarshalIndent(session, "", "  ")
	storyPayload, _ := json.MarshalIndent(story, "", "  ")
	var b strings.Builder
	fmt.Fprintln(&b, "You are a strict issue priority allocator for autonomous agent execution.")
	fmt.Fprintln(&b, "Return STRICT JSON only.")
	fmt.Fprintln(&b, `Schema: {"priority":1000,"reason":"..."}`)
	fmt.Fprintln(&b, "Rules:")
	fmt.Fprintln(&b, "- Lower number means higher priority.")
	fmt.Fprintln(&b, "- Use integer range 100..3000.")
	fmt.Fprintln(&b, "- Consider role urgency, business risk, operational impact, and PRD context.")
	fmt.Fprintln(&b, "- Keep reason concise in Korean.")
	fmt.Fprintln(&b, "\nPRD Session JSON:")
	fmt.Fprintln(&b, string(payload))
	fmt.Fprintln(&b, "\nCandidate Story JSON:")
	fmt.Fprintln(&b, string(storyPayload))
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "\nRecent Conversation (Markdown):")
		fmt.Fprintln(&b, conversationTail)
	}
	return b.String()
}

func parseTelegramPRDCodexStoryPriorityResponse(raw string) (telegramPRDCodexStoryPriorityResponse, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return telegramPRDCodexStoryPriorityResponse{}, fmt.Errorf("empty codex priority response")
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}
	var parsed telegramPRDCodexStoryPriorityResponse
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		start := strings.Index(text, "{")
		end := strings.LastIndex(text, "}")
		if start < 0 || end <= start {
			return telegramPRDCodexStoryPriorityResponse{}, fmt.Errorf("invalid codex priority json")
		}
		if unmarshalErr := json.Unmarshal([]byte(text[start:end+1]), &parsed); unmarshalErr != nil {
			return telegramPRDCodexStoryPriorityResponse{}, fmt.Errorf("parse codex priority json: %w", unmarshalErr)
		}
	}
	parsed.Priority = clampTelegramPRDStoryPriority(parsed.Priority)
	parsed.Reason = compactSingleLine(strings.TrimSpace(parsed.Reason), 160)
	return parsed, nil
}

func clampTelegramPRDStoryPriority(v int) int {
	if v < 100 {
		return 100
	}
	if v > 3000 {
		return 3000
	}
	return v
}

func analyzeTelegramPRDScoreWithCodex(paths ralph.Paths, session telegramPRDSession) (telegramPRDCodexScoreResponse, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return telegramPRDCodexScoreResponse{}, fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return telegramPRDCodexScoreResponse{}, err
	}
	if !profile.RequireCodex {
		return telegramPRDCodexScoreResponse{}, fmt.Errorf("codex scoring disabled (require_codex=false)")
	}
	timeoutSec := profile.CodexExecTimeoutSec
	if timeoutSec <= 0 || timeoutSec > 120 {
		timeoutSec = telegramPRDCodexAssistTimeoutSec
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	tmpDir, err := os.MkdirTemp("", "ralph-telegram-prd-score-*")
	if err != nil {
		return telegramPRDCodexScoreResponse{}, err
	}
	defer os.RemoveAll(tmpDir)
	outPath := filepath.Join(tmpDir, "assistant-last-message.txt")

	model := strings.TrimSpace(profile.CodexModelForRole("planner"))
	args := []string{
		"--ask-for-approval", profile.CodexApproval,
		"exec",
		"--sandbox", profile.CodexSandbox,
	}
	if model != "" {
		args = append(args, "--model", model)
	}
	args = append(args,
		"--cd", paths.ProjectDir,
		"--skip-git-repo-check",
		"--output-last-message", outPath,
		"-",
	)

	conversationTail := readTelegramPRDConversationTail(paths, session.ChatID, 8000)
	prompt := buildTelegramPRDScorePrompt(session, conversationTail)
	cmd := exec.CommandContext(ctx, "codex", args...)
	cmd.Stdin = strings.NewReader(prompt)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return telegramPRDCodexScoreResponse{}, err
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		return telegramPRDCodexScoreResponse{}, fmt.Errorf("read codex score output: %w", err)
	}
	return parseTelegramPRDCodexScoreResponse(string(raw))
}

func refreshTelegramPRDScoreWithCodex(paths ralph.Paths, session telegramPRDSession) (telegramPRDSession, bool, error) {
	score, err := analyzeTelegramPRDScoreWithCodex(paths, session)
	if err != nil {
		return session, false, err
	}
	session.CodexScore = clampTelegramPRDScore(score.Score)
	session.CodexReady = score.ReadyToApply && session.CodexScore >= telegramPRDClarityMinScore
	session.CodexMissing = sanitizeTelegramPRDMissingList(score.Missing)
	session.CodexSummary = strings.TrimSpace(score.Summary)
	session.CodexScoredAtUT = time.Now().UTC().Format(time.RFC3339)
	return session, true, nil
}

func refreshTelegramPRDRefineWithCodex(paths ralph.Paths, session telegramPRDSession) (telegramPRDSession, telegramPRDCodexRefineResponse, bool, error) {
	refine, err := telegramPRDRefineAnalyzer(paths, session)
	if err != nil {
		score, scoreErr := telegramPRDScoreAnalyzer(paths, session)
		if scoreErr != nil {
			return session, telegramPRDCodexRefineResponse{}, false, fmt.Errorf("codex refine failed: %w (score fallback failed: %v)", err, scoreErr)
		}
		refine = buildTelegramPRDRefineFromCodexScore(session, score)
	}
	session.CodexScore = clampTelegramPRDScore(refine.Score)
	session.CodexReady = refine.ReadyToApply && session.CodexScore >= telegramPRDClarityMinScore
	session.CodexMissing = sanitizeTelegramPRDMissingList(refine.Missing)
	session.CodexSummary = compactSingleLine(strings.TrimSpace(refine.Reason), 200)
	session.CodexScoredAtUT = time.Now().UTC().Format(time.RFC3339)
	refine.Score = session.CodexScore
	refine.ReadyToApply = session.CodexReady
	refine.Missing = append([]string(nil), session.CodexMissing...)
	return session, refine, true, nil
}

func buildTelegramPRDRefineFromCodexScore(session telegramPRDSession, score telegramPRDCodexScoreResponse) telegramPRDCodexRefineResponse {
	missing := sanitizeTelegramPRDMissingList(score.Missing)
	ask := "현재 PRD에서 가장 불명확한 지점을 한 문장으로 구체화해 주세요."
	if len(missing) > 0 {
		ask = fmt.Sprintf("`%s` 항목을 실행 가능하게 한 문장으로 구체화해 주세요.", missing[0])
	}
	suggestedStage := guessTelegramPRDStageFromMissing(missing, session.Stage)
	return telegramPRDCodexRefineResponse{
		Score:          clampTelegramPRDScore(score.Score),
		ReadyToApply:   score.ReadyToApply && score.Score >= telegramPRDClarityMinScore,
		Ask:            ask,
		Missing:        missing,
		SuggestedStage: suggestedStage,
		Reason:         compactSingleLine(strings.TrimSpace(score.Summary), 200),
	}
}

func guessTelegramPRDStageFromMissing(missing []string, fallbackStage string) string {
	if len(missing) == 0 {
		if stage, ok := normalizeTelegramPRDRefineSuggestedStage(fallbackStage); ok {
			return stage
		}
		return telegramPRDStageAwaitStoryTitle
	}
	top := strings.ToLower(strings.TrimSpace(missing[0]))
	switch {
	case strings.Contains(top, "product"):
		return telegramPRDStageAwaitProduct
	case strings.Contains(top, "problem"):
		return telegramPRDStageAwaitProblem
	case strings.Contains(top, "goal"):
		return telegramPRDStageAwaitGoal
	case strings.Contains(top, "in-scope"), strings.Contains(top, "scope"):
		return telegramPRDStageAwaitInScope
	case strings.Contains(top, "out-of-scope"):
		return telegramPRDStageAwaitOutOfScope
	case strings.Contains(top, "acceptance"):
		return telegramPRDStageAwaitAcceptance
	case strings.Contains(top, "constraint"):
		return telegramPRDStageAwaitConstraints
	case strings.Contains(top, "story"):
		return telegramPRDStageAwaitStoryTitle
	default:
		if stage, ok := normalizeTelegramPRDRefineSuggestedStage(fallbackStage); ok {
			return stage
		}
		return telegramPRDStageAwaitProblem
	}
}

func analyzeTelegramPRDRefineWithCodex(paths ralph.Paths, session telegramPRDSession) (telegramPRDCodexRefineResponse, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return telegramPRDCodexRefineResponse{}, fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return telegramPRDCodexRefineResponse{}, err
	}
	if !profile.RequireCodex {
		return telegramPRDCodexRefineResponse{}, fmt.Errorf("codex refine disabled (require_codex=false)")
	}
	timeoutSec := profile.CodexExecTimeoutSec
	if timeoutSec <= 0 || timeoutSec > 120 {
		timeoutSec = telegramPRDCodexAssistTimeoutSec
	}
	retryAttempts := profile.CodexRetryMaxAttempts
	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	if retryAttempts > 5 {
		retryAttempts = 5
	}
	retryBackoffSec := profile.CodexRetryBackoffSec
	if retryBackoffSec <= 0 {
		retryBackoffSec = 1
	}
	if retryBackoffSec > 3 {
		retryBackoffSec = 3
	}
	conversationTail := readTelegramPRDConversationTail(paths, session.ChatID, 8000)
	prompt := buildTelegramPRDRefinePrompt(session, conversationTail)
	model := strings.TrimSpace(profile.CodexModelForRole("planner"))

	var lastErr error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
		raw, execErr := runTelegramPRDCodexExec(ctx, paths, profile.CodexApproval, profile.CodexSandbox, model, prompt, "ralph-telegram-prd-refine-*")
		cancel()
		if execErr == nil {
			parsed, parseErr := parseTelegramPRDCodexRefineResponse(raw)
			if parseErr == nil {
				return parsed, nil
			}
			lastErr = parseErr
		} else {
			lastErr = execErr
		}
		if attempt < retryAttempts {
			time.Sleep(time.Duration(attempt*retryBackoffSec) * time.Second)
		}
	}
	return telegramPRDCodexRefineResponse{}, fmt.Errorf("codex refine retries exhausted: %w", lastErr)
}

func runTelegramPRDCodexExec(
	ctx context.Context,
	paths ralph.Paths,
	approval string,
	sandbox string,
	model string,
	prompt string,
	tmpPrefix string,
) (string, error) {
	tmpDir, err := os.MkdirTemp("", tmpPrefix)
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpDir)

	outPath := filepath.Join(tmpDir, "assistant-last-message.txt")
	args := []string{
		"--ask-for-approval", approval,
		"exec",
		"--sandbox", sandbox,
	}
	if strings.TrimSpace(model) != "" {
		args = append(args, "--model", model)
	}
	args = append(args,
		"--cd", paths.ProjectDir,
		"--skip-git-repo-check",
		"--output-last-message", outPath,
		"-",
	)

	cmd := exec.CommandContext(ctx, "codex", args...)
	cmd.Stdin = strings.NewReader(prompt)
	cmd.Stdout = io.Discard
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("codex exec timeout: context deadline exceeded")
		}
		errText := compactSingleLine(strings.TrimSpace(stderr.String()), 220)
		if errText != "" {
			return "", fmt.Errorf("codex exec failed: %w: %s", err, errText)
		}
		return "", fmt.Errorf("codex exec failed: %w", err)
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		return "", fmt.Errorf("read codex output: %w", err)
	}
	return string(raw), nil
}

func buildTelegramPRDRefinePrompt(session telegramPRDSession, conversationTail string) string {
	payload, _ := json.MarshalIndent(session, "", "  ")
	var b strings.Builder
	fmt.Fprintln(&b, "You are a PRD refinement orchestrator for autonomous agent execution.")
	fmt.Fprintln(&b, "Return STRICT JSON only.")
	fmt.Fprintln(&b, `Schema: {"score":0,"ready_to_apply":false,"ask":"...","missing":["..."],"suggested_stage":"await_problem","reason":"..."}`)
	fmt.Fprintln(&b, "Rules:")
	fmt.Fprintln(&b, "- score must be 0..100 and reflect execution readiness.")
	fmt.Fprintf(&b, "- ready_to_apply=true only when score>=%d and critical context is sufficient.\n", telegramPRDClarityMinScore)
	fmt.Fprintln(&b, "- ask must be ONE concrete next question in Korean (not a list).")
	fmt.Fprintln(&b, "- missing should include top missing/weak items.")
	fmt.Fprintln(&b, "- suggested_stage should be one of:")
	fmt.Fprintln(&b, "  await_product, await_problem, await_goal, await_in_scope, await_out_of_scope, await_acceptance, await_constraints, await_story_title")
	fmt.Fprintln(&b, "- reason should summarize why this question is the highest leverage next step.")
	fmt.Fprintln(&b, "\nCurrent session JSON:")
	fmt.Fprintln(&b, string(payload))
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "\nRecent conversation (markdown):")
		fmt.Fprintln(&b, conversationTail)
	}
	return b.String()
}

func parseTelegramPRDCodexRefineResponse(raw string) (telegramPRDCodexRefineResponse, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return telegramPRDCodexRefineResponse{}, fmt.Errorf("empty codex refine response")
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}
	var parsed telegramPRDCodexRefineResponse
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		start := strings.Index(text, "{")
		end := strings.LastIndex(text, "}")
		if start < 0 || end <= start {
			return telegramPRDCodexRefineResponse{}, fmt.Errorf("invalid codex refine json")
		}
		if unmarshalErr := json.Unmarshal([]byte(text[start:end+1]), &parsed); unmarshalErr != nil {
			return telegramPRDCodexRefineResponse{}, fmt.Errorf("parse codex refine json: %w", unmarshalErr)
		}
	}
	parsed.Score = clampTelegramPRDScore(parsed.Score)
	parsed.Ask = compactSingleLine(strings.TrimSpace(parsed.Ask), 240)
	parsed.Missing = sanitizeTelegramPRDMissingList(parsed.Missing)
	parsed.SuggestedStage = strings.TrimSpace(parsed.SuggestedStage)
	parsed.Reason = compactSingleLine(strings.TrimSpace(parsed.Reason), 200)
	return parsed, nil
}

func normalizeTelegramPRDRefineSuggestedStage(raw string) (string, bool) {
	stage := strings.ToLower(strings.TrimSpace(raw))
	switch stage {
	case telegramPRDStageAwaitProduct,
		telegramPRDStageAwaitProblem,
		telegramPRDStageAwaitGoal,
		telegramPRDStageAwaitInScope,
		telegramPRDStageAwaitOutOfScope,
		telegramPRDStageAwaitAcceptance,
		telegramPRDStageAwaitConstraints,
		telegramPRDStageAwaitStoryTitle:
		return stage, true
	default:
		return "", false
	}
}

func buildTelegramPRDScorePrompt(session telegramPRDSession, conversationTail string) string {
	payload, _ := json.MarshalIndent(session, "", "  ")
	var b strings.Builder
	fmt.Fprintln(&b, "You are a strict PRD quality evaluator for autonomous agent execution.")
	fmt.Fprintln(&b, "Return STRICT JSON only.")
	fmt.Fprintln(&b, `Schema: {"score":0,"ready_to_apply":false,"missing":["..."],"summary":"..."}`)
	fmt.Fprintln(&b, "Scoring rubric:")
	fmt.Fprintln(&b, "- 0-100 overall completeness and execution clarity.")
	fmt.Fprintln(&b, "- Must consider: problem, goal, in-scope, out-of-scope, acceptance, stories quality.")
	fmt.Fprintf(&b, "- ready_to_apply=true only when score>=%d and no critical missing context.\n", telegramPRDClarityMinScore)
	fmt.Fprintln(&b, "- missing should contain the top missing/weak items.")
	fmt.Fprintln(&b, "- summary should be concise, practical, in Korean.")
	fmt.Fprintln(&b, "\nSession JSON:")
	fmt.Fprintln(&b, string(payload))
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "\nRecent Conversation (Markdown):")
		fmt.Fprintln(&b, conversationTail)
	}
	return b.String()
}

func parseTelegramPRDCodexScoreResponse(raw string) (telegramPRDCodexScoreResponse, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return telegramPRDCodexScoreResponse{}, fmt.Errorf("empty codex score response")
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}
	var parsed telegramPRDCodexScoreResponse
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		start := strings.Index(text, "{")
		end := strings.LastIndex(text, "}")
		if start < 0 || end <= start {
			return telegramPRDCodexScoreResponse{}, fmt.Errorf("invalid codex score json")
		}
		if unmarshalErr := json.Unmarshal([]byte(text[start:end+1]), &parsed); unmarshalErr != nil {
			return telegramPRDCodexScoreResponse{}, fmt.Errorf("parse codex score json: %w", unmarshalErr)
		}
	}
	parsed.Score = clampTelegramPRDScore(parsed.Score)
	parsed.Missing = sanitizeTelegramPRDMissingList(parsed.Missing)
	parsed.Summary = compactSingleLine(strings.TrimSpace(parsed.Summary), 200)
	return parsed, nil
}

func clampTelegramPRDScore(v int) int {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

func sanitizeTelegramPRDMissingList(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		v := compactSingleLine(strings.TrimSpace(item), 120)
		if v == "" {
			continue
		}
		out = append(out, v)
		if len(out) >= 8 {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildTelegramPRDAssistPrompt(session telegramPRDSession, userInput, conversationTail string) string {
	status := evaluateTelegramPRDClarity(session)
	var b strings.Builder
	fmt.Fprintln(&b, "You are a PRD wizard assistant for a Telegram chat.")
	fmt.Fprintln(&b, "Classify the user input intent and return STRICT JSON only.")
	fmt.Fprintln(&b, "No markdown, no prose outside JSON.")
	fmt.Fprintln(&b, `Schema: {"intent":"answer|clarify|recommend","reply":"string","normalized_answer":"string"}`)
	fmt.Fprintln(&b, "Critical rules:")
	fmt.Fprintln(&b, "- Use intent=answer only when the user clearly provided a usable field value for the current stage.")
	fmt.Fprintln(&b, "- If the user asks for suggestion/example/template/explanation or says prior save is wrong, NEVER use answer.")
	fmt.Fprintln(&b, "- Use intent=clarify for 'what does this mean/how to fill this field' type inputs.")
	fmt.Fprintln(&b, "- Use intent=recommend for 'recommend/propose/show examples' type inputs.")
	fmt.Fprintln(&b, "- Keep reply in Korean, concise, and actionable.")
	fmt.Fprintln(&b, "- If intent=answer, set normalized_answer to a cleaned value and leave reply empty.")
	fmt.Fprintln(&b, "- If intent!=answer, leave normalized_answer empty.")
	fmt.Fprintln(&b, "- For role stage, normalized_answer must be one of: manager|planner|developer|qa.")
	fmt.Fprintln(&b, "- For role stage, role + explicit priority is allowed (e.g. developer 900).")
	fmt.Fprintln(&b, "- For priority stage, normalized_answer must be a positive integer or 'default'.")
	fmt.Fprintf(&b, "\nCurrent stage: %s\n", session.Stage)
	fmt.Fprintf(&b, "Stage prompt: %s\n", telegramPRDStagePrompt(session.Stage))
	fmt.Fprintf(&b, "Expected answer format: %s\n", telegramPRDStageAnswerFormat(session.Stage))
	fmt.Fprintf(&b, "Product: %s\n", valueOrDash(strings.TrimSpace(session.ProductName)))
	fmt.Fprintf(&b, "Agent default priorities: %s\n", formatTelegramPRDAgentPriorityInline(session.Context.AgentPriority))
	if strings.TrimSpace(session.DraftTitle) != "" {
		fmt.Fprintf(&b, "Current draft title: %s\n", session.DraftTitle)
	}
	if strings.TrimSpace(session.DraftDesc) != "" {
		fmt.Fprintf(&b, "Current draft description: %s\n", session.DraftDesc)
	}
	fmt.Fprintf(&b, "Current clarity score: %d/100\n", status.Score)
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "Recent conversation (markdown):")
		fmt.Fprintln(&b, conversationTail)
	}
	fmt.Fprintf(&b, "User input: %s\n", strings.TrimSpace(userInput))
	return b.String()
}

func parseTelegramPRDCodexAssistResponse(raw string) (telegramPRDCodexAssistResponse, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return telegramPRDCodexAssistResponse{}, fmt.Errorf("empty codex assist response")
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}
	var parsed telegramPRDCodexAssistResponse
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		start := strings.Index(text, "{")
		end := strings.LastIndex(text, "}")
		if start < 0 || end <= start {
			return telegramPRDCodexAssistResponse{}, fmt.Errorf("invalid codex assist json")
		}
		if unmarshalErr := json.Unmarshal([]byte(text[start:end+1]), &parsed); unmarshalErr != nil {
			return telegramPRDCodexAssistResponse{}, fmt.Errorf("parse codex assist json: %w", unmarshalErr)
		}
	}
	parsed.Intent = strings.ToLower(strings.TrimSpace(parsed.Intent))
	switch parsed.Intent {
	case "answer", "clarify", "recommend":
	default:
		return telegramPRDCodexAssistResponse{}, fmt.Errorf("invalid assist intent: %s", parsed.Intent)
	}
	parsed.Reply = strings.TrimSpace(parsed.Reply)
	parsed.NormalizedAnswer = strings.TrimSpace(parsed.NormalizedAnswer)
	return parsed, nil
}

func normalizeTelegramPRDContextAnswer(input, defaultAssumption string) string {
	v := strings.TrimSpace(input)
	if v == "" {
		return ""
	}
	lower := strings.ToLower(v)
	if lower == "skip" || lower == "default" || lower == "n/a" {
		return fmt.Sprintf("%s %s", telegramPRDAssumedPrefix, strings.TrimSpace(defaultAssumption))
	}
	return v
}

func recordTelegramPRDAssumption(ctx *telegramPRDContext, field, value string) {
	if ctx == nil {
		return
	}
	if !isTelegramPRDAssumedValue(value) {
		return
	}
	entry := fmt.Sprintf("%s: %s", field, strings.TrimSpace(strings.TrimPrefix(value, telegramPRDAssumedPrefix)))
	for _, existing := range ctx.Assumptions {
		if existing == entry {
			return
		}
	}
	ctx.Assumptions = append(ctx.Assumptions, entry)
}

func isTelegramPRDAssumedValue(value string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), strings.ToLower(telegramPRDAssumedPrefix))
}

func evaluateTelegramPRDClarity(session telegramPRDSession) telegramPRDClarityStatus {
	type requiredField struct {
		Label      string
		Value      string
		Stage      string
		Prompt     string
		Assumption string
	}
	required := []requiredField{
		{
			Label:      "problem statement",
			Value:      session.Context.Problem,
			Stage:      telegramPRDStageAwaitProblem,
			Prompt:     "문제 정의를 입력하세요 (왜 이 작업이 필요한가?)",
			Assumption: "skip/default 입력 시: 현재 운영 pain point 해결이 우선이라고 가정",
		},
		{
			Label:      "goal",
			Value:      session.Context.Goal,
			Stage:      telegramPRDStageAwaitGoal,
			Prompt:     "목표를 입력하세요 (완료 기준 한 줄)",
			Assumption: "skip/default 입력 시: 첫 안정 운영 가능 상태 도달로 가정",
		},
		{
			Label:      "in-scope",
			Value:      session.Context.InScope,
			Stage:      telegramPRDStageAwaitInScope,
			Prompt:     "포함 범위를 입력하세요 (이번 사이클에서 반드시 할 것)",
			Assumption: "skip/default 입력 시: 핵심 사용자 흐름 중심으로 가정",
		},
		{
			Label:      "out-of-scope",
			Value:      session.Context.OutOfScope,
			Stage:      telegramPRDStageAwaitOutOfScope,
			Prompt:     "제외 범위를 입력하세요 (이번 사이클에서 하지 않을 것)",
			Assumption: "skip/default 입력 시: 대규모 리팩터/인프라 변경 제외로 가정",
		},
		{
			Label:      "acceptance criteria",
			Value:      session.Context.Acceptance,
			Stage:      telegramPRDStageAwaitAcceptance,
			Prompt:     "수용 기준을 입력하세요 (검증 가능한 기준)",
			Assumption: "skip/default 입력 시: 핵심 시나리오 성공 + 회귀 없음으로 가정",
		},
	}

	score := 0
	missing := []string{}
	requiredReady := 0
	assumedRequired := 0
	nextStage := ""
	nextPrompt := ""
	firstAssumedStage := ""
	firstAssumedLabel := ""

	product := strings.TrimSpace(session.ProductName)
	if product != "" {
		score += 10
	} else {
		missing = append(missing, "product name")
		nextStage = telegramPRDStageAwaitProduct
		nextPrompt = "제품/프로젝트 이름을 입력하세요"
	}

	for _, f := range required {
		v := strings.TrimSpace(f.Value)
		if v == "" {
			missing = append(missing, f.Label)
			if nextStage == "" {
				nextStage = f.Stage
				nextPrompt = fmt.Sprintf("%s\n- %s", f.Prompt, f.Assumption)
			}
			continue
		}
		requiredReady++
		if isTelegramPRDAssumedValue(v) {
			score += 9
			assumedRequired++
			if firstAssumedStage == "" {
				firstAssumedStage = f.Stage
				firstAssumedLabel = f.Label
			}
		} else {
			score += 14
		}
	}

	storyCount := len(session.Stories)
	if storyCount == 0 {
		missing = append(missing, "at least 1 user story")
		if nextStage == "" {
			nextStage = telegramPRDStageAwaitStoryTitle
			nextPrompt = "첫 user story 제목을 입력하세요"
		}
	} else {
		score += 20
		if storyCount >= 3 {
			score += 4
		}
	}

	if strings.TrimSpace(session.Context.Constraints) != "" {
		if isTelegramPRDAssumedValue(session.Context.Constraints) {
			score += 4
		} else {
			score += 8
		}
	}

	if score > 100 {
		score = 100
	}

	ready := score >= telegramPRDClarityMinScore && requiredReady == len(required) && storyCount > 0 && assumedRequired == 0
	if !ready && nextStage == "" && firstAssumedStage != "" {
		nextStage = firstAssumedStage
		nextPrompt = fmt.Sprintf("%s의 실제 값을 입력하세요 (현재 가정값으로 설정됨)", firstAssumedLabel)
		missing = append([]string{"replace assumed value: " + firstAssumedLabel}, missing...)
	}
	if ready {
		nextStage = ""
		nextPrompt = ""
	}

	return telegramPRDClarityStatus{
		Score:         score,
		RequiredTotal: len(required),
		RequiredReady: requiredReady,
		ReadyToApply:  ready,
		Missing:       missing,
		NextStage:     nextStage,
		NextPrompt:    nextPrompt,
	}
}

func formatTelegramPRDClarityQuestion(status telegramPRDClarityStatus) string {
	if status.ReadyToApply {
		return fmt.Sprintf("clarity check complete\n- score: %d/100\n- next: /prd apply", status.Score)
	}
	lines := []string{
		"prd refine question",
		fmt.Sprintf("- score: %d/100 (gate=%d)", status.Score, telegramPRDClarityMinScore),
	}
	if status.NextPrompt != "" {
		lines = append(lines, "- ask: "+status.NextPrompt)
	}
	if len(status.Missing) > 0 {
		lines = append(lines, "- missing_top: "+status.Missing[0])
	}
	lines = append(lines, "- hint: 답변이 애매하면 `skip` 또는 `default` 입력")
	return strings.Join(lines, "\n")
}

func formatTelegramPRDCodexRefineQuestion(refine telegramPRDCodexRefineResponse) string {
	lines := []string{
		"prd refine question",
		fmt.Sprintf("- score: %d/100 (gate=%d)", refine.Score, telegramPRDClarityMinScore),
		"- scoring_mode: codex",
	}
	if refine.ReadyToApply {
		lines = append(lines, "- status: ready_to_apply")
		lines = append(lines, "- next: /prd apply")
		return strings.Join(lines, "\n")
	}
	if strings.TrimSpace(refine.Ask) != "" {
		lines = append(lines, "- ask: "+refine.Ask)
	}
	if stage, ok := normalizeTelegramPRDRefineSuggestedStage(refine.SuggestedStage); ok {
		lines = append(lines, "- next_stage: "+stage)
	}
	if len(refine.Missing) > 0 {
		lines = append(lines, "- missing_top: "+refine.Missing[0])
	}
	if strings.TrimSpace(refine.Reason) != "" {
		lines = append(lines, "- reason: "+refine.Reason)
	}
	lines = append(lines, "- hint: 답변이 애매하면 `skip` 또는 `default` 입력")
	return strings.Join(lines, "\n")
}

func formatTelegramPRDRefineUnavailable(currentStage string, fallbackScore int, err error) string {
	lines := []string{
		"prd refine unavailable",
		fmt.Sprintf("- score: %d/100 (gate=%d)", fallbackScore, telegramPRDClarityMinScore),
		"- scoring_mode: codex_unavailable",
		fmt.Sprintf("- current_stage: %s", valueOrDash(currentStage)),
		"- reason: codex refine 실패로 동적 질문 생성 불가",
		"- next: codex 상태 복구 후 `/prd refine` 재시도",
	}
	if err != nil {
		lines = append(lines, "- note: codex refine unavailable")
		category, detail := classifyTelegramCodexFailure(err)
		if category != "" {
			lines = append(lines, "- codex_error: "+category)
		}
		if detail != "" {
			lines = append(lines, "- codex_detail: "+detail)
		}
	}
	lines = append(lines, "- hint: `/doctor` 또는 telegram tail 로그로 원인 확인")
	return strings.Join(lines, "\n")
}

func classifyTelegramCodexFailure(err error) (string, string) {
	if err == nil {
		return "", ""
	}
	raw := strings.ToLower(strings.TrimSpace(err.Error()))
	detail := compactSingleLine(strings.TrimSpace(err.Error()), 180)
	switch {
	case strings.Contains(raw, "not found"):
		return "not_installed", detail
	case strings.Contains(raw, "timeout"), strings.Contains(raw, "deadline exceeded"):
		return "timeout", detail
	case strings.Contains(raw, "operation not permitted"), strings.Contains(raw, "permission denied"):
		return "permission", detail
	case strings.Contains(raw, "could not resolve host"), strings.Contains(raw, "connection refused"),
		strings.Contains(raw, "network"), strings.Contains(raw, "i/o timeout"), strings.Contains(raw, "temporary failure in name resolution"):
		return "network", detail
	case strings.Contains(raw, "json"), strings.Contains(raw, "parse"):
		return "invalid_response", detail
	default:
		return "exec_failure", detail
	}
}

func formatTelegramPRDScore(status telegramPRDClarityStatus) string {
	lines := []string{
		"prd clarity score",
		fmt.Sprintf("- score: %d/100", status.Score),
		fmt.Sprintf("- gate: %d", telegramPRDClarityMinScore),
		fmt.Sprintf("- required_ready: %d/%d", status.RequiredReady, status.RequiredTotal),
	}
	if status.ReadyToApply {
		lines = append(lines, "- status: ready_to_apply")
		lines = append(lines, "- next: /prd apply")
		return strings.Join(lines, "\n")
	}
	lines = append(lines, "- status: needs_input")
	if len(status.Missing) > 0 {
		preview := strings.Join(status.Missing, ", ")
		if len(preview) > 150 {
			preview = compactSingleLine(preview, 150)
		}
		lines = append(lines, "- missing: "+preview)
	}
	lines = append(lines, "- next: /prd refine")
	return strings.Join(lines, "\n")
}

func formatTelegramPRDCodexScore(session telegramPRDSession) string {
	lines := []string{
		"prd clarity score",
		fmt.Sprintf("- score: %d/100", session.CodexScore),
		fmt.Sprintf("- gate: %d", telegramPRDClarityMinScore),
		"- scoring_mode: codex",
	}
	if session.CodexReady {
		lines = append(lines, "- status: ready_to_apply")
		lines = append(lines, "- next: /prd apply")
	} else {
		lines = append(lines, "- status: needs_input")
		if len(session.CodexMissing) > 0 {
			lines = append(lines, "- missing: "+strings.Join(session.CodexMissing, ", "))
		}
		lines = append(lines, "- next: /prd refine")
	}
	if strings.TrimSpace(session.CodexSummary) != "" {
		lines = append(lines, "- summary: "+session.CodexSummary)
	}
	if strings.TrimSpace(session.CodexScoredAtUT) != "" {
		lines = append(lines, "- scored_at: "+session.CodexScoredAtUT)
	}
	return strings.Join(lines, "\n")
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

func parseTelegramPRDStoryRoleAndPriorityInput(session telegramPRDSession, rawRole, rawPriority string) (string, int, bool, error) {
	roleInput := strings.TrimSpace(rawRole)
	priorityInput := strings.TrimSpace(rawPriority)

	if priorityInput == "" {
		fields := strings.Fields(roleInput)
		if len(fields) > 0 {
			roleInput = fields[0]
		}
		if len(fields) == 2 {
			priorityInput = fields[1]
		}
		if len(fields) > 2 {
			return "", 0, false, fmt.Errorf("invalid role input: %q (use role or role priority)", rawRole)
		}
	}

	role, err := parseTelegramPRDStoryRole(roleInput)
	if err != nil {
		return "", 0, false, err
	}
	if strings.TrimSpace(priorityInput) == "" {
		return role, 0, false, nil
	}
	if strings.EqualFold(strings.TrimSpace(priorityInput), "default") || strings.EqualFold(strings.TrimSpace(priorityInput), "skip") {
		return role, 0, false, nil
	}

	priority, err := parseTelegramPRDStoryPriority(priorityInput)
	if err != nil {
		return "", 0, false, err
	}
	return role, priority, true, nil
}

func parseTelegramPRDQuickStoryInput(session telegramPRDSession, input string) (telegramPRDStory, bool, error) {
	if !strings.Contains(input, "|") {
		return telegramPRDStory{}, false, nil
	}
	partsRaw := strings.Split(input, "|")
	parts := make([]string, 0, len(partsRaw))
	for _, p := range partsRaw {
		parts = append(parts, strings.TrimSpace(p))
	}
	if len(parts) < 3 || len(parts) > 4 {
		return telegramPRDStory{}, true, fmt.Errorf("quick format: 제목 | 설명 | role [priority] 또는 제목 | 설명 | role | priority")
	}
	title := strings.TrimSpace(parts[0])
	desc := strings.TrimSpace(parts[1])
	if title == "" || desc == "" {
		return telegramPRDStory{}, true, fmt.Errorf("quick format requires non-empty title and description")
	}
	rawRole := strings.TrimSpace(parts[2])
	rawPriority := ""
	if len(parts) == 4 {
		rawPriority = strings.TrimSpace(parts[3])
	}
	role, priority, explicitPriority, err := parseTelegramPRDStoryRoleAndPriorityInput(session, rawRole, rawPriority)
	if err != nil {
		return telegramPRDStory{}, true, err
	}
	if !explicitPriority {
		priority = 0
	}
	return telegramPRDStory{
		Title:       title,
		Description: desc,
		Role:        role,
		Priority:    priority,
	}, true, nil
}

func telegramPRDAppendStoryFromDraft(paths ralph.Paths, session telegramPRDSession, role string, priority int, explicitPriority bool) (telegramPRDSession, telegramPRDStory, string, error) {
	story := telegramPRDStory{
		Title:       strings.TrimSpace(session.DraftTitle),
		Description: strings.TrimSpace(session.DraftDesc),
		Role:        strings.TrimSpace(role),
		Priority:    priority,
	}
	if strings.TrimSpace(story.Title) == "" || strings.TrimSpace(story.Description) == "" || strings.TrimSpace(story.Role) == "" {
		return session, telegramPRDStory{}, "", fmt.Errorf("incomplete story draft; run /prd cancel then /prd start")
	}
	prioritySource := "manual"
	if !explicitPriority || story.Priority <= 0 {
		resolvedPriority, source := resolveTelegramPRDStoryPriority(paths, session, story)
		story.Priority = resolvedPriority
		prioritySource = source
	} else if story.Priority <= 0 {
		story.Priority = telegramPRDStoryPriorityForRole(session, story.Role)
		prioritySource = "fallback_role_profile"
	}
	story.ID = telegramPRDStoryID(session, len(session.Stories)+1)
	session.Stories = append(session.Stories, story)
	session.DraftTitle = ""
	session.DraftDesc = ""
	session.DraftRole = ""
	session.Stage = telegramPRDStageAwaitStoryTitle
	return session, story, prioritySource, nil
}

func telegramPRDAppendStoryFromQuick(paths ralph.Paths, session telegramPRDSession, story telegramPRDStory) (telegramPRDSession, string, error) {
	s := story
	if strings.TrimSpace(s.Role) == "" {
		return session, "", fmt.Errorf("quick story role is required")
	}
	prioritySource := "manual"
	if s.Priority <= 0 {
		resolvedPriority, source := resolveTelegramPRDStoryPriority(paths, session, s)
		s.Priority = resolvedPriority
		prioritySource = source
	}
	s.ID = telegramPRDStoryID(session, len(session.Stories)+1)
	session.Stories = append(session.Stories, s)
	session.DraftTitle = ""
	session.DraftDesc = ""
	session.DraftRole = ""
	session.Stage = telegramPRDStageAwaitStoryTitle
	return session, telegramPRDStoryAddedReply(session, s, prioritySource), nil
}

func telegramPRDStoryAddedReply(session telegramPRDSession, story telegramPRDStory, prioritySource string) string {
	clarity := evaluateTelegramPRDClarity(session)
	next := "다음 story 제목 입력 또는 /prd preview /prd save /prd apply"
	if !clarity.ReadyToApply {
		next = "/prd refine (부족 컨텍스트 질문 진행) 또는 다음 story 제목 입력"
	}
	if strings.TrimSpace(prioritySource) == "" {
		prioritySource = "manual"
	}
	return fmt.Sprintf(
		"story added\n- id: %s\n- title: %s\n- role: %s\n- priority: %d\n- priority_source: %s\n- stories_total: %d\n- clarity_score: %d/100\n- next: %s",
		story.ID,
		compactSingleLine(story.Title, 90),
		story.Role,
		story.Priority,
		prioritySource,
		len(session.Stories),
		clarity.Score,
		next,
	)
}

func telegramPRDStageAnswerFormat(stage string) string {
	switch stage {
	case telegramPRDStageAwaitProduct:
		return "제품/프로젝트 이름 1줄"
	case telegramPRDStageAwaitProblem:
		return "현재 핵심 문제를 한 줄로 명확히 입력"
	case telegramPRDStageAwaitGoal:
		return "이번 사이클 완료 목표를 한 줄로 입력"
	case telegramPRDStageAwaitInScope:
		return "이번 사이클에서 반드시 할 항목 1~3개"
	case telegramPRDStageAwaitOutOfScope:
		return "이번 사이클에서 제외할 항목 1~3개"
	case telegramPRDStageAwaitAcceptance:
		return "검증 가능한 수용 기준 2~3개"
	case telegramPRDStageAwaitConstraints:
		return "제약 사항(없으면 skip)"
	case telegramPRDStageAwaitStoryTitle:
		return "story 제목 1줄 또는 quick 입력: 제목 | 설명 | role [priority]"
	case telegramPRDStageAwaitStoryDesc:
		return "story 설명 1~3문장(배경/가치/완료조건)"
	case telegramPRDStageAwaitStoryRole:
		return "manager|planner|developer|qa 또는 role priority (예: developer 900)"
	case telegramPRDStageAwaitStoryPrio:
		return fmt.Sprintf("양의 정수 priority (예: %d) 또는 default", telegramPRDDefaultPriority)
	default:
		return "현재 단계 요구값에 맞는 직접 답변"
	}
}

func telegramPRDStageAssistFallback(stage string) string {
	switch stage {
	case telegramPRDStageAwaitStoryTitle:
		return strings.Join([]string{
			"story title 가이드",
			"- 사용자 행동 + 기대 결과를 한 줄로 입력하세요",
			"- 예시: 결제 실패 시 자동 재시도 상태 알림",
			"- quick 입력: 제목 | 설명 | role [priority]",
			"- 추천이 필요하면 `스토리 제목 3개 추천`처럼 요청하세요",
		}, "\n")
	case telegramPRDStageAwaitStoryDesc:
		return strings.Join([]string{
			"story description 가이드",
			"- 배경/문제, 기대 효과, 완료 조건을 짧게 적으세요",
			"- 예시: 실패 감지 시 3회 재시도 후 알림을 보내 운영자 수동 개입을 줄인다",
		}, "\n")
	case telegramPRDStageAwaitStoryRole:
		return "role 가이드\n- manager|planner|developer|qa 입력\n- 선택: role priority (예: developer 900)"
	case telegramPRDStageAwaitStoryPrio:
		return fmt.Sprintf("priority 가이드\n- 양의 정수를 입력하세요\n- 기본값 사용은 `default` (=%d)", telegramPRDDefaultPriority)
	default:
		return strings.Join([]string{
			"입력 가이드",
			"- 현재 단계 질문에 맞는 값을 직접 입력하세요",
			fmt.Sprintf("- 현재 단계: %s", telegramPRDStagePrompt(stage)),
		}, "\n")
	}
}

func telegramPRDStagePrompt(stage string) string {
	switch stage {
	case telegramPRDStageAwaitProduct:
		return "제품/프로젝트 이름을 입력하세요"
	case telegramPRDStageAwaitProblem:
		return "문제 정의를 입력하세요 (왜 이 작업이 필요한가?)"
	case telegramPRDStageAwaitGoal:
		return "목표를 입력하세요 (완료 기준 한 줄)"
	case telegramPRDStageAwaitInScope:
		return "포함 범위를 입력하세요 (이번 사이클에서 반드시 할 것)"
	case telegramPRDStageAwaitOutOfScope:
		return "제외 범위를 입력하세요 (이번 사이클에서 하지 않을 것)"
	case telegramPRDStageAwaitAcceptance:
		return "수용 기준을 입력하세요 (검증 가능한 기준)"
	case telegramPRDStageAwaitConstraints:
		return "제약 사항을 입력하세요 (옵션, skip 가능)"
	case telegramPRDStageAwaitStoryTitle:
		return "story 제목을 입력하세요 (quick: 제목 | 설명 | role [priority])"
	case telegramPRDStageAwaitStoryDesc:
		return "story 설명을 입력하세요"
	case telegramPRDStageAwaitStoryRole:
		return "role 입력 (manager|planner|developer|qa, optional: role priority)"
	case telegramPRDStageAwaitStoryPrio:
		return "priority 입력 (숫자, default=role 기본값)"
	default:
		return "unknown stage"
	}
}

func telegramHasActivePRDSession(paths ralph.Paths, chatID int64) (bool, error) {
	_, found, err := telegramLoadPRDSession(paths, chatID)
	return found, err
}

func telegramPRDSessionStoreDir(paths ralph.Paths) string {
	return filepath.Join(paths.ReportsDir, "telegram-prd")
}

func telegramPRDSessionFile(paths ralph.Paths) string {
	return filepath.Join(telegramPRDSessionStoreDir(paths), "sessions.json")
}

func legacyTelegramPRDSessionFile(paths ralph.Paths) string {
	return filepath.Join(paths.ControlDir, "telegram-prd-sessions.json")
}

func telegramSessionKey(chatID int64) string {
	return strconv.FormatInt(chatID, 10)
}

const (
	telegramPRDSessionLockWait  = 5 * time.Second
	telegramPRDSessionLockStale = 30 * time.Second
)

func withTelegramPRDSessionStoreLock(paths ralph.Paths, fn func(path string) error) error {
	path := telegramPRDSessionFile(paths)
	lockPath := path + ".lock"
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return fmt.Errorf("create prd lock dir: %w", err)
	}

	telegramPRDSessionStoreMu.Lock()
	defer telegramPRDSessionStoreMu.Unlock()

	deadline := time.Now().Add(telegramPRDSessionLockWait)
	for {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = fmt.Fprintf(f, "%d\n%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
			_ = f.Close()
			defer func() {
				_ = os.Remove(lockPath)
			}()
			return fn(path)
		}
		if !os.IsExist(err) {
			return fmt.Errorf("acquire prd session lock: %w", err)
		}
		shouldBreak, reason := shouldBreakTelegramPRDSessionLock(lockPath)
		if shouldBreak {
			_ = os.Remove(lockPath)
			continue
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("acquire prd session lock timeout (%s)", reason)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

func shouldBreakTelegramPRDSessionLock(lockPath string) (bool, string) {
	info, err := os.Stat(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return true, "lock disappeared"
		}
		return false, "lock stat failed"
	}
	if time.Since(info.ModTime()) > telegramPRDSessionLockStale {
		return true, fmt.Sprintf("lock stale>%s", telegramPRDSessionLockStale)
	}
	pid, ok := telegramPRDLockOwnerPID(lockPath)
	if !ok {
		return false, "owner pid unknown"
	}
	alive, aliveErr := telegramPRDProcessAlive(pid)
	if aliveErr != nil {
		return false, fmt.Sprintf("owner pid check failed(%d)", pid)
	}
	if !alive {
		return true, fmt.Sprintf("owner pid dead(%d)", pid)
	}
	return false, fmt.Sprintf("owner pid alive(%d)", pid)
}

func telegramPRDLockOwnerPID(lockPath string) (int, bool) {
	data, err := os.ReadFile(lockPath)
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return 0, false
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

func telegramPRDProcessAlive(pid int) (bool, error) {
	if pid <= 0 {
		return false, nil
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	if errors.Is(err, syscall.EPERM) {
		return true, nil
	}
	return false, err
}

func parseTelegramPRDSessionStoreData(data []byte) (telegramPRDSessionStore, error) {
	store := telegramPRDSessionStore{Sessions: map[string]telegramPRDSession{}}
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

func loadTelegramPRDSessionStoreUnlocked(paths ralph.Paths, path string) (telegramPRDSessionStore, error) {
	store := telegramPRDSessionStore{Sessions: map[string]telegramPRDSession{}}
	data, err := os.ReadFile(path)
	if err == nil {
		parsed, parseErr := parseTelegramPRDSessionStoreData(data)
		if parseErr != nil {
			return store, fmt.Errorf("parse prd session store: %w", parseErr)
		}
		return parsed, nil
	}
	if !os.IsNotExist(err) {
		return store, fmt.Errorf("read prd session store: %w", err)
	}

	legacyPath := legacyTelegramPRDSessionFile(paths)
	legacyData, legacyErr := os.ReadFile(legacyPath)
	if legacyErr != nil {
		if os.IsNotExist(legacyErr) {
			return store, nil
		}
		return store, fmt.Errorf("read legacy prd session store: %w", legacyErr)
	}
	legacyStore, parseErr := parseTelegramPRDSessionStoreData(legacyData)
	if parseErr != nil {
		return store, fmt.Errorf("parse legacy prd session store: %w", parseErr)
	}

	if writeErr := saveTelegramPRDSessionStoreUnlocked(path, legacyStore); writeErr == nil {
		_ = os.Remove(legacyPath)
	}
	return legacyStore, nil
}

func saveTelegramPRDSessionStoreUnlocked(path string, store telegramPRDSessionStore) error {
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
	if err := writeTelegramPRDAtomicFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write prd session store: %w", err)
	}
	return nil
}

func writeTelegramPRDAtomicFile(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(dir, ".telegram-prd-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Chmod(mode); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

func telegramLoadPRDSession(paths ralph.Paths, chatID int64) (telegramPRDSession, bool, error) {
	var (
		store telegramPRDSessionStore
		err   error
	)
	lockErr := withTelegramPRDSessionStoreLock(paths, func(path string) error {
		store, err = loadTelegramPRDSessionStoreUnlocked(paths, path)
		return err
	})
	if lockErr != nil {
		return telegramPRDSession{}, false, lockErr
	}
	key := telegramSessionKey(chatID)
	session, ok := store.Sessions[key]
	return session, ok, nil
}

func telegramUpsertPRDSession(paths ralph.Paths, session telegramPRDSession) error {
	return withTelegramPRDSessionStoreLock(paths, func(path string) error {
		store, err := loadTelegramPRDSessionStoreUnlocked(paths, path)
		if err != nil {
			return err
		}
		key := telegramSessionKey(session.ChatID)
		store.Sessions[key] = session
		return saveTelegramPRDSessionStoreUnlocked(path, store)
	})
}

func telegramDeletePRDSession(paths ralph.Paths, chatID int64) error {
	return withTelegramPRDSessionStoreLock(paths, func(path string) error {
		store, err := loadTelegramPRDSessionStoreUnlocked(paths, path)
		if err != nil {
			return err
		}
		delete(store.Sessions, telegramSessionKey(chatID))
		return saveTelegramPRDSessionStoreUnlocked(path, store)
	})
}

func telegramPRDConversationDir(paths ralph.Paths, chatID int64) string {
	return filepath.Join(telegramPRDSessionStoreDir(paths), "conversations", strconv.FormatInt(chatID, 10))
}

func telegramPRDConversationFile(paths ralph.Paths, chatID int64) string {
	return filepath.Join(telegramPRDConversationDir(paths, chatID), "conversation.md")
}

func logTelegramPRDConversationWarning(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "[telegram] prd conversation warning: %v\n", err)
}

func clearTelegramPRDConversation(paths ralph.Paths, chatID int64) error {
	return os.RemoveAll(telegramPRDConversationDir(paths, chatID))
}

func appendTelegramPRDConversation(paths ralph.Paths, chatID int64, role, text string) error {
	role = strings.TrimSpace(strings.ToLower(role))
	if role == "" {
		role = "assistant"
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	path := telegramPRDConversationFile(paths, chatID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create conversation dir: %w", err)
	}
	entry := fmt.Sprintf("\n### %s | %s\n%s\n", time.Now().UTC().Format(time.RFC3339), role, text)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open conversation file: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(entry); err != nil {
		return fmt.Errorf("append conversation file: %w", err)
	}
	return nil
}

func readTelegramPRDConversationTail(paths ralph.Paths, chatID int64, maxRunes int) string {
	if maxRunes <= 0 {
		maxRunes = 5000
	}
	data, err := os.ReadFile(telegramPRDConversationFile(paths, chatID))
	if err != nil {
		return ""
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= maxRunes {
		return text
	}
	return "...(truncated)\n" + string(runes[len(runes)-maxRunes:])
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
	clarity := evaluateTelegramPRDClarity(session)
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
			s.Priority = telegramPRDStoryPriorityForRole(session, s.Role)
		}
		stories = append(stories, s)
	}
	doc := map[string]any{
		"metadata": map[string]any{
			"product":          product,
			"source":           "telegram-prd-wizard",
			"generated_at_utc": time.Now().UTC().Format(time.RFC3339),
			"clarity_score":    clarity.Score,
			"clarity_gate":     telegramPRDClarityMinScore,
			"context": map[string]any{
				"problem":        strings.TrimSpace(session.Context.Problem),
				"goal":           strings.TrimSpace(session.Context.Goal),
				"in_scope":       strings.TrimSpace(session.Context.InScope),
				"out_of_scope":   strings.TrimSpace(session.Context.OutOfScope),
				"acceptance":     strings.TrimSpace(session.Context.Acceptance),
				"constraints":    strings.TrimSpace(session.Context.Constraints),
				"assumptions":    session.Context.Assumptions,
				"agent_priority": normalizeTelegramPRDAgentPriorityMap(session.Context.AgentPriority),
			},
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
		"Ralph Bot Commands",
		"==================",
		"",
		"Read",
		"- /help",
		"- /ping",
		"- /status [all|<project_id>]",
		"- /doctor [all|<project_id>]",
		"- /fleet [all|<project_id>]",
	}
	if allowControl {
		lines = append(lines,
			"",
			"Control",
			"- /start [all|<project_id>]",
			"- /stop [all|<project_id>]",
			"- /restart [all|<project_id>]",
			"- /doctor_repair [all|<project_id>]",
			"- /recover [all|<project_id>]",
			"- /new [role] <title> (default role: developer)",
			"",
			"PRD Wizard",
			"- /prd help",
			"- /prd start | /prd refine | /prd priority | /prd score | /prd apply | /prd approve",
		)
	} else {
		lines = append(lines, "", "Control", "- disabled (--allow-control=false)")
	}
	return strings.Join(lines, "\n")
}

func formatStatusForTelegram(st ralph.Status) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Ralph Status\n")
	fmt.Fprintf(&b, "============\n")
	fmt.Fprintf(&b, "- Project: %s\n", st.ProjectDir)
	fmt.Fprintf(&b, "- Plugin:  %s\n", st.PluginName)
	fmt.Fprintf(&b, "- Daemon:  %s\n", st.Daemon)
	fmt.Fprintf(&b, "\nQueue\n")
	fmt.Fprintf(&b, "- Ready:       %d\n", st.QueueReady)
	fmt.Fprintf(&b, "- In Progress: %d\n", st.InProgress)
	fmt.Fprintf(&b, "- Done:        %d\n", st.Done)
	fmt.Fprintf(&b, "- Blocked:     %d\n", st.Blocked)
	fmt.Fprintf(&b, "- Next:        %s\n", st.NextReady)
	if ralph.IsInputRequiredStatus(st) {
		fmt.Fprintf(&b, "\nInput Required\n")
		fmt.Fprintf(&b, "- No queued work\n")
		fmt.Fprintf(&b, "- Add issue: ./ralph new developer \"<title>\"\n")
		fmt.Fprintf(&b, "- Import PRD: ./ralph import-prd --file prd.json\n")
		fmt.Fprintf(&b, "- Telegram PRD Wizard: /prd start -> /prd refine -> /prd apply\n")
	}
	if st.LastProfileReloadAt != "" || st.ProfileReloadCount > 0 {
		fmt.Fprintf(&b, "\nRuntime\n")
		fmt.Fprintf(&b, "- Profile Reload At: %s\n", valueOrDash(st.LastProfileReloadAt))
		fmt.Fprintf(&b, "- Profile Reload #:  %d\n", st.ProfileReloadCount)
	}
	if st.LastFailureCause != "" || st.LastCodexRetryCount > 0 || st.LastPermissionStreak > 0 {
		if st.LastProfileReloadAt == "" && st.ProfileReloadCount == 0 {
			fmt.Fprintf(&b, "\nRuntime\n")
		}
		fmt.Fprintf(
			&b,
			"- Last Failure: %s\n- Codex Retries: %d\n- Permission Streak: %d\n",
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
			"[ralph alert][input_required]\n- project: %s\n- message: no queued work. add issue (`./ralph new ...`) or run PRD wizard (`/prd start -> /prd refine -> /prd apply`)",
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
