package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
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
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --control-dir DIR --project-dir DIR telegram <run|setup> [flags]")
		fmt.Fprintln(os.Stderr, "Env: RALPH_TELEGRAM_BOT_TOKEN, RALPH_TELEGRAM_CHAT_IDS, RALPH_TELEGRAM_ALLOW_CONTROL, RALPH_TELEGRAM_NOTIFY")
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
	token := fs.String("token", firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_BOT_TOKEN")), cfg.Token), "telegram bot token")
	chatIDsRaw := fs.String("chat-ids", firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_CHAT_IDS")), cfg.ChatIDs), "allowed chat IDs CSV (required)")
	allowControl := fs.Bool("allow-control", envBoolDefault("RALPH_TELEGRAM_ALLOW_CONTROL", cfg.AllowControl), "allow control commands (/start,/stop,/restart,/doctor_repair,/recover)")
	enableNotify := fs.Bool("notify", envBoolDefault("RALPH_TELEGRAM_NOTIFY", cfg.Notify), "push alerts for blocked/retry/stuck")
	notifyIntervalSec := fs.Int("notify-interval-sec", envIntDefault("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", cfg.NotifyIntervalSec), "status poll interval for notify alerts")
	notifyRetryThreshold := fs.Int("notify-retry-threshold", envIntDefault("RALPH_TELEGRAM_NOTIFY_RETRY_THRESHOLD", cfg.NotifyRetryThreshold), "codex retry alert threshold")
	notifyPermStreakThreshold := fs.Int("notify-perm-streak-threshold", envIntDefault("RALPH_TELEGRAM_NOTIFY_PERM_STREAK_THRESHOLD", cfg.NotifyPermStreakThreshold), "permission streak alert threshold")
	pollTimeoutSec := fs.Int("poll-timeout-sec", 30, "telegram getUpdates timeout (seconds)")
	offsetFile := fs.String("offset-file", filepath.Join(controlDir, "telegram.offset"), "telegram update offset file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_ = configFileFlag

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
	if *pollTimeoutSec <= 0 {
		return fmt.Errorf("--poll-timeout-sec must be > 0")
	}
	if *notifyIntervalSec <= 0 {
		return fmt.Errorf("--notify-interval-sec must be > 0")
	}

	fmt.Println("telegram bot started")
	fmt.Printf("- control_dir: %s\n", controlDir)
	fmt.Printf("- project_dir: %s\n", paths.ProjectDir)
	fmt.Printf("- config_file: %s\n", configFile)
	fmt.Printf("- allow_control: %t\n", *allowControl)
	fmt.Printf("- notify: %t\n", *enableNotify)
	fmt.Printf("- notify_interval_sec: %d\n", *notifyIntervalSec)
	fmt.Printf("- notify_retry_threshold: %d\n", *notifyRetryThreshold)
	fmt.Printf("- notify_perm_streak_threshold: %d\n", *notifyPermStreakThreshold)
	fmt.Printf("- allowed_chats: %d\n", len(allowedChatIDs))
	fmt.Printf("- offset_file: %s\n", *offsetFile)

	notifyHandler := ralph.TelegramNotifyHandler(nil)
	if *enableNotify {
		notifyHandler = newStatusNotifyHandler(paths, *notifyRetryThreshold, *notifyPermStreakThreshold)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return ralph.RunTelegramBot(ctx, ralph.TelegramBotOptions{
		Token:             *token,
		AllowedChatIDs:    allowedChatIDs,
		PollTimeoutSec:    *pollTimeoutSec,
		NotifyIntervalSec: *notifyIntervalSec,
		OffsetFile:        *offsetFile,
		Out:               os.Stdout,
		OnCommand:         telegramCommandHandler(controlDir, paths, *allowControl),
		OnNotifyTick:      notifyHandler,
	})
}

func runTelegramSetupCommand(controlDir string, args []string) error {
	configFile := telegramConfigFileFromArgs(controlDir, args)
	cfg, err := loadTelegramCLIConfig(configFile)
	if err != nil {
		return err
	}

	defaultToken := firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_BOT_TOKEN")), cfg.Token)
	defaultChatIDs := firstNonEmpty(strings.TrimSpace(os.Getenv("RALPH_TELEGRAM_CHAT_IDS")), cfg.ChatIDs)
	defaultAllowControl := envBoolDefault("RALPH_TELEGRAM_ALLOW_CONTROL", cfg.AllowControl)
	defaultNotify := envBoolDefault("RALPH_TELEGRAM_NOTIFY", cfg.Notify)
	defaultNotifyInterval := envIntDefault("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC", cfg.NotifyIntervalSec)
	defaultNotifyRetry := envIntDefault("RALPH_TELEGRAM_NOTIFY_RETRY_THRESHOLD", cfg.NotifyRetryThreshold)
	defaultNotifyPerm := envIntDefault("RALPH_TELEGRAM_NOTIFY_PERM_STREAK_THRESHOLD", cfg.NotifyPermStreakThreshold)

	fs := flag.NewFlagSet("telegram setup", flag.ContinueOnError)
	configFileFlag := fs.String("config-file", configFile, "telegram config file path")
	nonInteractive := fs.Bool("non-interactive", false, "save config without interactive prompts")
	tokenFlag := fs.String("token", defaultToken, "telegram bot token")
	chatIDsFlag := fs.String("chat-ids", defaultChatIDs, "allowed chat IDs CSV")
	allowControlFlag := fs.Bool("allow-control", defaultAllowControl, "allow control commands")
	notifyFlag := fs.Bool("notify", defaultNotify, "enable notify alerts")
	notifyIntervalFlag := fs.Int("notify-interval-sec", defaultNotifyInterval, "notify interval seconds")
	notifyRetryFlag := fs.Int("notify-retry-threshold", defaultNotifyRetry, "notify retry threshold")
	notifyPermFlag := fs.Int("notify-perm-streak-threshold", defaultNotifyPerm, "notify permission streak threshold")
	if err := fs.Parse(args); err != nil {
		return err
	}

	final := telegramCLIConfig{
		Token:                     strings.TrimSpace(*tokenFlag),
		ChatIDs:                   strings.TrimSpace(*chatIDsFlag),
		AllowControl:              *allowControlFlag,
		Notify:                    *notifyFlag,
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
	if _, err := ralph.ParseTelegramChatIDs(final.ChatIDs); err != nil {
		return err
	}
	if final.NotifyIntervalSec <= 0 {
		return fmt.Errorf("notify-interval-sec must be > 0")
	}
	if err := saveTelegramCLIConfig(configFile, final); err != nil {
		return err
	}

	fmt.Println("telegram setup complete")
	fmt.Printf("- config_file: %s\n", configFile)
	fmt.Printf("- allow_control: %t\n", final.AllowControl)
	fmt.Printf("- notify: %t\n", final.Notify)
	fmt.Printf("- run: ralphctl --project-dir \"$PWD\" telegram run --config-file %s\n", configFile)
	return nil
}

type telegramCLIConfig struct {
	Token                     string
	ChatIDs                   string
	AllowControl              bool
	Notify                    bool
	NotifyIntervalSec         int
	NotifyRetryThreshold      int
	NotifyPermStreakThreshold int
}

func defaultTelegramCLIConfig() telegramCLIConfig {
	return telegramCLIConfig{
		AllowControl:              false,
		Notify:                    true,
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
	if v, ok := parseBoolRaw(values["RALPH_TELEGRAM_ALLOW_CONTROL"]); ok {
		cfg.AllowControl = v
	}
	if v, ok := parseBoolRaw(values["RALPH_TELEGRAM_NOTIFY"]); ok {
		cfg.Notify = v
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
	b.WriteString("RALPH_TELEGRAM_ALLOW_CONTROL=" + strconv.FormatBool(cfg.AllowControl) + "\n")
	b.WriteString("RALPH_TELEGRAM_NOTIFY=" + strconv.FormatBool(cfg.Notify) + "\n")
	b.WriteString("RALPH_TELEGRAM_NOTIFY_INTERVAL_SEC=" + strconv.Itoa(cfg.NotifyIntervalSec) + "\n")
	b.WriteString("RALPH_TELEGRAM_NOTIFY_RETRY_THRESHOLD=" + strconv.Itoa(cfg.NotifyRetryThreshold) + "\n")
	b.WriteString("RALPH_TELEGRAM_NOTIFY_PERM_STREAK_THRESHOLD=" + strconv.Itoa(cfg.NotifyPermStreakThreshold) + "\n")
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

func telegramCommandHandler(controlDir string, paths ralph.Paths, allowControl bool) ralph.TelegramCommandHandler {
	return func(ctx context.Context, chatID int64, text string) (string, error) {
		_ = ctx
		_ = chatID

		cmd, _ := parseTelegramCommandLine(text)
		switch cmd {
		case "", "/help":
			return buildTelegramHelp(allowControl), nil

		case "/ping":
			return "pong " + time.Now().UTC().Format(time.RFC3339), nil

		case "/status":
			st, err := ralph.GetStatus(paths)
			if err != nil {
				return "", err
			}
			return formatStatusForTelegram(st), nil

		case "/fleet", "/fleet_status", "/dashboard":
			var b bytes.Buffer
			if err := renderFleetDashboard(controlDir, "", true, &b); err != nil {
				return "", err
			}
			return b.String(), nil

		case "/doctor":
			report, err := ralph.RunDoctor(paths)
			if err != nil {
				return "", err
			}
			return formatDoctorReportForTelegram(report), nil

		case "/start":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			res, err := startProjectDaemon(paths, startOptions{
				DoctorRepair: true,
				FixPerms:     false,
				Out:          io.Discard,
			})
			if err != nil {
				return "", err
			}
			return res, nil

		case "/stop":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			if err := ralph.StopDaemon(paths); err != nil {
				return "", err
			}
			return "ralph-loop stopped", nil

		case "/restart":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			if err := ralph.StopDaemon(paths); err != nil {
				return "", err
			}
			pid, _, err := ralph.StartDaemon(paths)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("ralph-loop restarted (pid=%d)", pid), nil

		case "/doctor_repair":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			actions, err := ralph.RepairProject(paths)
			if err != nil {
				return "", err
			}
			return formatDoctorRepairActions(actions), nil

		case "/recover":
			if !allowControl {
				return "control commands are disabled (run with --allow-control)", nil
			}
			recovered, err := ralph.RecoverInProgressWithCount(paths)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("recovered in-progress issues: %d", recovered), nil

		default:
			return "unknown command\n\n" + buildTelegramHelp(allowControl), nil
		}
	}
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
		"- /status",
		"- /doctor",
		"- /fleet",
	}
	if allowControl {
		lines = append(lines,
			"- /start",
			"- /stop",
			"- /restart",
			"- /doctor_repair",
			"- /recover",
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

	if current.LastBusyWaitDetectedAt != "" && current.LastBusyWaitDetectedAt != prev.LastBusyWaitDetectedAt {
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

	return out
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
