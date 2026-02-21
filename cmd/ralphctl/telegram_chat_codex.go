package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"codex-ralph/internal/ralph"
)

const (
	telegramCodexChatMaxTailRunes = 8000
	telegramCodexChatReplyMaxLen  = 7000
)

var telegramChatApprovalTTL = 5 * time.Minute
var telegramChatControlStoreMu sync.Mutex
var telegramCodexChatAnalyzer = analyzeTelegramChatWithCodex

type telegramChatControlStore struct {
	Sessions map[string]telegramChatControlSession `json:"sessions"`
}

type telegramChatControlSession struct {
	ChatID          int64                        `json:"chat_id"`
	OpsShortcuts    bool                         `json:"ops_shortcuts"`
	PendingApproval *telegramChatPendingApproval `json:"pending_approval,omitempty"`
	LastUpdatedAtUT string                       `json:"last_updated_at_utc,omitempty"`
}

type telegramChatPendingApproval struct {
	ID           string `json:"id"`
	Command      string `json:"command"`
	Args         string `json:"args,omitempty"`
	CreatedAtUTC string `json:"created_at_utc"`
	ExpiresAtUTC string `json:"expires_at_utc"`
}

func telegramChatControlStoreFile(paths ralph.Paths) string {
	return filepath.Join(paths.ControlDir, "telegram-chat-control.json")
}

func defaultTelegramChatControlSession(chatID int64) telegramChatControlSession {
	return telegramChatControlSession{
		ChatID:       chatID,
		OpsShortcuts: true,
	}
}

func loadTelegramChatControlStoreUnlocked(path string) (telegramChatControlStore, error) {
	store := telegramChatControlStore{
		Sessions: map[string]telegramChatControlSession{},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return store, nil
		}
		return store, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return store, nil
	}
	if err := json.Unmarshal(data, &store); err != nil {
		return store, err
	}
	if store.Sessions == nil {
		store.Sessions = map[string]telegramChatControlSession{}
	}
	return store, nil
}

func saveTelegramChatControlStoreUnlocked(path string, store telegramChatControlStore) error {
	if store.Sessions == nil {
		store.Sessions = map[string]telegramChatControlSession{}
	}
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return writeTelegramPRDAtomicFile(path, data, 0o600)
}

func telegramLoadChatControlSession(paths ralph.Paths, chatID int64) (telegramChatControlSession, error) {
	telegramChatControlStoreMu.Lock()
	defer telegramChatControlStoreMu.Unlock()

	store, err := loadTelegramChatControlStoreUnlocked(telegramChatControlStoreFile(paths))
	if err != nil {
		return telegramChatControlSession{}, err
	}
	session, ok := store.Sessions[telegramSessionKey(chatID)]
	if !ok {
		session = defaultTelegramChatControlSession(chatID)
	}
	return session, nil
}

func telegramUpdateChatControlSession(
	paths ralph.Paths,
	chatID int64,
	mutate func(*telegramChatControlSession) error,
) (telegramChatControlSession, error) {
	telegramChatControlStoreMu.Lock()
	defer telegramChatControlStoreMu.Unlock()

	storePath := telegramChatControlStoreFile(paths)
	store, err := loadTelegramChatControlStoreUnlocked(storePath)
	if err != nil {
		return telegramChatControlSession{}, err
	}
	key := telegramSessionKey(chatID)
	session, ok := store.Sessions[key]
	if !ok {
		session = defaultTelegramChatControlSession(chatID)
	}
	if err := mutate(&session); err != nil {
		return telegramChatControlSession{}, err
	}
	session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
	store.Sessions[key] = session
	if err := saveTelegramChatControlStoreUnlocked(storePath, store); err != nil {
		return telegramChatControlSession{}, err
	}
	return session, nil
}

func telegramSetOpsShortcuts(paths ralph.Paths, chatID int64, enabled bool) (telegramChatControlSession, error) {
	return telegramUpdateChatControlSession(paths, chatID, func(session *telegramChatControlSession) error {
		session.OpsShortcuts = enabled
		return nil
	})
}

func telegramGenerateApprovalToken() string {
	return strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
}

func telegramQueuePendingApproval(paths ralph.Paths, chatID int64, cmd, args string, ttl time.Duration) (telegramChatPendingApproval, error) {
	if ttl <= 0 {
		ttl = telegramChatApprovalTTL
	}
	now := time.Now().UTC()
	pending := telegramChatPendingApproval{
		ID:           telegramGenerateApprovalToken(),
		Command:      strings.TrimSpace(cmd),
		Args:         strings.TrimSpace(args),
		CreatedAtUTC: now.Format(time.RFC3339),
		ExpiresAtUTC: now.Add(ttl).Format(time.RFC3339),
	}
	_, err := telegramUpdateChatControlSession(paths, chatID, func(session *telegramChatControlSession) error {
		session.PendingApproval = &pending
		return nil
	})
	if err != nil {
		return telegramChatPendingApproval{}, err
	}
	return pending, nil
}

func telegramConsumePendingApproval(paths ralph.Paths, chatID int64, token string) (telegramChatPendingApproval, bool, error) {
	token = strings.TrimSpace(strings.ToLower(token))
	if token == "" {
		return telegramChatPendingApproval{}, false, nil
	}

	var (
		matched bool
		pending telegramChatPendingApproval
	)
	_, err := telegramUpdateChatControlSession(paths, chatID, func(session *telegramChatControlSession) error {
		if session.PendingApproval == nil {
			return nil
		}
		current := session.PendingApproval
		expired := false
		if strings.TrimSpace(current.ExpiresAtUTC) != "" {
			if expAt, parseErr := time.Parse(time.RFC3339, current.ExpiresAtUTC); parseErr == nil && time.Now().UTC().After(expAt) {
				expired = true
			}
		}
		if expired {
			session.PendingApproval = nil
			return nil
		}
		if strings.ToLower(strings.TrimSpace(current.ID)) != token {
			return nil
		}
		pending = *current
		matched = true
		session.PendingApproval = nil
		return nil
	})
	if err != nil {
		return telegramChatPendingApproval{}, false, err
	}
	return pending, matched, nil
}

func telegramOpsCommand(paths ralph.Paths, chatID int64, rawArgs string) (string, error) {
	fields := strings.Fields(strings.ToLower(strings.TrimSpace(rawArgs)))
	mode := ""
	if len(fields) > 0 {
		mode = fields[0]
	}

	switch mode {
	case "", "status":
		session, err := telegramLoadChatControlSession(paths, chatID)
		if err != nil {
			return "", err
		}
		return formatTelegramOpsStatus(session), nil
	case "on":
		session, err := telegramSetOpsShortcuts(paths, chatID, true)
		if err != nil {
			return "", err
		}
		return "ops shortcuts enabled\n" + formatTelegramOpsStatus(session), nil
	case "off":
		session, err := telegramSetOpsShortcuts(paths, chatID, false)
		if err != nil {
			return "", err
		}
		return "ops shortcuts disabled\n" + formatTelegramOpsStatus(session), nil
	default:
		return "usage: /ops [status|on|off]", nil
	}
}

func formatTelegramOpsStatus(session telegramChatControlSession) string {
	lines := []string{
		"ops status",
		fmt.Sprintf("- shortcuts: %t", session.OpsShortcuts),
	}
	if session.PendingApproval != nil {
		lines = append(lines,
			fmt.Sprintf("- pending: %s", session.PendingApproval.ID),
			fmt.Sprintf("- pending_command: %s %s", session.PendingApproval.Command, session.PendingApproval.Args),
			fmt.Sprintf("- pending_expires_at_utc: %s", session.PendingApproval.ExpiresAtUTC),
		)
	} else {
		lines = append(lines, "- pending: none")
	}
	return strings.Join(lines, "\n")
}

func formatTelegramPendingApproval(pending telegramChatPendingApproval) string {
	return strings.Join([]string{
		"approval required",
		fmt.Sprintf("- command: %s %s", pending.Command, pending.Args),
		fmt.Sprintf("- token: %s", pending.ID),
		fmt.Sprintf("- approve: /approve %s", pending.ID),
		fmt.Sprintf("- expires_at_utc: %s", pending.ExpiresAtUTC),
	}, "\n")
}

func telegramApproveCommand(controlDir string, paths ralph.Paths, allowControl bool, chatID int64, rawArgs string) (string, error) {
	token := strings.TrimSpace(rawArgs)
	if token == "" {
		return "usage: /approve <token>", nil
	}
	pending, matched, err := telegramConsumePendingApproval(paths, chatID, token)
	if err != nil {
		return "", err
	}
	if !matched {
		return "no pending approval matched\n- next: run command again to request a new token", nil
	}
	reply, err := dispatchTelegramCommand(controlDir, paths, allowControl, chatID, pending.Command, pending.Args)
	if err != nil {
		return "", err
	}
	return strings.Join([]string{
		"approval accepted",
		fmt.Sprintf("- command: %s %s", pending.Command, pending.Args),
		"",
		reply,
	}, "\n"), nil
}

func telegramChatCommand(paths ralph.Paths, chatID int64, rawArgs string) (string, error) {
	text := strings.TrimSpace(rawArgs)
	if text == "" {
		session, err := telegramLoadChatControlSession(paths, chatID)
		if err != nil {
			return "", err
		}
		lines := []string{
			"codex chat",
			"- usage: /chat <message>",
			"- status: /chat status",
			"- reset: /chat reset",
			"",
			formatTelegramOpsStatus(session),
		}
		return strings.Join(lines, "\n"), nil
	}

	fields := strings.Fields(strings.ToLower(text))
	if len(fields) == 1 {
		switch fields[0] {
		case "reset", "clear":
			logTelegramChatConversationWarning(clearTelegramChatConversation(paths, chatID))
			return "codex chat context reset", nil
		case "status":
			return telegramChatStatus(paths, chatID)
		}
	}

	return telegramChatConversationInput(paths, chatID, rawArgs)
}

func telegramChatStatus(paths ralph.Paths, chatID int64) (string, error) {
	session, err := telegramLoadChatControlSession(paths, chatID)
	if err != nil {
		return "", err
	}
	tail := readTelegramChatConversationTail(paths, chatID, 1200)
	runes := len([]rune(tail))
	lines := []string{
		"codex chat status",
		fmt.Sprintf("- shortcuts: %t", session.OpsShortcuts),
		fmt.Sprintf("- context_tail_runes: %d", runes),
		"- reset: /chat reset",
	}
	if session.PendingApproval != nil {
		lines = append(lines, fmt.Sprintf("- pending_approval: %s", session.PendingApproval.ID))
	}
	return strings.Join(lines, "\n"), nil
}

func telegramChatConversationInput(paths ralph.Paths, chatID int64, input string) (string, error) {
	input = strings.TrimSpace(sanitizeTelegramUTF8String(input))
	if input == "" {
		return "", nil
	}
	logTelegramChatConversationWarning(appendTelegramChatConversation(paths, chatID, "user", input))
	reply, err := telegramCodexChatAnalyzer(paths, chatID, input)
	if err != nil {
		return formatTelegramCodexChatUnavailable(err), nil
	}
	reply = strings.TrimSpace(sanitizeTelegramUTF8String(reply))
	if reply == "" {
		return "codex returned an empty response\n- next: retry with a more specific instruction", nil
	}
	logTelegramChatConversationWarning(appendTelegramChatConversation(paths, chatID, "assistant", reply))
	return reply, nil
}

func formatTelegramCodexChatUnavailable(err error) string {
	category, detail := classifyTelegramCodexFailure(err)
	lines := []string{
		"codex chat unavailable",
		"- reason: codex execution failed",
		"- next: check codex login/network/profile and retry",
	}
	if category != "" {
		lines = append(lines, "- codex_error: "+category)
	}
	if detail != "" {
		lines = append(lines, "- codex_detail: "+detail)
	}
	return strings.Join(lines, "\n")
}

func analyzeTelegramChatWithCodex(paths ralph.Paths, chatID int64, input string) (string, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return "", fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return "", err
	}
	if !profile.RequireCodex {
		return "", fmt.Errorf("codex chat disabled (require_codex=false)")
	}

	timeoutSec := profile.CodexExecTimeoutSec
	if timeoutSec <= 0 || timeoutSec > 300 {
		timeoutSec = 180
	}
	retryAttempts := profile.CodexRetryMaxAttempts
	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	if retryAttempts > 3 {
		retryAttempts = 3
	}
	retryBackoffSec := profile.CodexRetryBackoffSec
	if retryBackoffSec <= 0 {
		retryBackoffSec = 1
	}
	if retryBackoffSec > 3 {
		retryBackoffSec = 3
	}

	conversationTail := readTelegramChatConversationTail(paths, chatID, telegramCodexChatMaxTailRunes)
	prompt := buildTelegramCodexChatPrompt(paths.ProjectDir, conversationTail, input)
	model := strings.TrimSpace(profile.CodexModelForRole("developer"))

	var lastErr error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
		raw, execErr := runTelegramPRDCodexExec(ctx, paths, profile.CodexApproval, profile.CodexSandbox, model, prompt, "ralph-telegram-chat-*")
		cancel()
		if execErr == nil {
			reply := sanitizeTelegramCodexChatReply(raw)
			if reply != "" {
				return reply, nil
			}
			lastErr = fmt.Errorf("empty codex chat response")
		} else {
			lastErr = execErr
		}
		if attempt < retryAttempts {
			time.Sleep(time.Duration(attempt*retryBackoffSec) * time.Second)
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("unknown codex chat failure")
	}
	return "", fmt.Errorf("codex chat retries exhausted: %w", lastErr)
}

func sanitizeTelegramCodexChatReply(raw string) string {
	text := strings.TrimSpace(sanitizeTelegramUTF8String(raw))
	if text == "" {
		return ""
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```markdown")
		text = strings.TrimPrefix(text, "```text")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}
	if text == "" {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= telegramCodexChatReplyMaxLen {
		return text
	}
	return string(runes[:telegramCodexChatReplyMaxLen]) + "\n...(truncated)"
}

func buildTelegramCodexChatPrompt(projectDir, conversationTail, input string) string {
	var b strings.Builder
	fmt.Fprintln(&b, "You are Codex in a Telegram bridge for a software project.")
	fmt.Fprintln(&b, "Respond in concise Korean unless the user asks otherwise.")
	fmt.Fprintln(&b, "Prioritize practical execution in the current project repository.")
	fmt.Fprintln(&b, "If the user asks for PRD work, help refine requirements and propose concrete deliverables.")
	fmt.Fprintln(&b, "If shell/code actions are requested, describe what you changed or what command should run.")
	fmt.Fprintf(&b, "Project directory: %s\n", strings.TrimSpace(projectDir))
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "\nRecent conversation (markdown):")
		fmt.Fprintln(&b, conversationTail)
	}
	fmt.Fprintln(&b, "\nUser message:")
	fmt.Fprintln(&b, strings.TrimSpace(input))
	return b.String()
}

func telegramChatConversationDir(paths ralph.Paths, chatID int64) string {
	return filepath.Join(paths.ControlDir, "telegram-chat", strconv.FormatInt(chatID, 10))
}

func telegramChatConversationFile(paths ralph.Paths, chatID int64) string {
	return filepath.Join(telegramChatConversationDir(paths, chatID), "conversation.md")
}

func clearTelegramChatConversation(paths ralph.Paths, chatID int64) error {
	return os.RemoveAll(telegramChatConversationDir(paths, chatID))
}

func appendTelegramChatConversation(paths ralph.Paths, chatID int64, role, text string) error {
	role = strings.TrimSpace(strings.ToLower(role))
	if role == "" {
		role = "assistant"
	}
	text = strings.TrimSpace(sanitizeTelegramUTF8String(text))
	if text == "" {
		return nil
	}
	path := telegramChatConversationFile(paths, chatID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create chat conversation dir: %w", err)
	}
	entry := fmt.Sprintf("\n### %s | %s\n%s\n", time.Now().UTC().Format(time.RFC3339), role, text)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open chat conversation file: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(entry); err != nil {
		return fmt.Errorf("append chat conversation file: %w", err)
	}
	return nil
}

func readTelegramChatConversationTail(paths ralph.Paths, chatID int64, maxRunes int) string {
	if maxRunes <= 0 {
		maxRunes = telegramCodexChatMaxTailRunes
	}
	data, err := os.ReadFile(telegramChatConversationFile(paths, chatID))
	if err != nil {
		return ""
	}
	data = bytes.ToValidUTF8(data, []byte("?"))
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

func logTelegramChatConversationWarning(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	fmt.Fprintf(os.Stderr, "[telegram] chat conversation warning: %v\n", err)
}
