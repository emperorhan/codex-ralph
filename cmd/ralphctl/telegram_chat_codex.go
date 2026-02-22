package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"codex-ralph/internal/ralph"
)

const (
	telegramCodexChatMaxTailRunes = 8000
	telegramCodexChatReplyMaxLen  = 7000
)

var telegramCodexChatAnalyzer = analyzeTelegramChatWithCodex

var telegramCodexChatLogMaxLines = 1200
var telegramCodexChatLogTrimLines = 900
var telegramCodexChatLogMaxBytes = 1 * 1024 * 1024
var telegramCodexChatLogTrimBytes = 768 * 1024

func telegramChatCommand(paths ralph.Paths, chatID int64, rawArgs string) (string, error) {
	text := strings.TrimSpace(rawArgs)
	if text == "" {
		lines := []string{
			"codex chat",
			"- usage: /chat <message>",
			"- status: /chat status",
			"- reset: /chat reset",
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
	tail := readTelegramChatConversationTail(paths, chatID, 1200)
	runes := len([]rune(tail))
	lines := []string{
		"codex chat status",
		fmt.Sprintf("- context_tail_runes: %d", runes),
		fmt.Sprintf("- policy_lines: max=%d trim=%d", telegramCodexChatLogMaxLines, telegramCodexChatLogTrimLines),
		fmt.Sprintf("- policy_bytes: max=%d trim=%d", telegramCodexChatLogMaxBytes, telegramCodexChatLogTrimBytes),
		"- reset: /chat reset",
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
	next := "check codex login/network/profile and retry"
	if category == "file_not_found" {
		next = "check project path/working dir, restart telegram daemon (`ralphctl telegram stop && ralphctl telegram run`), then retry"
	}
	lines := []string{
		"codex chat unavailable",
		"- reason: codex execution failed",
		"- next: " + next,
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

	timeoutSec := resolveTelegramCodexTimeoutSec(profile.CodexExecTimeoutSec, 180)
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
		raw, execErr := runTelegramPRDCodexExec(ctx, paths, profile, model, prompt, "ralph-telegram-chat-*")
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
	if _, err := f.WriteString(entry); err != nil {
		_ = f.Close()
		return fmt.Errorf("append chat conversation file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close chat conversation file: %w", err)
	}
	if err := compactTelegramChatConversationFile(path); err != nil {
		return fmt.Errorf("compact chat conversation file: %w", err)
	}
	return nil
}

func compactTelegramChatConversationFile(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(raw) == 0 {
		return nil
	}

	data := bytes.ToValidUTF8(raw, []byte("?"))
	changed := !bytes.Equal(data, raw)

	if telegramCodexChatLogMaxLines > 0 && telegramChatLineCount(data) > telegramCodexChatLogMaxLines {
		keep := telegramCodexChatLogTrimLines
		if keep <= 0 || keep > telegramCodexChatLogMaxLines {
			keep = telegramCodexChatLogMaxLines
		}
		data = telegramTailLines(data, keep)
		changed = true
	}

	if telegramCodexChatLogMaxBytes > 0 && len(data) > telegramCodexChatLogMaxBytes {
		keepBytes := telegramCodexChatLogTrimBytes
		if keepBytes <= 0 || keepBytes > telegramCodexChatLogMaxBytes {
			keepBytes = telegramCodexChatLogMaxBytes
		}
		data = telegramTailValidUTF8Bytes(data, keepBytes)
		if idx := bytes.Index(data, []byte("\n### ")); idx > 0 {
			data = data[idx+1:]
		} else if idx := bytes.IndexByte(data, '\n'); idx >= 0 && idx+1 < len(data) {
			data = data[idx+1:]
		}
		changed = true
	}

	data = bytes.TrimSpace(data)
	if len(data) > 0 {
		data = append(data, '\n')
	}
	if !changed {
		return nil
	}
	return writeTelegramPRDAtomicFile(path, data, 0o644)
}

func telegramChatLineCount(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	lines := bytes.Count(data, []byte{'\n'})
	if data[len(data)-1] != '\n' {
		lines++
	}
	return lines
}

func telegramTailLines(data []byte, keepLines int) []byte {
	if keepLines <= 0 {
		return data
	}
	text := string(data)
	lines := strings.Split(text, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) <= keepLines {
		return []byte(text)
	}
	trimmed := strings.Join(lines[len(lines)-keepLines:], "\n")
	if strings.TrimSpace(trimmed) == "" {
		return []byte{}
	}
	return []byte(trimmed + "\n")
}

func telegramTailValidUTF8Bytes(data []byte, keepBytes int) []byte {
	if keepBytes <= 0 || len(data) <= keepBytes {
		return data
	}
	start := len(data) - keepBytes
	for start < len(data) && !utf8.RuneStart(data[start]) {
		start++
	}
	if start >= len(data) {
		start = len(data) - keepBytes
	}
	return data[start:]
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
