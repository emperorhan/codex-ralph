package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"codex-ralph/internal/ralph"
)

func newTelegramChatTestPaths(t *testing.T) ralph.Paths {
	t.Helper()

	controlDir := filepath.Join(t.TempDir(), "control")
	projectDir := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		t.Fatalf("mkdir control dir: %v", err)
	}
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	paths, err := ralph.NewPaths(controlDir, projectDir)
	if err != nil {
		t.Fatalf("new paths failed: %v", err)
	}
	return paths
}

func TestAppendTelegramChatConversationCompactsByLines(t *testing.T) {
	paths := newTelegramChatTestPaths(t)
	chatID := int64(101)

	for i := 0; i < 500; i++ {
		msg := fmt.Sprintf("line-msg-%03d", i)
		if err := appendTelegramChatConversation(paths, chatID, "user", msg); err != nil {
			t.Fatalf("append chat conversation failed at %d: %v", i, err)
		}
	}

	data, err := os.ReadFile(telegramChatConversationFile(paths, chatID))
	if err != nil {
		t.Fatalf("read chat conversation failed: %v", err)
	}
	if got := telegramChatLineCount(data); got > telegramCodexChatLogMaxLines {
		t.Fatalf("line compaction failed: got=%d max=%d", got, telegramCodexChatLogMaxLines)
	}
	text := string(data)
	if !strings.Contains(text, "line-msg-499") {
		t.Fatalf("latest line should be preserved")
	}
	if strings.Contains(text, "line-msg-000") {
		t.Fatalf("oldest line should be trimmed after compaction")
	}
}

func TestAppendTelegramChatConversationCompactsByBytes(t *testing.T) {
	paths := newTelegramChatTestPaths(t)
	chatID := int64(202)
	payload := strings.Repeat("가", 1600)

	for i := 0; i < 260; i++ {
		msg := fmt.Sprintf("byte-msg-%03d %s", i, payload)
		if err := appendTelegramChatConversation(paths, chatID, "assistant", msg); err != nil {
			t.Fatalf("append chat conversation failed at %d: %v", i, err)
		}
	}

	data, err := os.ReadFile(telegramChatConversationFile(paths, chatID))
	if err != nil {
		t.Fatalf("read chat conversation failed: %v", err)
	}
	if len(data) > telegramCodexChatLogMaxBytes {
		t.Fatalf("byte compaction failed: got=%d max=%d", len(data), telegramCodexChatLogMaxBytes)
	}
	if !utf8.Valid(data) {
		t.Fatalf("chat conversation must remain valid UTF-8 after compaction")
	}
	text := string(data)
	if !strings.Contains(text, "byte-msg-259") {
		t.Fatalf("latest byte-heavy entry should be preserved")
	}
	if strings.Contains(text, "byte-msg-000") {
		t.Fatalf("oldest byte-heavy entry should be trimmed after compaction")
	}
}

func TestCompactTelegramChatConversationSanitizesInvalidUTF8(t *testing.T) {
	paths := newTelegramChatTestPaths(t)
	chatID := int64(303)
	path := telegramChatConversationFile(paths, chatID)

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir conversation dir: %v", err)
	}
	invalid := []byte{0xff, 0xfe, 'a', 'b', 'c', '\n'}
	if err := os.WriteFile(path, invalid, 0o644); err != nil {
		t.Fatalf("write invalid conversation failed: %v", err)
	}
	if err := compactTelegramChatConversationFile(path); err != nil {
		t.Fatalf("compact invalid conversation failed: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read compacted conversation failed: %v", err)
	}
	if !utf8.Valid(data) {
		t.Fatalf("compacted conversation must be valid UTF-8")
	}
	if !strings.Contains(string(data), "abc") {
		t.Fatalf("compacted conversation should preserve readable content: %q", string(data))
	}
}

func TestTelegramPRDHelpDoesNotIncludeApprove(t *testing.T) {
	if strings.Contains(telegramPRDHelp(), "/prd approve") {
		t.Fatalf("help should not include deprecated /prd approve")
	}
}
