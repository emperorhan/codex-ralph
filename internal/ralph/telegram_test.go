package ralph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestParseTelegramChatIDs(t *testing.T) {
	t.Parallel()

	m, err := ParseTelegramChatIDs("123, -456 ,789")
	if err != nil {
		t.Fatalf("parse chat ids: %v", err)
	}
	if len(m) != 3 {
		t.Fatalf("chat id count mismatch: got=%d want=3", len(m))
	}
	if _, ok := m[int64(123)]; !ok {
		t.Fatalf("chat id 123 missing")
	}
	if _, ok := m[int64(-456)]; !ok {
		t.Fatalf("chat id -456 missing")
	}
}

func TestParseTelegramChatIDsInvalid(t *testing.T) {
	t.Parallel()

	if _, err := ParseTelegramChatIDs("abc"); err == nil {
		t.Fatalf("expected parse error")
	}
	if _, err := ParseTelegramChatIDs("0"); err == nil {
		t.Fatalf("expected parse error for zero chat id")
	}
}

func TestParseTelegramUserIDs(t *testing.T) {
	t.Parallel()

	m, err := ParseTelegramUserIDs("123,456")
	if err != nil {
		t.Fatalf("parse user ids: %v", err)
	}
	if len(m) != 2 {
		t.Fatalf("user id count mismatch: got=%d want=2", len(m))
	}
	if _, ok := m[int64(123)]; !ok {
		t.Fatalf("user id 123 missing")
	}
}

func TestParseTelegramUserIDsInvalid(t *testing.T) {
	t.Parallel()

	if _, err := ParseTelegramUserIDs("-1"); err == nil {
		t.Fatalf("expected parse error")
	}
	if _, err := ParseTelegramUserIDs("abc"); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestSplitTelegramMessage(t *testing.T) {
	t.Parallel()

	msg := "line1\nline2\nline3\nline4"
	parts := splitTelegramMessage(msg, 8)
	if len(parts) < 2 {
		t.Fatalf("expected split chunks")
	}
	for _, p := range parts {
		if len([]rune(p)) > 8 {
			t.Fatalf("chunk too long: %q", p)
		}
	}
}

func TestCompactTelegramErrorUnicodeSafe(t *testing.T) {
	t.Parallel()

	raw := strings.Repeat("🔥비트코인 자동화 ", 40)
	got := compactTelegramError(raw)
	if !utf8.ValidString(got) {
		t.Fatalf("output must be valid UTF-8: %q", got)
	}
	if len([]rune(got)) > 300 {
		t.Fatalf("output should be capped at 300 runes: %d", len([]rune(got)))
	}
}

func TestCompactTelegramErrorInvalidUTF8Sanitized(t *testing.T) {
	t.Parallel()

	raw := string([]byte{0xff, 0xfe, 'a', 'b', 'c'})
	got := compactTelegramError(raw)
	if !utf8.ValidString(got) {
		t.Fatalf("output must be valid UTF-8: %q", got)
	}
	if !strings.Contains(got, "abc") {
		t.Fatalf("sanitized output should preserve readable content: %q", got)
	}
}

func TestIsTelegramChatAllowed(t *testing.T) {
	t.Parallel()

	allowed := map[int64]struct{}{42: {}}
	if !isTelegramChatAllowed(allowed, 42) {
		t.Fatalf("expected allowed chat")
	}
	if isTelegramChatAllowed(allowed, 43) {
		t.Fatalf("unexpected allowed chat")
	}
}

func TestSortedTelegramChatIDs(t *testing.T) {
	t.Parallel()

	ids := sortedTelegramChatIDs(map[int64]struct{}{
		5:  {},
		-1: {},
		3:  {},
	})
	if len(ids) != 3 {
		t.Fatalf("id count mismatch: got=%d want=3", len(ids))
	}
	if ids[0] != -1 || ids[1] != 3 || ids[2] != 5 {
		t.Fatalf("sorted ids mismatch: got=%v", ids)
	}
}

func TestIsTelegramUserAllowed(t *testing.T) {
	t.Parallel()

	if !isTelegramUserAllowed(nil, 0) {
		t.Fatalf("empty allowlist should allow")
	}
	allowed := map[int64]struct{}{1001: {}}
	if !isTelegramUserAllowed(allowed, 1001) {
		t.Fatalf("expected allowed user")
	}
	if isTelegramUserAllowed(allowed, 1002) {
		t.Fatalf("unexpected allowed user")
	}
	if isTelegramUserAllowed(allowed, 0) {
		t.Fatalf("zero user id should be rejected when allowlist is set")
	}
}

func TestTelegramCommandDispatcherQueuesWithoutDrop(t *testing.T) {
	t.Parallel()

	requests := make(chan telegramSendMessageRequest, 4)
	client := newTelegramMockClient(requests)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dispatcher := newTelegramCommandDispatcher(ctx, telegramCommandDispatcherOptions{
		CommandTimeout: 3 * time.Second,
		Concurrency:    1,
		OnCommand: func(ctx context.Context, chatID int64, text string) (string, error) {
			// Force queueing under concurrency=1.
			time.Sleep(80 * time.Millisecond)
			return "ack:" + text, nil
		},
		Client:  client,
		BaseURL: "https://api.telegram.org",
		Token:   "token",
		Out:     io.Discard,
	})

	dispatcher.Submit(99, "one")
	dispatcher.Submit(99, "two")
	dispatcher.Submit(99, "three")

	got := make([]telegramSendMessageRequest, 0, 3)
	deadline := time.After(3 * time.Second)
	for len(got) < 3 {
		select {
		case req := <-requests:
			got = append(got, req)
		case <-deadline:
			t.Fatalf("expected 3 replies, got=%d", len(got))
		}
	}
	if got[0].Text != "ack:one" || got[1].Text != "ack:two" || got[2].Text != "ack:three" {
		t.Fatalf("queued order mismatch: got=%q,%q,%q", got[0].Text, got[1].Text, got[2].Text)
	}
}

func TestTelegramCommandDispatcherPerChatOrdering(t *testing.T) {
	t.Parallel()

	requests := make(chan telegramSendMessageRequest, 8)
	client := newTelegramMockClient(requests)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dispatcher := newTelegramCommandDispatcher(ctx, telegramCommandDispatcherOptions{
		CommandTimeout: 3 * time.Second,
		Concurrency:    2,
		OnCommand: func(ctx context.Context, chatID int64, text string) (string, error) {
			time.Sleep(40 * time.Millisecond)
			return fmt.Sprintf("%d:%s", chatID, text), nil
		},
		Client:  client,
		BaseURL: "https://api.telegram.org",
		Token:   "token",
		Out:     io.Discard,
	})

	dispatcher.Submit(1, "a")
	dispatcher.Submit(1, "b")
	dispatcher.Submit(2, "x")
	dispatcher.Submit(2, "y")

	gotByChat := map[int64][]string{}
	deadline := time.After(3 * time.Second)
	for len(gotByChat[1]) < 2 || len(gotByChat[2]) < 2 {
		select {
		case req := <-requests:
			gotByChat[req.ChatID] = append(gotByChat[req.ChatID], req.Text)
		case <-deadline:
			t.Fatalf("timed out waiting replies: %+v", gotByChat)
		}
	}
	if strings.Join(gotByChat[1], ",") != "1:a,1:b" {
		t.Fatalf("chat1 order mismatch: %v", gotByChat[1])
	}
	if strings.Join(gotByChat[2], ",") != "2:x,2:y" {
		t.Fatalf("chat2 order mismatch: %v", gotByChat[2])
	}
}

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTelegramMockClient(requests chan telegramSendMessageRequest) *http.Client {
	return &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			defer req.Body.Close()
			var payload telegramSendMessageRequest
			_ = json.NewDecoder(req.Body).Decode(&payload)
			requests <- payload
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			}, nil
		}),
	}
}
