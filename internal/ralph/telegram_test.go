package ralph

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

func TestDispatchTelegramCommandQueueFullSendsBusyMessage(t *testing.T) {
	t.Parallel()

	requests := make(chan telegramSendMessageRequest, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req telegramSendMessageRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		requests <- req
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slots := make(chan struct{}, 1)
	slots <- struct{}{} // force queue-full path

	var handlerCalled int32
	dispatchTelegramCommand(
		ctx,
		slots,
		2*time.Second,
		func(ctx context.Context, chatID int64, text string) (string, error) {
			atomic.AddInt32(&handlerCalled, 1)
			return "pong", nil
		},
		server.Client(),
		server.URL,
		"token",
		123,
		"/ping",
		io.Discard,
	)

	select {
	case req := <-requests:
		if req.ChatID != 123 {
			t.Fatalf("busy notice chat mismatch: got=%d want=123", req.ChatID)
		}
		if !strings.HasPrefix(req.Text, "system busy") {
			t.Fatalf("expected busy notice, got=%q", req.Text)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("expected busy notice to be sent")
	}

	if atomic.LoadInt32(&handlerCalled) != 0 {
		t.Fatalf("handler should not run when queue is full")
	}
}

func TestDispatchTelegramCommandAsyncExecutesHandler(t *testing.T) {
	t.Parallel()

	requests := make(chan telegramSendMessageRequest, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var req telegramSendMessageRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		requests <- req
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slots := make(chan struct{}, 1)
	dispatchTelegramCommand(
		ctx,
		slots,
		2*time.Second,
		func(ctx context.Context, chatID int64, text string) (string, error) {
			return "pong async", nil
		},
		server.Client(),
		server.URL,
		"token",
		77,
		"/ping",
		io.Discard,
	)

	select {
	case req := <-requests:
		if req.ChatID != 77 {
			t.Fatalf("reply chat mismatch: got=%d want=77", req.ChatID)
		}
		if req.Text != "pong async" {
			t.Fatalf("reply text mismatch: got=%q want=%q", req.Text, "pong async")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("expected async reply to be sent")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case slots <- struct{}{}:
			return
		default:
			if time.Now().After(deadline) {
				t.Fatalf("command slot should be released after handler completes")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
