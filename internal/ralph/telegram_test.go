package ralph

import "testing"

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
