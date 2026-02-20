package ralph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultTelegramBaseURL = "https://api.telegram.org"

type TelegramCommandHandler func(ctx context.Context, chatID int64, text string) (string, error)
type TelegramNotifyHandler func(ctx context.Context) ([]string, error)

type TelegramBotOptions struct {
	Token              string
	AllowedChatIDs     map[int64]struct{}
	AllowedUserIDs     map[int64]struct{}
	PollTimeoutSec     int
	NotifyIntervalSec  int
	CommandTimeoutSec  int
	CommandConcurrency int
	OffsetFile         string
	BaseURL            string
	Client             *http.Client
	Out                io.Writer
	OnCommand          TelegramCommandHandler
	OnNotifyTick       TelegramNotifyHandler
}

type telegramGetUpdatesResponse struct {
	OK          bool             `json:"ok"`
	Description string           `json:"description,omitempty"`
	Result      []telegramUpdate `json:"result"`
}

type telegramUpdate struct {
	UpdateID int64            `json:"update_id"`
	Message  *telegramMessage `json:"message,omitempty"`
}

type telegramMessage struct {
	Chat telegramChat  `json:"chat"`
	From *telegramUser `json:"from,omitempty"`
	Text string        `json:"text"`
}

type telegramChat struct {
	ID int64 `json:"id"`
}

type telegramUser struct {
	ID int64 `json:"id"`
}

type telegramSendMessageRequest struct {
	ChatID int64  `json:"chat_id"`
	Text   string `json:"text"`
}

type telegramSendMessageResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description,omitempty"`
}

func RunTelegramBot(ctx context.Context, opts TelegramBotOptions) error {
	token := strings.TrimSpace(opts.Token)
	if token == "" {
		return fmt.Errorf("telegram token is required")
	}
	if opts.OnCommand == nil {
		return fmt.Errorf("telegram command handler is required")
	}
	if len(opts.AllowedChatIDs) == 0 {
		return fmt.Errorf("telegram allowed chat IDs are required")
	}
	pollTimeoutSec := opts.PollTimeoutSec
	if pollTimeoutSec <= 0 {
		pollTimeoutSec = 30
	}
	notifyIntervalSec := opts.NotifyIntervalSec
	if notifyIntervalSec <= 0 {
		notifyIntervalSec = 30
	}
	commandTimeoutSec := opts.CommandTimeoutSec
	if commandTimeoutSec <= 0 {
		commandTimeoutSec = 300
	}
	commandConcurrency := opts.CommandConcurrency
	if commandConcurrency <= 0 {
		commandConcurrency = 4
	}

	baseURL := strings.TrimSpace(opts.BaseURL)
	if baseURL == "" {
		baseURL = defaultTelegramBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")

	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: time.Duration(pollTimeoutSec+15) * time.Second}
	}
	out := opts.Out
	if out == nil {
		out = io.Discard
	}

	offset, err := loadTelegramOffset(opts.OffsetFile)
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "[telegram] bot started (poll_timeout=%ds, allowed_chats=%d)\n", pollTimeoutSec, len(opts.AllowedChatIDs))
	backoff := 2 * time.Second
	nextNotifyAt := time.Now().UTC()
	chatIDs := sortedTelegramChatIDs(opts.AllowedChatIDs)
	unauthorizedLogCooldown := 60 * time.Second
	lastUnauthorizedLogAt := map[string]time.Time{}
	commandSlots := make(chan struct{}, commandConcurrency)

	for {
		if err := ctx.Err(); err != nil {
			fmt.Fprintln(out, "[telegram] interrupted; stopping")
			return nil
		}

		if opts.OnNotifyTick != nil && !time.Now().UTC().Before(nextNotifyAt) {
			nextNotifyAt = time.Now().UTC().Add(time.Duration(notifyIntervalSec) * time.Second)
			messages, notifyErr := opts.OnNotifyTick(ctx)
			if notifyErr != nil {
				fmt.Fprintf(out, "[telegram] warning: notify tick failed: %v\n", notifyErr)
			} else {
				for _, msg := range messages {
					msg = strings.TrimSpace(msg)
					if msg == "" {
						continue
					}
					for _, chatID := range chatIDs {
						for _, chunk := range splitTelegramMessage(msg, 3500) {
							if sendErr := telegramSendMessage(ctx, client, baseURL, token, chatID, chunk); sendErr != nil {
								fmt.Fprintf(out, "[telegram] warning: notify send failed chat=%d: %v\n", chatID, sendErr)
								break
							}
						}
					}
				}
			}
		}

		updates, nextOffset, err := telegramGetUpdates(ctx, client, baseURL, token, offset, pollTimeoutSec)
		if err != nil {
			fmt.Fprintf(out, "[telegram] warning: getUpdates failed: %v\n", err)
			if sleepErr := sleepOrCancel(ctx, backoff); sleepErr != nil {
				return nil
			}
			if backoff < 15*time.Second {
				backoff *= 2
				if backoff > 15*time.Second {
					backoff = 15 * time.Second
				}
			}
			continue
		}
		backoff = 2 * time.Second

		for _, upd := range updates {
			if upd.Message == nil {
				continue
			}
			chatID := upd.Message.Chat.ID
			text := strings.TrimSpace(upd.Message.Text)
			if chatID == 0 || text == "" {
				continue
			}

			if !isTelegramChatAllowed(opts.AllowedChatIDs, chatID) {
				telegramLogUnauthorized(out, lastUnauthorizedLogAt, unauthorizedLogCooldown, fmt.Sprintf("chat:%d", chatID), fmt.Sprintf("chat %d is not allowed", chatID))
				continue
			}
			userID := telegramMessageUserID(upd.Message)
			if !isTelegramUserAllowed(opts.AllowedUserIDs, userID) {
				telegramLogUnauthorized(out, lastUnauthorizedLogAt, unauthorizedLogCooldown, fmt.Sprintf("user:%d:chat:%d", userID, chatID), fmt.Sprintf("user %d in chat %d is not allowed", userID, chatID))
				continue
			}

			dispatchTelegramCommand(
				ctx,
				commandSlots,
				time.Duration(commandTimeoutSec)*time.Second,
				opts.OnCommand,
				client,
				baseURL,
				token,
				chatID,
				text,
				out,
			)
		}

		if nextOffset > offset {
			offset = nextOffset
			if err := saveTelegramOffset(opts.OffsetFile, offset); err != nil {
				fmt.Fprintf(out, "[telegram] warning: save offset failed: %v\n", err)
			}
		}
	}
}

func dispatchTelegramCommand(
	ctx context.Context,
	commandSlots chan struct{},
	commandTimeout time.Duration,
	onCommand TelegramCommandHandler,
	client *http.Client,
	baseURL, token string,
	chatID int64,
	text string,
	out io.Writer,
) {
	select {
	case commandSlots <- struct{}{}:
	case <-ctx.Done():
		return
	default:
		busy := "system busy: processing previous commands. please retry in a few seconds."
		if err := telegramSendMessage(ctx, client, baseURL, token, chatID, busy); err != nil {
			fmt.Fprintf(out, "[telegram] warning: queue full and busy notice send failed chat=%d: %v\n", chatID, err)
		}
		fmt.Fprintf(out, "[telegram] warning: command queue full; dropped chat=%d text=%q\n", chatID, compactTelegramError(text))
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(out, "[telegram] warning: command panic chat=%d: %v\n", chatID, r)
			}
			<-commandSlots
		}()

		cmdCtx, cancel := context.WithTimeout(ctx, commandTimeout)
		defer cancel()

		reply, cmdErr := onCommand(cmdCtx, chatID, text)
		if cmdErr != nil {
			reply = "error: " + compactTelegramError(cmdErr.Error())
		}
		reply = strings.TrimSpace(reply)
		if reply == "" {
			return
		}

		for _, chunk := range splitTelegramMessage(reply, 3500) {
			if sendErr := telegramSendMessage(cmdCtx, client, baseURL, token, chatID, chunk); sendErr != nil {
				fmt.Fprintf(out, "[telegram] warning: sendMessage failed chat=%d: %v\n", chatID, sendErr)
				break
			}
		}
	}()
}

func sortedTelegramChatIDs(chats map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(chats))
	for chatID := range chats {
		out = append(out, chatID)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

func ParseTelegramChatIDs(raw string) (map[int64]struct{}, error) {
	out := map[int64]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		v := strings.TrimSpace(part)
		if v == "" {
			continue
		}
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid chat id %q: %w", v, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("invalid chat id %q: must not be 0", v)
		}
		out[id] = struct{}{}
	}
	return out, nil
}

func ParseTelegramUserIDs(raw string) (map[int64]struct{}, error) {
	out := map[int64]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		v := strings.TrimSpace(part)
		if v == "" {
			continue
		}
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid user id %q: %w", v, err)
		}
		if id <= 0 {
			return nil, fmt.Errorf("invalid user id %q: must be positive", v)
		}
		out[id] = struct{}{}
	}
	return out, nil
}

func isTelegramChatAllowed(allowed map[int64]struct{}, chatID int64) bool {
	if len(allowed) == 0 {
		return false
	}
	_, ok := allowed[chatID]
	return ok
}

func isTelegramUserAllowed(allowed map[int64]struct{}, userID int64) bool {
	// Backward-compatible default: if user allowlist is unset, chat allowlist is sufficient.
	if len(allowed) == 0 {
		return true
	}
	if userID <= 0 {
		return false
	}
	_, ok := allowed[userID]
	return ok
}

func telegramMessageUserID(msg *telegramMessage) int64 {
	if msg == nil || msg.From == nil {
		return 0
	}
	return msg.From.ID
}

func telegramLogUnauthorized(out io.Writer, last map[string]time.Time, cooldown time.Duration, key, detail string) {
	if out == nil {
		return
	}
	now := time.Now().UTC()
	lastAt, ok := last[key]
	if ok && now.Sub(lastAt) < cooldown {
		return
	}
	last[key] = now
	fmt.Fprintf(out, "[telegram] unauthorized access blocked: %s\n", detail)
}

func telegramGetUpdates(ctx context.Context, client *http.Client, baseURL, token string, offset int64, timeoutSec int) ([]telegramUpdate, int64, error) {
	endpoint := fmt.Sprintf("%s/bot%s/getUpdates", baseURL, token)
	values := url.Values{}
	values.Set("timeout", strconv.Itoa(timeoutSec))
	if offset > 0 {
		values.Set("offset", strconv.FormatInt(offset, 10))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+values.Encode(), nil)
	if err != nil {
		return nil, offset, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, offset, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return nil, offset, fmt.Errorf("telegram getUpdates http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload telegramGetUpdatesResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, offset, err
	}
	if !payload.OK {
		if strings.TrimSpace(payload.Description) == "" {
			return nil, offset, fmt.Errorf("telegram getUpdates failed")
		}
		return nil, offset, fmt.Errorf("telegram getUpdates failed: %s", payload.Description)
	}

	nextOffset := offset
	for _, upd := range payload.Result {
		if upd.UpdateID >= nextOffset {
			nextOffset = upd.UpdateID + 1
		}
	}
	return payload.Result, nextOffset, nil
}

func telegramSendMessage(ctx context.Context, client *http.Client, baseURL, token string, chatID int64, text string) error {
	endpoint := fmt.Sprintf("%s/bot%s/sendMessage", baseURL, token)
	reqBody := telegramSendMessageRequest{
		ChatID: chatID,
		Text:   text,
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return fmt.Errorf("telegram sendMessage http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var res telegramSendMessageResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}
	if !res.OK {
		if strings.TrimSpace(res.Description) == "" {
			return fmt.Errorf("telegram sendMessage failed")
		}
		return fmt.Errorf("telegram sendMessage failed: %s", res.Description)
	}
	return nil
}

func splitTelegramMessage(text string, maxRunes int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if maxRunes <= 0 {
		maxRunes = 3500
	}
	runes := []rune(text)
	if len(runes) <= maxRunes {
		return []string{text}
	}

	out := []string{}
	for start := 0; start < len(runes); {
		end := start + maxRunes
		if end >= len(runes) {
			out = append(out, strings.TrimSpace(string(runes[start:])))
			break
		}
		split := end
		for i := end; i > start+(maxRunes/2); i-- {
			if runes[i-1] == '\n' {
				split = i
				break
			}
		}
		chunk := strings.TrimSpace(string(runes[start:split]))
		if chunk != "" {
			out = append(out, chunk)
		}
		start = split
	}
	if len(out) == 0 {
		return []string{text}
	}
	return out
}

func loadTelegramOffset(path string) (int64, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return 0, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read telegram offset file: %w", err)
	}
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return 0, nil
	}
	offset, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse telegram offset: %w", err)
	}
	if offset < 0 {
		return 0, nil
	}
	return offset, nil
}

func saveTelegramOffset(path string, offset int64) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create telegram offset dir: %w", err)
	}
	content := strconv.FormatInt(offset, 10) + "\n"
	return os.WriteFile(path, []byte(content), 0o644)
}

func compactTelegramError(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "unknown error"
	}
	raw = strings.ReplaceAll(raw, "\n", " ")
	raw = strings.Join(strings.Fields(raw), " ")
	if len(raw) > 300 {
		return raw[:297] + "..."
	}
	return raw
}
