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
	Token             string
	AllowedChatIDs    map[int64]struct{}
	PollTimeoutSec    int
	NotifyIntervalSec int
	OffsetFile        string
	BaseURL           string
	Client            *http.Client
	Out               io.Writer
	OnCommand         TelegramCommandHandler
	OnNotifyTick      TelegramNotifyHandler
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
	Chat telegramChat `json:"chat"`
	Text string       `json:"text"`
}

type telegramChat struct {
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
				_ = telegramSendMessage(ctx, client, baseURL, token, chatID, "unauthorized chat")
				continue
			}

			reply, cmdErr := opts.OnCommand(ctx, chatID, text)
			if cmdErr != nil {
				reply = "error: " + compactTelegramError(cmdErr.Error())
			}
			reply = strings.TrimSpace(reply)
			if reply == "" {
				continue
			}

			for _, chunk := range splitTelegramMessage(reply, 3500) {
				if sendErr := telegramSendMessage(ctx, client, baseURL, token, chatID, chunk); sendErr != nil {
					fmt.Fprintf(out, "[telegram] warning: sendMessage failed chat=%d: %v\n", chatID, sendErr)
					break
				}
			}
		}

		if nextOffset > offset {
			offset = nextOffset
			if err := saveTelegramOffset(opts.OffsetFile, offset); err != nil {
				fmt.Fprintf(out, "[telegram] warning: save offset failed: %v\n", err)
			}
		}
	}
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
