package ralph

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type BusyWaitState struct {
	LastDetectedAt     time.Time
	LastSelfHealAt     time.Time
	SelfHealAttempts   int
	LastSelfHealResult string
	LastSelfHealError  string
	LastSelfHealLog    string
	LastRecoveredCount int
	LastReadyAfter     int
	LastIdleCount      int
}

type BusyWaitEvent struct {
	TimeUTC          string `json:"time_utc"`
	Type             string `json:"type"`
	IdleCount        int    `json:"idle_count"`
	LoopCount        int    `json:"loop_count"`
	ReadyBefore      int    `json:"ready_before"`
	ReadyAfter       int    `json:"ready_after"`
	InProgressBefore int    `json:"in_progress_before"`
	RecoveredCount   int    `json:"recovered_count"`
	SelfHealAttempt  int    `json:"self_heal_attempt"`
	SelfHealApplied  bool   `json:"self_heal_applied"`
	Result           string `json:"result,omitempty"`
	Error            string `json:"error,omitempty"`
	LogFile          string `json:"log_file,omitempty"`
	Detail           string `json:"detail,omitempty"`
}

func LoadBusyWaitState(paths Paths) (BusyWaitState, error) {
	state := BusyWaitState{}
	m, err := ReadEnvFile(paths.BusyWaitStateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return state, fmt.Errorf("read busywait state: %w", err)
	}

	if t := parseTime(m["LAST_DETECTED_AT"]); !t.IsZero() {
		state.LastDetectedAt = t
	}
	if t := parseTime(m["LAST_SELF_HEAL_AT"]); !t.IsZero() {
		state.LastSelfHealAt = t
	}
	if v, ok := parseInt(m["SELF_HEAL_ATTEMPTS"]); ok {
		state.SelfHealAttempts = v
	}
	if v, ok := parseInt(m["LAST_RECOVERED_COUNT"]); ok {
		state.LastRecoveredCount = v
	}
	if v, ok := parseInt(m["LAST_READY_AFTER"]); ok {
		state.LastReadyAfter = v
	}
	if v, ok := parseInt(m["LAST_IDLE_COUNT"]); ok {
		state.LastIdleCount = v
	}
	state.LastSelfHealResult = m["LAST_SELF_HEAL_RESULT"]
	state.LastSelfHealError = m["LAST_SELF_HEAL_ERROR"]
	state.LastSelfHealLog = m["LAST_SELF_HEAL_LOG"]

	return state, nil
}

func SaveBusyWaitState(paths Paths, state BusyWaitState) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	lines := []string{
		"LAST_DETECTED_AT=" + formatTime(state.LastDetectedAt),
		"LAST_SELF_HEAL_AT=" + formatTime(state.LastSelfHealAt),
		"SELF_HEAL_ATTEMPTS=" + strconv.Itoa(state.SelfHealAttempts),
		"LAST_SELF_HEAL_RESULT=" + sanitizeEnvValue(state.LastSelfHealResult),
		"LAST_SELF_HEAL_ERROR=" + sanitizeEnvValue(state.LastSelfHealError),
		"LAST_SELF_HEAL_LOG=" + sanitizeEnvValue(state.LastSelfHealLog),
		"LAST_RECOVERED_COUNT=" + strconv.Itoa(state.LastRecoveredCount),
		"LAST_READY_AFTER=" + strconv.Itoa(state.LastReadyAfter),
		"LAST_IDLE_COUNT=" + strconv.Itoa(state.LastIdleCount),
	}
	content := strings.Join(lines, "\n") + "\n"
	return os.WriteFile(paths.BusyWaitStateFile, []byte(content), 0o644)
}

func AppendBusyWaitEvent(paths Paths, event BusyWaitEvent) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	if event.TimeUTC == "" {
		event.TimeUTC = time.Now().UTC().Format(time.RFC3339)
	}
	b, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal busywait event: %w", err)
	}
	f, err := os.OpenFile(paths.BusyWaitEventsFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open busywait events file: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("append busywait event: %w", err)
	}
	return nil
}

func sanitizeEnvValue(v string) string {
	return strings.ReplaceAll(v, "\n", " ")
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func parseTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return t
}
