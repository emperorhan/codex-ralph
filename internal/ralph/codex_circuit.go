package ralph

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type CodexCircuitState struct {
	ConsecutiveFailures int
	OpenUntil           time.Time
	LastFailure         string
	LastOpenedAt        time.Time
	LastSuccessAt       time.Time
}

func LoadCodexCircuitState(paths Paths) (CodexCircuitState, error) {
	state := CodexCircuitState{}
	m, err := ReadEnvFile(paths.CodexCircuitStateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return state, fmt.Errorf("read codex circuit state: %w", err)
	}
	if v, ok := parseInt(m["CONSECUTIVE_FAILURES"]); ok {
		state.ConsecutiveFailures = v
	}
	if t := parseTime(m["OPEN_UNTIL"]); !t.IsZero() {
		state.OpenUntil = t
	}
	if t := parseTime(m["LAST_OPENED_AT"]); !t.IsZero() {
		state.LastOpenedAt = t
	}
	if t := parseTime(m["LAST_SUCCESS_AT"]); !t.IsZero() {
		state.LastSuccessAt = t
	}
	state.LastFailure = strings.TrimSpace(m["LAST_FAILURE"])
	return state, nil
}

func SaveCodexCircuitState(paths Paths, state CodexCircuitState) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	lines := []string{
		"CONSECUTIVE_FAILURES=" + strconv.Itoa(maxInt(state.ConsecutiveFailures, 0)),
		"OPEN_UNTIL=" + formatTime(state.OpenUntil),
		"LAST_FAILURE=" + sanitizeEnvValue(state.LastFailure),
		"LAST_OPENED_AT=" + formatTime(state.LastOpenedAt),
		"LAST_SUCCESS_AT=" + formatTime(state.LastSuccessAt),
	}
	content := strings.Join(lines, "\n") + "\n"
	return os.WriteFile(paths.CodexCircuitStateFile, []byte(content), 0o644)
}

func (s CodexCircuitState) IsOpen(now time.Time) bool {
	if s.OpenUntil.IsZero() {
		return false
	}
	return now.Before(s.OpenUntil)
}

func maxInt(v, min int) int {
	if v < min {
		return min
	}
	return v
}
