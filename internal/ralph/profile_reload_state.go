package ralph

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type ProfileReloadState struct {
	LastReloadAt time.Time
	ReloadCount  int
	LastSummary  string
}

func LoadProfileReloadState(paths Paths) (ProfileReloadState, error) {
	state := ProfileReloadState{}
	m, err := ReadEnvFile(paths.ProfileReloadStateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return state, fmt.Errorf("read profile reload state: %w", err)
	}

	if t := parseTime(m["LAST_RELOAD_AT"]); !t.IsZero() {
		state.LastReloadAt = t
	}
	if v, ok := parseInt(m["RELOAD_COUNT"]); ok {
		state.ReloadCount = v
	}
	state.LastSummary = m["LAST_SUMMARY"]
	return state, nil
}

func SaveProfileReloadState(paths Paths, state ProfileReloadState) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	lines := []string{
		"LAST_RELOAD_AT=" + formatTime(state.LastReloadAt),
		"RELOAD_COUNT=" + strconv.Itoa(state.ReloadCount),
		"LAST_SUMMARY=" + sanitizeEnvValue(state.LastSummary),
	}
	content := strings.Join(lines, "\n") + "\n"
	return os.WriteFile(paths.ProfileReloadStateFile, []byte(content), 0o644)
}
