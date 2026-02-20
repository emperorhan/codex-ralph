package ralph

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Status struct {
	UpdatedUTC             time.Time
	ProjectDir             string
	PluginName             string
	Enabled                bool
	Daemon                 string
	DaemonRoles            []string
	QueueReady             int
	InProgress             int
	Done                   int
	Blocked                int
	NextReady              string
	LastBusyWaitDetectedAt string
	LastBusyWaitIdleCount  int
	LastSelfHealAt         string
	SelfHealAttempts       int
	LastSelfHealResult     string
	LastSelfHealError      string
	LastProfileReloadAt    string
	ProfileReloadCount     int
	LastFailureCause       string
	LastFailureUpdatedAt   string
	LastCodexRetryCount    int
	LastPermissionStreak   int
}

func IsInputRequiredStatus(s Status) bool {
	return s.QueueReady == 0 && s.InProgress == 0 && s.Blocked == 0
}

var codexAttemptHeaderPattern = regexp.MustCompile(`codex attempt [0-9]+/[0-9]+`)

func GetStatus(paths Paths) (Status, error) {
	if err := EnsureLayout(paths); err != nil {
		return Status{}, err
	}
	profile, err := LoadProfile(paths)
	if err != nil {
		return Status{}, err
	}
	enabled, err := IsEnabled(paths)
	if err != nil {
		return Status{}, err
	}
	readyCount, err := CountReadyIssues(paths)
	if err != nil {
		return Status{}, err
	}
	inProgressCount, err := CountIssueFiles(paths.InProgressDir)
	if err != nil {
		return Status{}, err
	}
	doneCount, err := CountIssueFiles(paths.DoneDir)
	if err != nil {
		return Status{}, err
	}
	blockedCount, err := CountIssueFiles(paths.BlockedDir)
	if err != nil {
		return Status{}, err
	}
	nextIssuePath, nextMeta, err := PickNextReadyIssue(paths)
	if err != nil {
		return Status{}, err
	}
	nextReady := "none"
	if nextIssuePath != "" {
		nextReady = fmt.Sprintf("%s | %s | %s", nextMeta.ID, nextMeta.Role, nextMeta.Title)
	}

	busyState, err := LoadBusyWaitState(paths)
	if err != nil {
		return Status{}, err
	}
	profileReloadState, profileReloadErr := LoadProfileReloadState(paths)
	if profileReloadErr != nil {
		profileReloadState = ProfileReloadState{}
	}

	generalPID, generalRunning := daemonPID(paths)
	roleRunning, rolePIDs := RunningRoleDaemons(paths)
	daemon := "stopped"
	if generalRunning && len(roleRunning) > 0 {
		daemon = fmt.Sprintf("running(general_pid=%d + role_workers=%d)", generalPID, len(roleRunning))
	} else if generalRunning {
		daemon = fmt.Sprintf("running(general_pid=%d)", generalPID)
	} else if len(roleRunning) > 0 {
		daemon = fmt.Sprintf("running(role_workers=%d)", len(roleRunning))
	}

	_ = rolePIDs

	lastDetected := ""
	if !busyState.LastDetectedAt.IsZero() {
		lastDetected = busyState.LastDetectedAt.Format(time.RFC3339)
	}
	lastSelfHeal := ""
	if !busyState.LastSelfHealAt.IsZero() {
		lastSelfHeal = busyState.LastSelfHealAt.Format(time.RFC3339)
	}
	lastProfileReload := ""
	if !profileReloadState.LastReloadAt.IsZero() {
		lastProfileReload = profileReloadState.LastReloadAt.Format(time.RFC3339)
	}
	lastFailureCause, lastFailureUpdatedAt, lastFailureLog := latestBlockedFailure(paths.BlockedDir)
	lastCodexRetryCount := 0
	if strings.TrimSpace(lastFailureLog) != "" {
		lastCodexRetryCount = codexRetryCountFromLog(lastFailureLog)
	}
	lastPermissionStreak, lastPermissionErr := latestPermissionErrorSummary(paths.BusyWaitEventsFile)
	if lastFailureCause == "" && strings.TrimSpace(lastPermissionErr) != "" {
		lastFailureCause = lastPermissionErr
	}

	return Status{
		UpdatedUTC:             time.Now().UTC(),
		ProjectDir:             paths.ProjectDir,
		PluginName:             profile.PluginName,
		Enabled:                enabled,
		Daemon:                 daemon,
		DaemonRoles:            roleRunning,
		QueueReady:             readyCount,
		InProgress:             inProgressCount,
		Done:                   doneCount,
		Blocked:                blockedCount,
		NextReady:              nextReady,
		LastBusyWaitDetectedAt: lastDetected,
		LastBusyWaitIdleCount:  busyState.LastIdleCount,
		LastSelfHealAt:         lastSelfHeal,
		SelfHealAttempts:       busyState.SelfHealAttempts,
		LastSelfHealResult:     busyState.LastSelfHealResult,
		LastSelfHealError:      busyState.LastSelfHealError,
		LastProfileReloadAt:    lastProfileReload,
		ProfileReloadCount:     profileReloadState.ReloadCount,
		LastFailureCause:       lastFailureCause,
		LastFailureUpdatedAt:   lastFailureUpdatedAt,
		LastCodexRetryCount:    lastCodexRetryCount,
		LastPermissionStreak:   lastPermissionStreak,
	}, nil
}

func (s Status) Print(w io.Writer) {
	fmt.Fprintln(w, "Ralph Status")
	fmt.Fprintln(w, "============")
	fmt.Fprintf(w, "Updated: %s\n\n", s.UpdatedUTC.Format(time.RFC3339))

	fmt.Fprintln(w, "[Project]")
	fmt.Fprintf(w, "Path:    %s\n", s.ProjectDir)
	fmt.Fprintf(w, "Plugin:  %s\n", s.PluginName)
	fmt.Fprintf(w, "Enabled: %t\n", s.Enabled)
	fmt.Fprintf(w, "Daemon:  %s\n", s.Daemon)
	if len(s.DaemonRoles) > 0 {
		fmt.Fprintf(w, "Workers: %s\n", strings.Join(s.DaemonRoles, ","))
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "[Queue]")
	fmt.Fprintf(w, "Ready:       %d\n", s.QueueReady)
	fmt.Fprintf(w, "In Progress: %d\n", s.InProgress)
	fmt.Fprintf(w, "Done:        %d\n", s.Done)
	fmt.Fprintf(w, "Blocked:     %d\n", s.Blocked)
	fmt.Fprintf(w, "Next:        %s\n", s.NextReady)
	if IsInputRequiredStatus(s) {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "[Input Required]")
		fmt.Fprintln(w, "No queued work detected.")
		fmt.Fprintln(w, "Next actions:")
		fmt.Fprintln(w, "  - ./ralph new developer \"<title>\"")
		fmt.Fprintln(w, "  - ./ralph import-prd --file prd.json")
	}
	fmt.Fprintln(w)

	fmt.Fprintln(w, "[Runtime]")
	if s.LastBusyWaitDetectedAt != "" {
		fmt.Fprintf(w, "Busywait Detected At: %s\n", s.LastBusyWaitDetectedAt)
		fmt.Fprintf(w, "Busywait Idle Count:  %d\n", s.LastBusyWaitIdleCount)
	}
	if s.LastSelfHealAt != "" {
		fmt.Fprintf(w, "Last Self Heal At:    %s\n", s.LastSelfHealAt)
	}
	if s.SelfHealAttempts > 0 {
		fmt.Fprintf(w, "Self Heal Attempts:   %d\n", s.SelfHealAttempts)
	}
	if s.LastSelfHealResult != "" {
		fmt.Fprintf(w, "Last Self Heal:       %s\n", s.LastSelfHealResult)
	}
	if s.LastSelfHealError != "" {
		fmt.Fprintf(w, "Last Self Heal Error: %s\n", s.LastSelfHealError)
	}
	if s.LastProfileReloadAt != "" {
		fmt.Fprintf(w, "Profile Reload At:    %s\n", s.LastProfileReloadAt)
	}
	if s.ProfileReloadCount > 0 {
		fmt.Fprintf(w, "Profile Reload Count: %d\n", s.ProfileReloadCount)
	}
	if s.LastFailureCause != "" {
		fmt.Fprintf(w, "Last Failure Cause:   %s\n", s.LastFailureCause)
	}
	if s.LastFailureUpdatedAt != "" {
		fmt.Fprintf(w, "Last Failure At:      %s\n", s.LastFailureUpdatedAt)
	}
	if s.LastCodexRetryCount > 0 {
		fmt.Fprintf(w, "Codex Retries:        %d\n", s.LastCodexRetryCount)
	}
	if s.LastPermissionStreak > 0 {
		fmt.Fprintf(w, "Permission Streak:    %d\n", s.LastPermissionStreak)
	}
}

func latestBlockedFailure(blockedDir string) (string, string, string) {
	files, err := filepath.Glob(filepath.Join(blockedDir, "I-*.md"))
	if err != nil || len(files) == 0 {
		return "", "", ""
	}
	type candidate struct {
		path    string
		modTime time.Time
	}
	candidates := make([]candidate, 0, len(files))
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}
		candidates = append(candidates, candidate{path: file, modTime: info.ModTime()})
	}
	if len(candidates) == 0 {
		return "", "", ""
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].modTime.After(candidates[j].modTime)
	})
	data, err := os.ReadFile(candidates[0].path)
	if err != nil {
		return "", "", ""
	}
	reason := ""
	updatedAt := ""
	logFile := ""
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "- reason:"):
			reason = strings.TrimSpace(strings.TrimPrefix(trimmed, "- reason:"))
		case strings.HasPrefix(trimmed, "- updated_at_utc:"):
			updatedAt = strings.TrimSpace(strings.TrimPrefix(trimmed, "- updated_at_utc:"))
		case strings.HasPrefix(trimmed, "- log_file:"):
			logFile = strings.TrimSpace(strings.TrimPrefix(trimmed, "- log_file:"))
		}
	}
	return reason, updatedAt, logFile
}

func codexRetryCountFromLog(logPath string) int {
	f, err := os.Open(logPath)
	if err != nil {
		return 0
	}
	defer f.Close()

	attemptHeaders := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if codexAttemptHeaderPattern.MatchString(line) {
			attemptHeaders++
		}
	}
	if attemptHeaders <= 1 {
		return 0
	}
	return attemptHeaders - 1
}

func latestPermissionErrorSummary(eventsPath string) (int, string) {
	data, err := os.ReadFile(eventsPath)
	if err != nil {
		return 0, ""
	}
	lines := strings.Split(string(data), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		ev := BusyWaitEvent{}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if ev.Type != "process_permission_error" {
			continue
		}
		streak := parseDetailInt(ev.Detail, "streak")
		if streak <= 0 {
			streak = 1
		}
		return streak, strings.TrimSpace(ev.Error)
	}
	return 0, ""
}

func parseDetailInt(detail, key string) int {
	key = strings.TrimSpace(key)
	if key == "" {
		return 0
	}
	parts := strings.Split(detail, ";")
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		if strings.TrimSpace(kv[0]) != key {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}
