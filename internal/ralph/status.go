package ralph

import (
	"fmt"
	"io"
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
}

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
	}, nil
}

func (s Status) Print(w io.Writer) {
	fmt.Fprintln(w, "## Ralph Status")
	fmt.Fprintf(w, "- updated_utc: %s\n", s.UpdatedUTC.Format(time.RFC3339))
	fmt.Fprintf(w, "- project: %s\n", s.ProjectDir)
	fmt.Fprintf(w, "- plugin: %s\n", s.PluginName)
	fmt.Fprintf(w, "- enabled: %t\n", s.Enabled)
	fmt.Fprintf(w, "- daemon: %s\n", s.Daemon)
	if len(s.DaemonRoles) > 0 {
		fmt.Fprintf(w, "- daemon_roles: %s\n", strings.Join(s.DaemonRoles, ","))
	}
	fmt.Fprintf(w, "- queue_ready: %d\n", s.QueueReady)
	fmt.Fprintf(w, "- in_progress: %d\n", s.InProgress)
	fmt.Fprintf(w, "- done: %d\n", s.Done)
	fmt.Fprintf(w, "- blocked: %d\n", s.Blocked)
	fmt.Fprintf(w, "- next_ready: %s\n", s.NextReady)
	if s.LastBusyWaitDetectedAt != "" {
		fmt.Fprintf(w, "- last_busywait_detected_at: %s\n", s.LastBusyWaitDetectedAt)
		fmt.Fprintf(w, "- last_busywait_idle_count: %d\n", s.LastBusyWaitIdleCount)
	}
	if s.LastSelfHealAt != "" {
		fmt.Fprintf(w, "- last_self_heal_at: %s\n", s.LastSelfHealAt)
	}
	if s.SelfHealAttempts > 0 {
		fmt.Fprintf(w, "- self_heal_attempts: %d\n", s.SelfHealAttempts)
	}
	if s.LastSelfHealResult != "" {
		fmt.Fprintf(w, "- last_self_heal_result: %s\n", s.LastSelfHealResult)
	}
	if s.LastSelfHealError != "" {
		fmt.Fprintf(w, "- last_self_heal_error: %s\n", s.LastSelfHealError)
	}
}
