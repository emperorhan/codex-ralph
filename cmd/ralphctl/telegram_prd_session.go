package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"codex-ralph/internal/ralph"
)

const (
	telegramPRDStageAwaitProduct      = "await_product"
	telegramPRDStageAwaitStoryTitle   = "await_story_title"
	telegramPRDStageAwaitStoryDesc    = "await_story_desc"
	telegramPRDStageAwaitStoryRole    = "await_story_role"
	telegramPRDStageAwaitStoryPrio    = "await_story_priority"
	telegramPRDStageAwaitProblem      = "await_problem"
	telegramPRDStageAwaitGoal         = "await_goal"
	telegramPRDStageAwaitInScope      = "await_in_scope"
	telegramPRDStageAwaitOutOfScope   = "await_out_of_scope"
	telegramPRDStageAwaitAcceptance   = "await_acceptance"
	telegramPRDStageAwaitConstraints  = "await_constraints"
	telegramPRDDefaultPriority        = 1000
	telegramPRDDefaultProductFallback = "Telegram PRD"
	telegramPRDClarityMinScore        = 80
	telegramPRDAssumedPrefix          = "[assumed]"
	telegramPRDCodexAssistTimeoutSec  = 45
)

var telegramPRDRoleOrder = []string{"manager", "planner", "developer", "qa"}

type telegramPRDStory struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Role        string `json:"role"`
	Priority    int    `json:"priority"`
}

type telegramPRDDocument struct {
	UserStories []telegramPRDStory `json:"userStories"`
}

type telegramPRDContext struct {
	Problem       string         `json:"problem,omitempty"`
	Goal          string         `json:"goal,omitempty"`
	InScope       string         `json:"in_scope,omitempty"`
	OutOfScope    string         `json:"out_of_scope,omitempty"`
	Acceptance    string         `json:"acceptance,omitempty"`
	Constraints   string         `json:"constraints,omitempty"`
	Assumptions   []string       `json:"assumptions,omitempty"`
	AgentPriority map[string]int `json:"agent_priority,omitempty"`
}

type telegramPRDSession struct {
	ChatID          int64              `json:"chat_id"`
	Stage           string             `json:"stage"`
	ProductName     string             `json:"product_name"`
	Stories         []telegramPRDStory `json:"stories"`
	Context         telegramPRDContext `json:"context,omitempty"`
	DraftTitle      string             `json:"draft_title,omitempty"`
	DraftDesc       string             `json:"draft_desc,omitempty"`
	DraftRole       string             `json:"draft_role,omitempty"`
	CodexScore      int                `json:"codex_score,omitempty"`
	CodexReady      bool               `json:"codex_ready,omitempty"`
	CodexMissing    []string           `json:"codex_missing,omitempty"`
	CodexSummary    string             `json:"codex_summary,omitempty"`
	CodexScoredAtUT string             `json:"codex_scored_at_utc,omitempty"`
	Approved        bool               `json:"approved,omitempty"`
	CreatedAtUTC    string             `json:"created_at_utc,omitempty"`
	LastUpdatedAtUT string             `json:"last_updated_at_utc,omitempty"`
}

type telegramPRDClarityStatus struct {
	Score         int
	RequiredTotal int
	RequiredReady int
	ReadyToApply  bool
	Missing       []string
	NextStage     string
	NextPrompt    string
}

type telegramPRDCodexSessionPatch struct {
	ProductName string `json:"product_name,omitempty"`
	Problem     string `json:"problem,omitempty"`
	Goal        string `json:"goal,omitempty"`
	InScope     string `json:"in_scope,omitempty"`
	OutOfScope  string `json:"out_of_scope,omitempty"`
	Acceptance  string `json:"acceptance,omitempty"`
	Constraints string `json:"constraints,omitempty"`
}

type telegramPRDCodexStoryPatch struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Role        string `json:"role,omitempty"`
	Priority    int    `json:"priority,omitempty"`
}

type telegramPRDCodexTurnResponse struct {
	Reply          string                       `json:"reply"`
	NextQuestion   string                       `json:"next_question"`
	SuggestedStage string                       `json:"suggested_stage"`
	ReadyToApply   bool                         `json:"ready_to_apply"`
	SessionPatch   telegramPRDCodexSessionPatch `json:"session_patch"`
	Story          *telegramPRDCodexStoryPatch  `json:"story,omitempty"`
}

type telegramPRDCodexScoreResponse struct {
	Score        int      `json:"score"`
	ReadyToApply bool     `json:"ready_to_apply"`
	Missing      []string `json:"missing"`
	Summary      string   `json:"summary"`
}

type telegramPRDCodexStoryPriorityResponse struct {
	Priority int    `json:"priority"`
	Reason   string `json:"reason"`
}

type telegramPRDCodexRefineResponse struct {
	Score          int      `json:"score"`
	ReadyToApply   bool     `json:"ready_to_apply"`
	Ask            string   `json:"ask"`
	Missing        []string `json:"missing"`
	SuggestedStage string   `json:"suggested_stage"`
	Reason         string   `json:"reason"`
}

type telegramPRDSessionStore struct {
	Sessions map[string]telegramPRDSession `json:"sessions"`
}

var telegramPRDSessionStoreMu sync.Mutex
var telegramPRDTurnAnalyzer = analyzeTelegramPRDTurnWithCodex
var telegramPRDStoryPriorityEstimator = estimateTelegramPRDStoryPriorityWithCodex
var telegramPRDRefineAnalyzer = analyzeTelegramPRDRefineWithCodex
var telegramPRDScoreAnalyzer = analyzeTelegramPRDScoreWithCodex

func telegramPRDCommand(paths ralph.Paths, chatID int64, rawArgs string) (string, error) {
	fields := strings.Fields(strings.TrimSpace(rawArgs))
	if len(fields) == 0 {
		return telegramPRDHelp(), nil
	}
	sub := strings.ToLower(strings.TrimSpace(fields[0]))
	arg := strings.TrimSpace(strings.Join(fields[1:], " "))

	var (
		reply string
		err   error
	)
	switch sub {
	case "help":
		return telegramPRDHelp(), nil
	case "start":
		reply, err = telegramPRDStartSession(paths, chatID, arg)
	case "refine":
		reply, err = telegramPRDRefineSession(paths, chatID)
	case "score":
		reply, err = telegramPRDScoreSession(paths, chatID)
	case "preview", "status":
		reply, err = telegramPRDPreviewSession(paths, chatID)
	case "priority":
		reply, err = telegramPRDPrioritySession(paths, chatID, arg)
	case "save":
		reply, err = telegramPRDSaveSession(paths, chatID, arg)
	case "apply":
		reply, err = telegramPRDApplySession(paths, chatID, arg)
	case "cancel", "stop":
		reply, err = telegramPRDCancelSession(paths, chatID)
	default:
		return "unknown /prd subcommand\n\n" + telegramPRDHelp(), nil
	}
	if err != nil {
		return "", err
	}
	commandText := "/prd " + sub
	if strings.TrimSpace(arg) != "" {
		commandText += " " + strings.TrimSpace(arg)
	}
	logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "user", commandText))
	logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "assistant", reply))
	return reply, nil
}

func telegramPRDHelp() string {
	return strings.Join([]string{
		"Ralph PRD Wizard",
		"================",
		"",
		"Commands",
		"- /prd start [product_name]",
		"- /prd refine",
		"- /prd score",
		"- /prd preview",
		"- /prd priority [manager=900 planner=950 developer=1000 qa=1100|default]",
		"- /prd save [file]",
		"- /prd apply [file]",
		"- /prd cancel",
		"",
		"Flow",
		"1) /prd start",
		"2) /prd refine (Codex가 부족한 컨텍스트를 동적으로 질문)",
		"3) (optional) /prd priority 로 에이전트별 기본 priority 조정",
		"4) answer prompts, then add stories",
		"   - 기본: title -> description -> role(선택: priority)",
		"   - 빠른 입력: title | description | role [priority]",
		"5) /prd score or /prd preview",
		"6) /prd apply",
	}, "\n")
}

func telegramPRDStartSession(paths ralph.Paths, chatID int64, productName string) (string, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	session := telegramPRDSession{
		ChatID:      chatID,
		Stage:       telegramPRDStageAwaitProduct,
		ProductName: "",
		Stories:     []telegramPRDStory{},
		Context: telegramPRDContext{
			AgentPriority: telegramPRDDefaultAgentPriorityMap(),
		},
		Approved:        false,
		CreatedAtUTC:    now,
		LastUpdatedAtUT: now,
	}
	productName = strings.TrimSpace(productName)
	if productName != "" {
		session.ProductName = productName
		session.Stage = telegramPRDStageAwaitProblem
	}
	if err := clearTelegramPRDConversation(paths, chatID); err != nil {
		return "", err
	}
	if err := telegramUpsertPRDSession(paths, session); err != nil {
		return "", err
	}
	if session.Stage == telegramPRDStageAwaitProblem {
		return fmt.Sprintf("PRD wizard started\n- product: %s\n- next: /prd refine", session.ProductName), nil
	}
	return "PRD wizard started\n- next: 제품/프로젝트 이름을 입력하세요", nil
}

func telegramPRDDefaultPriorityForRole(role string) int {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "manager":
		return 900
	case "planner":
		return 950
	case "developer":
		return 1000
	case "qa":
		return 1100
	default:
		return telegramPRDDefaultPriority
	}
}

func telegramPRDDefaultAgentPriorityMap() map[string]int {
	out := make(map[string]int, len(telegramPRDRoleOrder))
	for _, role := range telegramPRDRoleOrder {
		out[role] = telegramPRDDefaultPriorityForRole(role)
	}
	return out
}

func copyTelegramPRDAgentPriorityMap(src map[string]int) map[string]int {
	if len(src) == 0 {
		return map[string]int{}
	}
	out := make(map[string]int, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func normalizeTelegramPRDAgentPriorityMap(src map[string]int) map[string]int {
	out := telegramPRDDefaultAgentPriorityMap()
	for _, role := range telegramPRDRoleOrder {
		if src == nil {
			continue
		}
		if v := src[role]; v > 0 {
			out[role] = v
		}
	}
	return out
}

func formatTelegramPRDAgentPriorityInline(priorityMap map[string]int) string {
	normalized := normalizeTelegramPRDAgentPriorityMap(priorityMap)
	parts := make([]string, 0, len(telegramPRDRoleOrder))
	for _, role := range telegramPRDRoleOrder {
		parts = append(parts, fmt.Sprintf("%s=%d", role, normalized[role]))
	}
	return strings.Join(parts, " ")
}

func parseTelegramPRDAgentPriorityArgs(raw string) (map[string]int, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return nil, fmt.Errorf("usage: /prd priority manager=900 planner=950 developer=1000 qa=1100")
	}
	text = strings.ReplaceAll(text, ",", " ")
	fields := strings.Fields(text)
	out := map[string]int{}
	for _, field := range fields {
		token := strings.TrimSpace(field)
		if token == "" {
			continue
		}
		sep := ""
		if strings.Contains(token, "=") {
			sep = "="
		} else if strings.Contains(token, ":") {
			sep = ":"
		}
		if sep == "" {
			return nil, fmt.Errorf("invalid token: %q (expected role=priority)", token)
		}
		parts := strings.SplitN(token, sep, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid token: %q", token)
		}
		role := strings.ToLower(strings.TrimSpace(parts[0]))
		if !ralph.IsSupportedRole(role) {
			return nil, fmt.Errorf("invalid role: %q", role)
		}
		n, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid priority for %s: %q", role, parts[1])
		}
		out[role] = n
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one role priority is required")
	}
	return out, nil
}

func telegramPRDPrioritySession(paths ralph.Paths, chatID int64, raw string) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}

	current := normalizeTelegramPRDAgentPriorityMap(session.Context.AgentPriority)
	arg := strings.TrimSpace(raw)
	if arg == "" {
		return strings.Join([]string{
			"agent priority profile",
			fmt.Sprintf("- current: %s", formatTelegramPRDAgentPriorityInline(current)),
			"- update: /prd priority manager=900 planner=950 developer=1000 qa=1100",
			"- reset: /prd priority default",
		}, "\n"), nil
	}

	if strings.EqualFold(arg, "default") || strings.EqualFold(arg, "reset") {
		session.Context.AgentPriority = telegramPRDDefaultAgentPriorityMap()
		session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
		if err := telegramUpsertPRDSession(paths, session); err != nil {
			return "", err
		}
		return fmt.Sprintf("agent priorities reset\n- current: %s", formatTelegramPRDAgentPriorityInline(session.Context.AgentPriority)), nil
	}

	updates, err := parseTelegramPRDAgentPriorityArgs(arg)
	if err != nil {
		return "", err
	}
	merged := copyTelegramPRDAgentPriorityMap(current)
	for role, priority := range updates {
		merged[role] = priority
	}
	session.Context.AgentPriority = normalizeTelegramPRDAgentPriorityMap(merged)
	session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
	if err := telegramUpsertPRDSession(paths, session); err != nil {
		return "", err
	}
	return fmt.Sprintf("agent priorities updated\n- current: %s", formatTelegramPRDAgentPriorityInline(session.Context.AgentPriority)), nil
}

func telegramPRDStoryPriorityForRole(session telegramPRDSession, role string) int {
	role = strings.ToLower(strings.TrimSpace(role))
	if v := session.Context.AgentPriority[role]; v > 0 {
		return v
	}
	return telegramPRDDefaultPriorityForRole(role)
}

func resolveTelegramPRDStoryPriority(paths ralph.Paths, session telegramPRDSession, story telegramPRDStory) (int, string) {
	fallback := telegramPRDStoryPriorityForRole(session, story.Role)
	priority, source, err := telegramPRDStoryPriorityEstimator(paths, session, story)
	if err != nil || priority <= 0 {
		return fallback, "fallback_role_profile"
	}
	return priority, source
}

func telegramPRDRefineSession(paths ralph.Paths, chatID int64) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}
	session, codexRefine, usedCodexRefine, codexRefineErr := refreshTelegramPRDRefineWithCodex(paths, session)
	if usedCodexRefine && codexRefineErr == nil {
		if codexRefine.ReadyToApply {
			session.Stage = telegramPRDStageAwaitStoryTitle
		} else if stage, ok := normalizeTelegramPRDRefineSuggestedStage(codexRefine.SuggestedStage); ok {
			session.Stage = stage
		} else {
			status := evaluateTelegramPRDClarity(session)
			if status.NextStage != "" {
				session.Stage = status.NextStage
			}
		}
		session.Approved = false
		session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
		if err := telegramUpsertPRDSession(paths, session); err != nil {
			return "", err
		}
		return formatTelegramPRDCodexRefineQuestion(codexRefine), nil
	}

	status := evaluateTelegramPRDClarity(session)
	if codexRefineErr != nil {
		fmt.Fprintf(os.Stderr, "[telegram] prd refine codex fallback: %v\n", codexRefineErr)
	}
	return formatTelegramPRDRefineUnavailable(session.Stage, status.Score, codexRefineErr), nil
}

func telegramPRDScoreSession(paths ralph.Paths, chatID int64) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}

	updated, usedCodex, scoreErr := refreshTelegramPRDScoreWithCodex(paths, session)
	if scoreErr == nil && usedCodex {
		if err := telegramUpsertPRDSession(paths, updated); err != nil {
			return "", err
		}
		return formatTelegramPRDCodexScore(updated), nil
	}
	category, detail := classifyTelegramCodexFailure(scoreErr)
	lines := []string{
		"prd score unavailable",
		"- scoring_mode: codex_unavailable",
		"- reason: codex scoring 실패",
		"- next: codex 상태 복구 후 `/prd score` 재시도",
	}
	if category != "" {
		lines = append(lines, "- codex_error: "+category)
	}
	if detail != "" {
		lines = append(lines, "- codex_detail: "+detail)
	}
	return strings.Join(lines, "\n"), nil
}

func telegramPRDPreviewSession(paths ralph.Paths, chatID int64) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "no active PRD session\n- run: /prd start", nil
	}
	var b strings.Builder
	clarity := evaluateTelegramPRDClarity(session)
	displayScore := clarity.Score
	displayReady := clarity.ReadyToApply
	displayMissing := clarity.Missing
	scoringMode := "heuristic"
	if session.CodexScore > 0 || session.CodexScoredAtUT != "" {
		displayScore = session.CodexScore
		displayReady = session.CodexReady
		if len(session.CodexMissing) > 0 {
			displayMissing = session.CodexMissing
		}
		scoringMode = "codex"
	}
	fmt.Fprintln(&b, "PRD session")
	fmt.Fprintf(&b, "- product: %s\n", valueOrDash(strings.TrimSpace(session.ProductName)))
	fmt.Fprintf(&b, "- stage: %s\n", session.Stage)
	fmt.Fprintf(&b, "- clarity_score: %d/100\n", displayScore)
	fmt.Fprintf(&b, "- clarity_gate: %d\n", telegramPRDClarityMinScore)
	fmt.Fprintf(&b, "- scoring_mode: %s\n", scoringMode)
	if displayReady {
		fmt.Fprintf(&b, "- clarity_status: ready\n")
	} else {
		fmt.Fprintf(&b, "- clarity_status: needs_input (%d/%d required)\n", clarity.RequiredReady, clarity.RequiredTotal)
	}
	fmt.Fprintf(&b, "- stories: %d\n", len(session.Stories))
	if strings.TrimSpace(session.Context.Problem) != "" {
		fmt.Fprintf(&b, "- problem: %s\n", compactSingleLine(session.Context.Problem, 120))
	}
	if strings.TrimSpace(session.Context.Goal) != "" {
		fmt.Fprintf(&b, "- goal: %s\n", compactSingleLine(session.Context.Goal, 120))
	}
	if strings.TrimSpace(session.Context.InScope) != "" {
		fmt.Fprintf(&b, "- in_scope: %s\n", compactSingleLine(session.Context.InScope, 120))
	}
	if strings.TrimSpace(session.Context.OutOfScope) != "" {
		fmt.Fprintf(&b, "- out_of_scope: %s\n", compactSingleLine(session.Context.OutOfScope, 120))
	}
	if strings.TrimSpace(session.Context.Acceptance) != "" {
		fmt.Fprintf(&b, "- acceptance: %s\n", compactSingleLine(session.Context.Acceptance, 120))
	}
	if strings.TrimSpace(session.Context.Constraints) != "" {
		fmt.Fprintf(&b, "- constraints: %s\n", compactSingleLine(session.Context.Constraints, 120))
	}
	fmt.Fprintf(&b, "- agent_priorities: %s\n", formatTelegramPRDAgentPriorityInline(session.Context.AgentPriority))
	if len(session.Context.Assumptions) > 0 {
		fmt.Fprintf(&b, "- assumptions: %d\n", len(session.Context.Assumptions))
	}
	maxRows := len(session.Stories)
	if maxRows > 10 {
		maxRows = 10
	}
	for i := 0; i < maxRows; i++ {
		s := session.Stories[i]
		fmt.Fprintf(&b, "- [%d] %s | role=%s | priority=%d\n", i+1, compactSingleLine(s.Title, 70), s.Role, s.Priority)
	}
	if len(session.Stories) > maxRows {
		fmt.Fprintf(&b, "- ... and %d more\n", len(session.Stories)-maxRows)
	}
	if len(displayMissing) > 0 {
		fmt.Fprintln(&b, "- missing:")
		for i, m := range displayMissing {
			if i >= 5 {
				fmt.Fprintf(&b, "  - ... and %d more\n", len(displayMissing)-i)
				break
			}
			fmt.Fprintf(&b, "  - %s\n", m)
		}
	}
	fmt.Fprintf(&b, "- next: %s\n", telegramPRDStagePrompt(session.Stage))
	return b.String(), nil
}

func telegramPRDSaveSession(paths ralph.Paths, chatID int64, rawPath string) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("no active PRD session (run: /prd start)")
	}
	if len(session.Stories) == 0 {
		return "", fmt.Errorf("no stories in session yet")
	}
	targetPath, err := resolveTelegramPRDFilePath(paths, chatID, rawPath)
	if err != nil {
		return "", err
	}
	if err := writeTelegramPRDFile(targetPath, session); err != nil {
		return "", err
	}
	return fmt.Sprintf("prd saved\n- file: %s\n- stories: %d", targetPath, len(session.Stories)), nil
}

func telegramPRDApplySession(paths ralph.Paths, chatID int64, rawPath string) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("no active PRD session (run: /prd start)")
	}
	if len(session.Stories) == 0 {
		return "", fmt.Errorf("no stories in session yet")
	}

	// Prefer codex-based scoring when available.
	sessionForGate, usedCodexGate, codexScoreErr := refreshTelegramPRDScoreWithCodex(paths, session)
	if codexScoreErr == nil && usedCodexGate {
		session = sessionForGate
		if err := telegramUpsertPRDSession(paths, session); err != nil {
			return "", err
		}
	}

	if codexScoreErr != nil {
		category, detail := classifyTelegramCodexFailure(codexScoreErr)
		lines := []string{
			"prd apply blocked",
			"- scoring_mode: codex_unavailable",
			"- reason: codex scoring 실패로 apply gate 판단 불가",
			"- next: codex 상태 복구 후 `/prd score` 또는 `/prd refine` 재시도",
		}
		if category != "" {
			lines = append(lines, "- codex_error: "+category)
		}
		if detail != "" {
			lines = append(lines, "- codex_detail: "+detail)
		}
		return strings.Join(lines, "\n"), nil
	}

	readyToApply := session.CodexReady && session.CodexScore >= telegramPRDClarityMinScore
	scoreForReply := session.CodexScore
	missingForReply := append([]string(nil), session.CodexMissing...)
	if !usedCodexGate && codexScoreErr == nil {
		readyToApply = false
	}
	if !readyToApply {
		missingPreview := "-"
		if len(missingForReply) > 0 {
			missingPreview = compactSingleLine(strings.Join(missingForReply, ", "), 180)
		}
		return strings.Join([]string{
			"prd apply blocked",
			fmt.Sprintf("- clarity_score: %d/100", scoreForReply),
			fmt.Sprintf("- clarity_gate: %d", telegramPRDClarityMinScore),
			"- scoring_mode: codex",
			"- reason: missing required context",
			fmt.Sprintf("- missing: %s", missingPreview),
			"- next: /prd refine",
		}, "\n"), nil
	}
	targetPath, err := resolveTelegramPRDFilePath(paths, chatID, rawPath)
	if err != nil {
		return "", err
	}
	if err := writeTelegramPRDFile(targetPath, session); err != nil {
		return "", err
	}
	result, err := ralph.ImportPRDStories(paths, targetPath, "developer", false)
	if err != nil {
		return "", err
	}
	if err := telegramDeletePRDSession(paths, chatID); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"prd applied\n- file: %s\n- stories_total: %d\n- imported: %d\n- skipped_existing: %d\n- skipped_invalid: %d\n- clarity_score: %d/100\n- next: /status",
		targetPath,
		result.StoriesTotal,
		result.Imported,
		result.SkippedExisting,
		result.SkippedInvalid,
		scoreForReply,
	), nil
}

func telegramPRDCancelSession(paths ralph.Paths, chatID int64) (string, error) {
	if err := telegramDeletePRDSession(paths, chatID); err != nil {
		return "", err
	}
	logTelegramPRDConversationWarning(clearTelegramPRDConversation(paths, chatID))
	return "PRD session canceled", nil
}

func telegramPRDHandleInput(paths ralph.Paths, chatID int64, input string) (string, error) {
	session, found, err := telegramLoadPRDSession(paths, chatID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("no active PRD session")
	}

	updatedByTurn, turnReply, turnHandled, turnErr := telegramPRDHandleInputWithCodex(paths, session, input)
	if turnErr != nil {
		fmt.Fprintf(os.Stderr, "[telegram] prd codex turn fallback: %v\n", turnErr)
	}
	if turnHandled {
		if err := telegramUpsertPRDSession(paths, updatedByTurn); err != nil {
			return "", err
		}
		logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "user", input))
		logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "assistant", turnReply))
		return turnReply, nil
	}

	updated, reply, err := advanceTelegramPRDSession(paths, session, input)
	if err != nil {
		return "", err
	}
	if err := telegramUpsertPRDSession(paths, updated); err != nil {
		return "", err
	}
	logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "user", input))
	logTelegramPRDConversationWarning(appendTelegramPRDConversation(paths, chatID, "assistant", reply))
	return reply, nil
}

func telegramPRDHandleInputWithCodex(paths ralph.Paths, session telegramPRDSession, input string) (telegramPRDSession, string, bool, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return session, "", false, nil
	}
	turn, err := telegramPRDTurnAnalyzer(paths, session, input)
	if err != nil {
		return session, "", false, err
	}
	updated, reply, handled := applyTelegramPRDCodexTurn(paths, session, turn)
	if !handled {
		return session, "", false, nil
	}
	updated.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
	updated.Approved = false
	return updated, reply, true, nil
}

func applyTelegramPRDCodexTurn(paths ralph.Paths, session telegramPRDSession, turn telegramPRDCodexTurnResponse) (telegramPRDSession, string, bool) {
	updatedFields := []string{}
	appendUpdated := func(field string) {
		field = strings.TrimSpace(field)
		if field == "" {
			return
		}
		for _, existing := range updatedFields {
			if existing == field {
				return
			}
		}
		updatedFields = append(updatedFields, field)
	}

	patch := turn.SessionPatch
	productName := strings.TrimSpace(patch.ProductName)
	if productName != "" && productName != strings.TrimSpace(session.ProductName) {
		session.ProductName = productName
		appendUpdated("product")
	}
	if applyTelegramPRDContextPatch(&session.Context, "problem", &session.Context.Problem, patch.Problem, "현재 기능/운영상 pain point는 명시되지 않음") {
		appendUpdated("problem")
	}
	if applyTelegramPRDContextPatch(&session.Context, "goal", &session.Context.Goal, patch.Goal, "단기 목표는 첫 동작 가능한 자동화 루프 확보") {
		appendUpdated("goal")
	}
	if applyTelegramPRDContextPatch(&session.Context, "in_scope", &session.Context.InScope, patch.InScope, "초기 릴리즈에서는 핵심 사용자 흐름만 포함") {
		appendUpdated("in_scope")
	}
	if applyTelegramPRDContextPatch(&session.Context, "out_of_scope", &session.Context.OutOfScope, patch.OutOfScope, "대규모 리팩터/새 인프라 구축은 제외") {
		appendUpdated("out_of_scope")
	}
	if applyTelegramPRDContextPatch(&session.Context, "acceptance", &session.Context.Acceptance, patch.Acceptance, "주요 시나리오 성공 + 실패 시 복구 경로 확인") {
		appendUpdated("acceptance")
	}
	if applyTelegramPRDContextPatch(&session.Context, "constraints", &session.Context.Constraints, patch.Constraints, "시간/리소스 제약은 일반적인 단일 개발자 환경 가정") {
		appendUpdated("constraints")
	}

	storyReply := ""
	if turn.Story != nil {
		storyPatch := *turn.Story
		title := strings.TrimSpace(storyPatch.Title)
		desc := strings.TrimSpace(storyPatch.Description)
		roleInput := strings.TrimSpace(storyPatch.Role)
		if title != "" && desc != "" && roleInput != "" {
			if role, roleErr := parseTelegramPRDStoryRole(roleInput); roleErr == nil {
				story := telegramPRDStory{
					Title:       title,
					Description: desc,
					Role:        role,
					Priority:    storyPatch.Priority,
				}
				if updatedSession, addReply, addErr := telegramPRDAppendStoryFromQuick(paths, session, story); addErr == nil {
					session = updatedSession
					storyReply = addReply
					appendUpdated("story")
				}
			}
		}
	}

	if turn.ReadyToApply {
		session.Stage = telegramPRDStageAwaitStoryTitle
	} else if stage, ok := normalizeTelegramPRDRefineSuggestedStage(turn.SuggestedStage); ok {
		session.Stage = stage
	}
	if strings.TrimSpace(session.Stage) == "" {
		status := evaluateTelegramPRDClarity(session)
		if strings.TrimSpace(status.NextStage) != "" {
			session.Stage = status.NextStage
		} else {
			session.Stage = telegramPRDStageAwaitStoryTitle
		}
	}

	hasCodexSignal := len(updatedFields) > 0 ||
		strings.TrimSpace(turn.Reply) != "" ||
		strings.TrimSpace(turn.NextQuestion) != "" ||
		strings.TrimSpace(turn.SuggestedStage) != "" ||
		turn.ReadyToApply
	if !hasCodexSignal {
		return session, "", false
	}

	reply := formatTelegramPRDCodexTurnReply(session, turn, updatedFields, storyReply)
	return session, reply, true
}

func applyTelegramPRDContextPatch(ctx *telegramPRDContext, field string, dst *string, rawValue string, defaultAssumption string) bool {
	value := strings.TrimSpace(rawValue)
	if value == "" {
		return false
	}
	normalized := normalizeTelegramPRDContextAnswer(value, defaultAssumption)
	if strings.TrimSpace(*dst) == strings.TrimSpace(normalized) {
		return false
	}
	*dst = normalized
	recordTelegramPRDAssumption(ctx, field, normalized)
	return true
}

func formatTelegramPRDCodexTurnReply(session telegramPRDSession, turn telegramPRDCodexTurnResponse, updatedFields []string, storyReply string) string {
	reply := strings.TrimSpace(turn.Reply)
	nextQuestion := strings.TrimSpace(turn.NextQuestion)
	status := evaluateTelegramPRDClarity(session)

	if reply == "" && storyReply != "" && nextQuestion == "" && len(updatedFields) == 1 && updatedFields[0] == "story" {
		return storyReply
	}

	lines := []string{}
	if reply != "" {
		lines = append(lines, reply)
	}
	if len(updatedFields) > 0 {
		lines = append(lines, fmt.Sprintf("updated: %s", strings.Join(updatedFields, ", ")))
	}
	if storyReply != "" {
		lines = append(lines, storyReply)
	}
	if nextQuestion == "" && !status.ReadyToApply {
		if strings.TrimSpace(status.NextStage) != "" {
			nextQuestion = telegramPRDStagePrompt(status.NextStage)
		}
	}
	if nextQuestion != "" {
		lines = append(lines, "next question: "+nextQuestion)
	}
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n")
}

func advanceTelegramPRDSession(paths ralph.Paths, session telegramPRDSession, input string) (telegramPRDSession, string, error) {
	session.LastUpdatedAtUT = time.Now().UTC().Format(time.RFC3339)
	session.Approved = false
	input = strings.TrimSpace(input)
	if input == "" {
		return session, telegramPRDStagePrompt(session.Stage), nil
	}

	switch session.Stage {
	case telegramPRDStageAwaitProduct:
		session.ProductName = input
		status := evaluateTelegramPRDClarity(session)
		session.Stage = status.NextStage
		if session.Stage == "" {
			session.Stage = telegramPRDStageAwaitStoryTitle
		}
		return session, fmt.Sprintf("product set: %s\n- next: /prd refine", session.ProductName), nil

	case telegramPRDStageAwaitProblem:
		session.Context.Problem = normalizeTelegramPRDContextAnswer(input, "현재 기능/운영상 pain point는 명시되지 않음")
		recordTelegramPRDAssumption(&session.Context, "problem", session.Context.Problem)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitGoal:
		session.Context.Goal = normalizeTelegramPRDContextAnswer(input, "단기 목표는 첫 동작 가능한 자동화 루프 확보")
		recordTelegramPRDAssumption(&session.Context, "goal", session.Context.Goal)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitInScope:
		session.Context.InScope = normalizeTelegramPRDContextAnswer(input, "초기 릴리즈에서는 핵심 사용자 흐름만 포함")
		recordTelegramPRDAssumption(&session.Context, "in_scope", session.Context.InScope)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitOutOfScope:
		session.Context.OutOfScope = normalizeTelegramPRDContextAnswer(input, "대규모 리팩터/새 인프라 구축은 제외")
		recordTelegramPRDAssumption(&session.Context, "out_of_scope", session.Context.OutOfScope)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitAcceptance:
		session.Context.Acceptance = normalizeTelegramPRDContextAnswer(input, "주요 시나리오 성공 + 실패 시 복구 경로 확인")
		recordTelegramPRDAssumption(&session.Context, "acceptance", session.Context.Acceptance)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitConstraints:
		session.Context.Constraints = normalizeTelegramPRDContextAnswer(input, "시간/리소스 제약은 일반적인 단일 개발자 환경 가정")
		recordTelegramPRDAssumption(&session.Context, "constraints", session.Context.Constraints)
		return advanceTelegramPRDRefineFlow(paths, session)

	case telegramPRDStageAwaitStoryTitle:
		if story, quick, err := parseTelegramPRDQuickStoryInput(session, input); err != nil {
			if quick {
				return session, "", err
			}
		} else if quick {
			updated, reply, err := telegramPRDAppendStoryFromQuick(paths, session, story)
			return updated, reply, err
		}
		session.DraftTitle = input
		session.Stage = telegramPRDStageAwaitStoryDesc
		return session, "story title saved\n- next: 설명을 입력하세요 (quick: 제목 | 설명 | role [priority])", nil

	case telegramPRDStageAwaitStoryDesc:
		session.DraftDesc = input
		session.Stage = telegramPRDStageAwaitStoryRole
		return session, "story description saved\n- next: role 입력 (manager|planner|developer|qa, optional: role priority)", nil

	case telegramPRDStageAwaitStoryRole:
		role, priority, explicitPriority, err := parseTelegramPRDStoryRoleAndPriorityInput(session, input, "")
		if err != nil {
			return session, "", err
		}
		updated, story, source, err := telegramPRDAppendStoryFromDraft(paths, session, role, priority, explicitPriority)
		if err != nil {
			return session, "", err
		}
		return updated, telegramPRDStoryAddedReply(updated, story, source), nil

	case telegramPRDStageAwaitStoryPrio:
		priority, err := parseTelegramPRDStoryPriority(input)
		if err != nil {
			return session, "", err
		}
		rawPriority := strings.TrimSpace(strings.ToLower(input))
		explicitPriority := !(rawPriority == "" || rawPriority == "default" || rawPriority == "skip")
		updated, story, source, err := telegramPRDAppendStoryFromDraft(paths, session, strings.TrimSpace(session.DraftRole), priority, explicitPriority)
		if err != nil {
			return session, "", err
		}
		return updated, telegramPRDStoryAddedReply(updated, story, source), nil

	default:
		status := evaluateTelegramPRDClarity(session)
		session.Stage = status.NextStage
		if session.Stage == "" {
			session.Stage = telegramPRDStageAwaitProduct
		}
		return session, "session stage reset\n- next: /prd refine", nil
	}
}

func advanceTelegramPRDRefineFlow(paths ralph.Paths, session telegramPRDSession) (telegramPRDSession, string, error) {
	sessionForCodex, codexRefine, usedCodexRefine, codexRefineErr := refreshTelegramPRDRefineWithCodex(paths, session)
	if usedCodexRefine && codexRefineErr == nil {
		session = sessionForCodex
		if codexRefine.ReadyToApply {
			session.Stage = telegramPRDStageAwaitStoryTitle
			return session, formatTelegramPRDCodexRefineQuestion(codexRefine), nil
		}
		if stage, ok := normalizeTelegramPRDRefineSuggestedStage(codexRefine.SuggestedStage); ok {
			session.Stage = stage
		}
		if strings.TrimSpace(session.Stage) == "" {
			session.Stage = telegramPRDStageAwaitStoryTitle
		}
		return session, formatTelegramPRDCodexRefineQuestion(codexRefine), nil
	}

	status := evaluateTelegramPRDClarity(session)
	if codexRefineErr != nil {
		fmt.Fprintf(os.Stderr, "[telegram] prd refine codex fallback: %v\n", codexRefineErr)
	}
	return session, formatTelegramPRDRefineUnavailable(session.Stage, status.Score, codexRefineErr), nil
}

func normalizeTelegramPRDContextAnswer(input, defaultAssumption string) string {
	v := strings.TrimSpace(input)
	if v == "" {
		return ""
	}
	lower := strings.ToLower(v)
	if lower == "skip" || lower == "default" || lower == "n/a" {
		return fmt.Sprintf("%s %s", telegramPRDAssumedPrefix, strings.TrimSpace(defaultAssumption))
	}
	return v
}

func recordTelegramPRDAssumption(ctx *telegramPRDContext, field, value string) {
	if ctx == nil {
		return
	}
	if !isTelegramPRDAssumedValue(value) {
		return
	}
	entry := fmt.Sprintf("%s: %s", field, strings.TrimSpace(strings.TrimPrefix(value, telegramPRDAssumedPrefix)))
	for _, existing := range ctx.Assumptions {
		if existing == entry {
			return
		}
	}
	ctx.Assumptions = append(ctx.Assumptions, entry)
}

func isTelegramPRDAssumedValue(value string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), strings.ToLower(telegramPRDAssumedPrefix))
}

func evaluateTelegramPRDClarity(session telegramPRDSession) telegramPRDClarityStatus {
	type requiredField struct {
		Label      string
		Value      string
		Stage      string
		Prompt     string
		Assumption string
	}
	required := []requiredField{
		{
			Label:      "problem statement",
			Value:      session.Context.Problem,
			Stage:      telegramPRDStageAwaitProblem,
			Prompt:     "문제 정의를 입력하세요 (왜 이 작업이 필요한가?)",
			Assumption: "skip/default 입력 시: 현재 운영 pain point 해결이 우선이라고 가정",
		},
		{
			Label:      "goal",
			Value:      session.Context.Goal,
			Stage:      telegramPRDStageAwaitGoal,
			Prompt:     "목표를 입력하세요 (완료 기준 한 줄)",
			Assumption: "skip/default 입력 시: 첫 안정 운영 가능 상태 도달로 가정",
		},
		{
			Label:      "in-scope",
			Value:      session.Context.InScope,
			Stage:      telegramPRDStageAwaitInScope,
			Prompt:     "포함 범위를 입력하세요 (이번 사이클에서 반드시 할 것)",
			Assumption: "skip/default 입력 시: 핵심 사용자 흐름 중심으로 가정",
		},
		{
			Label:      "out-of-scope",
			Value:      session.Context.OutOfScope,
			Stage:      telegramPRDStageAwaitOutOfScope,
			Prompt:     "제외 범위를 입력하세요 (이번 사이클에서 하지 않을 것)",
			Assumption: "skip/default 입력 시: 대규모 리팩터/인프라 변경 제외로 가정",
		},
		{
			Label:      "acceptance criteria",
			Value:      session.Context.Acceptance,
			Stage:      telegramPRDStageAwaitAcceptance,
			Prompt:     "수용 기준을 입력하세요 (검증 가능한 기준)",
			Assumption: "skip/default 입력 시: 핵심 시나리오 성공 + 회귀 없음으로 가정",
		},
	}

	score := 0
	missing := []string{}
	requiredReady := 0
	assumedRequired := 0
	nextStage := ""
	nextPrompt := ""
	firstAssumedStage := ""
	firstAssumedLabel := ""

	product := strings.TrimSpace(session.ProductName)
	if product != "" {
		score += 10
	} else {
		missing = append(missing, "product name")
		nextStage = telegramPRDStageAwaitProduct
		nextPrompt = "제품/프로젝트 이름을 입력하세요"
	}

	for _, f := range required {
		v := strings.TrimSpace(f.Value)
		if v == "" {
			missing = append(missing, f.Label)
			if nextStage == "" {
				nextStage = f.Stage
				nextPrompt = fmt.Sprintf("%s\n- %s", f.Prompt, f.Assumption)
			}
			continue
		}
		requiredReady++
		if isTelegramPRDAssumedValue(v) {
			score += 9
			assumedRequired++
			if firstAssumedStage == "" {
				firstAssumedStage = f.Stage
				firstAssumedLabel = f.Label
			}
		} else {
			score += 14
		}
	}

	storyCount := len(session.Stories)
	if storyCount == 0 {
		missing = append(missing, "at least 1 user story")
		if nextStage == "" {
			nextStage = telegramPRDStageAwaitStoryTitle
			nextPrompt = "첫 user story 제목을 입력하세요"
		}
	} else {
		score += 20
		if storyCount >= 3 {
			score += 4
		}
	}

	if strings.TrimSpace(session.Context.Constraints) != "" {
		if isTelegramPRDAssumedValue(session.Context.Constraints) {
			score += 4
		} else {
			score += 8
		}
	}

	if score > 100 {
		score = 100
	}

	ready := score >= telegramPRDClarityMinScore && requiredReady == len(required) && storyCount > 0 && assumedRequired == 0
	if !ready && nextStage == "" && firstAssumedStage != "" {
		nextStage = firstAssumedStage
		nextPrompt = fmt.Sprintf("%s의 실제 값을 입력하세요 (현재 가정값으로 설정됨)", firstAssumedLabel)
		missing = append([]string{"replace assumed value: " + firstAssumedLabel}, missing...)
	}
	if ready {
		nextStage = ""
		nextPrompt = ""
	}

	return telegramPRDClarityStatus{
		Score:         score,
		RequiredTotal: len(required),
		RequiredReady: requiredReady,
		ReadyToApply:  ready,
		Missing:       missing,
		NextStage:     nextStage,
		NextPrompt:    nextPrompt,
	}
}

func formatTelegramPRDCodexRefineQuestion(refine telegramPRDCodexRefineResponse) string {
	lines := []string{
		"prd refine question",
		fmt.Sprintf("- score: %d/100 (gate=%d)", refine.Score, telegramPRDClarityMinScore),
		"- scoring_mode: codex",
	}
	if refine.ReadyToApply {
		lines = append(lines, "- status: ready_to_apply")
		lines = append(lines, "- next: /prd apply")
		return strings.Join(lines, "\n")
	}
	if strings.TrimSpace(refine.Ask) != "" {
		lines = append(lines, "- ask: "+refine.Ask)
	}
	if stage, ok := normalizeTelegramPRDRefineSuggestedStage(refine.SuggestedStage); ok {
		lines = append(lines, "- next_stage: "+stage)
	}
	if len(refine.Missing) > 0 {
		lines = append(lines, "- missing_top: "+refine.Missing[0])
	}
	if strings.TrimSpace(refine.Reason) != "" {
		lines = append(lines, "- reason: "+refine.Reason)
	}
	lines = append(lines, "- hint: 답변이 애매하면 `skip` 또는 `default` 입력")
	return strings.Join(lines, "\n")
}

func formatTelegramPRDRefineUnavailable(currentStage string, fallbackScore int, err error) string {
	lines := []string{
		"prd refine unavailable",
		fmt.Sprintf("- score: %d/100 (gate=%d)", fallbackScore, telegramPRDClarityMinScore),
		"- scoring_mode: codex_unavailable",
		fmt.Sprintf("- current_stage: %s", valueOrDash(currentStage)),
		"- reason: codex refine 실패로 동적 질문 생성 불가",
		"- next: codex 상태 복구 후 `/prd refine` 재시도",
	}
	if err != nil {
		lines = append(lines, "- note: codex refine unavailable")
		category, detail := classifyTelegramCodexFailure(err)
		if category != "" {
			lines = append(lines, "- codex_error: "+category)
		}
		if detail != "" {
			lines = append(lines, "- codex_detail: "+detail)
		}
	}
	lines = append(lines, "- hint: `/doctor` 또는 telegram tail 로그로 원인 확인")
	return strings.Join(lines, "\n")
}

func classifyTelegramCodexFailure(err error) (string, string) {
	if err == nil {
		return "", ""
	}
	raw := strings.ToLower(strings.TrimSpace(err.Error()))
	detail := compactSingleLine(strings.TrimSpace(err.Error()), 180)
	switch {
	case strings.Contains(raw, "not found"):
		return "not_installed", detail
	case strings.Contains(raw, "no such file or directory"), strings.Contains(raw, "os error 2"):
		return "file_not_found", detail
	case strings.Contains(raw, "timeout"), strings.Contains(raw, "deadline exceeded"):
		return "timeout", detail
	case strings.Contains(raw, "operation not permitted"), strings.Contains(raw, "permission denied"):
		return "permission", detail
	case strings.Contains(raw, "could not resolve host"), strings.Contains(raw, "connection refused"),
		strings.Contains(raw, "network"), strings.Contains(raw, "i/o timeout"), strings.Contains(raw, "temporary failure in name resolution"):
		return "network", detail
	case strings.Contains(raw, "json"), strings.Contains(raw, "parse"):
		return "invalid_response", detail
	default:
		return "exec_failure", detail
	}
}

func formatTelegramPRDCodexScore(session telegramPRDSession) string {
	lines := []string{
		"prd clarity score",
		fmt.Sprintf("- score: %d/100", session.CodexScore),
		fmt.Sprintf("- gate: %d", telegramPRDClarityMinScore),
		"- scoring_mode: codex",
	}
	if session.CodexReady {
		lines = append(lines, "- status: ready_to_apply")
		lines = append(lines, "- next: /prd apply")
	} else {
		lines = append(lines, "- status: needs_input")
		if len(session.CodexMissing) > 0 {
			lines = append(lines, "- missing: "+strings.Join(session.CodexMissing, ", "))
		}
		lines = append(lines, "- next: /prd refine")
	}
	if strings.TrimSpace(session.CodexSummary) != "" {
		lines = append(lines, "- summary: "+session.CodexSummary)
	}
	if strings.TrimSpace(session.CodexScoredAtUT) != "" {
		lines = append(lines, "- scored_at: "+session.CodexScoredAtUT)
	}
	return strings.Join(lines, "\n")
}

func parseTelegramPRDStoryRole(input string) (string, error) {
	v := strings.ToLower(strings.TrimSpace(input))
	switch v {
	case "1":
		v = "manager"
	case "2":
		v = "planner"
	case "3":
		v = "developer"
	case "4":
		v = "qa"
	}
	if !ralph.IsSupportedRole(v) {
		return "", fmt.Errorf("invalid role: %q (use manager|planner|developer|qa)", input)
	}
	return v, nil
}

func parseTelegramPRDStoryPriority(input string) (int, error) {
	v := strings.TrimSpace(strings.ToLower(input))
	if v == "" || v == "default" || v == "skip" {
		return telegramPRDDefaultPriority, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid priority: %q (use positive number)", input)
	}
	return n, nil
}

func parseTelegramPRDStoryRoleAndPriorityInput(session telegramPRDSession, rawRole, rawPriority string) (string, int, bool, error) {
	roleInput := strings.TrimSpace(rawRole)
	priorityInput := strings.TrimSpace(rawPriority)

	if priorityInput == "" {
		fields := strings.Fields(roleInput)
		if len(fields) > 0 {
			roleInput = fields[0]
		}
		if len(fields) == 2 {
			priorityInput = fields[1]
		}
		if len(fields) > 2 {
			return "", 0, false, fmt.Errorf("invalid role input: %q (use role or role priority)", rawRole)
		}
	}

	role, err := parseTelegramPRDStoryRole(roleInput)
	if err != nil {
		return "", 0, false, err
	}
	if strings.TrimSpace(priorityInput) == "" {
		return role, 0, false, nil
	}
	if strings.EqualFold(strings.TrimSpace(priorityInput), "default") || strings.EqualFold(strings.TrimSpace(priorityInput), "skip") {
		return role, 0, false, nil
	}

	priority, err := parseTelegramPRDStoryPriority(priorityInput)
	if err != nil {
		return "", 0, false, err
	}
	return role, priority, true, nil
}

func parseTelegramPRDQuickStoryInput(session telegramPRDSession, input string) (telegramPRDStory, bool, error) {
	if !strings.Contains(input, "|") {
		return telegramPRDStory{}, false, nil
	}
	partsRaw := strings.Split(input, "|")
	parts := make([]string, 0, len(partsRaw))
	for _, p := range partsRaw {
		parts = append(parts, strings.TrimSpace(p))
	}
	if len(parts) < 3 || len(parts) > 4 {
		return telegramPRDStory{}, true, fmt.Errorf("quick format: 제목 | 설명 | role [priority] 또는 제목 | 설명 | role | priority")
	}
	title := strings.TrimSpace(parts[0])
	desc := strings.TrimSpace(parts[1])
	if title == "" || desc == "" {
		return telegramPRDStory{}, true, fmt.Errorf("quick format requires non-empty title and description")
	}
	rawRole := strings.TrimSpace(parts[2])
	rawPriority := ""
	if len(parts) == 4 {
		rawPriority = strings.TrimSpace(parts[3])
	}
	role, priority, explicitPriority, err := parseTelegramPRDStoryRoleAndPriorityInput(session, rawRole, rawPriority)
	if err != nil {
		return telegramPRDStory{}, true, err
	}
	if !explicitPriority {
		priority = 0
	}
	return telegramPRDStory{
		Title:       title,
		Description: desc,
		Role:        role,
		Priority:    priority,
	}, true, nil
}

func telegramPRDAppendStoryFromDraft(paths ralph.Paths, session telegramPRDSession, role string, priority int, explicitPriority bool) (telegramPRDSession, telegramPRDStory, string, error) {
	story := telegramPRDStory{
		Title:       strings.TrimSpace(session.DraftTitle),
		Description: strings.TrimSpace(session.DraftDesc),
		Role:        strings.TrimSpace(role),
		Priority:    priority,
	}
	if strings.TrimSpace(story.Title) == "" || strings.TrimSpace(story.Description) == "" || strings.TrimSpace(story.Role) == "" {
		return session, telegramPRDStory{}, "", fmt.Errorf("incomplete story draft; run /prd cancel then /prd start")
	}
	prioritySource := "manual"
	if !explicitPriority || story.Priority <= 0 {
		resolvedPriority, source := resolveTelegramPRDStoryPriority(paths, session, story)
		story.Priority = resolvedPriority
		prioritySource = source
	} else if story.Priority <= 0 {
		story.Priority = telegramPRDStoryPriorityForRole(session, story.Role)
		prioritySource = "fallback_role_profile"
	}
	story.ID = telegramPRDStoryID(session, len(session.Stories)+1)
	session.Stories = append(session.Stories, story)
	session.DraftTitle = ""
	session.DraftDesc = ""
	session.DraftRole = ""
	session.Stage = telegramPRDStageAwaitStoryTitle
	return session, story, prioritySource, nil
}

func telegramPRDAppendStoryFromQuick(paths ralph.Paths, session telegramPRDSession, story telegramPRDStory) (telegramPRDSession, string, error) {
	s := story
	if strings.TrimSpace(s.Role) == "" {
		return session, "", fmt.Errorf("quick story role is required")
	}
	prioritySource := "manual"
	if s.Priority <= 0 {
		resolvedPriority, source := resolveTelegramPRDStoryPriority(paths, session, s)
		s.Priority = resolvedPriority
		prioritySource = source
	}
	s.ID = telegramPRDStoryID(session, len(session.Stories)+1)
	session.Stories = append(session.Stories, s)
	session.DraftTitle = ""
	session.DraftDesc = ""
	session.DraftRole = ""
	session.Stage = telegramPRDStageAwaitStoryTitle
	return session, telegramPRDStoryAddedReply(session, s, prioritySource), nil
}

func telegramPRDStoryAddedReply(session telegramPRDSession, story telegramPRDStory, prioritySource string) string {
	clarity := evaluateTelegramPRDClarity(session)
	next := "다음 story 제목 입력 또는 /prd preview /prd save /prd apply"
	if !clarity.ReadyToApply {
		next = "/prd refine (부족 컨텍스트 질문 진행) 또는 다음 story 제목 입력"
	}
	if strings.TrimSpace(prioritySource) == "" {
		prioritySource = "manual"
	}
	return fmt.Sprintf(
		"story added\n- id: %s\n- title: %s\n- role: %s\n- priority: %d\n- priority_source: %s\n- stories_total: %d\n- clarity_score: %d/100\n- next: %s",
		story.ID,
		compactSingleLine(story.Title, 90),
		story.Role,
		story.Priority,
		prioritySource,
		len(session.Stories),
		clarity.Score,
		next,
	)
}

func telegramPRDStagePrompt(stage string) string {
	switch stage {
	case telegramPRDStageAwaitProduct:
		return "제품/프로젝트 이름을 입력하세요"
	case telegramPRDStageAwaitProblem:
		return "문제 정의를 입력하세요 (왜 이 작업이 필요한가?)"
	case telegramPRDStageAwaitGoal:
		return "목표를 입력하세요 (완료 기준 한 줄)"
	case telegramPRDStageAwaitInScope:
		return "포함 범위를 입력하세요 (이번 사이클에서 반드시 할 것)"
	case telegramPRDStageAwaitOutOfScope:
		return "제외 범위를 입력하세요 (이번 사이클에서 하지 않을 것)"
	case telegramPRDStageAwaitAcceptance:
		return "수용 기준을 입력하세요 (검증 가능한 기준)"
	case telegramPRDStageAwaitConstraints:
		return "제약 사항을 입력하세요 (옵션, skip 가능)"
	case telegramPRDStageAwaitStoryTitle:
		return "story 제목을 입력하세요 (quick: 제목 | 설명 | role [priority])"
	case telegramPRDStageAwaitStoryDesc:
		return "story 설명을 입력하세요"
	case telegramPRDStageAwaitStoryRole:
		return "role 입력 (manager|planner|developer|qa, optional: role priority)"
	case telegramPRDStageAwaitStoryPrio:
		return "priority 입력 (숫자, default=role 기본값)"
	default:
		return "unknown stage"
	}
}

func telegramHasActivePRDSession(paths ralph.Paths, chatID int64) (bool, error) {
	_, found, err := telegramLoadPRDSession(paths, chatID)
	return found, err
}

func telegramPRDSessionStoreDir(paths ralph.Paths) string {
	return filepath.Join(paths.ReportsDir, "telegram-prd")
}

func telegramPRDSessionFile(paths ralph.Paths) string {
	return filepath.Join(telegramPRDSessionStoreDir(paths), "sessions.json")
}

func legacyTelegramPRDSessionFile(paths ralph.Paths) string {
	return filepath.Join(paths.ControlDir, "telegram-prd-sessions.json")
}

func telegramSessionKey(chatID int64) string {
	return strconv.FormatInt(chatID, 10)
}

const (
	telegramPRDSessionLockWait  = 5 * time.Second
	telegramPRDSessionLockStale = 30 * time.Second
)

func withTelegramPRDSessionStoreLock(paths ralph.Paths, fn func(path string) error) error {
	path := telegramPRDSessionFile(paths)
	lockPath := path + ".lock"
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return fmt.Errorf("create prd lock dir: %w", err)
	}

	telegramPRDSessionStoreMu.Lock()
	defer telegramPRDSessionStoreMu.Unlock()

	deadline := time.Now().Add(telegramPRDSessionLockWait)
	for {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = fmt.Fprintf(f, "%d\n%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
			_ = f.Close()
			defer func() {
				_ = os.Remove(lockPath)
			}()
			return fn(path)
		}
		if !os.IsExist(err) {
			return fmt.Errorf("acquire prd session lock: %w", err)
		}
		shouldBreak, reason := shouldBreakTelegramPRDSessionLock(lockPath)
		if shouldBreak {
			_ = os.Remove(lockPath)
			continue
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("acquire prd session lock timeout (%s)", reason)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

func shouldBreakTelegramPRDSessionLock(lockPath string) (bool, string) {
	info, err := os.Stat(lockPath)
	if err != nil {
		if os.IsNotExist(err) {
			return true, "lock disappeared"
		}
		return false, "lock stat failed"
	}
	if time.Since(info.ModTime()) > telegramPRDSessionLockStale {
		return true, fmt.Sprintf("lock stale>%s", telegramPRDSessionLockStale)
	}
	pid, ok := telegramPRDLockOwnerPID(lockPath)
	if !ok {
		return false, "owner pid unknown"
	}
	alive, aliveErr := telegramPRDProcessAlive(pid)
	if aliveErr != nil {
		return false, fmt.Sprintf("owner pid check failed(%d)", pid)
	}
	if !alive {
		return true, fmt.Sprintf("owner pid dead(%d)", pid)
	}
	return false, fmt.Sprintf("owner pid alive(%d)", pid)
}

func telegramPRDLockOwnerPID(lockPath string) (int, bool) {
	data, err := os.ReadFile(lockPath)
	if err != nil {
		return 0, false
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return 0, false
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

func telegramPRDProcessAlive(pid int) (bool, error) {
	if pid <= 0 {
		return false, nil
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	if errors.Is(err, syscall.EPERM) {
		return true, nil
	}
	return false, err
}

func parseTelegramPRDSessionStoreData(data []byte) (telegramPRDSessionStore, error) {
	store := telegramPRDSessionStore{Sessions: map[string]telegramPRDSession{}}
	if len(bytes.TrimSpace(data)) == 0 {
		return store, nil
	}
	if err := json.Unmarshal(data, &store); err != nil {
		return store, fmt.Errorf("parse prd session store: %w", err)
	}
	if store.Sessions == nil {
		store.Sessions = map[string]telegramPRDSession{}
	}
	return store, nil
}

func loadTelegramPRDSessionStoreUnlocked(paths ralph.Paths, path string) (telegramPRDSessionStore, error) {
	store := telegramPRDSessionStore{Sessions: map[string]telegramPRDSession{}}
	data, err := os.ReadFile(path)
	if err == nil {
		parsed, parseErr := parseTelegramPRDSessionStoreData(data)
		if parseErr != nil {
			return store, fmt.Errorf("parse prd session store: %w", parseErr)
		}
		return parsed, nil
	}
	if !os.IsNotExist(err) {
		return store, fmt.Errorf("read prd session store: %w", err)
	}

	legacyPath := legacyTelegramPRDSessionFile(paths)
	legacyData, legacyErr := os.ReadFile(legacyPath)
	if legacyErr != nil {
		if os.IsNotExist(legacyErr) {
			return store, nil
		}
		return store, fmt.Errorf("read legacy prd session store: %w", legacyErr)
	}
	legacyStore, parseErr := parseTelegramPRDSessionStoreData(legacyData)
	if parseErr != nil {
		return store, fmt.Errorf("parse legacy prd session store: %w", parseErr)
	}

	if writeErr := saveTelegramPRDSessionStoreUnlocked(path, legacyStore); writeErr == nil {
		_ = os.Remove(legacyPath)
	}
	return legacyStore, nil
}

func saveTelegramPRDSessionStoreUnlocked(path string, store telegramPRDSessionStore) error {
	if store.Sessions == nil {
		store.Sessions = map[string]telegramPRDSession{}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create prd session dir: %w", err)
	}
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal prd session store: %w", err)
	}
	data = append(data, '\n')
	if err := writeTelegramPRDAtomicFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write prd session store: %w", err)
	}
	return nil
}

func writeTelegramPRDAtomicFile(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(dir, ".telegram-prd-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Chmod(mode); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

func telegramLoadPRDSession(paths ralph.Paths, chatID int64) (telegramPRDSession, bool, error) {
	var (
		store telegramPRDSessionStore
		err   error
	)
	lockErr := withTelegramPRDSessionStoreLock(paths, func(path string) error {
		store, err = loadTelegramPRDSessionStoreUnlocked(paths, path)
		return err
	})
	if lockErr != nil {
		return telegramPRDSession{}, false, lockErr
	}
	key := telegramSessionKey(chatID)
	session, ok := store.Sessions[key]
	return session, ok, nil
}

func telegramUpsertPRDSession(paths ralph.Paths, session telegramPRDSession) error {
	return withTelegramPRDSessionStoreLock(paths, func(path string) error {
		store, err := loadTelegramPRDSessionStoreUnlocked(paths, path)
		if err != nil {
			return err
		}
		key := telegramSessionKey(session.ChatID)
		store.Sessions[key] = session
		return saveTelegramPRDSessionStoreUnlocked(path, store)
	})
}

func telegramDeletePRDSession(paths ralph.Paths, chatID int64) error {
	return withTelegramPRDSessionStoreLock(paths, func(path string) error {
		store, err := loadTelegramPRDSessionStoreUnlocked(paths, path)
		if err != nil {
			return err
		}
		delete(store.Sessions, telegramSessionKey(chatID))
		return saveTelegramPRDSessionStoreUnlocked(path, store)
	})
}

func telegramPRDConversationDir(paths ralph.Paths, chatID int64) string {
	return filepath.Join(telegramPRDSessionStoreDir(paths), "conversations", strconv.FormatInt(chatID, 10))
}

func telegramPRDConversationFile(paths ralph.Paths, chatID int64) string {
	return filepath.Join(telegramPRDConversationDir(paths, chatID), "conversation.md")
}

func logTelegramPRDConversationWarning(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "[telegram] prd conversation warning: %v\n", err)
}

func clearTelegramPRDConversation(paths ralph.Paths, chatID int64) error {
	return os.RemoveAll(telegramPRDConversationDir(paths, chatID))
}

func appendTelegramPRDConversation(paths ralph.Paths, chatID int64, role, text string) error {
	role = strings.TrimSpace(strings.ToLower(role))
	if role == "" {
		role = "assistant"
	}
	text = strings.TrimSpace(sanitizeTelegramUTF8String(text))
	if text == "" {
		return nil
	}
	path := telegramPRDConversationFile(paths, chatID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create conversation dir: %w", err)
	}
	entry := fmt.Sprintf("\n### %s | %s\n%s\n", time.Now().UTC().Format(time.RFC3339), role, text)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open conversation file: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(entry); err != nil {
		return fmt.Errorf("append conversation file: %w", err)
	}
	return nil
}

func readTelegramPRDConversationTail(paths ralph.Paths, chatID int64, maxRunes int) string {
	if maxRunes <= 0 {
		maxRunes = 5000
	}
	data, err := os.ReadFile(telegramPRDConversationFile(paths, chatID))
	if err != nil {
		return ""
	}
	data = bytes.ToValidUTF8(data, []byte("?"))
	text := strings.TrimSpace(string(data))
	if text == "" {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= maxRunes {
		return text
	}
	return "...(truncated)\n" + string(runes[len(runes)-maxRunes:])
}

func sanitizeTelegramUTF8String(raw string) string {
	if raw == "" {
		return ""
	}
	if utf8.ValidString(raw) {
		return raw
	}
	return string(bytes.ToValidUTF8([]byte(raw), []byte("?")))
}

func resolveTelegramPRDFilePath(paths ralph.Paths, chatID int64, raw string) (string, error) {
	if err := ralph.EnsureLayout(paths); err != nil {
		return "", err
	}
	target := strings.TrimSpace(raw)
	if target == "" {
		target = filepath.Join(paths.ReportsDir, fmt.Sprintf("telegram-prd-%d.json", chatID))
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(paths.ProjectDir, target)
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolve prd path: %w", err)
	}
	return absTarget, nil
}

func writeTelegramPRDFile(path string, session telegramPRDSession) error {
	product := strings.TrimSpace(session.ProductName)
	if product == "" {
		product = telegramPRDDefaultProductFallback
	}
	clarity := evaluateTelegramPRDClarity(session)
	stories := make([]telegramPRDStory, 0, len(session.Stories))
	for _, story := range session.Stories {
		s := story
		if strings.TrimSpace(s.ID) == "" {
			s.ID = telegramPRDStoryID(session, len(stories)+1)
		}
		if strings.TrimSpace(s.Role) == "" {
			s.Role = "developer"
		}
		if s.Priority <= 0 {
			s.Priority = telegramPRDStoryPriorityForRole(session, s.Role)
		}
		stories = append(stories, s)
	}
	doc := map[string]any{
		"metadata": map[string]any{
			"product":          product,
			"source":           "telegram-prd-wizard",
			"generated_at_utc": time.Now().UTC().Format(time.RFC3339),
			"clarity_score":    clarity.Score,
			"clarity_gate":     telegramPRDClarityMinScore,
			"context": map[string]any{
				"problem":        strings.TrimSpace(session.Context.Problem),
				"goal":           strings.TrimSpace(session.Context.Goal),
				"in_scope":       strings.TrimSpace(session.Context.InScope),
				"out_of_scope":   strings.TrimSpace(session.Context.OutOfScope),
				"acceptance":     strings.TrimSpace(session.Context.Acceptance),
				"constraints":    strings.TrimSpace(session.Context.Constraints),
				"assumptions":    session.Context.Assumptions,
				"agent_priority": normalizeTelegramPRDAgentPriorityMap(session.Context.AgentPriority),
			},
		},
		"userStories": telegramPRDDocument{
			UserStories: stories,
		}.UserStories,
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal prd json: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create prd dir: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write prd json: %w", err)
	}
	return nil
}

func telegramPRDStoryID(session telegramPRDSession, idx int) string {
	prefixTime := time.Now().UTC()
	if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(session.CreatedAtUTC)); err == nil {
		prefixTime = parsed.UTC()
	}
	if idx <= 0 {
		idx = 1
	}
	return fmt.Sprintf("TG-%s-%03d", prefixTime.Format("20060102T150405Z"), idx)
}
