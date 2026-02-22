package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"codex-ralph/internal/ralph"
)

type telegramTaskIntake struct {
	Role       string   `json:"role"`
	Title      string   `json:"title"`
	Objective  string   `json:"objective"`
	Acceptance []string `json:"acceptance"`
	Priority   int      `json:"priority"`
}

var telegramTaskIntakeAnalyzer = analyzeTelegramTaskIntakeWithCodex

func telegramTaskIssueCommand(paths ralph.Paths, chatID int64, rawArgs string) (string, error) {
	request := strings.TrimSpace(sanitizeTelegramUTF8String(rawArgs))
	if request == "" {
		return "usage: /task <natural language request>", nil
	}

	intake, err := telegramTaskIntakeAnalyzer(paths, chatID, request)
	if err != nil {
		return formatTelegramTaskUnavailable(err), nil
	}
	opts := ralph.IssueCreateOptions{
		Objective:          intake.Objective,
		AcceptanceCriteria: intake.Acceptance,
	}
	if intake.Priority > 0 {
		opts.Priority = intake.Priority
	}
	path, issueID, createErr := ralph.CreateIssueWithOptions(paths, intake.Role, intake.Title, opts)
	if createErr != nil {
		return "", createErr
	}
	return fmt.Sprintf(
		"task accepted\n- id: %s\n- role: %s\n- title: %s\n- path: %s",
		issueID,
		intake.Role,
		intake.Title,
		path,
	), nil
}

func analyzeTelegramTaskIntakeWithCodex(paths ralph.Paths, chatID int64, input string) (telegramTaskIntake, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return telegramTaskIntake{}, fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return telegramTaskIntake{}, err
	}
	if !profile.RequireCodex {
		return telegramTaskIntake{}, fmt.Errorf("codex intake disabled (require_codex=false)")
	}

	timeoutSec := resolveTelegramCodexTimeoutSec(profile.CodexExecTimeoutSec, 90)
	retryAttempts := profile.CodexRetryMaxAttempts
	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	if retryAttempts > 3 {
		retryAttempts = 3
	}
	retryBackoffSec := profile.CodexRetryBackoffSec
	if retryBackoffSec <= 0 {
		retryBackoffSec = 1
	}
	if retryBackoffSec > 3 {
		retryBackoffSec = 3
	}

	model := strings.TrimSpace(profile.CodexModelForRole("planner"))
	conversationTail := readTelegramChatConversationTail(paths, chatID, 3200)
	prompt := buildTelegramTaskIntakePrompt(paths.ProjectDir, conversationTail, input)

	var lastErr error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
		raw, execErr := runTelegramPRDCodexExec(ctx, paths, profile, model, prompt, "ralph-telegram-task-intake-*")
		cancel()
		if execErr == nil {
			parsed, parseErr := parseTelegramTaskIntake(raw, input)
			if parseErr == nil {
				return parsed, nil
			}
			lastErr = parseErr
		} else {
			lastErr = execErr
		}
		if attempt < retryAttempts {
			time.Sleep(time.Duration(attempt*retryBackoffSec) * time.Second)
		}
	}
	return telegramTaskIntake{}, fmt.Errorf("codex task intake retries exhausted: %w", lastErr)
}

func buildTelegramTaskIntakePrompt(projectDir, conversationTail, input string) string {
	var b strings.Builder
	fmt.Fprintln(&b, "You convert a natural-language task request into a Ralph issue draft.")
	fmt.Fprintln(&b, "Return STRICT JSON only.")
	fmt.Fprintln(&b, `Schema: {"role":"developer","title":"...","objective":"...","acceptance":["..."],"priority":1000}`)
	fmt.Fprintln(&b, "Rules:")
	fmt.Fprintln(&b, "- role must be one of manager|planner|developer|qa.")
	fmt.Fprintln(&b, "- title should be specific and executable (<=90 chars).")
	fmt.Fprintln(&b, "- objective should be concrete and short.")
	fmt.Fprintln(&b, "- acceptance should contain 2~4 checklist items, each testable.")
	fmt.Fprintln(&b, "- priority is optional; use 0 when unknown.")
	fmt.Fprintln(&b, "- Prefer Korean concise phrasing.")
	fmt.Fprintf(&b, "Project directory: %s\n", strings.TrimSpace(projectDir))
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "\nRecent chat context:")
		fmt.Fprintln(&b, conversationTail)
	}
	fmt.Fprintln(&b, "\nUser request:")
	fmt.Fprintln(&b, strings.TrimSpace(input))
	return b.String()
}

func parseTelegramTaskIntake(raw string, fallbackInput string) (telegramTaskIntake, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return telegramTaskIntake{}, fmt.Errorf("empty task intake response")
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}
	var parsed telegramTaskIntake
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		start := strings.Index(text, "{")
		end := strings.LastIndex(text, "}")
		if start < 0 || end <= start {
			return telegramTaskIntake{}, fmt.Errorf("invalid task intake json")
		}
		if unmarshalErr := json.Unmarshal([]byte(text[start:end+1]), &parsed); unmarshalErr != nil {
			return telegramTaskIntake{}, fmt.Errorf("parse task intake json: %w", unmarshalErr)
		}
	}

	role := strings.ToLower(strings.TrimSpace(parsed.Role))
	if !ralph.IsSupportedRole(role) {
		role = "developer"
	}
	title := compactSingleLine(strings.TrimSpace(sanitizeTelegramUTF8String(parsed.Title)), 120)
	if title == "" {
		title = compactSingleLine(strings.TrimSpace(sanitizeTelegramUTF8String(fallbackInput)), 120)
	}
	if title == "" {
		title = "업무 요청 정리"
	}
	objective := compactSingleLine(strings.TrimSpace(sanitizeTelegramUTF8String(parsed.Objective)), 240)
	if objective == "" {
		objective = title
	}
	acceptance := make([]string, 0, len(parsed.Acceptance))
	for _, item := range parsed.Acceptance {
		v := compactSingleLine(strings.TrimSpace(sanitizeTelegramUTF8String(item)), 160)
		if v == "" {
			continue
		}
		acceptance = append(acceptance, v)
		if len(acceptance) >= 4 {
			break
		}
	}

	priority := parsed.Priority
	if priority < 0 {
		priority = 0
	}
	if priority > 0 {
		if priority < 100 {
			priority = 100
		}
		if priority > 3000 {
			priority = 3000
		}
	}
	return telegramTaskIntake{
		Role:       role,
		Title:      title,
		Objective:  objective,
		Acceptance: acceptance,
		Priority:   priority,
	}, nil
}

func formatTelegramTaskUnavailable(err error) string {
	category, detail := classifyTelegramCodexFailure(err)
	lines := []string{
		"task intake unavailable",
		"- reason: codex issue structuring failed",
		"- next: retry /task or fallback to /new [role] <title>",
	}
	if category != "" {
		lines = append(lines, "- codex_error: "+category)
	}
	if detail != "" {
		lines = append(lines, "- codex_detail: "+detail)
	}
	return strings.Join(lines, "\n")
}
