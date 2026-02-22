package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"codex-ralph/internal/ralph"
)

func analyzeTelegramPRDTurnWithCodex(paths ralph.Paths, session telegramPRDSession, input string) (telegramPRDCodexTurnResponse, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return telegramPRDCodexTurnResponse{}, fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return telegramPRDCodexTurnResponse{}, err
	}
	if !profile.RequireCodex {
		return telegramPRDCodexTurnResponse{}, fmt.Errorf("codex turn disabled (require_codex=false)")
	}
	timeoutSec := resolveTelegramCodexTimeoutSec(profile.CodexExecTimeoutSec, telegramPRDCodexAssistTimeoutSec)
	retryAttempts := profile.CodexRetryMaxAttempts
	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	if retryAttempts > 5 {
		retryAttempts = 5
	}
	retryBackoffSec := profile.CodexRetryBackoffSec
	if retryBackoffSec <= 0 {
		retryBackoffSec = 1
	}
	if retryBackoffSec > 3 {
		retryBackoffSec = 3
	}

	conversationTail := readTelegramPRDConversationTail(paths, session.ChatID, 4000)
	prompt := buildTelegramPRDTurnPrompt(session, input, conversationTail)
	model := strings.TrimSpace(profile.CodexModelForRole("planner"))

	var lastErr error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
		raw, execErr := runTelegramPRDCodexExec(ctx, paths, profile, model, prompt, "ralph-telegram-prd-turn-*")
		cancel()
		if execErr == nil {
			parsed, parseErr := parseTelegramPRDCodexTurnResponse(raw)
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
	return telegramPRDCodexTurnResponse{}, fmt.Errorf("codex turn retries exhausted: %w", lastErr)
}

func buildTelegramPRDTurnPrompt(session telegramPRDSession, userInput, conversationTail string) string {
	payload, _ := json.Marshal(session)
	var b strings.Builder
	fmt.Fprintln(&b, "You are an interactive PRD copilot for Telegram.")
	fmt.Fprintln(&b, "Understand intent from natural language and update PRD state.")
	fmt.Fprintln(&b, "Return STRICT JSON only (no markdown, no prose outside JSON).")
	fmt.Fprintln(&b, `Schema: {"reply":"...","next_question":"...","suggested_stage":"await_problem","ready_to_apply":false,"session_patch":{"product_name":"","problem":"","goal":"","in_scope":"","out_of_scope":"","acceptance":"","constraints":""},"story":{"title":"","description":"","role":"developer","priority":0}}`)
	fmt.Fprintln(&b, "Rules:")
	fmt.Fprintln(&b, "- Prioritize semantic understanding over stage templates.")
	fmt.Fprintln(&b, "- If the user provided concrete facts, fill session_patch immediately.")
	fmt.Fprintln(&b, "- Keep unchanged fields as empty string.")
	fmt.Fprintln(&b, "- If user asks explanation/recommendation, answer in reply, and still extract concrete facts if present.")
	fmt.Fprintln(&b, "- Ask at most ONE next_question and only for highest-leverage missing context.")
	fmt.Fprintln(&b, "- Never re-ask dimensions already clear in the current session.")
	fmt.Fprintln(&b, "- If enough information for a story, set story with title+description+role. priority can be 0 when unknown.")
	fmt.Fprintln(&b, "- role must be one of manager|planner|developer|qa.")
	fmt.Fprintln(&b, "- Keep Korean concise and practical.")
	fmt.Fprintf(&b, "\nCurrent stage: %s\n", session.Stage)
	fmt.Fprintln(&b, "\nCurrent session JSON:")
	fmt.Fprintln(&b, string(payload))
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "\nRecent conversation (markdown):")
		fmt.Fprintln(&b, conversationTail)
	}
	fmt.Fprintf(&b, "\nUser input: %s\n", strings.TrimSpace(userInput))
	return b.String()
}

func parseTelegramPRDCodexTurnResponse(raw string) (telegramPRDCodexTurnResponse, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return telegramPRDCodexTurnResponse{}, fmt.Errorf("empty codex turn response")
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}

	var parsed telegramPRDCodexTurnResponse
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		start := strings.Index(text, "{")
		end := strings.LastIndex(text, "}")
		if start < 0 || end <= start {
			return telegramPRDCodexTurnResponse{}, fmt.Errorf("invalid codex turn json")
		}
		if unmarshalErr := json.Unmarshal([]byte(text[start:end+1]), &parsed); unmarshalErr != nil {
			return telegramPRDCodexTurnResponse{}, fmt.Errorf("parse codex turn json: %w", unmarshalErr)
		}
	}

	parsed.Reply = sanitizeTelegramPRDTurnText(parsed.Reply, 500)
	parsed.NextQuestion = sanitizeTelegramPRDTurnText(parsed.NextQuestion, 240)
	parsed.SuggestedStage = strings.TrimSpace(parsed.SuggestedStage)
	parsed.SessionPatch.ProductName = sanitizeTelegramPRDTurnText(parsed.SessionPatch.ProductName, 140)
	parsed.SessionPatch.Problem = sanitizeTelegramPRDTurnText(parsed.SessionPatch.Problem, 320)
	parsed.SessionPatch.Goal = sanitizeTelegramPRDTurnText(parsed.SessionPatch.Goal, 260)
	parsed.SessionPatch.InScope = sanitizeTelegramPRDTurnText(parsed.SessionPatch.InScope, 320)
	parsed.SessionPatch.OutOfScope = sanitizeTelegramPRDTurnText(parsed.SessionPatch.OutOfScope, 320)
	parsed.SessionPatch.Acceptance = sanitizeTelegramPRDTurnText(parsed.SessionPatch.Acceptance, 320)
	parsed.SessionPatch.Constraints = sanitizeTelegramPRDTurnText(parsed.SessionPatch.Constraints, 280)
	if parsed.Story != nil {
		parsed.Story.Title = sanitizeTelegramPRDTurnText(parsed.Story.Title, 140)
		parsed.Story.Description = sanitizeTelegramPRDTurnText(parsed.Story.Description, 320)
		parsed.Story.Role = strings.ToLower(strings.TrimSpace(parsed.Story.Role))
		if parsed.Story.Priority <= 0 {
			parsed.Story.Priority = 0
		} else {
			parsed.Story.Priority = clampTelegramPRDStoryPriority(parsed.Story.Priority)
		}
	}
	return parsed, nil
}

func sanitizeTelegramPRDTurnText(raw string, maxLen int) string {
	v := strings.TrimSpace(sanitizeTelegramUTF8String(raw))
	if v == "" {
		return ""
	}
	if maxLen <= 0 {
		return v
	}
	runes := []rune(v)
	if len(runes) <= maxLen {
		return v
	}
	return string(runes[:maxLen])
}

func estimateTelegramPRDStoryPriorityWithCodex(paths ralph.Paths, session telegramPRDSession, story telegramPRDStory) (int, string, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return 0, "", fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return 0, "", err
	}
	if !profile.RequireCodex {
		return 0, "", fmt.Errorf("codex priority disabled (require_codex=false)")
	}
	timeoutSec := resolveTelegramCodexTimeoutSec(profile.CodexExecTimeoutSec, telegramPRDCodexAssistTimeoutSec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	model := strings.TrimSpace(profile.CodexModelForRole("planner"))
	conversationTail := readTelegramPRDConversationTail(paths, session.ChatID, 3000)
	prompt := buildTelegramPRDStoryPriorityPrompt(session, story, conversationTail)
	raw, err := runTelegramPRDCodexExec(ctx, paths, profile, model, prompt, "ralph-telegram-prd-priority-*")
	if err != nil {
		return 0, "", err
	}
	parsed, err := parseTelegramPRDCodexStoryPriorityResponse(raw)
	if err != nil {
		return 0, "", err
	}
	priority := parsed.Priority
	if priority <= 0 {
		return 0, "", fmt.Errorf("invalid codex priority: %d", parsed.Priority)
	}
	return priority, "codex_auto", nil
}

func buildTelegramPRDStoryPriorityPrompt(session telegramPRDSession, story telegramPRDStory, conversationTail string) string {
	payload, _ := json.Marshal(session)
	storyPayload, _ := json.Marshal(story)
	var b strings.Builder
	fmt.Fprintln(&b, "You are a strict issue priority allocator for autonomous agent execution.")
	fmt.Fprintln(&b, "Return STRICT JSON only.")
	fmt.Fprintln(&b, `Schema: {"priority":1000,"reason":"..."}`)
	fmt.Fprintln(&b, "Rules:")
	fmt.Fprintln(&b, "- Lower number means higher priority.")
	fmt.Fprintln(&b, "- Use integer range 100..3000.")
	fmt.Fprintln(&b, "- Consider role urgency, business risk, operational impact, and PRD context.")
	fmt.Fprintln(&b, "- Keep reason concise in Korean.")
	fmt.Fprintln(&b, "\nPRD Session JSON:")
	fmt.Fprintln(&b, string(payload))
	fmt.Fprintln(&b, "\nCandidate Story JSON:")
	fmt.Fprintln(&b, string(storyPayload))
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "\nRecent Conversation (Markdown):")
		fmt.Fprintln(&b, conversationTail)
	}
	return b.String()
}

func parseTelegramPRDCodexStoryPriorityResponse(raw string) (telegramPRDCodexStoryPriorityResponse, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return telegramPRDCodexStoryPriorityResponse{}, fmt.Errorf("empty codex priority response")
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}
	var parsed telegramPRDCodexStoryPriorityResponse
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		start := strings.Index(text, "{")
		end := strings.LastIndex(text, "}")
		if start < 0 || end <= start {
			return telegramPRDCodexStoryPriorityResponse{}, fmt.Errorf("invalid codex priority json")
		}
		if unmarshalErr := json.Unmarshal([]byte(text[start:end+1]), &parsed); unmarshalErr != nil {
			return telegramPRDCodexStoryPriorityResponse{}, fmt.Errorf("parse codex priority json: %w", unmarshalErr)
		}
	}
	parsed.Priority = clampTelegramPRDStoryPriority(parsed.Priority)
	parsed.Reason = compactSingleLine(strings.TrimSpace(parsed.Reason), 160)
	return parsed, nil
}

func clampTelegramPRDStoryPriority(v int) int {
	if v < 100 {
		return 100
	}
	if v > 3000 {
		return 3000
	}
	return v
}

func analyzeTelegramPRDScoreWithCodex(paths ralph.Paths, session telegramPRDSession) (telegramPRDCodexScoreResponse, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return telegramPRDCodexScoreResponse{}, fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return telegramPRDCodexScoreResponse{}, err
	}
	if !profile.RequireCodex {
		return telegramPRDCodexScoreResponse{}, fmt.Errorf("codex scoring disabled (require_codex=false)")
	}
	timeoutSec := resolveTelegramCodexTimeoutSec(profile.CodexExecTimeoutSec, telegramPRDCodexAssistTimeoutSec)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	model := strings.TrimSpace(profile.CodexModelForRole("planner"))

	conversationTail := readTelegramPRDConversationTail(paths, session.ChatID, 4000)
	prompt := buildTelegramPRDScorePrompt(session, conversationTail)
	raw, err := runTelegramPRDCodexExec(ctx, paths, profile, model, prompt, "ralph-telegram-prd-score-*")
	if err != nil {
		return telegramPRDCodexScoreResponse{}, err
	}
	return parseTelegramPRDCodexScoreResponse(raw)
}

func refreshTelegramPRDScoreWithCodex(paths ralph.Paths, session telegramPRDSession) (telegramPRDSession, bool, error) {
	score, err := analyzeTelegramPRDScoreWithCodex(paths, session)
	if err != nil {
		return session, false, err
	}
	session.CodexScore = clampTelegramPRDScore(score.Score)
	session.CodexReady = score.ReadyToApply && session.CodexScore >= telegramPRDClarityMinScore
	session.CodexMissing = sanitizeTelegramPRDMissingList(score.Missing)
	session.CodexSummary = strings.TrimSpace(score.Summary)
	session.CodexScoredAtUT = time.Now().UTC().Format(time.RFC3339)
	return session, true, nil
}

func refreshTelegramPRDRefineWithCodex(paths ralph.Paths, session telegramPRDSession) (telegramPRDSession, telegramPRDCodexRefineResponse, bool, error) {
	refine, err := telegramPRDRefineAnalyzer(paths, session)
	if err != nil {
		return session, telegramPRDCodexRefineResponse{}, false, fmt.Errorf("codex refine failed: %w", err)
	}
	session.CodexScore = clampTelegramPRDScore(refine.Score)
	session.CodexReady = refine.ReadyToApply && session.CodexScore >= telegramPRDClarityMinScore
	session.CodexMissing = sanitizeTelegramPRDMissingList(refine.Missing)
	session.CodexSummary = compactSingleLine(strings.TrimSpace(refine.Reason), 200)
	session.CodexScoredAtUT = time.Now().UTC().Format(time.RFC3339)
	refine.Score = session.CodexScore
	refine.ReadyToApply = session.CodexReady
	refine.Missing = append([]string(nil), session.CodexMissing...)
	return session, refine, true, nil
}

func analyzeTelegramPRDRefineWithCodex(paths ralph.Paths, session telegramPRDSession) (telegramPRDCodexRefineResponse, error) {
	if _, err := exec.LookPath("codex"); err != nil {
		return telegramPRDCodexRefineResponse{}, fmt.Errorf("codex command not found")
	}
	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return telegramPRDCodexRefineResponse{}, err
	}
	if !profile.RequireCodex {
		return telegramPRDCodexRefineResponse{}, fmt.Errorf("codex refine disabled (require_codex=false)")
	}
	timeoutSec := resolveTelegramCodexTimeoutSec(profile.CodexExecTimeoutSec, telegramPRDCodexAssistTimeoutSec)
	retryAttempts := profile.CodexRetryMaxAttempts
	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	if retryAttempts > 5 {
		retryAttempts = 5
	}
	retryBackoffSec := profile.CodexRetryBackoffSec
	if retryBackoffSec <= 0 {
		retryBackoffSec = 1
	}
	if retryBackoffSec > 3 {
		retryBackoffSec = 3
	}
	conversationTail := readTelegramPRDConversationTail(paths, session.ChatID, 5000)
	prompt := buildTelegramPRDRefinePrompt(session, conversationTail)
	model := strings.TrimSpace(profile.CodexModelForRole("planner"))

	var lastErr error
	for attempt := 1; attempt <= retryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
		raw, execErr := runTelegramPRDCodexExec(ctx, paths, profile, model, prompt, "ralph-telegram-prd-refine-*")
		cancel()
		if execErr == nil {
			parsed, parseErr := parseTelegramPRDCodexRefineResponse(raw)
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
	return telegramPRDCodexRefineResponse{}, fmt.Errorf("codex refine retries exhausted: %w", lastErr)
}

func runTelegramPRDCodexExec(
	ctx context.Context,
	paths ralph.Paths,
	profile ralph.Profile,
	model string,
	prompt string,
	tmpPrefix string,
) (string, error) {
	if err := ralph.EnsureLayout(paths); err != nil {
		return "", err
	}
	tmpDir, err := telegramCodexTempDir(paths, tmpPrefix)
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpDir)

	outPath := filepath.Join(tmpDir, "assistant-last-message.txt")
	args := []string{
		"--ask-for-approval", profile.CodexApproval,
		"exec",
		"--sandbox", profile.CodexSandbox,
	}
	if strings.TrimSpace(model) != "" {
		args = append(args, "--model", model)
	}
	args = append(args,
		"--cd", paths.ProjectDir,
		"--skip-git-repo-check",
		"--output-last-message", outPath,
		"-",
	)

	cmd := exec.CommandContext(ctx, "codex", args...)
	codexHome, ensureErr := ralph.EnsureCodexHome(paths, profile)
	if ensureErr != nil {
		return "", fmt.Errorf("prepare codex home: %w", ensureErr)
	}
	cmd.Env = ralph.EnvWithCodexHome(os.Environ(), codexHome)
	cmd.Stdin = strings.NewReader(sanitizeTelegramUTF8String(prompt))
	cmd.Stdout = io.Discard
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("codex exec timeout: context deadline exceeded")
		}
		errText := compactSingleLine(strings.TrimSpace(stderr.String()), 220)
		if errText != "" {
			return "", fmt.Errorf("codex exec failed: %w: %s", err, errText)
		}
		return "", fmt.Errorf("codex exec failed: %w", err)
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		return "", fmt.Errorf("read codex output: %w", err)
	}
	return string(raw), nil
}

func telegramCodexTempDir(paths ralph.Paths, prefix string) (string, error) {
	base := filepath.Join(paths.RalphDir, "tmp")
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", fmt.Errorf("create telegram codex tmp base: %w", err)
	}
	tmpDir, err := os.MkdirTemp(base, prefix)
	if err != nil {
		return "", fmt.Errorf("create telegram codex tmp dir: %w", err)
	}
	return tmpDir, nil
}

func buildTelegramPRDRefinePrompt(session telegramPRDSession, conversationTail string) string {
	payload, _ := json.Marshal(session)
	var b strings.Builder
	fmt.Fprintln(&b, "You are a PRD refinement orchestrator for autonomous agent execution.")
	fmt.Fprintln(&b, "Return STRICT JSON only.")
	fmt.Fprintln(&b, `Schema: {"score":0,"ready_to_apply":false,"ask":"...","missing":["..."],"suggested_stage":"await_problem","reason":"..."}`)
	fmt.Fprintln(&b, "Rules:")
	fmt.Fprintln(&b, "- score must be 0..100 and reflect execution readiness.")
	fmt.Fprintf(&b, "- ready_to_apply=true only when score>=%d and critical context is sufficient.\n", telegramPRDClarityMinScore)
	fmt.Fprintln(&b, "- ask must be ONE concrete next question in Korean (not a list).")
	fmt.Fprintln(&b, "- missing should include top missing/weak items.")
	fmt.Fprintln(&b, "- suggested_stage should be one of:")
	fmt.Fprintln(&b, "  await_product, await_problem, await_goal, await_in_scope, await_out_of_scope, await_acceptance, await_constraints, await_story_title")
	fmt.Fprintln(&b, "- reason should summarize why this question is the highest leverage next step.")
	fmt.Fprintln(&b, "\nCurrent session JSON:")
	fmt.Fprintln(&b, string(payload))
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "\nRecent conversation (markdown):")
		fmt.Fprintln(&b, conversationTail)
	}
	return b.String()
}

func parseTelegramPRDCodexRefineResponse(raw string) (telegramPRDCodexRefineResponse, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return telegramPRDCodexRefineResponse{}, fmt.Errorf("empty codex refine response")
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}
	var parsed telegramPRDCodexRefineResponse
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		start := strings.Index(text, "{")
		end := strings.LastIndex(text, "}")
		if start < 0 || end <= start {
			return telegramPRDCodexRefineResponse{}, fmt.Errorf("invalid codex refine json")
		}
		if unmarshalErr := json.Unmarshal([]byte(text[start:end+1]), &parsed); unmarshalErr != nil {
			return telegramPRDCodexRefineResponse{}, fmt.Errorf("parse codex refine json: %w", unmarshalErr)
		}
	}
	parsed.Score = clampTelegramPRDScore(parsed.Score)
	parsed.Ask = compactSingleLine(strings.TrimSpace(parsed.Ask), 240)
	parsed.Missing = sanitizeTelegramPRDMissingList(parsed.Missing)
	parsed.SuggestedStage = strings.TrimSpace(parsed.SuggestedStage)
	parsed.Reason = compactSingleLine(strings.TrimSpace(parsed.Reason), 200)
	return parsed, nil
}

func normalizeTelegramPRDRefineSuggestedStage(raw string) (string, bool) {
	stage := strings.ToLower(strings.TrimSpace(raw))
	switch stage {
	case telegramPRDStageAwaitProduct,
		telegramPRDStageAwaitProblem,
		telegramPRDStageAwaitGoal,
		telegramPRDStageAwaitInScope,
		telegramPRDStageAwaitOutOfScope,
		telegramPRDStageAwaitAcceptance,
		telegramPRDStageAwaitConstraints,
		telegramPRDStageAwaitStoryTitle:
		return stage, true
	default:
		return "", false
	}
}

func buildTelegramPRDScorePrompt(session telegramPRDSession, conversationTail string) string {
	payload, _ := json.Marshal(session)
	var b strings.Builder
	fmt.Fprintln(&b, "You are a strict PRD quality evaluator for autonomous agent execution.")
	fmt.Fprintln(&b, "Return STRICT JSON only.")
	fmt.Fprintln(&b, `Schema: {"score":0,"ready_to_apply":false,"missing":["..."],"summary":"..."}`)
	fmt.Fprintln(&b, "Scoring rubric:")
	fmt.Fprintln(&b, "- 0-100 overall completeness and execution clarity.")
	fmt.Fprintln(&b, "- Must consider: problem, goal, in-scope, out-of-scope, acceptance, stories quality.")
	fmt.Fprintf(&b, "- ready_to_apply=true only when score>=%d and no critical missing context.\n", telegramPRDClarityMinScore)
	fmt.Fprintln(&b, "- missing should contain the top missing/weak items.")
	fmt.Fprintln(&b, "- summary should be concise, practical, in Korean.")
	fmt.Fprintln(&b, "\nSession JSON:")
	fmt.Fprintln(&b, string(payload))
	if strings.TrimSpace(conversationTail) != "" {
		fmt.Fprintln(&b, "\nRecent Conversation (Markdown):")
		fmt.Fprintln(&b, conversationTail)
	}
	return b.String()
}

func parseTelegramPRDCodexScoreResponse(raw string) (telegramPRDCodexScoreResponse, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return telegramPRDCodexScoreResponse{}, fmt.Errorf("empty codex score response")
	}
	if strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)
	}
	var parsed telegramPRDCodexScoreResponse
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		start := strings.Index(text, "{")
		end := strings.LastIndex(text, "}")
		if start < 0 || end <= start {
			return telegramPRDCodexScoreResponse{}, fmt.Errorf("invalid codex score json")
		}
		if unmarshalErr := json.Unmarshal([]byte(text[start:end+1]), &parsed); unmarshalErr != nil {
			return telegramPRDCodexScoreResponse{}, fmt.Errorf("parse codex score json: %w", unmarshalErr)
		}
	}
	parsed.Score = clampTelegramPRDScore(parsed.Score)
	parsed.Missing = sanitizeTelegramPRDMissingList(parsed.Missing)
	parsed.Summary = compactSingleLine(strings.TrimSpace(parsed.Summary), 200)
	return parsed, nil
}

func clampTelegramPRDScore(v int) int {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

func sanitizeTelegramPRDMissingList(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		v := compactSingleLine(strings.TrimSpace(item), 120)
		if v == "" {
			continue
		}
		out = append(out, v)
		if len(out) >= 8 {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
