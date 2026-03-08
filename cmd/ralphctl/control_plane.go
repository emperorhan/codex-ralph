package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"codex-ralph/internal/ralph"
)

type cutoverAutoDecisionReport struct {
	DryRun             bool                                `json:"dry_run"`
	Decision           string                              `json:"decision"`
	Reason             string                              `json:"reason,omitempty"`
	EvaluatedAtUTC     string                              `json:"evaluated_at_utc"`
	CurrentMode        string                              `json:"current_mode"`
	ResultMode         string                              `json:"result_mode"`
	ResultCanary       bool                                `json:"result_canary"`
	Applied            bool                                `json:"applied"`
	RollbackOn         string                              `json:"rollback_on,omitempty"`
	ShouldRollback     bool                                `json:"should_rollback,omitempty"`
	RequireBaseline    bool                                `json:"require_baseline,omitempty"`
	RequireSoakPass    bool                                `json:"require_soak_pass,omitempty"`
	SoakReportPath     string                              `json:"soak_report_path,omitempty"`
	MaxSoakAgeSec      int                                 `json:"max_soak_age_sec,omitempty"`
	PreRepairRequested bool                                `json:"pre_repair_requested,omitempty"`
	PreRepairApplied   bool                                `json:"pre_repair_applied,omitempty"`
	PreRepairActions   []ralph.ControlPlaneRepairAction    `json:"pre_repair_actions,omitempty"`
	FailureSummaries   []string                            `json:"failure_summaries,omitempty"`
	Evaluation         ralph.ControlPlaneCutoverEvaluation `json:"evaluation"`
}

type controlPlaneMigrationReport struct {
	Migration      ralph.ControlPlaneMigrationResult        `json:"migration"`
	VerifyEnabled  bool                                     `json:"verify_enabled"`
	VerifyExecuted bool                                     `json:"verify_executed"`
	VerifyStrict   bool                                     `json:"verify_strict"`
	Verify         *ralph.ControlPlaneMigrationVerifyResult `json:"verify,omitempty"`
}

func runControlPlaneCommand(controlDir, projectDir string, args []string) error {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --project-dir DIR cp <subcommand> [args]")
		fmt.Fprintln(os.Stderr, "Subcommands: init, import-intent, plan, run, verify, status, recover, metrics, baseline, doctor, soak, cutover, fault-inject, migrate-v1, api")
	}
	if len(args) == 0 {
		usage()
		return fmt.Errorf("cp subcommand is required")
	}

	sub := strings.TrimSpace(args[0])
	subArgs := args[1:]

	switch sub {
	case "init":
		res, err := ralph.ControlPlaneInit(projectDir)
		if err != nil {
			return err
		}
		fmt.Println("control plane initialized")
		fmt.Printf("- initialized: %t\n", res.Initialized)
		fmt.Printf("- project_dir: %s\n", res.Paths.ProjectDir)
		fmt.Printf("- intent_dir: %s\n", res.Paths.IntentDir)
		fmt.Printf("- graph_dir: %s\n", res.Paths.GraphDir)
		fmt.Printf("- tasks_dir: %s\n", res.Paths.TasksDir)
		fmt.Printf("- db_file: %s\n", res.Paths.DBFile)
		fmt.Printf("- events_file: %s\n", res.Paths.EventsFile)
		fmt.Printf("- reports_dir: %s\n", res.Paths.ReportsDir)
		fmt.Printf("- cutover_file: %s\n", res.Paths.CutoverFile)
		fmt.Printf("- baseline_file: %s\n", res.Paths.BaselineFile)
		return nil

	case "import-intent":
		fs := flag.NewFlagSet("cp import-intent", flag.ContinueOnError)
		file := fs.String("file", "", "intent spec JSON file")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		if strings.TrimSpace(*file) == "" {
			return fmt.Errorf("--file is required")
		}
		res, err := ralph.ControlPlaneImportIntent(projectDir, *file)
		if err != nil {
			return err
		}
		fmt.Println("intent imported")
		fmt.Printf("- intent_id: %s\n", res.IntentID)
		fmt.Printf("- version: %d\n", res.IntentVersion)
		fmt.Printf("- source: %s\n", res.SourcePath)
		fmt.Printf("- stored: %s\n", res.StoredPath)
		return nil

	case "plan":
		fs := flag.NewFlagSet("cp plan", flag.ContinueOnError)
		intentID := fs.String("intent-id", "", "intent id")
		force := fs.Bool("force", false, "rebuild task graph for this intent")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		if strings.TrimSpace(*intentID) == "" {
			return fmt.Errorf("--intent-id is required")
		}
		res, err := ralph.ControlPlanePlanIntent(projectDir, *intentID, ralph.ControlPlanePlanOptions{Force: *force})
		if err != nil {
			return err
		}
		fmt.Println("intent planned")
		fmt.Printf("- intent_id: %s\n", res.IntentID)
		fmt.Printf("- tasks_total: %d\n", res.TasksTotal)
		fmt.Printf("- ready: %d\n", res.ReadyTasks)
		fmt.Printf("- planned: %d\n", res.PlannedTasks)
		fmt.Printf("- graph: %s\n", res.GraphPath)
		fmt.Printf("- task_files: %d\n", res.TaskFileCount)
		return nil

	case "run":
		fs := flag.NewFlagSet("cp run", flag.ContinueOnError)
		maxWorkers := fs.Int("max-workers", 1, "max worker slots")
		maxTasks := fs.Int("max-tasks", 0, "max tasks to process (0=all ready)")
		leaseSec := fs.Int("lease-sec", 120, "lease duration for claimed tasks")
		executeWithCodex := fs.Bool("execute-with-codex", false, "run codex execution step before verify")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		res, err := ralph.ControlPlaneRun(projectDir, ralph.ControlPlaneRunOptions{
			MaxWorkers:       *maxWorkers,
			MaxTasks:         *maxTasks,
			LeaseSec:         *leaseSec,
			ExecuteWithCodex: *executeWithCodex,
			ControlDir:       controlDir,
		})
		if err != nil {
			return err
		}
		fmt.Println("control plane run complete")
		fmt.Printf("- processed: %d\n", res.Processed)
		fmt.Printf("- done: %d\n", res.Done)
		fmt.Printf("- blocked: %d\n", res.Blocked)
		fmt.Printf("- recovered: %d\n", res.Recovered)
		fmt.Printf("- remaining_ready: %d\n", res.RemainingReady)
		fmt.Printf("- execute_with_codex: %t\n", *executeWithCodex)
		return nil

	case "verify":
		fs := flag.NewFlagSet("cp verify", flag.ContinueOnError)
		taskID := fs.String("task-id", "", "task id")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		if strings.TrimSpace(*taskID) == "" {
			return fmt.Errorf("--task-id is required")
		}
		res, err := ralph.ControlPlaneVerifyTask(projectDir, *taskID)
		if err != nil {
			return err
		}
		fmt.Println("verification passed")
		fmt.Printf("- task_id: %s\n", res.TaskID)
		fmt.Printf("- verified_at_utc: %s\n", res.VerifiedAtUTC)
		for _, evidence := range res.Evidence {
			fmt.Printf("- evidence: %s\n", evidence)
		}
		return nil

	case "status":
		fs := flag.NewFlagSet("cp status", flag.ContinueOnError)
		asJSON := fs.Bool("json", false, "print status as JSON")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		st, err := ralph.ControlPlaneStatusReport(projectDir)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(st)
		}
		fmt.Println("control plane status")
		fmt.Printf("- updated_at_utc: %s\n", st.UpdatedAtUTC)
		fmt.Printf("- schema_version: %d\n", st.SchemaVersion)
		fmt.Printf("- intents_total: %d\n", st.IntentsTotal)
		fmt.Printf("- tasks_total: %d\n", st.TasksTotal)
		for _, state := range []string{
			ralph.ControlPlaneTaskStateDraft,
			ralph.ControlPlaneTaskStatePlanned,
			ralph.ControlPlaneTaskStateReady,
			ralph.ControlPlaneTaskStateRunning,
			ralph.ControlPlaneTaskStateVerifying,
			ralph.ControlPlaneTaskStateDone,
			ralph.ControlPlaneTaskStateBlocked,
		} {
			fmt.Printf("- %s: %d\n", state, st.StateCounts[state])
		}
		fmt.Printf("- expired_leases: %d\n", st.ExpiredLeases)
		fmt.Printf("- blocked_rate: %.4f\n", st.Metrics.BlockedRate)
		fmt.Printf("- recovery_success_rate: %.4f\n", st.Metrics.RecoverySuccessRate)
		fmt.Printf("- mttr_seconds: %.2f\n", st.Metrics.MeanTimeToRecovery)
		fmt.Printf("- false_done_prevented: %d\n", st.Metrics.FalseDonePrevented)
		return nil

	case "recover":
		fs := flag.NewFlagSet("cp recover", flag.ContinueOnError)
		limit := fs.Int("limit", 0, "max blocked tasks to recover (0=all)")
		force := fs.Bool("force", false, "bypass cooldown/circuit policy and recover immediately")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		res, err := ralph.ControlPlaneRecoverWithOptions(projectDir, ralph.ControlPlaneRecoverOptions{
			Limit: *limit,
			Force: *force,
		})
		if err != nil {
			return err
		}
		fmt.Println("recovery complete")
		fmt.Printf("- recovered: %d\n", res.Recovered)
		fmt.Printf("- skipped_deps: %d\n", res.SkippedDeps)
		fmt.Printf("- skipped_retry_budget: %d\n", res.SkippedRetryBudget)
		fmt.Printf("- skipped_cooldown: %d\n", res.SkippedCooldown)
		fmt.Printf("- skipped_circuit_open: %d\n", res.SkippedCircuitOpen)
		fmt.Printf("- force: %t\n", *force)
		return nil

	case "metrics":
		fs := flag.NewFlagSet("cp metrics", flag.ContinueOnError)
		asJSON := fs.Bool("json", true, "print metrics as JSON")
		withBaseline := fs.Bool("with-baseline", false, "include KPI target assessment and baseline deltas")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		if *withBaseline {
			summary, err := ralph.ControlPlaneMetricsSummaryReport(projectDir)
			if err != nil {
				return err
			}
			if *asJSON {
				return printJSON(summary)
			}
			fmt.Printf("updated_at_utc=%s\n", summary.UpdatedAtUTC)
			fmt.Printf("blocked_rate=%.4f target<=%.4f pass=%t\n", summary.Metrics.BlockedRate, summary.Targets.BlockedRateMax, summary.Assessment.BlockedRatePass)
			fmt.Printf("recovery_success_rate=%.4f target>=%.4f pass=%t\n", summary.Metrics.RecoverySuccessRate, summary.Targets.RecoverySuccessRateMin, summary.Assessment.RecoverySuccessRatePass)
			fmt.Printf("mttr_seconds=%.2f target<=%.2f pass=%t\n", summary.Metrics.MeanTimeToRecovery, summary.Targets.MTTRSecondsMax, summary.Assessment.MTTRSecondsPass)
			fmt.Printf("baseline_available=%t\n", summary.BaselineAvailable)
			if summary.BaselineAvailable {
				fmt.Printf("baseline_captured_at_utc=%s\n", summary.BaselineCapturedAtUTC)
				fmt.Printf("baseline_blocked_rate=%.4f\n", summary.BaselineBlockedRate)
				fmt.Printf("blocked_rate_improvement_ratio=%.4f\n", summary.BlockedRateImprovementRatio)
			}
			return nil
		}
		metrics, err := ralph.ControlPlaneMetricsReport(projectDir)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(metrics)
		}
		fmt.Printf("updated_at_utc=%s\n", metrics.UpdatedAtUTC)
		fmt.Printf("blocked_rate=%.4f\n", metrics.BlockedRate)
		fmt.Printf("recovery_success_rate=%.4f\n", metrics.RecoverySuccessRate)
		fmt.Printf("mttr_seconds=%.2f\n", metrics.MeanTimeToRecovery)
		return nil

	case "baseline":
		return runControlPlaneBaselineCommand(projectDir, subArgs)

	case "doctor":
		fs := flag.NewFlagSet("cp doctor", flag.ContinueOnError)
		strict := fs.Bool("strict", false, "exit non-zero on failures")
		asJSON := fs.Bool("json", false, "print report as JSON")
		repair := fs.Bool("repair", false, "recover stale leases and promote planned tasks before checks")
		repairRecoverLimit := fs.Int("repair-recover-limit", 0, "auto-recover max blocked tasks during --repair (0=all)")
		repairForceRecover := fs.Bool("repair-force-recover", false, "bypass cooldown/circuit policy during --repair auto-recover")
		repairResetCircuit := fs.Bool("repair-reset-circuit", false, "clear blocked task circuit/cooldown windows during --repair")
		repairResetRetryBudget := fs.Bool("repair-reset-retry-budget", false, "reset blocked task retry budget during --repair")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		if *repair {
			repairResult, repairErr := ralph.RepairControlPlaneWithOptions(projectDir, ralph.ControlPlaneRepairOptions{
				AutoRecover:      true,
				RecoverLimit:     *repairRecoverLimit,
				ForceRecover:     *repairForceRecover,
				ResetCircuit:     *repairResetCircuit,
				ResetRetryBudget: *repairResetRetryBudget,
			})
			if repairErr != nil {
				return repairErr
			}
			fmt.Println("control plane repair")
			for _, action := range repairResult.Actions {
				fmt.Printf("- %s: %s\n", action.Name, action.Detail)
			}
		}
		report, err := ralph.RunControlPlaneDoctor(projectDir)
		if err != nil {
			return err
		}
		if *asJSON {
			if err := printJSON(report); err != nil {
				return err
			}
		} else {
			fmt.Println("control plane doctor")
			fmt.Printf("- updated_at_utc: %s\n", report.UpdatedAtUTC)
			for _, check := range report.Checks {
				fmt.Printf("- [%s] %s: %s\n", check.Status, check.Name, check.Detail)
			}
		}
		if *strict && report.HasFailures() {
			return fmt.Errorf("control plane doctor found failing checks")
		}
		return nil

	case "soak":
		fs := flag.NewFlagSet("cp soak", flag.ContinueOnError)
		durationSec := fs.Int("duration-sec", 300, "soak run duration in seconds")
		intervalSec := fs.Int("interval-sec", 30, "status sampling interval in seconds")
		strict := fs.Bool("strict", true, "stop early when doctor reports failures")
		output := fs.String("output", "", "soak report output path")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		report, err := ralph.ControlPlaneRunSoak(projectDir, *durationSec, *intervalSec, *strict)
		if err != nil {
			return err
		}
		cpPaths, err := ralph.NewControlPlanePaths(projectDir)
		if err != nil {
			return err
		}
		outPath := strings.TrimSpace(*output)
		if outPath == "" {
			outPath = filepath.Join(cpPaths.ReportsDir, "soak-"+time.Now().UTC().Format("20060102T150405Z")+".json")
		} else if !filepath.IsAbs(outPath) {
			outPath = filepath.Join(projectDir, outPath)
		}
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		data = append(data, '\n')
		if err := os.WriteFile(outPath, data, 0o644); err != nil {
			return err
		}
		fmt.Println("soak complete")
		fmt.Printf("- started_at_utc: %s\n", report.StartedAtUTC)
		fmt.Printf("- finished_at_utc: %s\n", report.FinishedAtUTC)
		fmt.Printf("- snapshots: %d\n", len(report.Snapshots))
		fmt.Printf("- failure_detected: %t\n", report.FailureDetected)
		if strings.TrimSpace(report.FailureDetail) != "" {
			fmt.Printf("- failure_detail: %s\n", report.FailureDetail)
		}
		fmt.Printf("- report: %s\n", outPath)
		return nil

	case "cutover":
		return runControlPlaneCutoverCommand(projectDir, subArgs)

	case "fault-inject":
		fs := flag.NewFlagSet("cp fault-inject", flag.ContinueOnError)
		taskID := fs.String("task-id", "", "target task id")
		mode := fs.String("mode", "", "fault mode: lease-expire|verify-fail|execute-fail|permission-denied")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		if strings.TrimSpace(*taskID) == "" {
			return fmt.Errorf("--task-id is required")
		}
		if strings.TrimSpace(*mode) == "" {
			return fmt.Errorf("--mode is required")
		}
		res, err := ralph.ControlPlaneFaultInject(projectDir, *taskID, *mode)
		if err != nil {
			return err
		}
		fmt.Println("fault injected")
		fmt.Printf("- task_id: %s\n", res.TaskID)
		fmt.Printf("- mode: %s\n", res.Mode)
		fmt.Printf("- applied: %t\n", res.Applied)
		fmt.Printf("- detail: %s\n", res.Detail)
		fmt.Printf("- state: %s\n", res.State)
		fmt.Printf("- attempt: %d\n", res.Attempt)
		return nil

	case "migrate-v1":
		fs := flag.NewFlagSet("cp migrate-v1", flag.ContinueOnError)
		dryRun := fs.Bool("dry-run", true, "preview migration without writing")
		apply := fs.Bool("apply", false, "apply migration (overrides --dry-run)")
		verify := fs.Bool("verify", true, "verify v1/v2 state count parity after migration")
		strictVerify := fs.Bool("strict-verify", false, "exit non-zero when parity check fails")
		asJSON := fs.Bool("json", false, "print migration report as JSON")
		output := fs.String("output", "", "optional output path for migration report JSON")
		if err := fs.Parse(subArgs); err != nil {
			return err
		}
		if *apply {
			*dryRun = false
		}
		res, err := ralph.MigrateV1IssuesToControlPlane(projectDir, *dryRun)
		if err != nil {
			return err
		}
		report := controlPlaneMigrationReport{
			Migration:      res,
			VerifyEnabled:  *verify,
			VerifyExecuted: false,
			VerifyStrict:   *strictVerify,
		}
		fmt.Println("v1 migration summary")
		fmt.Printf("- intent_id: %s\n", res.IntentID)
		fmt.Printf("- dry_run: %t\n", res.DryRun)
		fmt.Printf("- scanned: %d\n", res.Scanned)
		fmt.Printf("- imported: %d\n", res.Imported)
		fmt.Printf("- skipped: %d\n", res.Skipped)
		if *verify && !*dryRun {
			verifyRes, verifyErr := ralph.VerifyV1ToV2Migration(projectDir)
			if verifyErr != nil {
				return verifyErr
			}
			report.VerifyExecuted = true
			report.Verify = &verifyRes
			fmt.Println("- verify:")
			fmt.Printf("  - matched: %t\n", verifyRes.Matched)
			fmt.Printf("  - detail: %s\n", verifyRes.Detail)
			fmt.Printf("  - v1: ready=%d in_progress=%d done=%d blocked=%d\n",
				verifyRes.V1Counts["ready"],
				verifyRes.V1Counts["in_progress"],
				verifyRes.V1Counts["done"],
				verifyRes.V1Counts["blocked"],
			)
			fmt.Printf("  - v2: ready=%d in_progress=%d done=%d blocked=%d\n",
				verifyRes.V2Counts["ready"],
				verifyRes.V2Counts["in_progress"],
				verifyRes.V2Counts["done"],
				verifyRes.V2Counts["blocked"],
			)
			if *strictVerify && !verifyRes.Matched {
				if err := renderControlPlaneMigrationReport(projectDir, report, *asJSON, *output); err != nil {
					return err
				}
				return fmt.Errorf("migration parity check failed: %s", verifyRes.Detail)
			}
		}
		if err := renderControlPlaneMigrationReport(projectDir, report, *asJSON, *output); err != nil {
			return err
		}
		return nil

	case "api":
		return runControlPlaneAPICommand(projectDir, subArgs)

	default:
		usage()
		return fmt.Errorf("unknown cp subcommand: %s", sub)
	}
}

func runControlPlaneAPICommand(projectDir string, args []string) error {
	fs := flag.NewFlagSet("cp api", flag.ContinueOnError)
	listen := fs.String("listen", "127.0.0.1:8787", "listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	mux := newControlPlaneAPIMux(projectDir)
	server := &http.Server{
		Addr:              strings.TrimSpace(*listen),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	fmt.Printf("control plane api listening on %s\n", server.Addr)
	return server.ListenAndServe()
}

func newControlPlaneAPIMux(projectDir string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeControlPlaneAPIJSON(w, http.StatusOK, map[string]any{
			"ok":       true,
			"time_utc": time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.HandleFunc("/v2/status", func(w http.ResponseWriter, _ *http.Request) {
		status, err := ralph.ControlPlaneStatusReport(projectDir)
		if err != nil {
			writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeControlPlaneAPIJSON(w, http.StatusOK, status)
	})
	mux.HandleFunc("/v2/metrics", func(w http.ResponseWriter, r *http.Request) {
		if queryBool(r, "with_baseline") {
			summary, err := ralph.ControlPlaneMetricsSummaryReport(projectDir)
			if err != nil {
				writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
				return
			}
			writeControlPlaneAPIJSON(w, http.StatusOK, summary)
			return
		}
		metrics, err := ralph.ControlPlaneMetricsReport(projectDir)
		if err != nil {
			writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeControlPlaneAPIJSON(w, http.StatusOK, metrics)
	})
	mux.HandleFunc("/v2/metrics/summary", func(w http.ResponseWriter, _ *http.Request) {
		summary, err := ralph.ControlPlaneMetricsSummaryReport(projectDir)
		if err != nil {
			writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeControlPlaneAPIJSON(w, http.StatusOK, summary)
	})
	mux.HandleFunc("/v2/doctor", func(w http.ResponseWriter, _ *http.Request) {
		report, err := ralph.RunControlPlaneDoctor(projectDir)
		if err != nil {
			writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeControlPlaneAPIJSON(w, http.StatusOK, report)
	})
	mux.HandleFunc("/v2/cutover", func(w http.ResponseWriter, _ *http.Request) {
		state, err := ralph.ControlPlaneGetCutoverState(projectDir)
		if err != nil {
			writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeControlPlaneAPIJSON(w, http.StatusOK, state)
	})
	mux.HandleFunc("/v2/events", func(w http.ResponseWriter, r *http.Request) {
		limit := queryInt(r, "limit", 200)
		if limit < 0 {
			writeControlPlaneAPIError(w, http.StatusBadRequest, fmt.Errorf("limit must be >= 0"))
			return
		}
		events, err := readControlPlaneEventsFromFile(projectDir, limit)
		if err != nil {
			writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
			return
		}
		writeControlPlaneAPIJSON(w, http.StatusOK, map[string]any{
			"count":  len(events),
			"events": events,
		})
	})
	mux.HandleFunc("/v2/events/stream", func(w http.ResponseWriter, r *http.Request) {
		streamControlPlaneEvents(w, r, projectDir)
	})
	return mux
}

func writeControlPlaneAPIError(w http.ResponseWriter, status int, err error) {
	writeControlPlaneAPIJSON(w, status, map[string]any{
		"error": strings.TrimSpace(err.Error()),
	})
}

func writeControlPlaneAPIJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func readControlPlaneEventsFromFile(projectDir string, limit int) ([]ralph.ControlPlaneEventV1, error) {
	cpPaths, err := ralph.EnsureControlPlaneLayout(projectDir)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(cpPaths.EventsFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	events := []ralph.ControlPlaneEventV1{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		entry := ralph.ControlPlaneEventV1{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		events = append(events, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}
	return events, nil
}

func streamControlPlaneEvents(w http.ResponseWriter, r *http.Request, projectDir string) {
	cpPaths, err := ralph.EnsureControlPlaneLayout(projectDir)
	if err != nil {
		writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
		return
	}
	f, err := os.Open(cpPaths.EventsFile)
	if err != nil {
		writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
		return
	}
	defer f.Close()

	from := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("from")))
	if from == "" {
		from = "end"
	}
	if from == "end" {
		if _, err := f.Seek(0, io.SeekEnd); err != nil {
			writeControlPlaneAPIError(w, http.StatusInternalServerError, err)
			return
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeControlPlaneAPIError(w, http.StatusInternalServerError, fmt.Errorf("streaming unsupported"))
		return
	}
	reader := bufio.NewReader(f)
	poll := time.NewTicker(1 * time.Second)
	defer poll.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		line, err := reader.ReadString('\n')
		if err == nil {
			payload := strings.TrimSpace(line)
			if payload == "" {
				continue
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
			continue
		}
		if err == io.EOF {
			select {
			case <-r.Context().Done():
				return
			case <-poll.C:
				continue
			}
		}
		_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", strings.TrimSpace(err.Error()))
		flusher.Flush()
		return
	}
}

func queryBool(r *http.Request, key string) bool {
	raw := strings.TrimSpace(strings.ToLower(r.URL.Query().Get(key)))
	switch raw {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func queryInt(r *http.Request, key string, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func runControlPlaneCutoverCommand(projectDir string, args []string) error {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --project-dir DIR cp cutover <status|evaluate|auto|enable-v2|disable-v2> [args]")
	}
	if len(args) == 0 {
		usage()
		return fmt.Errorf("cutover subcommand is required")
	}
	switch strings.TrimSpace(args[0]) {
	case "status":
		fs := flag.NewFlagSet("cp cutover status", flag.ContinueOnError)
		asJSON := fs.Bool("json", false, "print status as JSON")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		state, err := ralph.ControlPlaneGetCutoverState(projectDir)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(state)
		}
		fmt.Println("cutover status")
		fmt.Printf("- mode: %s\n", state.Mode)
		fmt.Printf("- canary: %t\n", state.Canary)
		fmt.Printf("- updated_at_utc: %s\n", state.UpdatedAtUTC)
		fmt.Printf("- last_switch_utc: %s\n", state.LastSwitchUTC)
		if strings.TrimSpace(state.Note) != "" {
			fmt.Printf("- note: %s\n", state.Note)
		}
		return nil
	case "evaluate":
		fs := flag.NewFlagSet("cp cutover evaluate", flag.ContinueOnError)
		asJSON := fs.Bool("json", false, "print evaluation as JSON")
		output := fs.String("output", "", "optional output file path for evaluation JSON")
		requireBaseline := fs.Bool("require-baseline", false, "fail evaluation when baseline is missing")
		requireSoakPass := fs.Bool("require-soak-pass", false, "fail evaluation when soak report is missing/failed")
		soakReport := fs.String("soak-report", "", "optional soak report path (defaults to latest .ralph-v2/reports/soak-*.json)")
		maxSoakAgeSec := fs.Int("max-soak-age-sec", 0, "fail when soak report age exceeds this value (0=disabled)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		eval, err := ralph.ControlPlaneEvaluateCutoverWithOptions(projectDir, ralph.ControlPlaneCutoverEvaluateOptions{
			RequireBaseline: *requireBaseline,
			RequireSoakPass: *requireSoakPass,
			SoakReportPath:  *soakReport,
			MaxSoakAgeSec:   *maxSoakAgeSec,
		})
		if err != nil {
			return err
		}
		if *asJSON || strings.TrimSpace(*output) != "" {
			data, err := json.MarshalIndent(eval, "", "  ")
			if err != nil {
				return err
			}
			data = append(data, '\n')
			if out := strings.TrimSpace(*output); out != "" {
				outPath := out
				if !filepath.IsAbs(outPath) {
					outPath = filepath.Join(projectDir, outPath)
				}
				if err := os.WriteFile(outPath, data, 0o644); err != nil {
					return err
				}
				if !*asJSON {
					fmt.Println("cutover evaluation")
					fmt.Printf("- report: %s\n", outPath)
					fmt.Printf("- ready: %t\n", eval.Ready)
					return nil
				}
			}
			if *asJSON {
				return printJSON(eval)
			}
		}
		fmt.Println("cutover evaluation")
		fmt.Printf("- ready: %t\n", eval.Ready)
		fmt.Printf("- require_baseline: %t\n", *requireBaseline)
		fmt.Printf("- require_soak_pass: %t\n", *requireSoakPass)
		if strings.TrimSpace(*soakReport) != "" {
			fmt.Printf("- soak_report_input: %s\n", strings.TrimSpace(*soakReport))
		}
		if *maxSoakAgeSec > 0 {
			fmt.Printf("- max_soak_age_sec: %d\n", *maxSoakAgeSec)
		}
		fmt.Printf("- evaluated_at_utc: %s\n", eval.EvaluatedAtUTC)
		fmt.Printf("- current_mode: %s\n", eval.CurrentMode)
		fmt.Printf("- doctor_failures: %d\n", eval.DoctorFailures)
		fmt.Printf("- critical_failures: %d\n", eval.CriticalFailureCount)
		fmt.Printf("- blocked_rate: %.4f\n", eval.KPIs.BlockedRate)
		fmt.Printf("- recovery_success_rate: %.4f\n", eval.KPIs.RecoverySuccessRate)
		fmt.Printf("- mttr_seconds: %.2f\n", eval.KPIs.MeanTimeToRecovery)
		fmt.Printf("- baseline_available: %t\n", eval.BaselineAvailable)
		if len(eval.FailureCategories) > 0 {
			fmt.Printf("- failure_categories: %s\n", strings.Join(eval.FailureCategories, ","))
		}
		fmt.Printf("- soak_required: %t\n", eval.SoakRequired)
		fmt.Printf("- soak_available: %t\n", eval.SoakAvailable)
		if strings.TrimSpace(eval.SoakReportPath) != "" {
			fmt.Printf("- soak_report_path: %s\n", eval.SoakReportPath)
		}
		fmt.Printf("- soak_failure_detected: %t\n", eval.SoakFailureDetected)
		fmt.Printf("- soak_age_sec: %d\n", eval.SoakAgeSec)
		if eval.BaselineAvailable {
			fmt.Printf("- baseline_blocked_rate: %.4f\n", eval.BaselineBlockedRate)
			fmt.Printf("- blocked_improvement_ratio: %.4f\n", eval.BlockedRateImprovementRatio)
			fmt.Printf("- blocked_improvement_target: %.4f\n", eval.BlockedRateImprovementTarget)
		}
		for _, f := range eval.FailureSummaries {
			fmt.Printf("- fail: %s\n", f)
		}
		return nil
	case "auto":
		fs := flag.NewFlagSet("cp cutover auto", flag.ContinueOnError)
		disableOnFail := fs.Bool("disable-on-fail", true, "automatically rollback to v1 when evaluation fails")
		rollbackOn := fs.String("rollback-on", "all", "rollback failure categories: all,critical,doctor,kpi,baseline,soak,data_integrity (comma-separated)")
		requireBaseline := fs.Bool("require-baseline", false, "treat missing baseline as evaluation failure")
		requireSoakPass := fs.Bool("require-soak-pass", false, "treat missing/failed soak report as evaluation failure")
		soakReport := fs.String("soak-report", "", "optional soak report path (defaults to latest .ralph-v2/reports/soak-*.json)")
		maxSoakAgeSec := fs.Int("max-soak-age-sec", 0, "treat soak report as stale when age exceeds this value (0=disabled)")
		allowKeepCurrent := fs.Bool("allow-keep-current", false, "exit success when decision is keep-current")
		preRepair := fs.Bool("pre-repair", false, "run cp doctor repair pipeline before evaluation")
		preRepairRecoverLimit := fs.Int("pre-repair-recover-limit", 0, "max blocked tasks recovered by pre-repair (0=all)")
		preRepairForceRecover := fs.Bool("pre-repair-force-recover", false, "bypass cooldown/circuit policy during pre-repair auto-recover")
		preRepairResetCircuit := fs.Bool("pre-repair-reset-circuit", false, "clear blocked task circuit/cooldown windows during pre-repair")
		preRepairResetRetryBudget := fs.Bool("pre-repair-reset-retry-budget", false, "reset blocked task retry budget during pre-repair")
		dryRun := fs.Bool("dry-run", false, "evaluate and print decision without changing cutover mode")
		asJSON := fs.Bool("json", false, "print auto decision as JSON")
		output := fs.String("output", "", "optional output file path for auto decision JSON")
		note := fs.String("note", "", "optional note")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		preRepairActions := []ralph.ControlPlaneRepairAction{}
		preRepairApplied := false
		if *preRepair {
			if *dryRun {
				fmt.Println("cutover auto pre-repair")
				fmt.Println("- skipped: true")
				fmt.Println("- reason: dry-run mode")
			} else {
				repairResult, repairErr := ralph.RepairControlPlaneWithOptions(projectDir, ralph.ControlPlaneRepairOptions{
					AutoRecover:      true,
					RecoverLimit:     *preRepairRecoverLimit,
					ForceRecover:     *preRepairForceRecover,
					ResetCircuit:     *preRepairResetCircuit,
					ResetRetryBudget: *preRepairResetRetryBudget,
				})
				if repairErr != nil {
					return repairErr
				}
				preRepairApplied = true
				preRepairActions = append(preRepairActions, repairResult.Actions...)
				fmt.Println("cutover auto pre-repair")
				for _, action := range repairResult.Actions {
					fmt.Printf("- %s: %s\n", action.Name, action.Detail)
				}
			}
		}
		eval, err := ralph.ControlPlaneEvaluateCutoverWithOptions(projectDir, ralph.ControlPlaneCutoverEvaluateOptions{
			RequireBaseline: *requireBaseline,
			RequireSoakPass: *requireSoakPass,
			SoakReportPath:  *soakReport,
			MaxSoakAgeSec:   *maxSoakAgeSec,
		})
		if err != nil {
			return err
		}
		resolvedSoakReportPath := strings.TrimSpace(*soakReport)
		if resolvedSoakReportPath == "" {
			resolvedSoakReportPath = strings.TrimSpace(eval.SoakReportPath)
		}
		currentState, currentStateErr := ralph.ControlPlaneGetCutoverState(projectDir)
		if currentStateErr != nil {
			return currentStateErr
		}
		normalizedRollbackPolicy := normalizeCutoverRollbackPolicy(*rollbackOn)
		if eval.Ready {
			report := cutoverAutoDecisionReport{
				DryRun:             *dryRun,
				Decision:           "enable-v2",
				Reason:             "evaluation_ready",
				EvaluatedAtUTC:     eval.EvaluatedAtUTC,
				CurrentMode:        eval.CurrentMode,
				ResultMode:         "v2",
				ResultCanary:       true,
				Applied:            !*dryRun,
				RequireBaseline:    *requireBaseline,
				RequireSoakPass:    *requireSoakPass,
				SoakReportPath:     resolvedSoakReportPath,
				MaxSoakAgeSec:      *maxSoakAgeSec,
				PreRepairRequested: *preRepair,
				PreRepairApplied:   preRepairApplied,
				PreRepairActions:   append([]ralph.ControlPlaneRepairAction(nil), preRepairActions...),
				Evaluation:         eval,
				FailureSummaries:   eval.FailureSummaries,
			}
			if *dryRun {
				fmt.Println("cutover auto decision")
				fmt.Println("- dry_run: true")
				fmt.Println("- decision: enable-v2")
				fmt.Printf("- current_mode: %s\n", eval.CurrentMode)
				fmt.Println("- would_set_mode: v2")
				fmt.Println("- would_set_canary: true")
				fmt.Printf("- require_baseline: %t\n", *requireBaseline)
				fmt.Printf("- require_soak_pass: %t\n", *requireSoakPass)
				if resolvedSoakReportPath != "" {
					fmt.Printf("- soak_report_path: %s\n", resolvedSoakReportPath)
				}
				if *maxSoakAgeSec > 0 {
					fmt.Printf("- max_soak_age_sec: %d\n", *maxSoakAgeSec)
				}
				fmt.Printf("- evaluated_at_utc: %s\n", eval.EvaluatedAtUTC)
				if err := renderCutoverAutoDecision(projectDir, report, *asJSON, *output); err != nil {
					return err
				}
				return nil
			}
			state, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, true, firstNonEmptyCutover(strings.TrimSpace(*note), "auto cutover approved by KPI+doctor gate"))
			if err != nil {
				return err
			}
			report.ResultMode = state.Mode
			report.ResultCanary = state.Canary
			fmt.Println("cutover auto decision")
			fmt.Println("- decision: enable-v2")
			fmt.Printf("- mode: %s\n", state.Mode)
			fmt.Printf("- canary: %t\n", state.Canary)
			fmt.Printf("- require_baseline: %t\n", *requireBaseline)
			fmt.Printf("- require_soak_pass: %t\n", *requireSoakPass)
			if resolvedSoakReportPath != "" {
				fmt.Printf("- soak_report_path: %s\n", resolvedSoakReportPath)
			}
			if *maxSoakAgeSec > 0 {
				fmt.Printf("- max_soak_age_sec: %d\n", *maxSoakAgeSec)
			}
			fmt.Printf("- evaluated_at_utc: %s\n", eval.EvaluatedAtUTC)
			if err := renderCutoverAutoDecision(projectDir, report, *asJSON, *output); err != nil {
				return err
			}
			return nil
		}
		shouldRollback, policyErr := shouldRollbackByCutoverPolicy(eval, *rollbackOn)
		if policyErr != nil {
			return policyErr
		}
		report := cutoverAutoDecisionReport{
			DryRun:             *dryRun,
			Decision:           "keep-current",
			Reason:             "evaluation_failed",
			EvaluatedAtUTC:     eval.EvaluatedAtUTC,
			CurrentMode:        eval.CurrentMode,
			ResultMode:         eval.CurrentMode,
			ResultCanary:       currentState.Canary,
			Applied:            false,
			RollbackOn:         normalizedRollbackPolicy,
			ShouldRollback:     shouldRollback,
			RequireBaseline:    *requireBaseline,
			RequireSoakPass:    *requireSoakPass,
			SoakReportPath:     resolvedSoakReportPath,
			MaxSoakAgeSec:      *maxSoakAgeSec,
			PreRepairRequested: *preRepair,
			PreRepairApplied:   preRepairApplied,
			PreRepairActions:   append([]ralph.ControlPlaneRepairAction(nil), preRepairActions...),
			FailureSummaries:   eval.FailureSummaries,
			Evaluation:         eval,
		}
		if *disableOnFail && shouldRollback {
			report.Decision = "disable-v2"
			report.ResultMode = "v1"
			report.ResultCanary = false
			report.Applied = !*dryRun
			if *dryRun {
				fmt.Println("cutover auto decision")
				fmt.Println("- dry_run: true")
				fmt.Println("- decision: disable-v2")
				fmt.Printf("- current_mode: %s\n", eval.CurrentMode)
				fmt.Println("- would_set_mode: v1")
				fmt.Println("- would_set_canary: false")
				fmt.Printf("- require_baseline: %t\n", *requireBaseline)
				fmt.Printf("- require_soak_pass: %t\n", *requireSoakPass)
				if resolvedSoakReportPath != "" {
					fmt.Printf("- soak_report_path: %s\n", resolvedSoakReportPath)
				}
				if *maxSoakAgeSec > 0 {
					fmt.Printf("- max_soak_age_sec: %d\n", *maxSoakAgeSec)
				}
				fmt.Printf("- evaluated_at_utc: %s\n", eval.EvaluatedAtUTC)
				fmt.Printf("- rollback_on: %s\n", normalizedRollbackPolicy)
				for _, f := range eval.FailureSummaries {
					fmt.Printf("- fail: %s\n", f)
				}
				if err := renderCutoverAutoDecision(projectDir, report, *asJSON, *output); err != nil {
					return err
				}
				return nil
			}
			state, err := ralph.ControlPlaneSetCutoverMode(projectDir, false, false, firstNonEmptyCutover(strings.TrimSpace(*note), "auto rollback: cutover evaluation failed"))
			if err != nil {
				return err
			}
			report.ResultMode = state.Mode
			report.ResultCanary = state.Canary
			fmt.Println("cutover auto decision")
			fmt.Println("- decision: disable-v2")
			fmt.Printf("- mode: %s\n", state.Mode)
			fmt.Printf("- canary: %t\n", state.Canary)
			fmt.Printf("- require_baseline: %t\n", *requireBaseline)
			fmt.Printf("- require_soak_pass: %t\n", *requireSoakPass)
			if resolvedSoakReportPath != "" {
				fmt.Printf("- soak_report_path: %s\n", resolvedSoakReportPath)
			}
			if *maxSoakAgeSec > 0 {
				fmt.Printf("- max_soak_age_sec: %d\n", *maxSoakAgeSec)
			}
			fmt.Printf("- evaluated_at_utc: %s\n", eval.EvaluatedAtUTC)
			fmt.Printf("- rollback_on: %s\n", normalizedRollbackPolicy)
			for _, f := range eval.FailureSummaries {
				fmt.Printf("- fail: %s\n", f)
			}
			if err := renderCutoverAutoDecision(projectDir, report, *asJSON, *output); err != nil {
				return err
			}
			return nil
		}
		fmt.Println("cutover auto decision")
		fmt.Printf("- dry_run: %t\n", *dryRun)
		fmt.Println("- decision: keep-current")
		fmt.Printf("- reason: evaluation_failed\n")
		fmt.Printf("- rollback_on: %s\n", normalizedRollbackPolicy)
		fmt.Printf("- allow_keep_current: %t\n", *allowKeepCurrent)
		fmt.Printf("- require_baseline: %t\n", *requireBaseline)
		fmt.Printf("- require_soak_pass: %t\n", *requireSoakPass)
		if resolvedSoakReportPath != "" {
			fmt.Printf("- soak_report_path: %s\n", resolvedSoakReportPath)
		}
		if *maxSoakAgeSec > 0 {
			fmt.Printf("- max_soak_age_sec: %d\n", *maxSoakAgeSec)
		}
		for _, f := range eval.FailureSummaries {
			fmt.Printf("- fail: %s\n", f)
		}
		if err := renderCutoverAutoDecision(projectDir, report, *asJSON, *output); err != nil {
			return err
		}
		if *allowKeepCurrent {
			return nil
		}
		return fmt.Errorf("cutover auto failed evaluation")
	case "enable-v2":
		fs := flag.NewFlagSet("cp cutover enable-v2", flag.ContinueOnError)
		canary := fs.Bool("canary", true, "enable v2 in canary mode")
		note := fs.String("note", "", "optional note")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		state, err := ralph.ControlPlaneSetCutoverMode(projectDir, true, *canary, *note)
		if err != nil {
			return err
		}
		fmt.Println("cutover updated")
		fmt.Printf("- mode: %s\n", state.Mode)
		fmt.Printf("- canary: %t\n", state.Canary)
		fmt.Printf("- updated_at_utc: %s\n", state.UpdatedAtUTC)
		return nil
	case "disable-v2":
		fs := flag.NewFlagSet("cp cutover disable-v2", flag.ContinueOnError)
		note := fs.String("note", "rollback to v1", "optional note")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		state, err := ralph.ControlPlaneSetCutoverMode(projectDir, false, false, *note)
		if err != nil {
			return err
		}
		fmt.Println("cutover updated")
		fmt.Printf("- mode: %s\n", state.Mode)
		fmt.Printf("- canary: %t\n", state.Canary)
		fmt.Printf("- updated_at_utc: %s\n", state.UpdatedAtUTC)
		return nil
	default:
		usage()
		return fmt.Errorf("unknown cutover subcommand: %s", args[0])
	}
}

func firstNonEmptyCutover(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func renderCutoverAutoDecision(projectDir string, report cutoverAutoDecisionReport, asJSON bool, outputPath string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if out := strings.TrimSpace(outputPath); out != "" {
		path := out
		if !filepath.IsAbs(path) {
			path = filepath.Join(projectDir, path)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
	}
	if asJSON {
		return printJSON(report)
	}
	return nil
}

func renderControlPlaneMigrationReport(projectDir string, report controlPlaneMigrationReport, asJSON bool, outputPath string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if out := strings.TrimSpace(outputPath); out != "" {
		path := out
		if !filepath.IsAbs(path) {
			path = filepath.Join(projectDir, path)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
	}
	if asJSON {
		return printJSON(report)
	}
	return nil
}

func normalizeCutoverRollbackPolicy(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return "all"
	}
	return value
}

func parseCutoverRollbackPolicy(raw string) (map[string]struct{}, error) {
	normalized := normalizeCutoverRollbackPolicy(raw)
	allowed := map[string]struct{}{
		"all":            {},
		"critical":       {},
		"doctor":         {},
		"kpi":            {},
		"baseline":       {},
		"soak":           {},
		"data_integrity": {},
	}
	policy := map[string]struct{}{}
	for _, token := range strings.Split(normalized, ",") {
		key := strings.TrimSpace(token)
		if key == "" {
			continue
		}
		if _, ok := allowed[key]; !ok {
			return nil, fmt.Errorf("invalid rollback category: %s", key)
		}
		policy[key] = struct{}{}
	}
	if len(policy) == 0 {
		policy["all"] = struct{}{}
	}
	return policy, nil
}

func shouldRollbackByCutoverPolicy(eval ralph.ControlPlaneCutoverEvaluation, rawPolicy string) (bool, error) {
	if eval.Ready {
		return false, nil
	}
	policy, err := parseCutoverRollbackPolicy(rawPolicy)
	if err != nil {
		return false, err
	}
	if _, ok := policy["all"]; ok {
		return len(eval.FailureSummaries) > 0, nil
	}
	if _, ok := policy["critical"]; ok && eval.CriticalFailureCount > 0 {
		return true, nil
	}
	categorySet := map[string]struct{}{}
	for _, failure := range eval.Failures {
		categorySet[strings.TrimSpace(failure.Category)] = struct{}{}
	}
	for category := range categorySet {
		if _, ok := policy[category]; ok {
			return true, nil
		}
	}
	return false, nil
}

func runControlPlaneBaselineCommand(projectDir string, args []string) error {
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: ralphctl --project-dir DIR cp baseline <capture|show> [args]")
	}
	if len(args) == 0 {
		usage()
		return fmt.Errorf("baseline subcommand is required")
	}
	switch strings.TrimSpace(args[0]) {
	case "capture":
		fs := flag.NewFlagSet("cp baseline capture", flag.ContinueOnError)
		note := fs.String("note", "", "optional capture note")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		baseline, err := ralph.CaptureControlPlaneMetricsBaseline(projectDir, *note)
		if err != nil {
			return err
		}
		fmt.Println("baseline captured")
		fmt.Printf("- captured_at_utc: %s\n", baseline.CapturedAtUTC)
		fmt.Printf("- blocked_rate: %.4f\n", baseline.BlockedRate)
		fmt.Printf("- recovery_success_rate: %.4f\n", baseline.RecoverySuccessRate)
		fmt.Printf("- mttr_seconds: %.2f\n", baseline.MeanTimeToRecovery)
		if strings.TrimSpace(baseline.Note) != "" {
			fmt.Printf("- note: %s\n", baseline.Note)
		}
		return nil
	case "show":
		baseline, ok, err := ralph.GetControlPlaneMetricsBaseline(projectDir)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("baseline not found")
			return nil
		}
		fmt.Println("baseline")
		fmt.Printf("- captured_at_utc: %s\n", baseline.CapturedAtUTC)
		fmt.Printf("- blocked_rate: %.4f\n", baseline.BlockedRate)
		fmt.Printf("- recovery_success_rate: %.4f\n", baseline.RecoverySuccessRate)
		fmt.Printf("- mttr_seconds: %.2f\n", baseline.MeanTimeToRecovery)
		fmt.Printf("- false_done_prevented: %d\n", baseline.FalseDonePrevented)
		fmt.Printf("- verification_failures: %d\n", baseline.VerificationFailures)
		if strings.TrimSpace(baseline.Note) != "" {
			fmt.Printf("- note: %s\n", baseline.Note)
		}
		return nil
	default:
		usage()
		return fmt.Errorf("unknown baseline subcommand: %s", args[0])
	}
}

func printJSON(v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(append(data, '\n'))
	return err
}
