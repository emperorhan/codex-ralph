package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"codex-ralph/internal/ralph"
)

type setupFleetRegistrationResult struct {
	Status           string
	Project          ralph.FleetProject
	BootstrapCreated int
}

func ensureFleetRegistrationOnSetup(controlDir string, paths ralph.Paths, fleetID, fleetPRD string) (setupFleetRegistrationResult, error) {
	prdPath := strings.TrimSpace(fleetPRD)
	if prdPath == "" {
		prdPath = "PRD.md"
	}

	cfg, err := ralph.LoadFleetConfig(controlDir)
	if err != nil {
		return setupFleetRegistrationResult{}, err
	}

	absProjectDir, err := normalizeProjectPath(paths.ProjectDir)
	if err != nil {
		return setupFleetRegistrationResult{}, fmt.Errorf("resolve project dir: %w", err)
	}

	if existing, idx, ok := findFleetProjectByDir(cfg, absProjectDir); ok {
		requestedID := strings.TrimSpace(fleetID)
		if requestedID != "" && requestedID != existing.ID {
			return setupFleetRegistrationResult{}, fmt.Errorf("project already registered as %q; requested fleet id %q mismatches", existing.ID, requestedID)
		}
		if strings.TrimSpace(existing.PRDPath) == "" {
			cfg.Projects[idx].PRDPath = prdPath
			if err := ralph.SaveFleetConfig(controlDir, cfg); err != nil {
				return setupFleetRegistrationResult{}, err
			}
			existing = cfg.Projects[idx]
		}
		if err := ralph.EnsureFleetAgentSetFile(paths, existing); err != nil {
			return setupFleetRegistrationResult{}, err
		}
		effectivePRD := strings.TrimSpace(existing.PRDPath)
		if effectivePRD == "" {
			effectivePRD = prdPath
		}
		created, err := ralph.EnsureRoleBootstrapIssues(paths, effectivePRD)
		if err != nil {
			return setupFleetRegistrationResult{}, err
		}
		return setupFleetRegistrationResult{
			Status:           "already-registered",
			Project:          existing,
			BootstrapCreated: len(created),
		}, nil
	}

	projectID := strings.TrimSpace(fleetID)
	if projectID == "" {
		projectID = suggestFleetProjectID(cfg, absProjectDir)
	}

	profile, err := ralph.LoadProfile(paths)
	if err != nil {
		return setupFleetRegistrationResult{}, err
	}
	pluginName := strings.TrimSpace(profile.PluginName)
	if pluginName == "" {
		pluginName = "universal-default"
	}

	project, err := ralph.RegisterFleetProject(controlDir, projectID, absProjectDir, pluginName, prdPath)
	if err != nil {
		return setupFleetRegistrationResult{}, err
	}
	if err := ralph.EnsureFleetAgentSetFile(paths, project); err != nil {
		return setupFleetRegistrationResult{}, err
	}
	created, err := ralph.EnsureRoleBootstrapIssues(paths, project.PRDPath)
	if err != nil {
		return setupFleetRegistrationResult{}, err
	}

	return setupFleetRegistrationResult{
		Status:           "registered",
		Project:          project,
		BootstrapCreated: len(created),
	}, nil
}

func findFleetProjectByDir(cfg ralph.FleetConfig, projectDir string) (ralph.FleetProject, int, bool) {
	target, err := normalizeProjectPath(projectDir)
	if err != nil {
		return ralph.FleetProject{}, -1, false
	}
	for i, p := range cfg.Projects {
		normalized, normErr := normalizeProjectPath(p.ProjectDir)
		if normErr != nil {
			continue
		}
		if normalized == target {
			return p, i, true
		}
	}
	return ralph.FleetProject{}, -1, false
}

func normalizeProjectPath(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(absPath), nil
}

func suggestFleetProjectID(cfg ralph.FleetConfig, projectDir string) string {
	base := sanitizeFleetID(filepath.Base(projectDir))
	if base == "" || base == "." {
		base = "project"
	}
	if !fleetIDExists(cfg, base) {
		return base
	}

	sum := sha1.Sum([]byte(strings.ToLower(filepath.Clean(projectDir))))
	withHash := fmt.Sprintf("%s-%s", base, hex.EncodeToString(sum[:3]))
	if !fleetIDExists(cfg, withHash) {
		return withHash
	}

	for i := 2; ; i++ {
		candidate := withHash + "-" + strconv.Itoa(i)
		if !fleetIDExists(cfg, candidate) {
			return candidate
		}
	}
}

func sanitizeFleetID(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	var out strings.Builder
	lastSep := false
	for _, ch := range raw {
		switch {
		case (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9'):
			out.WriteRune(ch)
			lastSep = false
		case ch == '-' || ch == '_' || ch == '.':
			out.WriteRune(ch)
			lastSep = false
		default:
			if !lastSep {
				out.WriteRune('-')
				lastSep = true
			}
		}
	}
	sanitized := strings.Trim(out.String(), "-_.")
	return sanitized
}

func fleetIDExists(cfg ralph.FleetConfig, id string) bool {
	for _, p := range cfg.Projects {
		if p.ID == id {
			return true
		}
	}
	return false
}
