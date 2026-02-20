package ralph

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type PermissionFixResult struct {
	UpdatedPaths []string
}

func AutoFixPermissions(paths Paths) (PermissionFixResult, error) {
	result := PermissionFixResult{}

	dirTargets := []string{
		paths.ControlDir,
		paths.ProjectDir,
		paths.RalphDir,
		paths.RulesDir,
		paths.IssuesDir,
		paths.InProgressDir,
		paths.DoneDir,
		paths.BlockedDir,
		paths.ReportsDir,
		paths.HandoffsDir,
		paths.LogsDir,
	}

	fileTargets := []string{
		paths.StateFile,
		paths.ProfileFile,
		paths.ProfileLocalFile,
		paths.ProfileYAMLFile,
		paths.ProfileLocalYAMLFile,
		paths.CommonRulesFile,
		paths.IssueTemplateFile,
		paths.RunnerLogFile,
		paths.TelegramLogFile(),
		paths.BusyWaitStateFile,
		paths.ProfileReloadStateFile,
		paths.BusyWaitEventsFile,
		paths.ProgressJournal,
		paths.AgentSetFile,
	}

	for _, dir := range dirTargets {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		updated, err := ensureDirMode(dir, 0o755)
		if err != nil {
			return result, fmt.Errorf("fix dir permissions %s: %w", dir, err)
		}
		if updated {
			result.UpdatedPaths = append(result.UpdatedPaths, dir)
		}
	}

	for _, file := range fileTargets {
		if strings.TrimSpace(file) == "" {
			continue
		}
		updated, err := ensureFileModeIfExists(file, 0o644)
		if err != nil {
			return result, fmt.Errorf("fix file permissions %s: %w", file, err)
		}
		if updated {
			result.UpdatedPaths = append(result.UpdatedPaths, file)
		}
	}

	return result, nil
}

func ensureDirMode(path string, mode os.FileMode) (bool, error) {
	if err := os.MkdirAll(path, mode); err != nil {
		return false, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("not a directory")
	}
	current := info.Mode().Perm()
	if current == mode {
		return false, nil
	}
	if err := os.Chmod(path, mode); err != nil {
		return false, err
	}
	return true, nil
}

func ensureFileModeIfExists(path string, mode os.FileMode) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if info.IsDir() {
		return false, nil
	}
	current := info.Mode().Perm()
	if current == mode {
		return false, nil
	}
	if err := os.Chmod(path, mode); err != nil {
		return false, err
	}
	return true, nil
}

func DefaultLinuxServicePath(serviceName string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "systemd", "user", serviceName+".service"), nil
}

func DefaultDarwinServicePath(label string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "LaunchAgents", label+".plist"), nil
}
