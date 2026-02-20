package ralph

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

func ListPlugins(controlDir string) ([]string, error) {
	pluginRoot := filepath.Join(controlDir, "plugins")
	entries, err := os.ReadDir(pluginRoot)
	if err != nil {
		return nil, fmt.Errorf("read plugins dir: %w", err)
	}

	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pluginName := e.Name()
		pluginFile := filepath.Join(pluginRoot, pluginName, "plugin.env")
		if _, statErr := os.Stat(pluginFile); statErr == nil {
			out = append(out, pluginName)
		}
	}
	sort.Strings(out)
	return out, nil
}

func pluginFilePath(controlDir, pluginName string) string {
	return filepath.Join(controlDir, "plugins", pluginName, "plugin.env")
}

func ApplyPlugin(paths Paths, pluginName string) error {
	src := pluginFilePath(paths.ControlDir, pluginName)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("plugin not found: %s", pluginName)
	}

	if err := EnsureLayout(paths); err != nil {
		return err
	}

	if _, err := os.Stat(paths.ProfileYAMLFile); err == nil {
		backup := paths.ProfileYAMLFile + ".bak"
		if copyErr := copyFile(paths.ProfileYAMLFile, backup); copyErr != nil {
			return fmt.Errorf("backup profile.yaml: %w", copyErr)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat profile.yaml: %w", err)
	}

	pluginEnv, err := ReadEnvFile(src)
	if err != nil {
		return fmt.Errorf("read plugin env: %w", err)
	}
	profile := DefaultProfile()
	applyProfileMap(&profile, pluginEnv)
	profile.PluginName = pluginName

	if err := WriteYAMLFlatMap(paths.ProfileYAMLFile, ProfileToYAMLMap(profile)); err != nil {
		return fmt.Errorf("write profile.yaml: %w", err)
	}

	// Keep env-file compatibility as optional overrides; clear default profile.env
	// so YAML remains the primary editable config.
	if _, err := os.Stat(paths.ProfileFile); err == nil {
		backup := paths.ProfileFile + ".bak"
		if copyErr := copyFile(paths.ProfileFile, backup); copyErr != nil {
			return fmt.Errorf("backup legacy profile.env: %w", copyErr)
		}
		if removeErr := os.Remove(paths.ProfileFile); removeErr != nil {
			return fmt.Errorf("remove legacy profile.env: %w", removeErr)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat legacy profile.env: %w", err)
	}

	return nil
}

func Install(paths Paths, pluginName, executablePath string) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	if err := ApplyPlugin(paths, pluginName); err != nil {
		return err
	}
	if err := SetEnabled(paths, true); err != nil {
		return err
	}

	configContent := fmt.Sprintf("CONTROL_DIR=%s\nPLUGIN=%s\n", paths.ControlDir, pluginName)
	if err := os.WriteFile(filepath.Join(paths.RalphDir, "config.env"), []byte(configContent), 0o644); err != nil {
		return fmt.Errorf("write config.env: %w", err)
	}

	wrapper := fmt.Sprintf("#!/usr/bin/env bash\nset -euo pipefail\nexec %q --control-dir %q --project-dir %q \"$@\"\n", executablePath, paths.ControlDir, paths.ProjectDir)
	wrapperPath := filepath.Join(paths.ProjectDir, "ralph")
	if err := os.WriteFile(wrapperPath, []byte(wrapper), 0o755); err != nil {
		return fmt.Errorf("write wrapper script: %w", err)
	}

	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}
