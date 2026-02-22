package ralph

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const defaultProjectCodexHomeDir = ".codex-home"

func ResolveCodexHomePath(paths Paths, profile Profile) (string, error) {
	raw := strings.TrimSpace(profile.CodexHome)
	if raw == "" {
		raw = filepath.Join(paths.ProjectDir, defaultProjectCodexHomeDir)
	} else if !filepath.IsAbs(raw) {
		raw = filepath.Join(paths.ProjectDir, raw)
	}
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", fmt.Errorf("resolve codex_home: %w", err)
	}
	return abs, nil
}

func EnsureCodexHome(paths Paths, profile Profile) (string, error) {
	codexHome, err := ResolveCodexHomePath(paths, profile)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		return "", fmt.Errorf("mkdir codex_home: %w", err)
	}
	info, err := os.Stat(codexHome)
	if err != nil {
		return "", fmt.Errorf("stat codex_home: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("codex_home is not a directory: %s", codexHome)
	}
	f, err := os.CreateTemp(codexHome, ".ralph-codex-home-write-check-*")
	if err != nil {
		return "", fmt.Errorf("codex_home not writable: %w", err)
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	seedCodexHomeFromGlobal(codexHome)
	return codexHome, nil
}

func EnvWithCodexHome(base []string, codexHome string) []string {
	if strings.TrimSpace(codexHome) == "" {
		return append([]string(nil), base...)
	}
	return envWithOverride(base, "CODEX_HOME", codexHome)
}

func envWithOverride(base []string, key, value string) []string {
	key = strings.TrimSpace(key)
	if key == "" {
		return append([]string(nil), base...)
	}
	prefix := key + "="
	out := make([]string, 0, len(base)+1)
	replaced := false
	for _, entry := range base {
		if strings.HasPrefix(entry, prefix) {
			if !replaced {
				out = append(out, prefix+value)
				replaced = true
			}
			continue
		}
		out = append(out, entry)
	}
	if !replaced {
		out = append(out, prefix+value)
	}
	return out
}

func seedCodexHomeFromGlobal(codexHome string) {
	if strings.TrimSpace(codexHome) == "" {
		return
	}
	sources := []string{}
	if v := strings.TrimSpace(os.Getenv("CODEX_HOME")); v != "" {
		sources = append(sources, v)
	}
	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		sources = append(sources, filepath.Join(home, ".codex"))
	}

	seen := map[string]struct{}{}
	for _, src := range sources {
		absSrc, err := filepath.Abs(src)
		if err != nil {
			continue
		}
		if absSrc == codexHome {
			continue
		}
		if _, ok := seen[absSrc]; ok {
			continue
		}
		seen[absSrc] = struct{}{}
		seedCodexHomeFromSource(codexHome, absSrc)
	}
}

func seedCodexHomeFromSource(dstHome, srcHome string) {
	files := []string{"auth.json", "config.toml"}
	for _, name := range files {
		src := filepath.Join(srcHome, name)
		dst := filepath.Join(dstHome, name)
		if err := copyFileIfMissing(dst, src); err == nil {
			_ = os.Chmod(dst, 0o600)
		}
	}
}

func copyFileIfMissing(dst, src string) error {
	if _, err := os.Stat(dst); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return nil
}
