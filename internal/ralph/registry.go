package ralph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const pluginRegistryVersion = 1

type PluginRegistry struct {
	Version        int                   `json:"version"`
	GeneratedAtUTC string                `json:"generated_at_utc"`
	Plugins        []PluginRegistryEntry `json:"plugins"`
}

type PluginRegistryEntry struct {
	Name        string `json:"name"`
	File        string `json:"file"`
	SHA256      string `json:"sha256"`
	Description string `json:"description,omitempty"`
}

type RegistryCheck struct {
	Name   string
	Status string
	Detail string
}

func PluginRegistryPath(controlDir string) string {
	return filepath.Join(controlDir, "plugins", "registry.json")
}

func LoadPluginRegistry(controlDir string) (PluginRegistry, error) {
	path := PluginRegistryPath(controlDir)
	data, err := os.ReadFile(path)
	if err != nil {
		return PluginRegistry{}, err
	}
	var reg PluginRegistry
	if err := json.Unmarshal(data, &reg); err != nil {
		return PluginRegistry{}, fmt.Errorf("parse plugin registry: %w", err)
	}
	if reg.Version == 0 {
		reg.Version = pluginRegistryVersion
	}
	if reg.Plugins == nil {
		reg.Plugins = []PluginRegistryEntry{}
	}
	sort.Slice(reg.Plugins, func(i, j int) bool {
		return reg.Plugins[i].Name < reg.Plugins[j].Name
	})
	return reg, nil
}

func SavePluginRegistry(controlDir string, reg PluginRegistry) error {
	reg.Version = pluginRegistryVersion
	if reg.GeneratedAtUTC == "" {
		reg.GeneratedAtUTC = time.Now().UTC().Format(time.RFC3339)
	}
	if reg.Plugins == nil {
		reg.Plugins = []PluginRegistryEntry{}
	}
	sort.Slice(reg.Plugins, func(i, j int) bool {
		return reg.Plugins[i].Name < reg.Plugins[j].Name
	})

	path := PluginRegistryPath(controlDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create plugin registry dir: %w", err)
	}

	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal plugin registry: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write plugin registry: %w", err)
	}
	return nil
}

func GeneratePluginRegistry(controlDir string) (PluginRegistry, error) {
	plugins, err := ListPlugins(controlDir)
	if err != nil {
		return PluginRegistry{}, err
	}
	entries := make([]PluginRegistryEntry, 0, len(plugins))
	for _, pluginName := range plugins {
		pluginFile := pluginFilePath(controlDir, pluginName)
		hash, err := sha256FileHex(pluginFile)
		if err != nil {
			return PluginRegistry{}, fmt.Errorf("hash plugin %s: %w", pluginName, err)
		}
		entries = append(entries, PluginRegistryEntry{
			Name:   pluginName,
			File:   filepath.ToSlash(filepath.Join(pluginName, "plugin.env")),
			SHA256: hash,
		})
	}

	return PluginRegistry{
		Version:        pluginRegistryVersion,
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Plugins:        entries,
	}, nil
}

func VerifyPluginRegistry(controlDir string) ([]RegistryCheck, error) {
	reg, err := LoadPluginRegistry(controlDir)
	if err != nil {
		return nil, err
	}

	checks := []RegistryCheck{}
	seen := map[string]struct{}{}
	for _, entry := range reg.Plugins {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			checks = append(checks, RegistryCheck{
				Name:   "plugin-entry",
				Status: "fail",
				Detail: "registry entry has empty plugin name",
			})
			continue
		}
		if _, ok := seen[name]; ok {
			checks = append(checks, RegistryCheck{
				Name:   name,
				Status: "fail",
				Detail: "duplicate plugin entry",
			})
			continue
		}
		seen[name] = struct{}{}

		fileRel := strings.TrimSpace(entry.File)
		if fileRel == "" {
			fileRel = filepath.ToSlash(filepath.Join(name, "plugin.env"))
		}
		fileRel = strings.TrimPrefix(fileRel, "/")
		pluginFile := filepath.Join(controlDir, "plugins", filepath.FromSlash(fileRel))
		hash, hashErr := sha256FileHex(pluginFile)
		if hashErr != nil {
			status := "fail"
			if os.IsNotExist(hashErr) {
				status = "warn"
			}
			checks = append(checks, RegistryCheck{
				Name:   name,
				Status: status,
				Detail: fmt.Sprintf("cannot read plugin file: %v", hashErr),
			})
			continue
		}
		expected := strings.ToLower(strings.TrimSpace(entry.SHA256))
		if expected == "" {
			checks = append(checks, RegistryCheck{
				Name:   name,
				Status: "warn",
				Detail: "missing sha256 in registry entry",
			})
			continue
		}
		if hash != expected {
			checks = append(checks, RegistryCheck{
				Name:   name,
				Status: "fail",
				Detail: fmt.Sprintf("checksum mismatch expected=%s actual=%s", expected, hash),
			})
			continue
		}
		checks = append(checks, RegistryCheck{
			Name:   name,
			Status: "pass",
			Detail: "checksum verified",
		})
	}

	actualPlugins, listErr := ListPlugins(controlDir)
	if listErr == nil {
		for _, name := range actualPlugins {
			if _, ok := seen[name]; ok {
				continue
			}
			checks = append(checks, RegistryCheck{
				Name:   name,
				Status: "warn",
				Detail: "plugin exists but not listed in registry",
			})
		}
	}
	return checks, nil
}

func RegistryFailureCount(checks []RegistryCheck) int {
	n := 0
	for _, c := range checks {
		if c.Status == "fail" {
			n++
		}
	}
	return n
}

func VerifyPluginWithRegistry(controlDir, pluginName string) error {
	reg, err := LoadPluginRegistry(controlDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	pluginName = strings.TrimSpace(pluginName)
	var entry *PluginRegistryEntry
	for i := range reg.Plugins {
		if strings.TrimSpace(reg.Plugins[i].Name) == pluginName {
			entry = &reg.Plugins[i]
			break
		}
	}
	if entry == nil {
		return fmt.Errorf("plugin not found in registry: %s", pluginName)
	}

	fileRel := strings.TrimSpace(entry.File)
	if fileRel == "" {
		fileRel = filepath.ToSlash(filepath.Join(pluginName, "plugin.env"))
	}
	fileRel = strings.TrimPrefix(fileRel, "/")
	pluginFile := filepath.Join(controlDir, "plugins", filepath.FromSlash(fileRel))
	actual, err := sha256FileHex(pluginFile)
	if err != nil {
		return err
	}
	expected := strings.ToLower(strings.TrimSpace(entry.SHA256))
	if expected == "" {
		return fmt.Errorf("registry checksum is empty for plugin: %s", pluginName)
	}
	if expected != actual {
		return fmt.Errorf("checksum mismatch expected=%s actual=%s", expected, actual)
	}
	return nil
}

func sha256FileHex(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
