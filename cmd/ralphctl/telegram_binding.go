package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const telegramTokenBindingStoreVersion = 1

type telegramTokenBindingStore struct {
	Version      int                             `json:"version"`
	UpdatedAtUTC string                          `json:"updated_at_utc"`
	Bindings     map[string]telegramTokenBinding `json:"bindings"`
}

type telegramTokenBinding struct {
	ProjectDir   string `json:"project_dir"`
	CreatedAtUTC string `json:"created_at_utc"`
	UpdatedAtUTC string `json:"updated_at_utc"`
}

func defaultTelegramOffsetFile(controlDir, projectDir string) string {
	key := telegramProjectKey(projectDir)
	return filepath.Join(controlDir, "telegram-offsets", key+".offset")
}

func telegramProjectKey(projectDir string) string {
	cleaned := filepath.Clean(strings.TrimSpace(projectDir))
	base := sanitizeProjectToken(filepath.Base(cleaned))
	if base == "" {
		base = "project"
	}
	sum := sha256.Sum256([]byte(cleaned))
	return fmt.Sprintf("%s-%s", base, hex.EncodeToString(sum[:6]))
}

func sanitizeProjectToken(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-_.")
}

func telegramTokenBindingsPath(controlDir string) string {
	return filepath.Join(controlDir, "telegram-token-bindings.json")
}

func ensureTelegramTokenBound(controlDir, token, projectDir string, rebind bool) error {
	token = strings.TrimSpace(token)
	projectDir = filepath.Clean(strings.TrimSpace(projectDir))
	if token == "" || projectDir == "" {
		return fmt.Errorf("token and project dir are required")
	}

	path := telegramTokenBindingsPath(controlDir)
	lockPath := path + ".lock"
	if err := acquireTelegramBindingLock(lockPath); err != nil {
		return err
	}
	defer releaseTelegramBindingLock(lockPath)

	store, err := loadTelegramTokenBindingStore(path)
	if err != nil {
		return err
	}
	hash := telegramTokenHash(token)
	now := time.Now().UTC().Format(time.RFC3339)
	entry, exists := store.Bindings[hash]
	if exists {
		existingProject := filepath.Clean(strings.TrimSpace(entry.ProjectDir))
		if existingProject != projectDir && !rebind {
			return fmt.Errorf(
				"bot token already bound to another project (%s). policy: 1 bot = 1 project. use a different token or run with --rebind-bot",
				existingProject,
			)
		}
		if entry.CreatedAtUTC == "" {
			entry.CreatedAtUTC = now
		}
		entry.ProjectDir = projectDir
		entry.UpdatedAtUTC = now
		store.Bindings[hash] = entry
	} else {
		store.Bindings[hash] = telegramTokenBinding{
			ProjectDir:   projectDir,
			CreatedAtUTC: now,
			UpdatedAtUTC: now,
		}
	}
	store.Version = telegramTokenBindingStoreVersion
	store.UpdatedAtUTC = now
	return saveTelegramTokenBindingStore(path, store)
}

func telegramTokenHash(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	// Keep full hash in file key; never persist raw token.
	return hex.EncodeToString(sum[:])
}

func acquireTelegramBindingLock(lockPath string) error {
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return fmt.Errorf("create telegram token binding lock dir: %w", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_, _ = f.WriteString(fmt.Sprintf("%d\n", os.Getpid()))
			_ = f.Close()
			return nil
		}
		if !os.IsExist(err) {
			return fmt.Errorf("acquire telegram token binding lock: %w", err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("acquire telegram token binding lock timeout")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func releaseTelegramBindingLock(lockPath string) {
	_ = os.Remove(lockPath)
}

func loadTelegramTokenBindingStore(path string) (telegramTokenBindingStore, error) {
	store := telegramTokenBindingStore{
		Version:  telegramTokenBindingStoreVersion,
		Bindings: map[string]telegramTokenBinding{},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return store, nil
		}
		return store, fmt.Errorf("read telegram token bindings: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return store, nil
	}
	if err := json.Unmarshal(data, &store); err != nil {
		return store, fmt.Errorf("parse telegram token bindings: %w", err)
	}
	if store.Bindings == nil {
		store.Bindings = map[string]telegramTokenBinding{}
	}
	return store, nil
}

func saveTelegramTokenBindingStore(path string, store telegramTokenBindingStore) error {
	if store.Bindings == nil {
		store.Bindings = map[string]telegramTokenBinding{}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create telegram token bindings dir: %w", err)
	}
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal telegram token bindings: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".telegram-token-bindings-*")
	if err != nil {
		return fmt.Errorf("create telegram token bindings tmp: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write telegram token bindings tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close telegram token bindings tmp: %w", err)
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("chmod telegram token bindings tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename telegram token bindings: %w", err)
	}
	return nil
}
