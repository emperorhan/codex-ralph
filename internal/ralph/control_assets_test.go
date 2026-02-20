package ralph

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestEnsureDefaultControlAssetsSeedsPluginsAndRegistry(t *testing.T) {
	t.Parallel()

	controlDir := t.TempDir()
	if err := EnsureDefaultControlAssets(controlDir); err != nil {
		t.Fatalf("EnsureDefaultControlAssets failed: %v", err)
	}

	plugins, err := ListPlugins(controlDir)
	if err != nil {
		t.Fatalf("ListPlugins failed: %v", err)
	}
	wantPlugins := []string{"go-default", "node-default", "universal-default"}
	if !reflect.DeepEqual(plugins, wantPlugins) {
		t.Fatalf("plugins mismatch: got=%v want=%v", plugins, wantPlugins)
	}

	checks, err := VerifyPluginRegistry(controlDir)
	if err != nil {
		t.Fatalf("VerifyPluginRegistry failed: %v", err)
	}
	if failures := RegistryFailureCount(checks); failures != 0 {
		t.Fatalf("registry verification failures: %d", failures)
	}

	if err := EnsureDefaultControlAssets(controlDir); err != nil {
		t.Fatalf("EnsureDefaultControlAssets second run failed: %v", err)
	}
}

func TestEnsureDefaultControlAssetsSkipsWhenPluginExists(t *testing.T) {
	t.Parallel()

	controlDir := t.TempDir()
	customPluginFile := filepath.Join(controlDir, "plugins", "custom", "plugin.env")
	if err := os.MkdirAll(filepath.Dir(customPluginFile), 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	if err := os.WriteFile(customPluginFile, []byte("RALPH_PLUGIN_NAME=custom\n"), 0o644); err != nil {
		t.Fatalf("write custom plugin failed: %v", err)
	}

	if err := EnsureDefaultControlAssets(controlDir); err != nil {
		t.Fatalf("EnsureDefaultControlAssets failed: %v", err)
	}

	plugins, err := ListPlugins(controlDir)
	if err != nil {
		t.Fatalf("ListPlugins failed: %v", err)
	}
	wantPlugins := []string{"custom"}
	if !reflect.DeepEqual(plugins, wantPlugins) {
		t.Fatalf("plugins mismatch: got=%v want=%v", plugins, wantPlugins)
	}

	if _, err := os.Stat(PluginRegistryPath(controlDir)); !os.IsNotExist(err) {
		t.Fatalf("registry should not be created when custom plugin already exists: err=%v", err)
	}
}
